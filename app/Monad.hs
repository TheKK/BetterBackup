{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Monad (
  runReadonlyRepositoryFromCwd,
  runRepositoryForBackupFromCwd,
) where

import Path qualified

import Control.Exception (bracket, bracket_)
import Control.Monad (unless, void)
import Control.Monad.Catch (onException, tryJust)
import Control.Monad.IO.Class (liftIO)

import Crypto.Cipher.AES (AES128)

import Data.ByteString qualified as BS
import Data.Coerce (coerce)
import Data.Foldable (fold)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Data.Text.IO qualified as T

import Database.LevelDB.Base qualified as LV

import Effectful qualified as E
import Effectful.Ki (StructuredConcurrency)
import Effectful.Ki qualified as EKi

import Better.Hash (VersionDigest)
import Better.Hash qualified as Hash
import Better.Internal.Repository.LowLevel (Version, runRepository)
import Better.Internal.Repository.LowLevel qualified as Repo
import Better.Logging.Effect (logging, runLogging)
import Better.Logging.Effect qualified as E
import Better.Repository.BackupCache.Class (BackupCache)
import Better.Repository.BackupCache.LevelDB (runBackupCacheLevelDB)
import Better.Repository.Class qualified as E
import Better.Statistics.Backup.Class (BackupStatistics, runBackupStatistics)
import Better.TempDir (runTmp)
import Better.TempDir.Class (Tmp)

import System.Directory qualified as D
import System.Environment.Blank (getEnv)
import System.IO (hGetEcho, hSetEcho, stderr, stdin)
import System.IO.Error qualified as IOE
import System.IO.Temp (withSystemTempDirectory)
import System.Posix qualified as P

import Katip qualified

import Config qualified
import Crypto qualified
import LocalCache qualified

read_passworld_from_stdin :: IO BS.ByteString
read_passworld_from_stdin = do
  old_echo <- hGetEcho stdin
  bracket_ (hSetEcho stdin False) (hSetEcho stdin old_echo) BS.getLine

-- TODO Support no encryption.
extract_block_cipher :: (E.Logging E.:> es, E.IOE E.:> es) => Config.Config -> E.Eff es AES128
extract_block_cipher config = E.withSeqEffToIO $ \un -> case Config.config_cipher config of
  Config.CipherConfigNoCipher -> error "we don't support no encryption now"
  Config.CipherConfigAES128 cfg -> do
    password <-
      getEnv "BETTER_PASS" >>= \case
        Just passfile -> do
          un $ logging Katip.InfoS $ Katip.ls @String "use password from $BETTER_PASS"
          fromMaybe BS.empty . BS.stripSuffix "\10" <$> BS.readFile passfile
        Nothing -> do
          T.putStrLn "Please enter password:"
          read_passworld_from_stdin
    Crypto.decryptAndVerifyAES128Key (Config.aes128_salt cfg) password (Config.aes128_verify cfg) (Config.aes128_secret cfg)

runReadonlyRepositoryFromCwd :: E.Eff '[StructuredConcurrency, E.Repository, E.Logging, E.IOE] a -> IO a
runReadonlyRepositoryFromCwd m = E.runEff $ runHandleScribeKatip $ do
  cwd <- liftIO P.getWorkingDirectory >>= Path.parseAbsDir
  config <- liftIO $ LocalCache.readConfig cwd

  let
    !repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  aes <- extract_block_cipher config

  m
    & EKi.runStructuredConcurrency
    & runRepository repository aes

runRepositoryForBackupFromCwd :: E.Eff [StructuredConcurrency, Tmp, BackupStatistics, BackupCache, E.Repository, E.Logging, E.IOE] (VersionDigest, Version) -> IO (VersionDigest, Version)
runRepositoryForBackupFromCwd m = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let !repository = case Config.config_repoType config of
        Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  let
    try_removing p =
      void $
        tryJust (\e -> if IOE.isDoesNotExistError e then Just e else Nothing) $
          D.removeDirectoryRecursive p

  pid <- P.getProcessID

  -- Remove cur before using it to prevent dirty env.
  try_removing "cur"

  let version_tag = "version_base16_digest" :: BS.ByteString

  ret <- E.runEff . runHandleScribeKatip $ do
    aes <- extract_block_cipher config
    runRepository repository aes $
      E.withSeqEffToIO $ \seq_un -> do
        -- Prev DB is invalid if we can't find corresponding version. In that case we can't tell if the 'file'
        -- is DB still exists in repository.
        prev_db_is_valid <- LV.withDB "prev" (LV.defaultOptions{LV.createIfMissing = True}) $ \prev -> do
          opt_v <- LV.get prev LV.defaultReadOptions version_tag
          case opt_v >>= Hash.digestFromByteString of
            Just raw_digest -> seq_un $ do
              exists <- Repo.doesVersionExists $ Hash.UnsafeMkVersionDigest raw_digest
              logging Katip.DebugS . fold $
                [ Katip.ls @T.Text "version digest from prev DB, "
                , Katip.showLS raw_digest
                , Katip.ls @T.Text $ if exists then ", exists" else ", doesn't exist"
                ]
              pure $! exists
            Nothing -> pure False

        unless prev_db_is_valid $ do
          seq_un $ logging Katip.InfoS $ Katip.ls @T.Text "remove prev DB since it's invalid"
          try_removing "prev"

        withSystemTempDirectory ("better-tmp-" <> show pid <> "-") $ \raw_tmp_dir -> do
          LV.withDB "prev" (LV.defaultOptions{LV.createIfMissing = True}) $ \prev -> do
            -- Remove cur if failed to backup and keep prev intact.
            (`onException` try_removing "cur") $
              LV.withDB "cur" (LV.defaultOptions{LV.createIfMissing = True, LV.errorIfExists = True}) $ \cur -> do
                abs_tmp_dir <- Path.parseAbsDir raw_tmp_dir
                (v_digest, v) <-
                  m
                    & EKi.runStructuredConcurrency
                    & runTmp abs_tmp_dir
                    & runBackupStatistics
                    & runBackupCacheLevelDB prev cur
                    & seq_un
                LV.put cur LV.defaultWriteOptions version_tag (Hash.digestToByteString $ coerce v_digest)
                pure (v_digest, v)

  try_removing "prev.bac"
  D.renameDirectory "prev" "prev.bac"
  D.renameDirectory "cur" "prev"
  D.removeDirectoryRecursive "prev.bac"

  pure ret

runHandleScribeKatip :: E.IOE E.:> es => E.Eff (E.Logging : es) a -> E.Eff es a
runHandleScribeKatip m = E.withSeqEffToIO $ \un -> do
  handleScribe <- Katip.mkHandleScribe Katip.ColorIfTerminal stderr (Katip.permitItem Katip.InfoS) Katip.V1
  let makeLogEnv = Katip.registerScribe "stdout" handleScribe Katip.defaultScribeSettings =<< Katip.initLogEnv "better" "prod"
  -- closeScribes will stop accepting new logs, flush existing ones and clean up resources
  bracket makeLogEnv Katip.closeScribes $ \le -> do
    un $ runLogging le m
