{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Strict #-}

module Cli
  ( cmds
  ) where

import qualified System.Posix.Directory as P

import Options.Applicative

import qualified UnliftIO.STM as Un
import qualified UnliftIO.Concurrent as Un
import qualified UnliftIO.Async as Un
import qualified UnliftIO.Exception as Un

import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Exception (Exception(displayException))

import Crypto.Hash (Digest, SHA256, digestFromByteString)

import Data.String (fromString)
import Data.Foldable
import Data.Function ((&))
import Data.Bifunctor (first)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import qualified Data.ByteString.Base16 as BSBase16

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.Console.Stdio as Stdio

import Path (Path, Abs, Dir)
import qualified Path

import Config (Config(..))
import qualified Config

import qualified Better.Repository as Repo
import Better.Statistics.Backup (MonadBackupStat)
import qualified Better.Statistics.Backup as BackupSt

import qualified LocalCache
import qualified Monad as M

-- TODO add ability to put trace markers
-- TODO add ability to collect running statistics
cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Better operations"
      ]

    parser = subparser $ fold
      [ (command "init" parser_info_init)
      , (command "versions" parser_info_versions)
      , (command "backup" parser_info_backup)
      , (command "gc" parser_info_gc)
      , (command "integrity-check" parser_info_integrity_check)
      , (command "cat-chunk" parser_info_cat_chunk)
      , (command "cat-file" parser_info_cat_file)
      , (command "cat-file-chunks" parser_info_cat_file_chunks)
      , (command "cat-tree" parser_info_cat_tree)
      ]

parser_info_init :: ParserInfo (IO ())
parser_info_init = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Initialize repository"
      ]

    parser = subparser $ fold
      [ command "local" parser_info_init_local
      ]

parser_info_init_local :: ParserInfo (IO ())
parser_info_init_local = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Initialize with local repository"
      ]

    parser = LocalCache.initialize
      <$> argument some_base_dir_read (fold
            [ metavar "CACHE_PATH"
            , help "path to store your local cache"
            ])
      <*> (Config <$> p_local_repo_config)

parser_info_versions :: ParserInfo (IO ())
parser_info_versions = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "List backuped versions"
      ]

    parser = pure go

    go = do
      hbk <- mk_hbk_from_cwd
      Repo.listVersions
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (F.drainMapM print)

parser_info_backup :: ParserInfo (IO ())
parser_info_backup = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Construct new version"
      ]

    parser = go
      <$> argument some_base_dir_read (fold
            [ metavar "BACKUP_ROOT"
            , help "directory you'd like to backup"
            ])

    go dir_to_backup = do
      hbk <- mk_hbk_from_cwd
      version <- flip M.runHbkT hbk $ do
        let 
          process_reporter = forever $ do
            Un.mask_ $ report_backup_stat
            Un.threadDelay (1000 * 1000)

        Un.withAsync process_reporter $ \_ -> do
          v <- Repo.backup $ T.pack $ Path.fromSomeDir dir_to_backup
          liftIO (putStrLn "result:") >> report_backup_stat
          pure v

      print version

report_backup_stat :: (MonadBackupStat m, MonadIO m) => m ()
report_backup_stat = do
  process_file_count <- BackupSt.readStatistics BackupSt.processedFileCount
  total_file_count <- BackupSt.readStatistics BackupSt.totalFileCount
  process_dir_count <- BackupSt.readStatistics BackupSt.processedDirCount
  total_dir_count <- BackupSt.readStatistics BackupSt.totalDirCount
  process_chunk_count <- BackupSt.readStatistics BackupSt.processedChunkCount
  upload_bytes <- BackupSt.readStatistics BackupSt.uploadedBytes
  liftIO $ putStrLn $ fold
    [ show process_file_count, "/", show total_file_count, " files, "
    , show process_dir_count, "/", show total_dir_count, " dirs, "
    , show process_chunk_count, " chunks, "
    , show upload_bytes, " bytes"
    ]

parser_info_gc :: ParserInfo (IO ())
parser_info_gc = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Running garbage collection to release unused data"
      ]

    parser = pure go

    go = do
      hbk <- mk_hbk_from_cwd
      flip M.runHbkT hbk $ Repo.garbageCollection

parser_info_integrity_check :: ParserInfo (IO ())
parser_info_integrity_check = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Verify and handle corrupted data"
      ]

    parser = pure go

    go = do
      hbk <- mk_hbk_from_cwd
      flip M.runHbkT hbk $ Repo.checksum 8

parser_info_cat_chunk :: ParserInfo (IO ())
parser_info_cat_chunk = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Display content of single chunk"
      ]

    parser = go
      <$> argument digest_read (fold
            [ metavar "SHA"
            , help "SHA of chunk"
            ])

    go sha = do
      hbk <- mk_hbk_from_cwd
      Repo.catChunk sha
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (Stdio.writeChunks)

parser_info_cat_file :: ParserInfo (IO ())
parser_info_cat_file = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Display content of single file"
      ]

    parser = go
      <$> argument digest_read (fold
            [ metavar "SHA"
            , help "SHA of file"
            ])

    go sha = do
      hbk <- mk_hbk_from_cwd
      Repo.catFile sha
        & S.concatMap (Repo.catChunk . Repo.chunk_name)
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (Stdio.writeChunks)

parser_info_cat_file_chunks :: ParserInfo (IO ())
parser_info_cat_file_chunks = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Display chunks a single file references to"
      ]

    parser = go
      <$> argument digest_read (fold
            [ metavar "SHA"
            , help "SHA of file"
            ])

    go sha = do
      hbk <- mk_hbk_from_cwd
      Repo.catFile sha
        & fmap (show . Repo.chunk_name)
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (F.drainMapM putStrLn)

parser_info_cat_tree :: ParserInfo (IO ())
parser_info_cat_tree = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Display content of tree"
      ]

    parser = go
      <$> argument digest_read (fold
            [ metavar "SHA"
            , help "SHA of tree"
            ])

    go sha = do
      hbk <- mk_hbk_from_cwd
      Repo.catTree sha
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (F.drainMapM $ T.putStrLn . T.pack . show)

p_local_repo_config :: Parser Config.RepoType
p_local_repo_config = (Config.Local . Config.LocalRepoConfig)
  <$> (argument abs_dir_read (metavar "REPO_PATH" <> help "path to store your backup"))

some_base_dir_read :: ReadM (Path.SomeBase Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir

abs_dir_read :: ReadM (Path Abs Dir)
abs_dir_read = eitherReader $ first displayException . Path.parseAbsDir

digest_read :: ReadM (Digest SHA256)
digest_read = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decode $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString @SHA256 sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length" 
    Just digest -> pure digest

  pure digest

mk_hbk_from_cwd :: Applicative m => IO (M.Hbk m)
mk_hbk_from_cwd = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let
    repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  process_file_count <- Un.newTVarIO 0
  total_file_count <- Un.newTVarIO 0
  process_dir_count <- Un.newTVarIO 0
  total_dir_count <- Un.newTVarIO 0
  process_chunk_count <- Un.newTVarIO 0
  uploaded_bytes <- Un.newTVarIO 0

  pure $ M.MkHbk
    [Path.absdir|/tmp|]
    cwd
    repository
    [Path.absdir|/tmp|]
    (pure ())
    process_file_count
    total_file_count
    process_dir_count
    total_dir_count
    process_chunk_count
    uploaded_bytes
