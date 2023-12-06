{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Monad (
  run_readonly_repo_t_from_cwd,
  run_backup_repo_t_from_cwd,
) where

import Path qualified

import Control.Exception (bracket)
import Control.Monad (void)
import Control.Monad.Catch (onException, tryJust)
import Control.Monad.IO.Class (liftIO)

import Data.Function ((&))

import Database.LevelDB.Base qualified as LV

import Effectful qualified as E

import Better.Internal.Repository.LowLevel (runRepository)
import Better.Logging.Effect (runLogging)
import Better.Logging.Effect qualified as E
import Better.Repository qualified as Repo
import Better.Repository.BackupCache.Class (BackupCache)
import Better.Repository.BackupCache.LevelDB (runBackupCacheLevelDB)
import Better.Repository.Class qualified as E
import Better.Statistics.Backup.Class (BackupStatistics, runBackupStatistics)
import Better.TempDir (runTmp)
import Better.TempDir.Class (Tmp)

import System.IO.Error qualified as IOE
import System.IO.Temp (withSystemTempDirectory)
import System.Posix qualified as P

import System.Directory qualified as D
import System.IO (stdout)

import Katip qualified

import Config qualified
import LocalCache qualified

run_readonly_repo_t_from_cwd :: E.Eff '[E.Repository, E.Logging, E.IOE] a -> IO a
run_readonly_repo_t_from_cwd m = E.runEff $ runHandleScribeKatip $ do
  cwd <- liftIO P.getWorkingDirectory >>= Path.parseAbsDir
  config <- liftIO $ LocalCache.readConfig cwd

  let
    !repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  m
    & runRepository repository

run_backup_repo_t_from_cwd :: E.Eff [Tmp, BackupStatistics, BackupCache, E.Repository, E.Logging, E.IOE] a -> IO a
run_backup_repo_t_from_cwd m = do
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

  ret <-
    LV.withDB "prev" (LV.defaultOptions{LV.createIfMissing = True}) $ \prev ->
      LV.withDB "cur" (LV.defaultOptions{LV.createIfMissing = True, LV.errorIfExists = True}) $ \cur ->
        -- Remove cur if failed to backup and keep prev intact.
        (`onException` try_removing "cur") $
          withSystemTempDirectory ("better-tmp-" <> show pid <> "-") $ \raw_tmp_dir -> do
            abs_tmp_dir <- Path.parseAbsDir raw_tmp_dir
            E.runEff $
              runHandleScribeKatip $
                runRepository repository $
                  runBackupCacheLevelDB prev cur $
                    runBackupStatistics $
                      runTmp abs_tmp_dir m

  try_removing "prev_bac"
  D.renameDirectory "prev" "prev.bac"
  D.renameDirectory "cur" "prev"
  D.removeDirectoryRecursive "prev.bac"

  pure ret

runHandleScribeKatip :: E.IOE E.:> es => E.Eff (E.Logging : es) a -> E.Eff es a
runHandleScribeKatip m = E.withSeqEffToIO $ \un -> do
  handleScribe <- Katip.mkHandleScribe Katip.ColorIfTerminal stdout (Katip.permitItem Katip.InfoS) Katip.V1
  let makeLogEnv = Katip.registerScribe "stdout" handleScribe Katip.defaultScribeSettings =<< Katip.initLogEnv "better" "prod"
  -- closeScribes will stop accepting new logs, flush existing ones and clean up resources
  bracket makeLogEnv Katip.closeScribes $ \le -> do
    un $ runLogging le m
