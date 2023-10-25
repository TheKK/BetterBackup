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

import qualified Path

import Control.Monad (void)
import Control.Monad.Catch (onException, tryJust)

import Control.Exception (bracket)

import qualified Database.LevelDB.Base as LV

import qualified Effectful as E

import Better.Internal.Repository.LowLevel (runRepository)
import Better.Logging.Effect (runLogging)
import qualified Better.Logging.Effect as E
import qualified Better.Repository as Repo
import Better.Repository.BackupCache.Class (BackupCache)
import Better.Repository.BackupCache.LevelDB (runBackupCacheLevelDB)
import qualified Better.Repository.Class as E
import Better.Statistics.Backup.Class (BackupStatistics, runBackupStatistics)
import Better.TempDir (runTmp)
import Better.TempDir.Class (Tmp)

import qualified System.IO.Error as IOE
import System.IO.Temp (withSystemTempDirectory)
import qualified System.Posix as P

import qualified System.Directory as D
import System.IO (stdout)

import qualified Katip

import qualified Config
import qualified LocalCache

run_readonly_repo_t_from_cwd :: E.Eff '[E.Repository, E.IOE] a -> IO a
run_readonly_repo_t_from_cwd m = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let
    !repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  E.runEff $
    runRepository repository m

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
