{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}

module Monad (
  run_readonly_repo_t_from_cwd,
  run_backup_repo_t_from_cwd,
) where

import qualified Path

import Control.Monad (void)
import Control.Monad.Catch (onException, tryJust)

import qualified Database.LevelDB.Base as LV

import qualified Effectful as E

import qualified Better.Repository as Repo
import Better.Repository.BackupCache.Class (BackupCache)
import Better.Repository.BackupCache.LevelDB (runBackupCacheLevelDB)
import Better.Statistics.Backup.Class (BackupStatistics, runBackupStatistics)
import Better.TempDir (runTmp)

import qualified System.IO.Error as IOE
import System.IO.Temp (withSystemTempDirectory)
import qualified System.Posix as P

import Better.Internal.Repository.LowLevel (runRepository)
import qualified Better.Repository.Class as E
import Better.TempDir.Class (Tmp)
import qualified Config
import qualified LocalCache
import qualified System.Directory as D

run_readonly_repo_t_from_cwd :: E.Eff '[E.Repository, E.IOE] a -> IO a
run_readonly_repo_t_from_cwd m = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let
    !repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  E.runEff $
    runRepository repository m

run_backup_repo_t_from_cwd :: E.Eff '[Tmp, BackupStatistics, BackupCache, E.Repository, E.IOE] a -> IO a
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
              runRepository repository $
                runBackupCacheLevelDB prev cur $
                  runBackupStatistics $
                    runTmp abs_tmp_dir m

  try_removing "prev_bac"
  D.renameDirectory "prev" "prev.bac"
  D.renameDirectory "cur" "prev"
  D.removeDirectoryRecursive "prev.bac"

  pure ret
