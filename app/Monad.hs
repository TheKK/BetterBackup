{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Monad (
  BackupRepoEnv (BackupRepoEnv),
  BackupRepoT (),
  runBackupRepoT,
  ReadonlyRepoEnv (..),
  ReadonlyRepoT (..),
  run_readonly_repo_t_from_cwd,
  run_backup_repo_t_from_cwd,
) where

import GHC.Generics (Generic (..))

import Path (Path)
import qualified Path

import qualified Capability.Reader as C
import qualified Capability.Source as C

import Control.Monad (replicateM_, void)
import Control.Monad.Catch (MonadCatch (..), MonadMask (..), MonadThrow (..))
import Control.Monad.IO.Class (MonadIO ())
import Control.Monad.Reader (ReaderT (..))

import UnliftIO (MonadUnliftIO (..))

import qualified Database.LevelDB.Base as LV

import Better.Repository (MonadRepository, Repository, TheMonadRepository (..))
import qualified Better.Repository as Repo
import Better.Repository.BackupCache.Class (MonadBackupCache ())
import Better.Repository.BackupCache.LevelDB (TheLevelDBBackupCache (TheLevelDBBackupCache))
import Better.Statistics.Backup.Class (MonadBackupStat)
import Better.Statistics.Backup.Default (TheTVarBackupStatistics)
import qualified Better.Statistics.Backup.Default as BackupSt
import Better.TempDir (MonadTmp, TheMonadTmp (..))

import qualified System.IO.Error as IOE
import qualified System.Posix as P
import qualified UnliftIO as Un
import qualified UnliftIO.Directory as D
import qualified UnliftIO.Directory as P

import qualified Config
import qualified LocalCache

data ReadonlyRepoEnv = ReadonlyRepoEnv
  { ro_repo_path :: Path Path.Abs Path.Dir
  , ro_repo_cwd :: Path Path.Abs Path.Dir
  , ro_repo_repo :: Repository
  , ro_repo_tmpdir :: Path Path.Abs Path.Dir
  }
  deriving (Generic)

newtype ReadonlyRepoT m a = ReadonlyRepoT {runReadonlyRepoT :: ReaderT ReadonlyRepoEnv m a}
  deriving (Generic)
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadIO
    , MonadCatch
    , MonadThrow
    , MonadMask
    , MonadUnliftIO
    )
    via (ReaderT ReadonlyRepoEnv m)
  deriving
    ( MonadRepository
    )
    via TheMonadRepository
          ( C.Rename
              "ro_repo_repo"
              ( C.Field
                  "ro_repo_repo"
                  ()
                  ( C.MonadReader
                      (ReaderT ReadonlyRepoEnv m)
                  )
              )
          )
  deriving
    ( MonadTmp
    )
    via TheMonadTmp
          ( C.Rename
              "ro_repo_tmpdir"
              ( C.Field
                  "ro_repo_tmpdir"
                  ()
                  ( C.MonadReader
                      (ReaderT ReadonlyRepoEnv m)
                  )
              )
          )

data BackupRepoEnv = BackupRepoEnv
  { backup_repo_path :: Path Path.Abs Path.Dir
  , backup_repo_cwd :: Path Path.Abs Path.Dir
  , backup_repo_repo :: Repository
  , backup_repo_tmpdir :: Path Path.Abs Path.Dir
  , backup_repo_statistic :: BackupSt.Statistics
  , backup_repo_previousBackupCache :: LV.DB
  , backup_repo_currentBackupCache :: LV.DB
  }
  deriving (Generic)

newtype BackupRepoT m a = BackupRepoT {_unBackupRepoT :: ReaderT BackupRepoEnv m a}
  deriving (Generic)
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadIO
    , MonadCatch
    , MonadThrow
    , MonadMask
    , MonadUnliftIO
    )
    via (ReaderT BackupRepoEnv m)
  deriving
    ( MonadRepository
    )
    via TheMonadRepository
          ( C.Rename
              "backup_repo_repo"
              ( C.Field
                  "backup_repo_repo"
                  ()
                  ( C.MonadReader
                      (ReaderT BackupRepoEnv m)
                  )
              )
          )
  deriving
    ( MonadTmp
    )
    via TheMonadTmp
          ( C.Rename
              "backup_repo_tmpdir"
              ( C.Field
                  "backup_repo_tmpdir"
                  ()
                  ( C.MonadReader
                      (ReaderT BackupRepoEnv m)
                  )
              )
          )
  deriving
    ( C.HasSource "prev_db" LV.DB
    , C.HasReader "prev_db" LV.DB
    )
    via C.Rename
          "backup_repo_previousBackupCache"
          ( C.Field
              "backup_repo_previousBackupCache"
              ()
              ( C.MonadReader
                  (ReaderT BackupRepoEnv m)
              )
          )
  deriving
    ( C.HasSource "cur_db" LV.DB
    , C.HasReader "cur_db" LV.DB
    )
    via C.Rename
          "backup_repo_currentBackupCache"
          ( C.Field
              "backup_repo_currentBackupCache"
              ()
              ( C.MonadReader
                  (ReaderT BackupRepoEnv m)
              )
          )
  deriving
    ( C.HasSource "backup_st" BackupSt.Statistics
    , C.HasReader "backup_st" BackupSt.Statistics
    )
    via C.Rename
          "backup_repo_statistic"
          ( C.Field
              "backup_repo_statistic"
              ()
              ( C.MonadReader
                  (ReaderT BackupRepoEnv m)
              )
          )

{-# INLINE runBackupRepoT #-}
runBackupRepoT :: BackupRepoT m a -> BackupRepoEnv -> m a
runBackupRepoT m env = flip runReaderT env $ _unBackupRepoT m

deriving via (TheLevelDBBackupCache (BackupRepoT IO)) instance MonadBackupCache (BackupRepoT IO)

deriving via (TheTVarBackupStatistics (BackupRepoT IO)) instance MonadBackupStat (BackupRepoT IO)

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)

run_readonly_repo_t_from_cwd :: ReadonlyRepoT IO a -> IO a
run_readonly_repo_t_from_cwd m = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let
    repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  let
    env =
      ReadonlyRepoEnv
        [Path.absdir|/tmp|]
        cwd
        repository
        [Path.absdir|/tmp|]

  flip runReaderT env $ runReadonlyRepoT m

run_backup_repo_t_from_cwd :: BackupRepoT IO a -> IO a
run_backup_repo_t_from_cwd m = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let repository = case Config.config_repoType config of
        Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  let
    try_removing p =
      void $
        Un.tryJust (\e -> if IOE.isDoesNotExistError e then Just e else Nothing) $
          D.removeDirectoryRecursive p

  pid <- P.getProcessID
  statistics <- BackupSt.initStatistics

  -- Remove cur before using it to prevent dirty env.
  try_removing "cur"

  ret <-
    LV.withDB "prev" (LV.defaultOptions{LV.createIfMissing = True}) $ \prev ->
      LV.withDB "cur" (LV.defaultOptions{LV.createIfMissing = True, LV.errorIfExists = True}) $ \cur ->
        -- Remove cur if failed to backup and keep prev intact.
        (`Un.onException` try_removing "cur") $
          Un.withSystemTempDirectory ("better-tmp-" <> show pid <> "-") $ \raw_tmp_dir -> do
            abs_tmp_dir <- Path.parseAbsDir raw_tmp_dir
            runBackupRepoT m $
              BackupRepoEnv
                abs_tmp_dir
                cwd
                repository
                abs_tmp_dir
                statistics
                prev
                cur

  try_removing "prev_bac"
  D.renameDirectory "prev" "prev.bac"
  D.renameDirectory "cur" "prev"
  D.removeDirectoryRecursive "prev.bac"

  pure ret
