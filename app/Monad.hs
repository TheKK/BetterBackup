{-# LANGUAGE Strict #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Monad
  ( BackupRepoEnv(BackupRepoEnv)
  , BackupRepoT()
  , runBackupRepoT
  , ReadonlyRepoEnv(..)
  , ReadonlyRepoT(..)
  ) where

import GHC.Generics (Generic(..))

import qualified Path
import Path (Path)

import qualified Capability.Source as C
import qualified Capability.Reader as C

import Control.Monad.IO.Class (MonadIO())
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..), MonadMask(..))
import Control.Monad.Reader (ReaderT(..))

import UnliftIO (MonadUnliftIO(..))

import qualified Database.LevelDB.Base as LV

import Better.Repository (Repository, MonadRepository, TheMonadRepository(..))
import Better.TempDir (MonadTmp, TheMonadTmp(..))
import Better.Statistics.Backup.Class (MonadBackupStat)
import Better.Statistics.Backup.Default (TheTVarBackupStatistics)
import qualified Better.Statistics.Backup.Default as BackupSt

import Better.Repository.BackupCache.Class (MonadBackupCache())
import Better.Repository.BackupCache.LevelDB (TheLevelDBBackupCache(TheLevelDBBackupCache))

data ReadonlyRepoEnv = ReadonlyRepoEnv
  { ro_repo_path :: Path Path.Abs Path.Dir
  , ro_repo_cwd :: Path Path.Abs Path.Dir
  , ro_repo_repo :: Repository
  , ro_repo_tmpdir :: Path Path.Abs Path.Dir
  }
  deriving (Generic)

newtype ReadonlyRepoT m a = ReadonlyRepoT { runReadonlyRepoT :: ReaderT ReadonlyRepoEnv m a }
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
    ) via (ReaderT ReadonlyRepoEnv m)
  deriving
    ( MonadRepository
    ) via TheMonadRepository
         (C.Rename "ro_repo_repo"
         (C.Field "ro_repo_repo" ()
         (C.MonadReader
         (ReaderT ReadonlyRepoEnv m))))
  deriving
    ( MonadTmp
    ) via TheMonadTmp
         (C.Rename "ro_repo_tmpdir"
         (C.Field "ro_repo_tmpdir" ()
         (C.MonadReader
         (ReaderT ReadonlyRepoEnv m))))

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

newtype BackupRepoT m a = BackupRepoT { _unBackupRepoT :: ReaderT BackupRepoEnv m a }
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
    ) via (ReaderT BackupRepoEnv m)
  deriving
    ( MonadRepository
    ) via TheMonadRepository
         (C.Rename "backup_repo_repo"
         (C.Field "backup_repo_repo" ()
         (C.MonadReader
         (ReaderT BackupRepoEnv m))))
  deriving
    ( MonadTmp
    ) via TheMonadTmp
         (C.Rename "backup_repo_tmpdir"
         (C.Field "backup_repo_tmpdir" ()
         (C.MonadReader
         (ReaderT BackupRepoEnv m))))
  deriving
    ( C.HasSource "prev_db" LV.DB, C.HasReader "prev_db" LV.DB
    ) via C.Rename "backup_repo_previousBackupCache"
         (C.Field "backup_repo_previousBackupCache" ()
         (C.MonadReader
         (ReaderT BackupRepoEnv m)))
  deriving
    ( C.HasSource "cur_db" LV.DB, C.HasReader "cur_db" LV.DB
    ) via C.Rename "backup_repo_currentBackupCache"
         (C.Field "backup_repo_currentBackupCache" ()
         (C.MonadReader
         (ReaderT BackupRepoEnv m)))
  deriving
    ( C.HasSource "backup_st" BackupSt.Statistics, C.HasReader "backup_st" BackupSt.Statistics
    ) via C.Rename "backup_repo_statistic"
         (C.Field "backup_repo_statistic" ()
         (C.MonadReader
         (ReaderT BackupRepoEnv m)))

{-# INLINE runBackupRepoT #-}
runBackupRepoT :: BackupRepoT m a -> BackupRepoEnv -> m a
runBackupRepoT m env = flip runReaderT env $ _unBackupRepoT m

deriving via (TheLevelDBBackupCache (BackupRepoT m)) instance (MonadIO m) => MonadBackupCache (BackupRepoT m)

deriving via (TheTVarBackupStatistics (BackupRepoT m)) instance (MonadIO m) => MonadBackupStat (BackupRepoT m)

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)
