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
  ( Hbk(..)
  , HbkT()
  , runHbkT
  , ReadonlyRepoEnv(..)
  , ReadonlyRepoT(..)
  ) where

import GHC.Generics (Generic(..))

import Data.Word (Word64)

import Control.Concurrent.STM.TVar (TVar)

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
import qualified Better.Statistics.Backup.Class as BackupSt

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

data Hbk m = MkHbk
  { hbk_path :: Path Path.Abs Path.Dir
  , hbk_cwd :: Path Path.Abs Path.Dir
  , hbk_repo :: Repository
  , hbk_tmpdir :: Path Path.Abs Path.Dir
  , what :: m ()
  , hbk_processedFileCount :: TVar Word64
  , hbk_processedDirCount :: TVar Word64
  , hbk_totalFileCount :: TVar Word64
  , hbk_totalDirCount :: TVar Word64
  , hbk_processChunkCount :: TVar Word64
  , hbk_uploadedBytes :: TVar Word64
  , hbk_previousBackupCache :: LV.DB
  , hbk_currentBackupCache :: LV.DB
  }
  deriving (Generic)

newtype HbkT m a = HbkT { _unHbkT :: ReaderT (Hbk (HbkT m)) m a }
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
    ) via (ReaderT (Hbk (HbkT m)) m)
  deriving
    ( MonadRepository
    ) via TheMonadRepository
         (C.Rename "hbk_repo"
         (C.Field "hbk_repo" ()
         (C.MonadReader
         (ReaderT (Hbk (HbkT m)) m))))
  deriving
    ( MonadTmp
    ) via TheMonadTmp
         (C.Rename "hbk_tmpdir"
         (C.Field "hbk_tmpdir" ()
         (C.MonadReader
         (ReaderT (Hbk (HbkT m)) m))))
  deriving
    ( C.HasSource "prev_db" LV.DB, C.HasReader "prev_db" LV.DB
    ) via C.Rename "hbk_previousBackupCache"
         (C.Field "hbk_previousBackupCache" ()
         (C.MonadReader
         (ReaderT (Hbk (HbkT m)) m)))
  deriving
    ( C.HasSource "cur_db" LV.DB, C.HasReader "cur_db" LV.DB
    ) via C.Rename "hbk_currentBackupCache"
         (C.Field "hbk_currentBackupCache" ()
         (C.MonadReader
         (ReaderT (Hbk (HbkT m)) m)))

{-# INLINE runHbkT #-}
runHbkT :: HbkT m a -> Hbk (HbkT m) -> m a
runHbkT m env = flip runReaderT env $ _unHbkT m

deriving via (TheLevelDBBackupCache (HbkT m)) instance (MonadIO m) => MonadBackupCache (HbkT m)

instance MonadUnliftIO m => BackupSt.MonadBackupStat (HbkT m) where
  processedFileCount = HbkT . ReaderT $ \hbk -> pure $ hbk_processedFileCount hbk
  totalFileCount = HbkT . ReaderT $ \hbk -> pure $ hbk_totalFileCount hbk

  processedDirCount = HbkT . ReaderT $ \hbk -> pure $ hbk_processedDirCount hbk
  totalDirCount = HbkT . ReaderT$ \hbk -> pure $ hbk_totalDirCount hbk

  processedChunkCount = HbkT . ReaderT$ \hbk -> pure $ hbk_processChunkCount hbk

  uploadedBytes = HbkT . ReaderT$ \hbk -> pure $ hbk_uploadedBytes hbk

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)
