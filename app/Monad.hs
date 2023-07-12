{-# LANGUAGE Strict #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}

module Monad
  ( Hbk(..)
  , HbkT(..)
  ) where

import GHC.Generics (Generic(..))

import Data.Word (Word64)
import Control.Concurrent.STM.TVar (TVar)

import qualified Path
import Path (Path)

import qualified Capability.Reader as C

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..), MonadMask(..))
import Control.Monad.Reader (ReaderT(..))

import UnliftIO (MonadUnliftIO(..))

import Better.Repository (Repository, MonadRepository, TheMonadRepository(..))
import Better.TempDir (MonadTmp, TheMonadTmp(..))
import qualified Better.Statistics.Backup.Class as BackupSt

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
  }
  deriving (Generic)

newtype HbkT m a = HbkT { runHbkT :: Hbk (HbkT m) -> m a }
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

instance MonadUnliftIO m => BackupSt.MonadBackupStat (HbkT m) where
  processedFileCount = HbkT $ \hbk -> pure $ hbk_processedFileCount hbk
  totalFileCount = HbkT $ \hbk -> pure $ hbk_totalFileCount hbk

  processedDirCount = HbkT $ \hbk -> pure $ hbk_processedDirCount hbk
  totalDirCount = HbkT $ \hbk -> pure $ hbk_totalDirCount hbk

  processedChunkCount = HbkT $ \hbk -> pure $ hbk_processChunkCount hbk

  uploadedBytes = HbkT $ \hbk -> pure $ hbk_uploadedBytes hbk

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)
