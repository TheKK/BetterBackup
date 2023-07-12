{-# LANGUAGE Strict #-}

module Better.Statistics.Backup.Class
  ( MonadBackupStat(..)
  ) where

import Data.Word (Word64)

import qualified UnliftIO as Un

import Control.Concurrent.STM.TVar (TVar)

-- TODO Exposing details (TVar) helps user understand that these methods could be
--  safely used concurrently. On the other hand, using TVar explicitly makes instances
--  of this monad less flexible to implement (eg. can't ignore statistics).
--
--  Therefore, the interfaces might change in the future.
class Un.MonadUnliftIO m => MonadBackupStat m where
  processedFileCount :: m (TVar Word64)
  totalFileCount :: m (TVar Word64)

  processedDirCount :: m (TVar Word64)
  totalDirCount :: m (TVar Word64)

  processedChunkCount :: m (TVar Word64)

  uploadedBytes :: m (TVar Word64)
