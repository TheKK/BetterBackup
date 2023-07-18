{-# LANGUAGE Strict #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Statistics.Backup.Default
  ( Statistics()
  , initStatistics
  , TheTVarBackupStatistics(..)
  ) where

import Data.Word (Word64)

import qualified Capability.Reader as C

import qualified UnliftIO as Un

import Control.Concurrent.STM.TVar (TVar)

import Better.Statistics.Backup.Class (MonadBackupStat(..))

data Statistics = Statistics
  { _processedFileCount :: TVar Word64
  , _processedDirCount :: TVar Word64
  , _totalFileCount :: TVar Word64
  , _totalDirCount :: TVar Word64
  , _processChunkCount :: TVar Word64
  , _uploadedBytes :: TVar Word64
  }

initStatistics :: Un.MonadUnliftIO m => m Statistics
initStatistics = do
  process_file_count <- Un.newTVarIO 0
  total_file_count <- Un.newTVarIO 0
  process_dir_count <- Un.newTVarIO 0
  total_dir_count <- Un.newTVarIO 0
  process_chunk_count <- Un.newTVarIO 0
  uploaded_bytes <- Un.newTVarIO 0

  pure $ Statistics
    process_file_count
    total_file_count
    process_dir_count
    total_dir_count
    process_chunk_count
    uploaded_bytes

-- | Use this newtype with DerivingVia to gain instance of MonadBackupStat.
newtype TheTVarBackupStatistics m a = TheTVarBackupStatistics (m a)

instance (C.HasReader "backup_st" Statistics m) => MonadBackupStat (TheTVarBackupStatistics m) where
  {-# INLINE processedFileCount #-}
  processedFileCount = TheTVarBackupStatistics $ C.asks @"backup_st" _processedFileCount
  {-# INLINE totalFileCount #-}
  totalFileCount = TheTVarBackupStatistics $ C.asks @"backup_st" _totalFileCount

  {-# INLINE processedDirCount #-}
  processedDirCount = TheTVarBackupStatistics $ C.asks @"backup_st" _processedDirCount
  {-# INLINE totalDirCount #-}
  totalDirCount = TheTVarBackupStatistics $ C.asks @"backup_st" _totalDirCount

  {-# INLINE processedChunkCount #-}
  processedChunkCount = TheTVarBackupStatistics $ C.asks @"backup_st" _processChunkCount

  {-# INLINE uploadedBytes #-}
  uploadedBytes = TheTVarBackupStatistics $ C.asks @"backup_st" _uploadedBytes
