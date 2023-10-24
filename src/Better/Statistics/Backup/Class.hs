{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Better.Statistics.Backup.Class (
  -- * Effectful
  BackupStatistics,
  runBackupStatistics,
  processedFileCount,
  newFileCount,
  totalFileCount,
  processedDirCount,
  newDirCount,
  totalDirCount,
  processedChunkCount,
  newChunkCount,
  uploadedBytes,
) where

import Data.Word (Word64)

import Control.Concurrent.STM.TVar (TVar, newTVarIO)

import Control.Monad.IO.Class (MonadIO (liftIO))

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as E
import qualified Effectful.Dispatch.Static as ES

-- TODO Exposing details (TVar) helps user understand that these methods could be
--  safely used concurrently. On the other hand, using TVar explicitly makes instances
--  of this monad less flexible to implement (eg. can't ignore statistics).
--
--  Therefore, the interfaces might change in the future.
data BackupStatistics :: E.Effect

type instance E.DispatchOf BackupStatistics = 'E.Static 'ES.WithSideEffects
data instance E.StaticRep BackupStatistics = BackupStatisticsRep
  { _backup_stat_processedFileCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_newFileCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_totalFileCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_processedDirCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_newDirCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_totalDirCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_processedChunkCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_newChunkCount :: {-# UNPACK #-} !(TVar Word64)
  , _backup_stat_uploadedBytes :: {-# UNPACK #-} !(TVar Word64)
  }

runBackupStatistics :: (E.IOE E.:> es) => E.Eff (BackupStatistics : es) a -> E.Eff es a
runBackupStatistics eff = do
  rep <-
    liftIO $
      BackupStatisticsRep
        <$> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0
        <*> newTVarIO 0

  ES.evalStaticRep rep eff

processedFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedFileCount = _backup_stat_processedFileCount <$> ES.getStaticRep

newFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newFileCount = _backup_stat_newFileCount <$> ES.getStaticRep

totalFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
totalFileCount = _backup_stat_totalFileCount <$> ES.getStaticRep

processedDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedDirCount = _backup_stat_processedDirCount <$> ES.getStaticRep

newDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newDirCount = _backup_stat_newDirCount <$> ES.getStaticRep

totalDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
totalDirCount = _backup_stat_totalDirCount <$> ES.getStaticRep

processedChunkCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedChunkCount = _backup_stat_processedChunkCount <$> ES.getStaticRep

newChunkCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newChunkCount = _backup_stat_newChunkCount <$> ES.getStaticRep

uploadedBytes :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
uploadedBytes = _backup_stat_uploadedBytes <$> ES.getStaticRep
