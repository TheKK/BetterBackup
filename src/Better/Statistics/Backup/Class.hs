{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Better.Statistics.Backup.Class (
  -- * Effect
  BackupStatistics,

  -- * Handler
  runBackupStatistics,
  localBackupStatistics,

  -- * A little lower level of handler
  BackupStatisticsRep,
  mkBackupStatisticsRep,
  runBackupStatisticsWithRep,
  localBackupStatisticsWithRep,

  -- * Operations
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

import Data.Coerce (coerce)
import Data.Word (Word64)

import Control.Concurrent.STM.TVar (TVar, newTVarIO)

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static qualified as ES

-- TODO Exposing details (TVar) helps user understand that these methods could be
--  safely used concurrently. On the other hand, using TVar explicitly makes instances
--  of this monad less flexible to implement (eg. can't ignore statistics).
--
--  Therefore, the interfaces might change in the future.
data BackupStatistics :: E.Effect

-- | Core of BackupStatistics.
--
-- It's save to use this data outside scope of `BackupStatistics`.
data BackupStatisticsRep = BackupStatisticsRep
  { _backup_stat_processedFileCount :: (TVar Word64)
  , _backup_stat_newFileCount :: (TVar Word64)
  , _backup_stat_totalFileCount :: (TVar Word64)
  , _backup_stat_processedDirCount :: (TVar Word64)
  , _backup_stat_newDirCount :: (TVar Word64)
  , _backup_stat_totalDirCount :: (TVar Word64)
  , _backup_stat_processedChunkCount :: (TVar Word64)
  , _backup_stat_newChunkCount :: (TVar Word64)
  , _backup_stat_uploadedBytes :: (TVar Word64)
  }

type instance E.DispatchOf BackupStatistics = 'E.Static 'ES.WithSideEffects
newtype instance E.StaticRep BackupStatistics = BackupStatisticsRep' BackupStatisticsRep

mkBackupStatisticsRep :: E.Eff es BackupStatisticsRep
mkBackupStatisticsRep = do
  E.unsafeEff_ $
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

runBackupStatistics :: (E.IOE E.:> es) => E.Eff (BackupStatistics : es) a -> E.Eff es a
runBackupStatistics eff = do
  rep <- mkBackupStatisticsRep
  ES.evalStaticRep (BackupStatisticsRep' rep) eff

runBackupStatisticsWithRep :: (E.IOE E.:> es) => BackupStatisticsRep -> E.Eff (BackupStatistics : es) a -> E.Eff es a
runBackupStatisticsWithRep rep eff = do
  ES.evalStaticRep (BackupStatisticsRep' rep) eff

localBackupStatistics :: (BackupStatistics E.:> es) => E.Eff es a -> E.Eff es a
localBackupStatistics eff = do
  rep <- mkBackupStatisticsRep
  ES.localStaticRep (const (BackupStatisticsRep' rep)) eff

localBackupStatisticsWithRep :: (BackupStatistics E.:> es) => BackupStatisticsRep -> E.Eff es a -> E.Eff es a
localBackupStatisticsWithRep rep = ES.localStaticRep (const $ BackupStatisticsRep' rep)

processedFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedFileCount = _backup_stat_processedFileCount . coerce <$> ES.getStaticRep @BackupStatistics

newFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newFileCount = _backup_stat_newFileCount . coerce <$> ES.getStaticRep @BackupStatistics

totalFileCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
totalFileCount = _backup_stat_totalFileCount . coerce <$> ES.getStaticRep @BackupStatistics

processedDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedDirCount = _backup_stat_processedDirCount . coerce <$> ES.getStaticRep @BackupStatistics

newDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newDirCount = _backup_stat_newDirCount . coerce <$> ES.getStaticRep @BackupStatistics

totalDirCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
totalDirCount = _backup_stat_totalDirCount . coerce <$> ES.getStaticRep @BackupStatistics

processedChunkCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
processedChunkCount = _backup_stat_processedChunkCount . coerce <$> ES.getStaticRep @BackupStatistics

newChunkCount :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
newChunkCount = _backup_stat_newChunkCount . coerce <$> ES.getStaticRep @BackupStatistics

uploadedBytes :: BackupStatistics E.:> es => E.Eff es (TVar Word64)
uploadedBytes = _backup_stat_uploadedBytes . coerce <$> ES.getStaticRep @BackupStatistics
