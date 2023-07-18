{-# LANGUAGE Strict #-}

module Better.Statistics.Backup
  ( Base.MonadBackupStat(..)
  -- | General operations
  , readStatistics
  , modifyStatistic'
  ) where

import qualified UnliftIO as Un

import qualified Better.Statistics.Backup.Class as Base

-- TODO Maybe could move to Better.Statistics
readStatistics :: (Base.MonadBackupStat m, Un.MonadUnliftIO m) => m (Un.TVar a) -> m a
readStatistics mtvar = mtvar >>= Un.readTVarIO

-- TODO Maybe could move to Better.Statistics
modifyStatistic' :: (Base.MonadBackupStat m, Un.MonadUnliftIO m) => m (Un.TVar a) -> (a -> a) -> m ()
modifyStatistic' mtvar f = do
  tvar <- mtvar
  Un.atomically $ Un.modifyTVar' tvar f
