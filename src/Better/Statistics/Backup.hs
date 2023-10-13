{-# LANGUAGE Strict #-}

module Better.Statistics.Backup (
  Base.MonadBackupStat (..),
  -- | General operations
  readStatistics,
  modifyStatistic',
) where

import qualified Control.Monad.IO.Unlift as Un

import Control.Monad.IO.Class (liftIO)

import Control.Concurrent.STM (TVar, atomically, modifyTVar', readTVarIO)

import qualified Better.Statistics.Backup.Class as Base

-- TODO Maybe could move to Better.Statistics
{-# INLINE readStatistics #-}
readStatistics :: (Un.MonadUnliftIO m) => m (TVar a) -> m a
readStatistics mtvar = Un.withRunInIO $ \un -> do
  tvar <- un mtvar
  liftIO $ readTVarIO tvar

-- TODO Maybe could move to Better.Statistics
{-# INLINE modifyStatistic' #-}
modifyStatistic' :: (Un.MonadUnliftIO m) => m (TVar a) -> (a -> a) -> m ()
modifyStatistic' mtvar f = Un.withRunInIO $ \un -> do
  tvar <- un mtvar
  atomically $ modifyTVar' tvar f
