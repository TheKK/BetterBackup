{-# LANGUAGE Strict #-}

module Better.Statistics.Backup
  ( Base.MonadBackupStat(..)
  -- | General operations
  , readStatistics
  , modifyStatistic'
  ) where

import qualified UnliftIO as Un

import qualified Better.Statistics.Backup.Class as Base
import Control.Concurrent.STM (atomically, modifyTVar')

-- TODO Maybe could move to Better.Statistics
{-# INLINE readStatistics #-}
readStatistics :: (Un.MonadUnliftIO m) => m (Un.TVar a) -> m a
readStatistics mtvar = mtvar >>= Un.readTVarIO

-- TODO Maybe could move to Better.Statistics
{-# INLINE modifyStatistic' #-}
modifyStatistic' :: (Un.MonadUnliftIO m) => m (Un.TVar a) -> (a -> a) -> m ()
modifyStatistic' mtvar f = Un.withRunInIO $ \un -> do
  tvar <- un mtvar
  atomically $ modifyTVar' tvar f
