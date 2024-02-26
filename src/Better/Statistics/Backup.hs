module Better.Statistics.Backup (
  -- | General operations
  readStatistics,
  modifyStatistic',
) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', readTVarIO)
import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E

-- TODO Maybe could move to Better.Statistics
{-# INLINE readStatistics #-}
readStatistics :: E.Eff es (TVar a) -> E.Eff es a
readStatistics mtvar = do
  tvar <- mtvar
  E.unsafeEff_ $ readTVarIO tvar

-- TODO Maybe could move to Better.Statistics
{-# INLINE modifyStatistic' #-}
modifyStatistic' :: E.Eff es (TVar a) -> (a -> a) -> E.Eff es ()
modifyStatistic' mtvar f = do
  tvar <- mtvar
  E.unsafeEff_ $ atomically $ modifyTVar' tvar f
