{-# LANGUAGE DataKinds #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeFamilies #-}

module Better.Repository.BackupCache.Class (
  -- * Effect
  BackupCache,
) where

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES

-- | Save & read file cache to reduce the cost of reading disk.
--
-- Here we'll need two storages, one for reading backup cache from previous version,
-- one for writing current constructed version.
data BackupCache :: E.Effect

type instance E.DispatchOf BackupCache = 'E.Static 'ES.WithSideEffects
