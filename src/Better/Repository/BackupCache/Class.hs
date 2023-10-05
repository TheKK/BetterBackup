{-# LANGUAGE Strict #-}

module Better.Repository.BackupCache.Class
  ( MonadBackupCache(..)
  ) where

import System.Posix.Files (FileStatus)

import Better.Hash (Digest)

-- | Save & read file cache to reduce the cost of reading disk.
--
-- Here we'll need two storages, one for reading backup cache from previous version,
-- one for writing current constructed version.
class MonadBackupCache m where
  saveCurrentFileHash :: FileStatus -> Digest -> m ()
  tryReadingCacheHash :: FileStatus -> m (Maybe Digest)
