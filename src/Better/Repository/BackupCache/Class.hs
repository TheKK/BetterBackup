{-# LANGUAGE Strict #-}

module Better.Repository.BackupCache.Class
  ( MonadBackupCache(..)
  ) where

import System.Posix.Files (FileStatus)

import Crypto.Hash (Digest, SHA256)

-- | Save & read file cache to reduce the cost of reading disk.
--
-- Here we'll need two storages, one for reading backup cache from previous version,
-- one for writing current constructed version.
class MonadBackupCache m where
  saveCurrentFileHash :: FileStatus -> Digest SHA256 -> m ()
  tryReadingCacheHash :: FileStatus -> m (Maybe (Digest SHA256))
