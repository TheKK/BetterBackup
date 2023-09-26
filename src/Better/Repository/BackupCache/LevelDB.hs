{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Repository.BackupCache.LevelDB
  ( TheLevelDBBackupCache (..),
  )
where

import Foreign.C.Types (CTime(CTime))
import qualified Capability.Reader as C
import Control.Monad.IO.Class (MonadIO)
import Crypto.Hash (Digest, HashAlgorithm (hashDigestSize), SHA256 (SHA256), digestFromByteString)
import qualified Data.Binary.Get as Bin
import qualified Data.Binary.Put as Bin
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.Coerce (coerce)
import Data.Foldable (for_)
import qualified Database.LevelDB.Base as LV
import qualified System.Posix as P
import Control.Monad (replicateM)

import Better.Repository.BackupCache.Class (MonadBackupCache (..))

newtype TheLevelDBBackupCache m a = TheLevelDBBackupCache (m a)

instance (C.HasReader "prev_db" LV.DB m, C.HasReader "cur_db" LV.DB m, MonadIO m) => MonadBackupCache (TheLevelDBBackupCache m) where
  {-# INLINE saveCurrentFileHash #-}
  saveCurrentFileHash st digest = TheLevelDBBackupCache $ do
    db <- C.ask @"cur_db"
    save_to db st digest

  {-# INLINE tryReadingCacheHash #-}
  tryReadingCacheHash st = TheLevelDBBackupCache $ do
    db <- C.ask @"prev_db"
    read_from db st

save_to :: (MonadIO m) => LV.DB -> P.FileStatus -> Digest SHA256 -> m ()
save_to db st digest = do
  LV.put db LV.defaultWriteOptions (key_of_st st) $
    BC.toStrict $
      Bin.runPut $ do
        for_ (BA.unpack digest) Bin.putWord8

read_from :: (MonadIO m) => LV.DB -> P.FileStatus -> m (Maybe (Digest SHA256))
read_from db st = do
  LV.get db LV.defaultReadOptions (key_of_st st)
    >>= (pure $) . \case
      Just raw_v' -> do
        flip Bin.runGet (BC.fromStrict raw_v') $ do
          (digestFromByteString . BS.pack) <$> replicateM (hashDigestSize SHA256) Bin.getWord8
      Nothing -> Nothing

{-# INLINE key_of_st #-}
key_of_st :: P.FileStatus -> BS.ByteString
key_of_st st = BC.toStrict $ Bin.runPut $ do
  Bin.putWord64le $ coerce $ P.fileID st
  Bin.putWord64le $ coerce $ P.deviceID st
  Bin.putInt64le $ coerce $ P.modificationTime st
  Bin.putInt64le $ coerce $ P.fileSize st
