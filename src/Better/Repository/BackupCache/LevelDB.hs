{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BangPatterns #-}

module Better.Repository.BackupCache.LevelDB
  ( TheLevelDBBackupCache (..),
  )
where

import Foreign.C.Types (CTime(CTime))
import qualified Capability.Reader as C
import qualified Data.Binary.Get as Bin
import qualified Data.Binary.Put as Bin
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.Coerce (coerce)
import Data.Foldable (for_)
import qualified Database.LevelDB.Base as LV
import qualified System.Posix as P
import Control.Monad (replicateM)

import qualified Control.Monad.IO.Unlift as Un

import Better.Repository.BackupCache.Class (MonadBackupCache (..))
import Better.Hash (Digest, digestSize, digestFromByteString, digestUnpack)

newtype TheLevelDBBackupCache m a = TheLevelDBBackupCache (m a)

instance (C.HasReader "prev_db" LV.DB m, C.HasReader "cur_db" LV.DB m, Un.MonadUnliftIO m) => MonadBackupCache (TheLevelDBBackupCache m) where
  {-# INLINE saveCurrentFileHash #-}
  saveCurrentFileHash st digest = TheLevelDBBackupCache $ Un.withRunInIO $ \un -> do
    db <- un $ C.ask @"cur_db"
    save_to db st digest

  {-# INLINE tryReadingCacheHash #-}
  tryReadingCacheHash st = TheLevelDBBackupCache $ Un.withRunInIO $ \un -> do
    db <- un $ C.ask @"prev_db"
    read_from db st

save_to :: LV.DB -> P.FileStatus -> Digest -> IO ()
save_to db st digest = do
  let !k = key_of_st st
  LV.put db LV.defaultWriteOptions k $
    BC.toStrict $
      Bin.runPut $ do
        for_ (digestUnpack digest) Bin.putWord8

read_from :: LV.DB -> P.FileStatus -> IO (Maybe Digest)
read_from db st = do
  let !k = key_of_st st
  LV.get db LV.defaultReadOptions k
    >>= (pure $!) . \case
      Just raw_v' -> do
        flip Bin.runGet (BC.fromStrict raw_v') $ do
          (digestFromByteString . BS.pack) <$> replicateM digestSize Bin.getWord8
      Nothing -> Nothing

key_of_st :: P.FileStatus -> BS.ByteString
key_of_st st = BC.toStrict $ Bin.runPut $ do
  Bin.putWord64le $ coerce $ P.fileID st
  Bin.putWord64le $ coerce $ P.deviceID st
  Bin.putInt64le $ coerce $ P.modificationTime st
  Bin.putInt64le $ coerce $ P.fileSize st
