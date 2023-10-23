{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Repository.BackupCache.LevelDB (
  saveCurrentFileHash,
  tryReadingCacheHash,
  runBackupCacheLevelDB,
)
where

import Control.Monad (replicateM)
import qualified Data.Binary.Get as Bin
import qualified Data.Binary.Put as Bin
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.Coerce (coerce)
import Data.Foldable (for_)
import qualified Database.LevelDB.Base as LV
import Foreign.C.Types (CTime (CTime))
import qualified System.Posix as P

import Effectful ((:>))
import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES

import Better.Hash (Digest, digestFromByteString, digestSize, digestUnpack)
import Better.Repository.BackupCache.Class (BackupCache)

data instance ES.StaticRep BackupCache = BackupCacheStat {-# UNPACK #-} !LV.DB {-# UNPACK #-} !LV.DB

saveCurrentFileHash :: BackupCache :> es => P.FileStatus -> Digest -> E.Eff es ()
saveCurrentFileHash st digest = do
  BackupCacheStat _prev cur <- ES.getStaticRep
  ES.unsafeEff_ $ save_to cur st digest

tryReadingCacheHash :: BackupCache :> es => P.FileStatus -> E.Eff es (Maybe Digest)
tryReadingCacheHash st = do
  BackupCacheStat prev _cur <- ES.getStaticRep
  ES.unsafeEff_ $ read_from prev st

runBackupCacheLevelDB :: E.IOE :> es => LV.DB -> LV.DB -> E.Eff (BackupCache : es) a -> E.Eff es a
runBackupCacheLevelDB prev cur = ES.evalStaticRep $ BackupCacheStat prev cur

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
