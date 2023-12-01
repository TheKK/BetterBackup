{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Better.Hash (
  -- * Digest
  VersionDigest (..),
  TreeDigest (..),
  FileDigest (..),
  ChunkDigest (..),
  Digest,
  digestSize,
  digestUnpack,
  digestToBase16ByteString,
  digestToBase16ShortByteString,
  digestFromByteString,

  -- * Pure versions
  hashByteStringFold,
  hashArrayFold,

  -- * IO versions
  hashByteStringFoldIO,
  hashArrayFoldIO,

  -- * Lowlevel Digest
  BLAKE3.Hasher,
  BLAKE3.init,
  finalize,
  hashByteArrayAccess,
  hashByteArrayAccess',
) where

import Data.Word (Word8)

import Data.Base16.Types qualified as B16

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Short qualified as BSS
import Data.ByteString.Short.Base16 qualified as BSS16
import Data.ByteString.Short.Internal qualified as BSS

import Data.Hashable (Hashable)

import Data.Binary qualified as Bin
import Data.Binary.Get qualified as Bin

import Streamly.Data.Array qualified as Array
import Streamly.Data.Fold qualified as F

import Data.ByteArray qualified as BA
import Data.ByteArray.Sized qualified as BAS

import BLAKE3 qualified
import BLAKE3.IO qualified as BIO

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA))

import Control.Parallel.Strategies (NFData)

import Control.Monad.ST (ST, stToIO)
import Control.Monad.ST.Unsafe (unsafeIOToST)

import Foreign.Marshal qualified as Alloc

newtype VersionDigest = UnsafeMkVersionDigest Digest
  deriving newtype (Ord, Eq, NFData, Hashable, Show, Bin.Binary)

newtype TreeDigest = UnsafeMkTreeDigest Digest
  deriving newtype (Ord, Eq, NFData, Hashable, Show, Bin.Binary)

newtype FileDigest = UnsafeMkFileDigest Digest
  deriving newtype (Ord, Eq, NFData, Hashable, Show, Bin.Binary)

newtype ChunkDigest = UnsafeMkChunkDigest Digest
  deriving newtype (Ord, Eq, NFData, Hashable, Show, Bin.Binary)

newtype Digest = Digest BSS.ShortByteString
  deriving (Ord, Eq, NFData, Hashable)

instance Show Digest where
  {-# INLINE show #-}
  show = BSC.unpack . digestToBase16ByteString

instance Bin.Binary Digest where
  {-# INLINE put #-}
  put d = mapM_ Bin.put $ digestUnpack d
  {-# INLINE get #-}
  get = Digest . BSS.toShort <$> Bin.getByteString digestSize

digestSize :: Int
digestSize = 32

digestUnpack :: Digest -> [Word8]
digestUnpack (Digest di) = BSS.unpack di

digestToBase16ByteString :: Digest -> BS.ByteString
digestToBase16ByteString = BSS.fromShort . digestToBase16ShortByteString

digestToBase16ShortByteString :: Digest -> BSS.ShortByteString
digestToBase16ShortByteString (Digest di) = B16.extractBase16 $ BSS16.encodeBase16' di
{-# INLINE digestToBase16ShortByteString #-}

digestFromByteString :: BS.ByteString -> Maybe Digest
digestFromByteString bs
  | BS.length bs == digestSize = Just $! Digest $ BSS.toShort bs
  | otherwise = Nothing

hashByteStringFold :: F.Fold (ST s) BS.ByteString Digest
hashByteStringFold = hashByteArrayAccess $ BLAKE3.init Nothing
{-# INLINE hashByteStringFold #-}

hashByteStringFoldIO :: F.Fold IO BS.ByteString Digest
hashByteStringFoldIO = F.morphInner stToIO hashByteStringFold
{-# INLINE hashByteStringFoldIO #-}

hashArrayFold :: F.Fold (ST s) (Array.Array Word8) Digest
hashArrayFold = F.lmap ArrayBA $ hashByteArrayAccess $ BLAKE3.init Nothing
{-# INLINE hashArrayFold #-}

hashArrayFoldIO :: F.Fold IO (Array.Array Word8) Digest
hashArrayFoldIO = F.morphInner stToIO hashArrayFold
{-# INLINE hashArrayFoldIO #-}

-- It's safe to unsafeIOToST here since we only do memroy operations in IO, and returns ShortByteString
-- which doesn't expose state inside ST.
{-# INLINE finalize #-}
finalize :: BIO.Hasher -> ST s Digest
finalize hasher = unsafeIOToST $ do
  BA.withByteArray hasher $ \hasher_ptr -> Alloc.allocaBytes digestSize $ \buf_ptr -> do
    BIO.c_finalize hasher_ptr buf_ptr $! fromIntegral digestSize
    Digest <$> BSS.createFromPtr buf_ptr digestSize

-- | ST version of blake3 hash
-- The benefit of using ST monad is low overhead of memory allocation, comparing to pure version.
{-# INLINE hashByteArrayAccess #-}
hashByteArrayAccess :: (BA.ByteArrayAccess ba) => BIO.Hasher -> F.Fold (ST s) ba Digest
hashByteArrayAccess = F.rmapM finalize . hashByteArrayAccess'

-- | hashByteArrayAccess but returns Hasher instead of Digest.
{-# INLINE hashByteArrayAccess' #-}
hashByteArrayAccess' :: (BA.ByteArrayAccess ba) => BIO.Hasher -> F.Fold (ST s) ba BIO.Hasher
hashByteArrayAccess' hasher0 = F.foldlM' step clone_hash0
  where
    -- For referential transparency, we shouldn't modify the input hasher.
    clone_hash0 = unsafeIOToST $ BAS.copy hasher0 $ \ptr -> BIO.update @BS.ByteString ptr []

    -- It's safe to unsafeIOToST here since we only do memroy operations in IO.
    {-# INLINE [0] step #-}
    step (!hasher :: BIO.Hasher) !bs = unsafeIOToST $ do
      BA.withByteArray hasher $ \hasher_ptr -> BA.withByteArray bs $ \bs_ptr ->
        BIO.c_update hasher_ptr bs_ptr $! fromIntegral (BA.length bs)
      pure $! hasher
