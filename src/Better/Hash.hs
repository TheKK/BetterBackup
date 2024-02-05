{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
  digestToByteString,

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

import GHC.Prim (indexWord64Array#)
import GHC.Word (Word64 (..))

import Data.Word (Word8)

import Data.Base16.Types qualified as B16

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Short qualified as BSS
import Data.ByteString.Short.Base16 qualified as BSS16
import Data.ByteString.Short.Internal qualified as BSS

import Data.Hashable (Hashable, hash, hashWithSalt)

import Data.Binary qualified as Bin
import Data.Binary.Get qualified as Bin

import Streamly.Data.Array qualified as Array
import Streamly.Data.Fold qualified as F

import Data.ByteArray qualified as BA

import BLAKE3 qualified
import BLAKE3.IO qualified as BIO

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA))

import Control.Parallel.Strategies (NFData)

import Control.Monad ((<$!>))
import Control.Monad.ST (ST, stToIO)
import Control.Monad.ST.Unsafe (unsafeIOToST)

import Foreign.ForeignPtr (
  ForeignPtr,
  mallocForeignPtr,
  withForeignPtr,
 )
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
  deriving (Ord, Eq, NFData)

instance Hashable Digest where
  hash (Digest (BSS.SBS ba)) = fromIntegral $! v1 + v2 + v3 + v4
    where
      !v1 = W64# (indexWord64Array# ba 0#)
      !v2 = W64# (indexWord64Array# ba 1#)
      !v3 = W64# (indexWord64Array# ba 2#)
      !v4 = W64# (indexWord64Array# ba 3#)

  hashWithSalt s (Digest (BSS.SBS ba)) =
    s `hashWithSalt` v1 `hashWithSalt` v2 `hashWithSalt` v3 `hashWithSalt` v4
    where
      !v1 = W64# (indexWord64Array# ba 0#)
      !v2 = W64# (indexWord64Array# ba 1#)
      !v3 = W64# (indexWord64Array# ba 2#)
      !v4 = W64# (indexWord64Array# ba 3#)

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

digestToByteString :: Digest -> BS.ByteString
digestToByteString (Digest d) = BSS.fromShort d

hashByteStringFold :: F.Fold (ST s) BS.ByteString Digest
hashByteStringFold = hashByteArrayAccess
{-# INLINE hashByteStringFold #-}

hashByteStringFoldIO :: F.Fold IO BS.ByteString Digest
hashByteStringFoldIO = F.morphInner stToIO hashByteStringFold
{-# INLINE hashByteStringFoldIO #-}

hashArrayFold :: F.Fold (ST s) (Array.Array Word8) Digest
hashArrayFold = F.lmap ArrayBA $ hashByteArrayAccess
{-# INLINE hashArrayFold #-}

hashArrayFoldIO :: F.Fold IO (Array.Array Word8) Digest
hashArrayFoldIO = F.morphInner stToIO hashArrayFold
{-# INLINE hashArrayFoldIO #-}

-- It's safe to unsafeIOToST here since we only do memroy operations in IO, and returns ShortByteString
-- which doesn't expose state inside ST.
{-# INLINE finalize #-}
finalize :: ForeignPtr BIO.Hasher -> ST s Digest
finalize hasher = unsafeIOToST $ do
  -- TODO Here we allocate twice (allocaBytes + createFromPtr). Can we create ShortByteString directly?
  withForeignPtr hasher $ \(!hasher_ptr) -> Alloc.allocaBytes digestSize $ \(!buf_ptr) -> do
    BIO.c_finalize hasher_ptr buf_ptr $! fromIntegral digestSize
    Digest <$!> BSS.createFromPtr buf_ptr digestSize

-- | ST version of blake3 hash
-- The benefit of using ST monad is low overhead of memory allocation, comparing to pure version.
{-# INLINE hashByteArrayAccess #-}
hashByteArrayAccess :: (BA.ByteArrayAccess ba) => F.Fold (ST s) ba Digest
hashByteArrayAccess = F.rmapM finalize hashByteArrayAccess'

-- | hashByteArrayAccess but returns Hasher instead of Digest.
{-# INLINE hashByteArrayAccess' #-}
hashByteArrayAccess' :: (BA.ByteArrayAccess ba) => F.Fold (ST s) ba (ForeignPtr BIO.Hasher)
hashByteArrayAccess' = F.foldlM' step mk_hasher
  where
    -- For referential transparency, we shouldn't modify the input hasher.
    mk_hasher = unsafeIOToST $ do
      -- `mallocForeignPtr` use Storeable which has no memset zero operation on hasher after finalized.
      -- This is what we want to reduce CPU time. We also don't require that kind of behaviour now.
      hasher <- mallocForeignPtr
      withForeignPtr hasher BIO.c_init
      pure $! hasher
    -- It's safe to unsafeIOToST here since we only do memroy operations in IO.
    {-# INLINE [0] step #-}
    step (!hasher :: ForeignPtr BIO.Hasher) !bs = unsafeIOToST $ do
      withForeignPtr hasher $ \(!hasher_ptr) -> BA.withByteArray bs $ \(!bs_ptr) ->
        BIO.c_update hasher_ptr bs_ptr $! fromIntegral $! BA.length bs
      pure $! hasher
