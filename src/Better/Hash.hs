{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Better.Hash (
  -- * Digest
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
) where

import Data.Word (Word8)

import qualified Data.Base16.Types as B16

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString.Short.Base16 as BSS16
import qualified Data.ByteString.Short.Internal as BSS

import Data.Hashable (Hashable)

import qualified Data.Binary as Bin
import qualified Data.Binary.Get as Bin

import qualified Streamly.Data.Array as Array
import qualified Streamly.Data.Fold as F

import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Sized as BAS

import qualified BLAKE3
import qualified BLAKE3.IO as BIO

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA))

import Control.Parallel.Strategies (NFData)

import Control.Monad.ST (ST, stToIO)
import Control.Monad.ST.Unsafe (unsafeIOToST)

import qualified Foreign.Marshal as Alloc

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
hashByteStringFold = fmap Digest hash_blake3_st
{-# INLINE hashByteStringFold #-}

hashByteStringFoldIO :: F.Fold IO BS.ByteString Digest
hashByteStringFoldIO = F.morphInner stToIO hashByteStringFold
{-# INLINE hashByteStringFoldIO #-}

hashArrayFold :: F.Fold (ST s) (Array.Array Word8) Digest
hashArrayFold = F.lmap ArrayBA $ fmap Digest hash_blake3_st
{-# INLINE hashArrayFold #-}

hashArrayFoldIO :: F.Fold IO (Array.Array Word8) Digest
hashArrayFoldIO = F.morphInner stToIO hashArrayFold
{-# INLINE hashArrayFoldIO #-}

-- | ST version of blake3 hash
--
-- The benefit of converting IO to ST is low overhead of memory allocation, comparing to pure version.
{-# INLINE hash_blake3_st #-}
hash_blake3_st :: (BA.ByteArrayAccess ba) => F.Fold (ST s) ba BSS.ShortByteString
hash_blake3_st = F.rmapM extract $ F.foldlM' step s0
  where
    -- It's safe to unsafeIOToST here since s0 is only visible by 'step' and 'extract'.
    s0 = unsafeIOToST $ BAS.alloc @_ @BLAKE3.Hasher $ flip BIO.init Nothing

    -- It's safe to unsafeIOToST here since we only do memroy operations in IO.
    {-# INLINE [0] step #-}
    step (!hasher :: BLAKE3.Hasher) !bs = unsafeIOToST $ do
      BA.withByteArray hasher $ \hasher_ptr -> BA.withByteArray bs $ \bs_ptr ->
        BIO.c_update hasher_ptr bs_ptr $! fromIntegral (BA.length bs)
      pure $! hasher

    -- It's safe to unsafeIOToST here since we only do memroy operations in IO, and returns ShortByteString
    -- which doesn't expose state inside ST.
    {-# INLINE [0] extract #-}
    extract !hasher = unsafeIOToST $ do
      BA.withByteArray hasher $ \hasher_ptr -> Alloc.allocaBytes digestSize $ \buf_ptr -> do
        BIO.c_finalize hasher_ptr buf_ptr $! fromIntegral digestSize
        BSS.createFromPtr buf_ptr digestSize
