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

import Control.Monad (replicateM)

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

digestFromByteString :: BS.ByteString -> Maybe Digest
digestFromByteString bs
  | BS.length bs == digestSize = Just $! Digest $ BSS.toShort bs
  | otherwise = Nothing

hashByteStringFold :: Monad m => F.Fold m BS.ByteString Digest
hashByteStringFold = fmap Digest hash_blake3
{-# INLINE hashByteStringFold #-}

hashByteStringFoldIO :: F.Fold IO BS.ByteString Digest
hashByteStringFoldIO = fmap Digest hash_blake3_io
{-# INLINE hashByteStringFoldIO #-}

hashArrayFold :: Monad m => F.Fold m (Array.Array Word8) Digest
hashArrayFold = F.lmap ArrayBA $ fmap Digest hash_blake3
{-# INLINE hashArrayFold #-}

hashArrayFoldIO :: F.Fold IO (Array.Array Word8) Digest
hashArrayFoldIO = F.lmap ArrayBA $ fmap Digest hash_blake3_io
{-# INLINE hashArrayFoldIO #-}

-- | IO version of blake3 hash
--
-- When you're able to do run stuff in IO monad, prefer using this one.
--
-- The benefit of IO version is low overhead of memory allocation.
--
-- The drawback of IO version is that you need to run them in IO, which might not
-- always the case.
{-# INLINE hash_blake3_io #-}
hash_blake3_io :: (BA.ByteArrayAccess ba) => F.Fold IO ba BSS.ShortByteString
hash_blake3_io = F.rmapM extract $ F.foldlM' step s0
  where
    s0 = BAS.alloc @_ @BLAKE3.Hasher $ flip BIO.init Nothing

    {-# INLINE [0] step #-}
    step (!hasher :: BLAKE3.Hasher) !bs = do
      BA.withByteArray hasher $ \hasher_ptr -> BA.withByteArray bs $ \bs_ptr ->
        BIO.c_update hasher_ptr bs_ptr $! fromIntegral (BA.length bs)
      pure $! hasher

    {-# INLINE [0] extract #-}
    extract !hasher =
      BA.withByteArray hasher $ \hasher_ptr -> Alloc.allocaBytes digestSize $ \buf_ptr -> do
        BIO.c_finalize hasher_ptr buf_ptr $! fromIntegral digestSize
        BSS.createFromPtr buf_ptr digestSize

-- | Pure version of blake3 hash
--
-- The benefit of pure version which is obvious: purity and referential transparency.
--
-- The drawback of pure version is overhead of memory allocation. We have no way to resuse
-- same Hasher during the step process due to pure interface (F.Foldl'). Instead we have to
-- copy them again and again to preserve referential transparency even Hasher never escapes
-- from these function.
--
-- We've tried using unsafe famliy but had no luck. Seems like that compiler treated `s0` as
-- pure value and share it between multiple call site to cause terrible wrong result.
{-# INLINE hash_blake3 #-}
hash_blake3 :: (Monad m, BA.ByteArrayAccess ba) => F.Fold m ba BSS.ShortByteString
hash_blake3 = extract <$> F.foldl' step s0
  where
    s0 = BLAKE3.init Nothing

    {-# INLINE [0] step #-}
    step !hasher !bs = BLAKE3.update hasher [bs]

    {-# INLINE [0] extract #-}
    extract !hasher = BSS.toShort . BAS.unSizedByteArray @32 . BLAKE3.finalize $ hasher
