{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Better.Hash (
  -- * Digest
  Digest,
  digestSize,
  digestUnpack,
  digestToBase16ByteString,
  digestFromByteString,

  -- * Pure versions
  hashByteStringFold,
  hashArrayFold,

  -- * IO versions
  hashByteStringFoldIO,
  hashArrayFoldIO,
) where

import Data.Word (Word8)

import qualified Data.ByteString as BS

import qualified Streamly.Data.Array as Array
import qualified Streamly.Data.Fold as F

import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Sized as BAS

import qualified BLAKE3
import qualified BLAKE3.IO as BIO

import Basement.Sized.Block (BlockN, toBlock)

import qualified Crypto.Hash as C

import Control.Monad.IO.Class (liftIO)

import UnliftIO (MonadUnliftIO)

import Unsafe.Coerce (unsafeCoerce)

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA))
import qualified Data.ByteArray.Encoding as BA

newtype Digest = Digest (C.Digest C.SHA256)
  deriving newtype Show
  deriving (Ord, Eq)

digestSize :: Int
digestSize = C.hashDigestSize C.SHA256

digestUnpack :: Digest -> [Word8]
digestUnpack (Digest di) = BA.unpack di

digestToBase16ByteString :: Digest -> BS.ByteString
digestToBase16ByteString (Digest di) = BA.convertToBase BA.Base16 di

digestFromByteString :: BS.ByteString -> Maybe Digest
digestFromByteString = fmap Digest . C.digestFromByteString

hashByteStringFold :: Monad m => F.Fold m BS.ByteString Digest
hashByteStringFold = unsafeCoerce . toBlock <$> hash_blake3
{-# INLINE hashByteStringFold #-}

hashByteStringFoldIO :: MonadUnliftIO m => F.Fold m BS.ByteString Digest
hashByteStringFoldIO = unsafeCoerce . toBlock <$> hash_blake3_io
{-# INLINE hashByteStringFoldIO #-}

hashArrayFold :: (Monad m) => F.Fold m (Array.Array Word8) Digest
hashArrayFold = F.lmap ArrayBA $ unsafeCoerce . toBlock <$> hash_blake3
{-# INLINE hashArrayFold #-}

hashArrayFoldIO :: (MonadUnliftIO m) => F.Fold m (Array.Array Word8) Digest
hashArrayFoldIO = F.lmap ArrayBA $ unsafeCoerce . toBlock <$> hash_blake3_io
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
hash_blake3_io :: (MonadUnliftIO m, BA.ByteArrayAccess ba) => F.Fold m ba (BlockN 32 Word8)
hash_blake3_io = F.rmapM extract $ F.foldlM' step s0
  where
    s0 = liftIO $ BAS.alloc @_ @BLAKE3.Hasher $ flip BIO.init Nothing

    {-# INLINE [0] step #-}
    step !hasher !bs = liftIO $ BA.withByteArray hasher (\ptr -> BIO.update ptr [bs]) >> pure hasher

    {-# INLINE [0] extract #-}
    extract !hasher = liftIO $ BA.withByteArray hasher BIO.finalize

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
hash_blake3 :: (Monad m, BA.ByteArrayAccess ba) => F.Fold m ba (BlockN 32 Word8)
hash_blake3 = extract <$> F.foldl' step s0
  where
    s0 = BLAKE3.init Nothing

    {-# INLINE [0] step #-}
    step !hasher !bs = BLAKE3.update hasher [bs]

    {-# INLINE extract #-}
    extract = BLAKE3.finalize
