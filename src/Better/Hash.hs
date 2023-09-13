{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}

module Better.Hash
  ( hashArray
  , hashArrayFold
  , hashByteStringFold
  ) where


import Data.Word
import Data.ByteArray (ByteArrayAccess(..))

import qualified Data.ByteString as BS

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe)
import qualified Streamly.Data.Fold as F

import Crypto.Hash
import qualified BLAKE3
import Basement.BlockN (BlockN, toBlock)
import Unsafe.Coerce (unsafeCoerce)

hashArray :: Array.Array Word8 -> Digest SHA256
hashArray = hashWith SHA256 . ArrayBA
{-# INLINE hashArray #-}

-- hashByteStringFold :: (Monad m) => F.Fold m BS.ByteString (Digest SHA256)
-- hashByteStringFold = fmap hashFinalize $ (F.foldl' hashUpdate hashInit)
{-# INLINE hashByteStringFold #-}
--
-- hashArrayFold :: (Monad m) => F.Fold m (Array.Array Word8) (Digest SHA256)
-- hashArrayFold = F.lmap ArrayBA $ fmap hashFinalize $ (F.foldl' hashUpdate hashInit)
{-# INLINE hashArrayFold #-}

hashByteStringFold :: (Monad m) => F.Fold m BS.ByteString (Digest SHA256)
hashByteStringFold = fmap (unsafeCoerce . toBlock . BLAKE3.finalize @32 @(BlockN 32 Word8)) $ (F.lmap (:[]) $ F.foldl' BLAKE3.update (BLAKE3.init Nothing))

hashArrayFold :: (Monad m) => F.Fold m (Array.Array Word8) (Digest SHA256)
hashArrayFold = F.lmap ArrayBA $ fmap (unsafeCoerce . toBlock . BLAKE3.finalize @32 @(BlockN 32 Word8)) $ (F.lmap (:[]) $ F.foldl' BLAKE3.update (BLAKE3.init Nothing))

newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  {-# INLINE length #-}

  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp
  {-# INLINE withByteArray #-}
