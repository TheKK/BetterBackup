{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

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

hashArray :: Array.Array Word8 -> Digest SHA256
hashArray = hashWith SHA256 . ArrayBA
{-# INLINE hashArray #-}

hashByteStringFold :: (Monad m) => F.Fold m BS.ByteString (Digest SHA256)
hashByteStringFold = fmap hashFinalize $ (F.foldl' hashUpdate hashInit)
{-# INLINE hashByteStringFold #-}

hashArrayFold :: (Monad m) => F.Fold m (Array.Array Word8) (Digest SHA256)
hashArrayFold = F.lmap ArrayBA $ fmap hashFinalize $ (F.foldl' hashUpdate hashInit)
{-# INLINE hashArrayFold #-}

newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  {-# INLINE length #-}

  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp
  {-# INLINE withByteArray #-}
