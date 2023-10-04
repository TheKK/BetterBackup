{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MagicHash #-}

module Better.Internal.Streamly.Array (
  ArrayBA (..),
  MutArrayBA (..),
  fastWriteChunkFold,
  fastArrayAsPtrUnsafe,
  fastMutArrayAsPtrUnsafe,
)
where

import Data.Function ()
import Data.Word (Word8)

import UnliftIO (IOMode (WriteMode), hClose, mask_, onException)

import Control.Monad ()

import qualified Streamly.Data.Fold as F
import qualified Streamly.Internal.Data.Fold as F

import Better.Repository.Class ()

import Data.ByteArray (ByteArrayAccess (..))
import qualified Data.ByteArray as BA

import qualified Streamly.Internal.Data.Array.Mut.Type as MA

import GHC.PrimopWrappers (byteArrayContents#)
import GHC.Ptr (Ptr (..), plusPtr)

import qualified Streamly.Data.Array as Array
import Streamly.Internal.Data.Array.Mut (touch)
import qualified Streamly.Internal.Data.Array.Type as Array
import Streamly.Internal.Data.Unboxed (getMutableByteArray#)

import System.IO (hPutBuf, openBinaryFile)

import Unsafe.Coerce (unsafeCoerce#)

-- * Functions that I need but not exists on upstream.

newtype ArrayBA = ArrayBA {un_array_ba :: Array.Array Word8}
  deriving (Eq, Ord, Monoid, Semigroup)

instance BA.ByteArray ArrayBA where
  allocRet n f = do
    arr <- MA.newPinned n
    ret <- fastMutArrayAsPtrUnsafe arr f
    let !ba = ArrayBA $! Array.unsafeFreeze $! arr{MA.arrEnd = MA.arrBound arr}
    pure (ret, ba)

instance ByteArrayAccess ArrayBA where
  {-# INLINE length #-}
  length (ArrayBA arr) = Array.byteLength arr
  {-# INLINE withByteArray #-}
  withByteArray (ArrayBA arr) = fastArrayAsPtrUnsafe arr

newtype MutArrayBA = MutArrayBA {un_mut_array_ba :: MA.MutArray Word8}

instance ByteArrayAccess MutArrayBA where
  {-# INLINE length #-}
  length (MutArrayBA marr) = MA.byteLength marr
  {-# INLINE withByteArray #-}
  withByteArray (MutArrayBA marr) = fastMutArrayAsPtrUnsafe marr

-- Copy from streamly-core
fastArrayAsPtrUnsafe :: Array.Array Word8 -> (Ptr a -> IO b) -> IO b
fastArrayAsPtrUnsafe arr f = do
  let marr = Array.unsafeThaw arr
      contents = MA.arrContents marr
      !ptr =
        Ptr
          ( byteArrayContents# (unsafeCoerce# (getMutableByteArray# contents))
          )
  -- XXX Check if the array is pinned, if not, copy it to a pinned array
  -- XXX We should probably pass to the IO action the byte length of the array
  -- as well so that bounds can be checked.
  r <- f (ptr `plusPtr` MA.arrStart marr)
  touch contents
  return r

fastMutArrayAsPtrUnsafe :: MA.MutArray Word8 -> (Ptr a -> IO b) -> IO b
fastMutArrayAsPtrUnsafe marr f = do
  let contents = MA.arrContents marr
      !ptr =
        Ptr
          ( byteArrayContents# (unsafeCoerce# (getMutableByteArray# contents))
          )
  -- XXX Check if the array is pinned, if not, copy it to a pinned array
  -- XXX We should probably pass to the IO action the byte length of the array
  -- as well so that bounds can be checked.
  r <- f (ptr `plusPtr` MA.arrStart marr)
  touch contents
  return r

{-# INLINE fastWriteChunkFold #-}
fastWriteChunkFold :: FilePath -> F.Fold IO (Array.Array Word8) ()
fastWriteChunkFold path = F.Fold step initial extract
  where
    {-# INLINE [0] initial #-}
    initial = mask_ $ do
      hd <- openBinaryFile path WriteMode
      pure $! F.Partial hd

    {-# INLINE [0] step #-}
    step hd arr = (`onException` (hClose hd)) $ do
      fastArrayAsPtrUnsafe arr $ \ptr -> hPutBuf hd ptr (Array.byteLength arr)
      pure $! F.Partial hd

    {-# INLINE [0] extract #-}
    extract hd = mask_ $ do
      hClose hd
