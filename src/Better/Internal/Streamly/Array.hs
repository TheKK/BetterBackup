{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Better.Internal.Streamly.Array (
  ArrayBA (..),
  MutArrayBA (..),
  fastWriteChunkFold,
  fastArrayAsPtrUnsafe,
  fastMutArrayAsPtrUnsafe,
  readChunks,
  chunkReaderFromToWith,
  chunkReaderFromToWithFdPread,
)
where

import Control.Exception (assert, mask_, onException)
import Control.Monad ()
import Data.ByteArray (ByteArrayAccess (..))
import Data.ByteArray qualified as BA
import Data.Function ()
import Data.Word (Word8)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (..), CSize (..))
import GHC.Exts (byteArrayContents#)
import GHC.Ptr (Ptr (..), plusPtr)
import Streamly.Data.Array qualified as Array
import Streamly.Data.Fold qualified as F
import Streamly.FileSystem.File qualified as File
import Streamly.Internal.Data.Array.Mut (touch)
import Streamly.Internal.Data.Array.Mut qualified as MArray
import Streamly.Internal.Data.Array.Mut.Type qualified as MA
import Streamly.Internal.Data.Array.Type qualified as Array
import Streamly.Internal.Data.Fold qualified as F
import Streamly.Internal.Data.Stream qualified as S
import Streamly.Internal.Data.Unboxed (getMutableByteArray#)
import Streamly.Internal.Data.Unfold.Type qualified as Un
import Streamly.Internal.System.IO (defaultChunkSize)
import System.IO (Handle, IOMode (..), SeekMode (AbsoluteSeek), hClose, hGetBufSome, hPutBuf, hSeek, openBinaryFile)
import System.Posix qualified as P
import System.Posix.Types (COff (..), CSsize (..))
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
{-# INLINE fastArrayAsPtrUnsafe #-}
fastArrayAsPtrUnsafe :: Array.Array Word8 -> (Ptr a -> IO b) -> IO b
fastArrayAsPtrUnsafe arr f = do
  let marr = Array.unsafeThaw arr
      !contents = MA.arrContents marr
      !ptr =
        Ptr
          ( byteArrayContents# (unsafeCoerce# (getMutableByteArray# contents))
          )
  -- XXX Check if the array is pinned, if not, copy it to a pinned array
  -- XXX We should probably pass to the IO action the byte length of the array
  -- as well so that bounds can be checked.
  r <- f $! (ptr `plusPtr` MA.arrStart marr)
  touch contents
  return r

{-# INLINE fastMutArrayAsPtrUnsafe #-}
fastMutArrayAsPtrUnsafe :: MA.MutArray Word8 -> (Ptr a -> IO b) -> IO b
fastMutArrayAsPtrUnsafe marr f = do
  let !contents = MA.arrContents marr
      !ptr =
        Ptr
          ( byteArrayContents# (unsafeCoerce# (getMutableByteArray# contents))
          )
  -- XXX Check if the array is pinned, if not, copy it to a pinned array
  -- XXX We should probably pass to the IO action the byte length of the array
  -- as well so that bounds can be checked.
  r <- f $! (ptr `plusPtr` MA.arrStart marr)
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
    step hd arr = (`onException` hClose hd) $ do
      fastArrayAsPtrUnsafe arr $ \ptr -> hPutBuf hd ptr (Array.byteLength arr)
      pure $! F.Partial hd

    {-# INLINE [0] extract #-}
    extract hd = mask_ $ do
      hClose hd

-- | Read from Fd with pread syscall, which won't alter position of Fd.
{-# INLINE chunkReaderFromToWithFdPread #-}
chunkReaderFromToWithFdPread :: Un.Unfold IO (Int, Int, Int, P.Fd) (Array.Array Word8)
chunkReaderFromToWithFdPread = Un.Unfold step inject
  where
    inject (!from :: Int, !to :: Int, !bufSize :: Int, !h) = do
      return (fromIntegral from, fromIntegral to, fromIntegral bufSize, h)

    {-# INLINE [0] step #-}
    step (!from, !to, !bufSize, !h) =
      let !remaining = fromIntegral $ to - from + 1
      in  if remaining <= 0
            then return S.Stop
            else do
              arr <- get_chunk_pread from (min bufSize remaining) h
              case Array.byteLength arr of
                0 -> pure S.Stop
                len ->
                  let !from' = from + fromIntegral len
                  in  pure $ S.Yield arr (from', to, bufSize, h)

-- Copy from streamly-core.
{-# INLINE chunkReaderFromToWith #-}
chunkReaderFromToWith :: Un.Unfold IO (Int, Int, Int, Handle) (Array.Array Word8)
chunkReaderFromToWith = Un.Unfold step inject
  where
    inject (!from :: Int, !to :: Int, !bufSize :: Int, !h) = do
      hSeek h AbsoluteSeek $! fromIntegral from
      -- XXX Use a strict Tuple?
      let !remaining = to - from + 1
      return (remaining, bufSize, h)

    {-# INLINE [0] step #-}
    step (!remaining, !bufSize, !h) =
      if remaining <= 0
        then return S.Stop
        else do
          arr <- getChunk (min bufSize remaining) h
          return $!
            case Array.byteLength arr of
              0 -> S.Stop
              len ->
                assert (len <= remaining) $
                  let !remaining' = remaining - len
                  in  S.Yield arr (remaining', bufSize, h)

{-# INLINE readChunks #-}
readChunks :: FilePath -> S.Stream IO (Array.Array Word8)
readChunks file = File.withFile file ReadMode (readChunksWith defaultChunkSize)

{-# INLINE readChunksWith #-}
readChunksWith :: Int -> Handle -> S.Stream IO (Array.Array Word8)
readChunksWith size h = S.Stream step ()
  where
    {-# INLINE [0] step #-}
    step _ _ = do
      arr <- getChunk size h
      case Array.byteLength arr of
        0 -> pure S.Stop
        _ -> pure $ S.Yield arr ()

-- Copy from streamly-core.
getChunk :: Int -> Handle -> IO (Array.Array Word8)
getChunk !size h = do
  arr <- MArray.newPinnedBytes size
  -- ptr <- mallocPlainForeignPtrAlignedBytes size (alignment (undefined :: Word8))
  fastMutArrayAsPtrUnsafe arr $ \p -> do
    !n <- hGetBufSome h p size
    -- XXX shrink only if the diff is significant
    return $!
      Array.unsafeFreezeWithShrink $
        arr{MArray.arrEnd = n, MArray.arrBound = size}

get_chunk_pread :: COff -> CSize -> P.Fd -> IO (Array.Array Word8)
get_chunk_pread !off !size h = do
  arr <- MArray.newPinnedBytes $! fromIntegral size
  fastMutArrayAsPtrUnsafe arr $ \p -> do
    -- I've gave unsafe version a try but found no difference on my machine.
    -- it's still worth a try on difference machine & load.
    !n <- c_pread_safe h p size off
    return $!
      Array.unsafeFreezeWithShrink $
        arr{MArray.arrEnd = fromIntegral n, MArray.arrBound = fromIntegral size}

foreign import ccall safe "pread" c_pread_safe :: P.Fd -> CString -> CSize -> System.Posix.Types.COff -> IO System.Posix.Types.CSsize
