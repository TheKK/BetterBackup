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

import UnliftIO (
  IOMode (WriteMode),
  MonadIO (liftIO),
  hClose,
  mask_,
  onException,
 )
import qualified UnliftIO.Directory as Un

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Data.Maybe (fromMaybe)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS16
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BSC

import qualified Data.ByteString.Lazy.Base16 as BL16

import qualified Data.ByteString.Short as BShort

import Text.Read (readMaybe)

import Control.Monad ()
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow, handleIf, throwM)

import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.Unicode.Stream as US
import qualified Streamly.Unicode.Stream as US

import System.IO.Error (isDoesNotExistError)
import System.Posix.Types (FileOffset)

import qualified System.Posix.Directory as P
import qualified System.Posix.Files as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash ()

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Internal.Data.Fold as F

import qualified Capability.Reader as C

import Better.Repository.Class ()
import qualified Better.Streamly.FileSystem.Dir as Dir

import Better.Repository.Types (Version (..))

import Better.Internal.Streamly.Crypto.AES (decryptCtr, that_aes)
import Data.ByteArray (ByteArrayAccess (..))
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA

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
