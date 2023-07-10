{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module Better.Streamly.FileSystem.Chunker
  ( Chunk(..)
  , GearHashConfig
  , defaultGearHashConfig
  , gearHash 
  ) where

import Control.Monad.Catch (MonadCatch)

import qualified Data.Bits as Bits
import Data.Function (fix)

import qualified Data.Vector.Unboxed as UV

import qualified System.Random as Rng
import qualified System.Random.SplitMix as Rng

import UnliftIO (MonadUnliftIO, MonadIO(liftIO))

import Streamly.Internal.System.IO (defaultChunkSize)
import qualified Streamly.Data.MutArray as MA
import qualified Streamly.Internal.Data.Array.Mut.Type as MA

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.Data.Stream as S

import System.IO (hGetBufSome, Handle, IOMode(ReadMode))

import Data.Word (Word8, Word64)

-- | Chunk section after chunking.
data Chunk = Chunk
  { chunk_begin :: {-# UNPACK #-} !Int
  -- ^ Begin of chunk, start from zero and inclusive.
  , chunk_end :: {-# UNPACK #-} !Int
  -- ^ End of chunk, start from zero and exclusive.
  }
  deriving (Show)

data GearHashConfig = GearHashConfig
  { gearhash_mask :: {-# UNPACK #-} !Word64
  }
  deriving (Show)

defaultGearHashConfig :: GearHashConfig
defaultGearHashConfig = GearHashConfig
  { gearhash_mask = Bits.shiftL maxBound (64 - 15) -- 32KiB
  }

{-# INLINE gearHash #-}
gearHash :: (MonadUnliftIO m, MonadCatch m)
          => GearHashConfig -> FilePath -> S.Stream m Chunk
gearHash (GearHashConfig mask) file = File.withFile file ReadMode $ \h ->
  (S.bracket (MA.newPinnedBytes defaultChunkSize) (const $ pure ())
    $ \arr -> S.Stream step (h, 0, 0, 0, arr)
  )
  where
    -- WARN st might be `undefined` therefore we'd like to keep it lazy here.
    {-# INLINE[0] step #-}
    step ~st (h, last_hash, chunk_begin', read_bytes, arr') = do
      arr <- if MA.byteLength arr' == 0
        then unsafe_refill_mutarray arr' h
        else pure arr'

      if MA.byteLength arr == 0
         then if read_bytes /= 0
           -- The last chunk to yield
           then do
             let chunk_end' = chunk_begin' + read_bytes
             pure
               $ S.Yield (Chunk chunk_begin' chunk_end')
               -- Make sure new stats here could result in S.Stop in the next step
               $ (h, last_hash, chunk_end', 0, arr)
           else pure $ S.Stop
        else do
           breakpoint_offset_and_hash <- flip fix (last_hash, 0) $ \(~rec) (cur_hash, i) -> do
             if i >= MA.byteLength arr
                then pure (Nothing, cur_hash)
                else do
                  w8 <- MA.getIndexUnsafe i arr
                  let new_hash = gear_hash_update cur_hash w8
                  if new_hash Bits..&. mask == 0
                    then pure $ (Just (i + 1), new_hash)
                    else rec (new_hash, i + 1)

           case breakpoint_offset_and_hash of
             -- No breakpoing was found in arr, try next block
             (Nothing, new_gear) -> do
               step st
                 ( h
                 , new_gear
                 , chunk_begin'
                 , read_bytes + MA.byteLength arr
                 , arr { MA.arrStart = MA.arrEnd arr }
                 )
             -- Breakpoint was found after reading n bytes from arr, try next block
             (Just read_bytes_of_arr, new_gear) -> do
               let chunk_end' = chunk_begin' + read_bytes + read_bytes_of_arr
               pure $ S.Yield (Chunk chunk_begin' chunk_end')
                 ( h
                 , new_gear
                 , chunk_end'
                 , 0
                 , arr { MA.arrStart = MA.arrStart arr + read_bytes_of_arr}
                 )

-- TODO Should this large vector be here or locally to its user to be memory usage
-- friendly?
gear_table :: UV.Vector Word64
gear_table = UV.fromListN 256
  $ Rng.randoms
  -- CAUTION Don't change seeds unless you'd like to break old deduplications.
  $ Rng.seedSMGen 123 456

{-# INLINE gear_hash_update #-}
gear_hash_update :: Word64 -> Word8 -> Word64
gear_hash_update w64 w8
  = Bits.unsafeShiftL w64 1
  + UV.unsafeIndex gear_table (fromIntegral w8)

-- Unsafe: input MutArray shouldn't be used anymore outside of this function.
{-# INLINE unsafe_refill_mutarray #-}
unsafe_refill_mutarray :: MonadIO m => MA.MutArray Word8 -> Handle -> m (MA.MutArray Word8)
unsafe_refill_mutarray arr h = MA.asPtrUnsafe (arr { MA.arrStart = 0 }) $ \p -> do
  n <- liftIO $ hGetBufSome h p (MA.arrBound arr)
  return $ arr { MA.arrStart = 0, MA.arrEnd = n }
