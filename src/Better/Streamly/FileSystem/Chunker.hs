{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Better.Streamly.FileSystem.Chunker (
  Chunk (..),
  GearHashConfig,
  defaultGearHashConfig,
  gearHash,
  gearHashPure,

  -- * Tests
  props_distribute,
) where

import Fusion.Plugin.Types (Fuse (Fuse))

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS

import GHC.Types (SPEC (SPEC))

import qualified Data.Bits as Bits
import Data.Function (fix, (&))

import qualified Data.Vector.Unboxed as UV

import qualified System.Random as Rng
import qualified System.Random.SplitMix as Rng

import qualified Streamly.Internal.Data.Array as A
import qualified Streamly.Internal.Data.Array.Type as A

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.Data.Stream as S

import qualified Data.ByteArray as BA
import Data.List (foldl')
import Data.Word (Word32, Word64, Word8)
import Streamly.Internal.Data.SVar (adaptState)

import Control.Monad (forM_)
import qualified Hedgehog as H
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

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
defaultGearHashConfig =
  GearHashConfig
    { gearhash_mask = Bits.shiftL maxBound (64 - 15) -- 32KiB
    }

-- TODO Consider using strict Maybe from Strict module.
data SMaybe a = SJust !a | SNothing

{-# ANN type GearState Fuse #-}
data GearState s
  = GearFinding !BS.ByteString !Word64 !Int !Int s
  | GearEnd

{-# INLINE gearHashPure #-}
gearHashPure :: (BA.ByteArrayAccess ba, Monad m) => GearHashConfig -> S.Stream m ba -> S.Stream m Chunk
gearHashPure (GearHashConfig !mask) (S.Stream !inner_step inner_s0) = S.Stream step $! GearFinding empty_view 0 0 0 inner_s0
  where
    empty_view = BS.empty

    {-# INLINE [0] step #-}
    step st (GearFinding buf' last_hash chunk_begin' read_bytes inner_s) = do
      ret <-
        if BS.length buf' /= 0
          then pure $ S.Yield buf' inner_s
          else fmap BA.convert <$> inner_step (adaptState st) inner_s

      case ret of
        S.Yield !buf next_inner_s -> do
          case search_break_point last_hash buf mask of
            -- No breakpoing was found in entire buf, try next block
            (SNothing, !new_gear) -> do
              pure $!
                S.Skip $!
                  GearFinding
                    empty_view
                    new_gear
                    chunk_begin'
                    (read_bytes + BS.length buf)
                    next_inner_s
            -- Breakpoint was found after reading n bytes from buf, try next block
            (SJust read_bytes_of_arr, !new_gear) -> do
              let
                !chunk_end' = chunk_begin' + read_bytes + read_bytes_of_arr
                !new_chunk = Chunk chunk_begin' chunk_end'

              pure $!
                S.Yield new_chunk $!
                  GearFinding
                    (BS.drop read_bytes_of_arr buf)
                    new_gear
                    chunk_end'
                    0
                    next_inner_s
        S.Skip next_inner_s -> pure $! S.Skip $! GearFinding buf' last_hash chunk_begin' read_bytes next_inner_s
        S.Stop ->
          if read_bytes /= 0
            then -- The last chunk to yield

              let
                !chunk_end' = chunk_begin' + read_bytes
                !next_chunk = Chunk chunk_begin' chunk_end'
              in
                pure $! S.Yield next_chunk GearEnd
            else pure S.Stop
    step _ GearEnd = pure S.Stop

{-# INLINE search_break_point #-}
search_break_point :: Word64 -> BS.ByteString -> Word64 -> (SMaybe Int, Word64)
search_break_point !init_hash !buf !mask = flip fix (SPEC, init_hash, 0) $ \rec (!spec, !cur_hash, !i) ->
  if i >= BA.length buf
    then (SNothing, cur_hash)
    else do
      let new_hash = gear_hash_update cur_hash $! buf `BS.unsafeIndex` i
      if new_hash Bits..&. mask == 0
        then (SJust (i + 1), new_hash)
        else rec (spec, new_hash, i + 1)

newtype ArrayBA = ArrayBA (A.Array Word8)

instance BA.ByteArrayAccess ArrayBA where
  length (ArrayBA arr) = A.byteLength arr
  {-# INLINE length #-}
  withByteArray (ArrayBA arr) = A.asPtrUnsafe (A.castUnsafe arr)
  {-# INLINE withByteArray #-}

{-# INLINE gearHash #-}
gearHash :: GearHashConfig -> FilePath -> S.Stream IO Chunk
gearHash config file = File.readChunks file & fmap ArrayBA & gearHashPure config

-- TODO Should this large vector be here or locally to its user to be memory usage
-- friendly?
gear_table :: UV.Vector Word64
gear_table =
  UV.fromListN 256 $
    Rng.randoms
    -- CAUTION Don't change seeds unless you'd like to break old deduplications.
    $
      Rng.seedSMGen 123 456

gear_hash_update :: Word64 -> Word8 -> Word64
gear_hash_update !w64 !w8 =
  Bits.unsafeShiftL w64 1
    + UV.unsafeIndex gear_table (fromIntegral w8)

distribute :: Word32 -> Word32 -> Word64
distribute 0 _ = 0
distribute _ 0 = 0
distribute n1 origin_n = fst . foldl' step (1 :: Word64, width) $ replicate (fromIntegral n - 1) (0 :: Int)
  where
    n = origin_n `min` 64

    step (!acc, !remain_width) _ =
      if remain_width - 1 > 0
        then ((acc `Bits.unsafeShiftL` 1) Bits..|. 0, remain_width - 1)
        else ((acc `Bits.unsafeShiftL` 1) Bits..|. 1, remain_width - 1 + width)

    width :: Double
    width = fromIntegral n / fromIntegral n1

props_distribute :: [(String, H.PropertyT IO ())]
props_distribute =
  [ ("64bit width", prop_distribute_64bits)
  , ("prop", prop_distribute)
  ]
  where
    prop_distribute_64bits :: H.PropertyT IO ()
    prop_distribute_64bits = do
      forM_ [0 .. fromIntegral (Bits.finiteBitSize (undefined :: Word64)) * 2] $ \n ->
        Bits.popCount (distribute n 64) H.=== (fromIntegral n `min` 64)

    prop_distribute :: H.PropertyT IO ()
    prop_distribute = do
      n <- H.forAll $ Gen.word32 $ Range.constant 0 100
      n1 <- H.forAll $ Gen.word32 $ Range.constant 0 100

      fromIntegral (Bits.popCount (distribute n1 n)) H.=== (n `min` n1 `min` fromIntegral (Bits.finiteBitSize (undefined :: Word64)))
