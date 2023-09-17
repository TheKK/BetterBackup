{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Better.Streamly.FileSystem.Chunker (
  Chunk (..),

  -- * Chunking algorithm
  gearHash,
  gearHashPure,
  gearHashWithFileUnfold,

  -- * Configuration for chunking algorithm
  GearHashConfig,
  defaultGearHashConfig,
  gearHashConfig,
  gearHashConfigMinChunkSize,
  gearHashConfigMaxChunkSize,

  -- * Tests
  props_distribute,
  props_fast_cdc,
) where

import Fusion.Plugin.Types (Fuse (Fuse))

import GHC.Types (SPEC (SPEC))

import qualified Data.Bits as Bits

import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Unsafe as BS

import Data.Coerce (coerce)
import Data.Foldable (for_)
import Data.Function (fix, (&))
import Data.Functor.Identity (runIdentity)
import Data.List (foldl')
import Data.Word (Word32, Word64, Word8)

import qualified Data.Vector.Unboxed as UV

import qualified Streamly.Data.Stream as S
import Streamly.Internal.Data.SVar (adaptState)
import qualified Streamly.Internal.Data.Stream as S
import qualified Streamly.Internal.Data.Unfold.Exception as Unfold
import Streamly.Internal.Data.Unfold.Type (Unfold (Unfold))
import Streamly.Internal.System.IO (defaultChunkSize)
import qualified System.Random as Rng
import qualified System.Random.SplitMix as Rng

import Control.Monad (forM_, unless)

import System.IO (Handle, IOMode (ReadMode), SeekMode (AbsoluteSeek, RelativeSeek), hClose, hFileSize, hGetBufSome, hIsEOF, hSeek, openFile, withFile)

import qualified Hedgehog as H
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import Basement.Block (MutableBlock)
import qualified Basement.Block as B
import qualified Basement.Block.Mutable as BM
import qualified Basement.Block.Mutable as MutableBlock
import qualified Basement.Block.Mutable as Mutableblock
import Basement.Monad (PrimState)
import Basement.Types.OffsetSize (CountOf (CountOf), Offset (Offset))
import Control.Monad.IO.Class (liftIO)
import qualified UnliftIO.Temporary as Un

-- | Chunk section after chunking.
data Chunk = Chunk
  { chunk_begin :: {-# UNPACK #-} !Int
  -- ^ Begin of chunk, start from zero and inclusive.
  , chunk_end :: {-# UNPACK #-} !Int
  -- ^ End of chunk, start from zero and exclusive.
  }
  deriving (Show, Eq)

data GearHashConfig = GearHashConfig
  { gearhash_low_mask :: {-# UNPACK #-} !Word64
  , gearhash_high_mask :: {-# UNPACK #-} !Word64
  , gearhash_min_size :: {-# UNPACK #-} !Word32
  , gearhash_avg_size :: {-# UNPACK #-} !Word32
  , gearhash_max_size :: {-# UNPACK #-} !Word32
  }
  deriving (Show)

gearHashConfig :: Word32 -> Word32 -> GearHashConfig
gearHashConfig normalize_level avg_bytes =
  GearHashConfig
    { gearhash_low_mask = distribute (floor @Double $ logBase 2 $ fromIntegral max_bytes) 48
    , gearhash_high_mask = distribute (ceiling @Double $ logBase 2 $ fromIntegral min_bytes) 48
    , gearhash_min_size = min_bytes
    , gearhash_avg_size = avg_bytes
    , gearhash_max_size = max_bytes
    }
  where
    min_bytes = avg_bytes `div` (2 ^ normalize_level)
    max_bytes = avg_bytes * (2 ^ normalize_level)

defaultGearHashConfig :: GearHashConfig
defaultGearHashConfig = gearHashConfig normalize_level avg_bytes
  where
    normalize_level :: Word32
    normalize_level = 1

    avg_bytes :: Word32
    avg_bytes = 32 * 2 ^ (10 :: Int)

gearHashConfigMinChunkSize :: GearHashConfig -> Word32
gearHashConfigMinChunkSize = gearhash_min_size

gearHashConfigMaxChunkSize :: GearHashConfig -> Word32
gearHashConfigMaxChunkSize c = gearhash_max_size c * 2

-- TODO Consider using strict Maybe from Strict module.
data SMaybe a = SJust !a | SNothing

{-# ANN type GearState Fuse #-}
data GearState s
  = GearFinding !BS.ByteString !Word64 !Int !Int s
  | GearSeeking !Int Word64 !Int !Int s
  | GearEnd

{- | Unsafe: we reuse same buffer underlying multiple yields, so comsumer of this Unfold MUST NOT use the value we
 yield after calling next "step" function.
-}
{-# INLINE unsafe_file_read_chunks_unfold #-}
unsafe_file_read_chunks_unfold :: Unfold IO FilePath (BA.View (B.Block Word8))
unsafe_file_read_chunks_unfold = Unfold.bracket (`openFile` ReadMode) hClose $ Unfold step inject
  where
    inject h = do
      buf <- BM.newPinned @_ @Word8 (CountOf defaultChunkSize)
      pure (h, buf)

    {-# INLINE [0] step #-}
    step (h, buf) = do
      read_bytes <- BM.withMutablePtr @IO @Word8 buf $ \ptr -> hGetBufSome h ptr (coerce $ BM.mutableLengthBytes buf)
      if read_bytes /= 0
        then do
          block_buf <- BM.unsafeFreeze buf
          let !ret = BA.takeView block_buf read_bytes
          pure $! S.Yield ret (h, buf)
        else pure S.Stop

{-# INLINE gearHash #-}
gearHash :: GearHashConfig -> FilePath -> S.Stream IO Chunk
-- gearHash cfg = S.unfold (Unfold.bracket (`openFile` ReadMode) hClose $ gearHashWithFileUnfold cfg)
gearHash cfg file = S.unfold unsafe_file_read_chunks_unfold file & gearHashPure cfg

data Buf = Buf
  { buf_offset :: {-# UNPACK #-} !Int
  , buf_len :: {-# UNPACK #-} !Int
  , buf_content :: {-# UNPACK #-} !(MutableBlock Word8 (PrimState IO))
  }

instance Show Buf where
  show (Buf o l _) = show (o, l)

buf_new_pinned :: Int -> IO Buf
buf_new_pinned n = do
  content <- Mutableblock.newPinned $ CountOf n
  pure $! Buf 0 0 content

buf_full :: Buf -> Buf
buf_full (Buf _ _ content) = Buf 0 (coerce $ MutableBlock.mutableLength content) content

buf_reset :: Buf -> Buf
buf_reset (Buf _ _ content) = Buf 0 0 content

buf_unsafe_index :: Int -> Buf -> IO Word8
buf_unsafe_index n (Buf off _ content) = MutableBlock.unsafeRead content $ Offset (n + off)

buf_length :: Buf -> Int
buf_length (Buf _ len _) = len

buf_capacity :: Buf -> Int
buf_capacity (Buf _ _ content) = coerce $ MutableBlock.mutableLength content

buf_take :: Int -> Buf -> Buf
buf_take !n (Buf off len content) =
  let
    new_length = min n len
  in
    Buf off new_length content

buf_drop :: Int -> Buf -> Buf
buf_drop !n (Buf off len content) =
  let
    effect_n = min n len
    new_off = off + effect_n
    new_length = len - effect_n
  in
    Buf new_off new_length content

buf_split_at :: Int -> Buf -> (Buf, Buf)
buf_split_at !n buf = (buf_take n buf, buf_drop n buf)

{-# ANN type GearStateFd Fuse #-}
data GearStateFd
  = GearFindingFd !Word64 !Int !Int !Handle !Buf
  | GearSeekingFd !Int Word64 !Int !Int !Handle !Buf
  | GearEndFd

-- TODO Decide to use this or not
{-# INLINE gearHashWithFileUnfold #-}
gearHashWithFileUnfold :: GearHashConfig -> Unfold IO Handle Chunk
gearHashWithFileUnfold cfg@(GearHashConfig lomask himask _ avg_chunk_size _) = Unfold step inject
  where
    min_chunk_size = gearHashConfigMinChunkSize cfg

    inject h = do
      buf <- buf_new_pinned defaultChunkSize
      hSeek h AbsoluteSeek 0
      pure $! GearSeekingFd (fromIntegral $ gearHashConfigMinChunkSize cfg) 0 0 0 h buf

    {-# INLINE [0] step #-}
    step (GearFindingFd last_hash chunk_begin' read_bytes h buf) = do
      ret <-
        if buf_length buf /= 0
          then pure $! SJust buf
          else do
            MutableBlock.withMutablePtr @IO @Word8 (buf_content buf) $ \ptr -> do
              bytes <- hGetBufSome h ptr $ buf_capacity buf
              if bytes /= 0
                then
                  let new_buf = buf_take bytes $ buf_full buf
                  in  pure $! SJust new_buf
                else pure SNothing

      case ret of
        SJust entire_buf -> do
          let
            bytes_until_max_chunk_size = fromIntegral (gearHashConfigMaxChunkSize cfg) - read_bytes
            (buf_before_max_limit, buf_after_max_limit) = buf_split_at bytes_until_max_chunk_size entire_buf
          search_break_point_for_muarray read_bytes (fromIntegral avg_chunk_size) last_hash buf_before_max_limit (lomask, himask) >>= \case
            -- No breakpoing was found in entire buf, try next block
            (SNothing, !new_gear) ->
              if buf_length buf_after_max_limit == 0
                then
                  pure $!
                    S.Skip $!
                      GearFindingFd
                        new_gear
                        chunk_begin'
                        (read_bytes + coerce (buf_length buf_before_max_limit))
                        h
                        (buf_reset entire_buf)
                else do
                  let
                    chunk_end' = chunk_begin' + read_bytes + coerce (buf_length buf_before_max_limit)
                    !new_chunk = Chunk chunk_begin' chunk_end'

                    remaining_buf = buf_after_max_limit
                    !next_state = next_state_from_remaining_buf new_gear chunk_end' h remaining_buf

                  pure $! S.Yield new_chunk $! next_state

            -- Breakpoint was found after reading n bytes from buf, try next block
            (SJust read_bytes_of_arr, !new_gear) -> do
              let
                chunk_end' = chunk_begin' + read_bytes + read_bytes_of_arr
                !new_chunk = Chunk chunk_begin' chunk_end'
                remaining_buf = buf_drop read_bytes_of_arr entire_buf
                !next_state = next_state_from_remaining_buf new_gear chunk_end' h remaining_buf

              pure $! S.Yield new_chunk $! next_state
        SNothing ->
          if read_bytes /= 0
            then -- The last chunk to yield

              let
                chunk_end' = chunk_begin' + read_bytes
                !next_chunk = Chunk chunk_begin' chunk_end'
              in
                pure $! S.Yield next_chunk GearEndFd
            else pure S.Stop
    step (GearSeekingFd remaining_bytes last_hash chunk_begin' read_bytes' h buf)
      | remaining_bytes <= 0 = pure $! S.Skip $! GearFindingFd last_hash chunk_begin' read_bytes' h (buf_reset buf)
      | otherwise = do
          hSeek h RelativeSeek (fromIntegral remaining_bytes)
          eof <- hIsEOF h
          if not eof
            then pure $! S.Skip $! GearFindingFd last_hash chunk_begin' (read_bytes' + remaining_bytes) h (buf_reset buf)
            else do
              file_size' <- hFileSize h
              let !chunk_end' = fromIntegral file_size'
                  !next_chunk = Chunk chunk_begin' chunk_end'
              if chunk_begin' == chunk_end'
                then pure S.Stop
                else pure $! S.Yield next_chunk GearEndFd
    step GearEndFd = pure S.Stop

    {-# INLINE next_state_from_remaining_buf #-}
    next_state_from_remaining_buf :: Word64 -> Int -> Handle -> Buf -> GearStateFd
    next_state_from_remaining_buf !new_gear !chunk_end' !h !buf =
      let remaining_buf_length = buf_length buf
      in  case fromIntegral min_chunk_size `compare` remaining_buf_length of
            GT -> GearSeekingFd (fromIntegral min_chunk_size - remaining_buf_length) new_gear chunk_end' remaining_buf_length h (buf_reset buf)
            LT -> GearFindingFd new_gear chunk_end' (fromIntegral min_chunk_size) h (buf_drop (fromIntegral min_chunk_size) buf)
            EQ -> GearFindingFd new_gear chunk_end' (fromIntegral min_chunk_size) h (buf_reset buf)

{-# INLINE gearHashPure #-}
gearHashPure :: (BA.ByteArrayAccess ba, Monad m) => GearHashConfig -> S.Stream m ba -> S.Stream m Chunk
gearHashPure (GearHashConfig lomask himask min_chunk_size avg_chunk_size max_chunk_size) (S.Stream !inner_step inner_s0) = S.Stream step $! GearSeeking (fromIntegral min_chunk_size) 0 0 0 inner_s0
  where
    empty_view = BS.empty

    maximum_chunk_size_limit :: Int
    maximum_chunk_size_limit = fromIntegral max_chunk_size * 2

    {-# INLINE [0] step #-}
    step st (GearFinding buf' last_hash chunk_begin' read_bytes inner_s) = do
      ret <-
        if BS.length buf' /= 0
          then pure $ S.Yield buf' inner_s
          else fmap BA.convert <$> inner_step (adaptState st) inner_s

      case ret of
        S.Yield !entire_buf next_inner_s -> do
          let (buf_before_max_limit, buf_after_max_limit) = BS.splitAt (maximum_chunk_size_limit - read_bytes) entire_buf
          case search_break_point read_bytes (fromIntegral avg_chunk_size) last_hash buf_before_max_limit (lomask, himask) of
            -- No breakpoing was found in entire buf, try next block
            (SNothing, !new_gear) ->
              if BS.null buf_after_max_limit
                then
                  pure $!
                    S.Skip $!
                      GearFinding
                        empty_view
                        new_gear
                        chunk_begin'
                        (read_bytes + BS.length buf_before_max_limit)
                        next_inner_s
                else do
                  let
                    !chunk_end' = chunk_begin' + read_bytes + BS.length buf_before_max_limit
                    !new_chunk = Chunk chunk_begin' chunk_end'

                    remaining_buf = buf_after_max_limit
                    next_state = next_state_from_remaining_buf remaining_buf new_gear chunk_end' next_inner_s

                  pure $! S.Yield new_chunk $! next_state

            -- Breakpoint was found after reading n bytes from buf, try next block
            (SJust read_bytes_of_arr, !new_gear) -> do
              let
                !chunk_end' = chunk_begin' + read_bytes + read_bytes_of_arr
                !new_chunk = Chunk chunk_begin' chunk_end'

                remaining_buf = BS.drop read_bytes_of_arr entire_buf
                next_state = next_state_from_remaining_buf remaining_buf new_gear chunk_end' next_inner_s

              pure $! S.Yield new_chunk $! next_state
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
    step st (GearSeeking remaining_bytes last_hash chunk_begin' read_bytes inner_s)
      | remaining_bytes <= 0 = pure $! S.Skip $! GearFinding empty_view last_hash chunk_begin' read_bytes inner_s
      | otherwise = do
          skipped_chunk_ret <- inner_step (adaptState st) inner_s
          case skipped_chunk_ret of
            S.Yield skipped_chunk next_s -> do
              let chunk_length = BA.length skipped_chunk
              case chunk_length `compare` remaining_bytes of
                LT -> pure $! S.Skip $! GearSeeking (remaining_bytes - chunk_length) last_hash chunk_begin' (read_bytes + chunk_length) next_s
                GT -> pure $! S.Skip $! GearFinding (BA.convert $ BA.dropView skipped_chunk remaining_bytes) last_hash chunk_begin' (read_bytes + remaining_bytes) next_s
                EQ -> pure $! S.Skip $! GearFinding empty_view last_hash chunk_begin' (read_bytes + chunk_length) next_s
            S.Skip next_s -> pure $! S.Skip $! GearSeeking remaining_bytes last_hash chunk_begin' read_bytes next_s
            S.Stop ->
              let !chunk_end' = chunk_begin' + read_bytes
                  !next_chunk = Chunk chunk_begin' chunk_end'
              in  if read_bytes /= 0
                    then pure $! S.Yield next_chunk GearEnd
                    else pure S.Stop
    step _ GearEnd = pure S.Stop

    {-# INLINE next_state_from_remaining_buf #-}
    next_state_from_remaining_buf :: BS.ByteString -> Word64 -> Int -> s -> GearState s
    next_state_from_remaining_buf !remaining_buf !new_gear !chunk_end' next_inner_s =
      let remaining_buf_length = BS.length remaining_buf
      in  case fromIntegral min_chunk_size `compare` remaining_buf_length of
            GT -> GearSeeking (fromIntegral min_chunk_size - remaining_buf_length) new_gear chunk_end' remaining_buf_length next_inner_s
            LT -> GearFinding (BS.drop (fromIntegral min_chunk_size) remaining_buf) new_gear chunk_end' (fromIntegral min_chunk_size) next_inner_s
            EQ -> GearFinding empty_view new_gear chunk_end' (fromIntegral min_chunk_size) next_inner_s

{-# INLINE search_break_point #-}
search_break_point :: Int -> Int -> Word64 -> BS.ByteString -> (Word64, Word64) -> (SMaybe Int, Word64)
search_break_point !read_bytes !avg_chunk_size !init_hash !buf (!lomask, !himask) = flip fix (SPEC, init_hash, 0) $ \rec (!spec, !cur_hash, !i) ->
  if i >= BA.length buf
    then (SNothing, cur_hash)
    else do
      let
        mask = if read_bytes + i < avg_chunk_size then lomask else himask
        new_hash = gear_hash_update cur_hash $! buf `BS.unsafeIndex` i

      if (new_hash Bits..&. mask) == 0
        then (SJust (i + 1), new_hash)
        else rec (spec, new_hash, i + 1)

{-# INLINE search_break_point_for_muarray #-}
search_break_point_for_muarray :: Int -> Int -> Word64 -> Buf -> (Word64, Word64) -> IO (SMaybe Int, Word64)
search_break_point_for_muarray !read_bytes !avg_chunk_size !init_hash !buf (!lomask, !himask) = flip fix (SPEC, init_hash, 0) $ \rec (!spec, !cur_hash, !i) ->
  if i >= buf_length buf
    then pure (SNothing, cur_hash)
    else do
      let
        mask = if read_bytes + i < avg_chunk_size then lomask else himask
      new_hash <- gear_hash_update cur_hash <$> buf_unsafe_index i buf

      if new_hash Bits..&. mask == 0
        then pure (SJust (i + 1), new_hash)
        else rec (spec, new_hash, i + 1)

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

props_distribute :: H.Group
props_distribute =
  H.Group
    "props_distribute"
    [ ("64bit width", prop_distribute_64bits)
    , ("prop", prop_distribute)
    ]
  where
    prop_distribute_64bits = H.withTests 1 $ H.property $ do
      forM_ [0 .. fromIntegral (Bits.finiteBitSize (undefined :: Word64)) * 2] $ \n ->
        Bits.popCount (distribute n 64) H.=== (fromIntegral n `min` 64)

    prop_distribute = H.property $ do
      n <- H.forAll $ Gen.word32 $ Range.constant 0 100
      n1 <- H.forAll $ Gen.word32 $ Range.constant 0 100

      fromIntegral (Bits.popCount (distribute n1 n)) H.=== (n `min` n1 `min` fromIntegral (Bits.finiteBitSize (undefined :: Word64)))

props_fast_cdc :: H.Group
props_fast_cdc =
  H.Group
    "props_fast_cdc"
    [ ("definition", prop_fast_cdc_definition)
    , ("single input equal splited inputs", prop_fast_cdc_single_input_equal_splited_inputs)
    , ("empty input has no effect", prop_fast_cdc_empty_input_has_no_effect)
    , ("pure and IO ver should work identical", prop_fast_cdc_pure_and_io)
    ]
  where
    gen_input_stream_and_chunk_config :: H.Gen ([BS.ByteString], GearHashConfig)
    gen_input_stream_and_chunk_config = do
      bs <-
        Gen.frequency
          [ (1, pure [])
          , (10, Gen.list (Range.linear 1 50) $ Gen.bytes $ Range.constant 0 32)
          ]
      cfg <- gearHashConfig <$> Gen.word32 (Range.constant 0 3) <*> Gen.word32 (Range.constant 8 16)
      pure (bs, cfg)

    prop_fast_cdc_definition = H.property $ do
      (input_stream, chunk_config) <- H.forAll gen_input_stream_and_chunk_config

      let
        chunks =
          S.fromList input_stream
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

        concated_input = mconcat $ fmap BS.fromStrict input_stream
        chunked_input = flip fmap chunks $ \(Chunk b e) ->
          BL.take (fromIntegral $ e - b) $ BL.drop (fromIntegral b) concated_input

      H.cover 1 "empty input stream" $ null input_stream
      H.cover 1 "empty input data" $ BL.null concated_input
      H.cover 1 "input length > min chunk size" $ BL.length concated_input > fromIntegral (gearHashConfigMinChunkSize chunk_config)
      H.cover 5 "input length <= min chunk size" $ BL.length concated_input <= fromIntegral (gearHashConfigMinChunkSize chunk_config)
      H.cover 5 "input length > max chunk size" $ BL.length concated_input > fromIntegral (gearHashConfigMaxChunkSize chunk_config)
      H.cover 5 "input length <= max chunk size" $ BL.length concated_input <= fromIntegral (gearHashConfigMaxChunkSize chunk_config)

      H.annotateShow chunks
      H.annotateShow chunked_input

      -- mconcat (chunk input) == input
      mconcat chunked_input H.=== concated_input

      -- All chunk should be smaller than max chunk size.
      for_ chunked_input $ \b -> do
        H.annotateShow (b, BL.length b)
        H.diff (fromIntegral $ BL.length b) (<=) (gearHashConfigMaxChunkSize chunk_config)

      -- All chunk should be greater than min chunk size except last one.
      unless (null chunked_input) $
        for_ (init chunked_input) $ \b -> do
          H.annotateShow (b, BL.length b)
          H.diff (fromIntegral $ BL.length b) (>=) (gearHashConfigMinChunkSize chunk_config)

    prop_fast_cdc_single_input_equal_splited_inputs = H.property $ do
      (input_stream, chunk_config) <- H.forAll gen_input_stream_and_chunk_config

      let
        concated_input = BL.toStrict $ mconcat $ fmap BS.fromStrict input_stream

        chunks_from_origin_input_stream =
          S.fromList input_stream
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

        chunks_from_concated_input_stream =
          S.fromList [concated_input]
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

      H.cover 50 "non-empty input stream" $ not $ null input_stream
      H.cover 50 "non-empty input data" $ not $ BS.null concated_input
      H.cover 1 "length of non-empty input data >= 2 * max chknu size" $ BS.length concated_input >= fromIntegral (2 * gearHashConfigMaxChunkSize chunk_config)
      H.cover 1 "length of non-empty input data >= 10 * max chknu size" $ BS.length concated_input >= fromIntegral (10 * gearHashConfigMaxChunkSize chunk_config)
      H.cover 1 "length of non-empty input data <= min chknu size" $ BS.length concated_input <= fromIntegral (gearHashConfigMinChunkSize chunk_config)

      -- chunk [input] == chunk (split input)
      chunks_from_origin_input_stream H.=== chunks_from_concated_input_stream

    prop_fast_cdc_empty_input_has_no_effect = H.property $ do
      (input_stream, chunk_config) <- H.forAll $ Gen.filter (any BS.null . fst) gen_input_stream_and_chunk_config
      let
        chunks_from_origin_input_stream =
          S.fromList input_stream
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

        chunks_from_filtered_input_stream =
          S.fromList (filter (not . BS.null) input_stream)
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

      H.cover 100 "non-empty input stream" $ not $ null input_stream
      H.cover 100 "empty element in input stream" $ any BS.null input_stream

      -- chunk input_stream == chunk (filter (not . null) input_stream)
      chunks_from_origin_input_stream H.=== chunks_from_filtered_input_stream

    prop_fast_cdc_pure_and_io = H.property $ do
      (input_stream, chunk_config) <- H.forAll gen_input_stream_and_chunk_config

      let
        concated_input = mconcat $ fmap BS.fromStrict input_stream

        chunks_from_pure =
          S.fromList input_stream
            & gearHashPure chunk_config
            & S.toList
            & runIdentity

      chunks_from_file <- liftIO $ Un.withSystemTempFile "tt" $ \tmp_p tmp_h -> do
        -- Close tmp_h so it can be read by others
        mapM_ (BS.hPut tmp_h) input_stream >> hClose tmp_h

        withFile tmp_p ReadMode $ \h ->
          S.unfold (gearHashWithFileUnfold chunk_config) h
            & S.toList

      H.cover 1 "empty input stream" $ null input_stream
      H.cover 1 "empty input data" $ BL.null concated_input
      H.cover 1 "input length > min chunk size" $ BL.length concated_input > fromIntegral (gearHashConfigMinChunkSize chunk_config)
      H.cover 5 "input length <= min chunk size" $ BL.length concated_input <= fromIntegral (gearHashConfigMinChunkSize chunk_config)
      H.cover 5 "input length > max chunk size" $ BL.length concated_input > fromIntegral (gearHashConfigMaxChunkSize chunk_config)
      H.cover 5 "input length <= max chunk size" $ BL.length concated_input <= fromIntegral (gearHashConfigMaxChunkSize chunk_config)

      chunks_from_pure H.=== chunks_from_file
