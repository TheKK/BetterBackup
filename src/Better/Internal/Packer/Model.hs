{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Models of packer & index that is used for describing/testing packer and works as an reference implementation.
--
-- Please checkout "Better.Internal.Packer" for details.
module Better.Internal.Packer.Model (
  -- * Model of packer
  PackerConfig (..),
  Packer,
  PackedIndexes,
  packing,
  packerToBuilder,
  packedIndexToBuilder,

  -- * Tests
  props_packer_model,
) where

import Better.Hash (Digest, hashByteStringFold)
import Better.Hash qualified as Hash
import Control.Monad (void)
import Control.Monad.ST.Strict (runST)
import Data.Bifunctor (bimap)
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy (LazyByteString)
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short (ShortByteString)
import Data.ByteString.Short qualified as SBS
import Data.Coerce (coerce)
import Data.Foldable (for_)
import Data.Function ((&))
import Data.Int (Int64)
import Data.List (find)
import Data.Maybe (catMaybes)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Word (Word64)
import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Rng
import Streamly.Data.Stream.Prelude qualified as S
import Streamly.Internal.Data.SVar qualified as S
import Streamly.Internal.Data.Stream qualified as S

compute_digest :: BL.ByteString -> Digest
compute_digest bl = coerce $ runST $ S.fromList (BL.toChunks bl) & S.fold hashByteStringFold

data Packer = Packer
  { packer_body :: !BL.LazyByteString
  , packer_header :: ![Header]
  }
  deriving (Show, Eq)

data Header = PackerHeader
  { header_key :: !Digest
  , header_value_offset :: !Word64
  , header_value_length :: !Word64
  }
  deriving (Show, Eq, Ord)

empty_packer :: Packer
empty_packer = Packer mempty []

packer_body_length :: Packer -> Int64
packer_body_length packer
  | packer == empty_packer = 0
  | otherwise = BL.length $ BB.toLazyByteString $ packerToBuilder packer

-- Unsafety: client must ensure that key is not duplicated.
append_lazy_bytestring :: BL.LazyByteString -> Packer -> Packer
append_lazy_bytestring !value (Packer body header) = Packer (body <> value) (header <> [new_header])
  where
    !new_header = PackerHeader (compute_digest value) cur_offset (fromIntegral $ BL.length value)
    !cur_offset = fromIntegral $ BL.length body

packerToBuilder :: Packer -> BB.Builder
packerToBuilder (Packer body headers) = body_bytes_builder <> BB.lazyByteString headers_bl <> headers_len_builder
  where
    body_bytes_builder = BB.lazyByteString body
    headers_len_builder = BB.int64LE (BL.length headers_bl)

    -- We need length of headers so must run its builder now.
    headers_bl = BB.toLazyByteString headers_builder
    headers_builder = foldMap header_builder headers

header_builder :: Header -> BB.Builder
header_builder (PackerHeader value_digest value_off value_len) =
  let
    key_builder = sbs_to_builder $ Hash.digestToShortByteString value_digest
    value_off_builder = BB.word64LE value_off
    value_len_builder = BB.word64LE value_len
  in
    key_builder <> value_off_builder <> value_len_builder

compute_packer_digest :: Packer -> Digest
compute_packer_digest packer = compute_digest $ BB.toLazyByteString $ packerToBuilder packer

newtype PackedIndexes = PackedIndexes [Index]
  deriving stock (Show)
  deriving newtype (Eq, Semigroup, Monoid)

packedIndexToBuilder :: PackedIndexes -> BB.Builder
packedIndexToBuilder (PackedIndexes idxes) = foldMap idx_to_builder idxes
  where
    idx_to_builder :: Index -> BB.Builder
    idx_to_builder (Index packer_digest keys_in_packer) =
      packer_digest_to_builder packer_digest <> foldMap key_to_builder keys_in_packer

    packer_digest_to_builder :: Digest -> BB.Builder
    packer_digest_to_builder d = BB.word8 0x00 <> sbs_to_builder (Hash.digestToShortByteString d)

    key_to_builder :: Header -> BB.Builder
    key_to_builder h = BB.word8 0x01 <> header_builder h

sbs_to_builder :: ShortByteString -> BB.Builder
sbs_to_builder sbs = BB.word64LE (fromIntegral $! SBS.length sbs) <> BB.shortByteString sbs

empty_packer_index :: PackedIndexes
empty_packer_index = PackedIndexes []

index_body_length :: PackedIndexes -> Int64
index_body_length packer_idx
  | packer_idx == empty_packer_index = 0
  | otherwise = BL.length $ BB.toLazyByteString $ packedIndexToBuilder packer_idx

compute_packer_index_digest :: PackedIndexes -> Digest
compute_packer_index_digest = compute_digest . BB.toLazyByteString . packedIndexToBuilder

data Index = Index
  { index_source_packer :: !Digest
  , index_content :: !(Seq Header)
  }
  deriving (Show, Eq)

append_index :: Index -> PackedIndexes -> PackedIndexes
append_index idx (PackedIndexes indexes) = PackedIndexes (indexes <> [idx])

type PackingState = (Packer, Seq Header, PackedIndexes)

init_packing_state :: (Packer, Seq digest, PackedIndexes)
init_packing_state = (empty_packer, Seq.empty, empty_packer_index)

extract_packing_state :: (Packer, Seq Header, PackedIndexes) -> (Maybe (Digest, Packer), Maybe (Digest, PackedIndexes))
extract_packing_state (packer, idx, packer_idx)
  | packer == empty_packer = (Nothing, yield_packer_idx)
  | otherwise = (Just (compute_packer_digest packer, packer), yield_packer_idx)
  where
    yield_packer_idx =
      if index_body_length packer_idx' == 0
        then Nothing
        else Just (compute_packer_index_digest packer_idx', packer_idx')

    packer_digest = compute_packer_digest packer
    packer_idx'
      | null idx = packer_idx
      | otherwise = append_index (Index packer_digest idx) packer_idx

data PackerConfig = PackerConfig
  { packer_config_max_packer_bytes :: !Word64
  , packer_config_max_index_bytes :: !Word64
  }
  deriving (Show)

fold_packing_state_once
  :: ()
  => PackerConfig
  -> PackingState
  -> BL.LazyByteString
  -> (PackingState, Maybe (Digest, Packer, Maybe (Digest, PackedIndexes)))
fold_packing_state_once cfg (prev_packer, prev_collecting_header_seq, prev_packer_index) value = ((packer', collecting_header_seq', packer_index'), yield)
  where
    yield = case yield_packer of
      Nothing -> Nothing
      Just (digest, packer) -> Just (digest, packer, yield_packer_index)

    (packer', yield_packer) =
      if fromIntegral (packer_body_length cur_packer) >= packer_config_max_packer_bytes cfg
        then (empty_packer, Just (compute_packer_digest cur_packer, cur_packer))
        else (cur_packer, Nothing)

    (packer_index', collecting_header_seq', yield_packer_index) = case yield_packer of
      Nothing -> (prev_packer_index, cur_collecting_header_seq, Nothing)
      Just (yield_packer_digest, _) ->
        let cur_packer_index = append_index (Index yield_packer_digest cur_collecting_header_seq) prev_packer_index
        in  if fromIntegral (index_body_length cur_packer_index) >= packer_config_max_index_bytes cfg
              then (empty_packer_index, Seq.empty, Just (compute_packer_index_digest cur_packer_index, cur_packer_index))
              else (cur_packer_index, Seq.empty, Nothing)

    cur_packer = append_lazy_bytestring value prev_packer
    -- Yes we compute digest of value of multiple times in different place, but since this is code
    -- for modling let's prioritize correctness over prioritize.
    cur_collecting_header_seq = prev_collecting_header_seq Seq.|> header_of_value

    header_of_value =
      PackerHeader
        (compute_digest value)
        (fromIntegral $ BL.length $ packer_body prev_packer)
        (fromIntegral $ BL.length value)

-- | Main entry of packer model.
--
-- Use 'packerToBuilder' and 'packedIndexToBuilder' to obtain corresponding binary.
packing
  :: (Monad m)
  => PackerConfig
  -> S.Stream m BL.LazyByteString
  -> S.Stream m (Maybe (Digest, Packer), Maybe (Digest, PackedIndexes))
packing cfg (S.Stream step' s0') = S.Stream step s0
  where
    s0 = Just (s0', init_packing_state)

    step _ Nothing = pure S.Stop
    step st (Just (s', packing_state)) =
      step' (S.adaptState st) s' >>= \case
        S.Skip next_s' -> pure $! S.Skip $ Just (next_s', packing_state)
        S.Stop -> case extract_packing_state packing_state of
          (Nothing, Nothing) -> pure S.Stop
          last_one -> pure $! S.Yield last_one Nothing
        S.Yield value next_s' -> do
          let (next_packing_state, yield) = fold_packing_state_once cfg packing_state value
          let next_s = Just (next_s', next_packing_state)
          case yield of
            Nothing -> pure $! S.Skip next_s
            Just (digest'', packer'', yield_packer_idx) -> pure $! S.Yield (Just (digest'', packer''), yield_packer_idx) next_s

props_packer_model :: H.Group
props_packer_model =
  H.Group
    "packer_model"
    [ ("prop_body_of_packers_should_be_able_to_construct_original_input", prop_body_of_packers_should_be_able_to_construct_original_input)
    , ("prop_digest_in_header_of_packer_should_match_body_content", prop_digest_in_header_of_packer_should_match_body_content)
    , ("prop_sum_of_length_in_header_should_match_body", prop_sum_of_length_in_header_should_match_body)
    , ("prop_index_should_be_bijection_to_packer", prop_index_should_be_bijection_to_packer)
    , ("gen_test", gen_coverage_test)
    ]
  where
    prop_index_should_be_bijection_to_packer :: H.Property
    prop_index_should_be_bijection_to_packer = H.property $ do
      (_original_input, _packer_cfg, (digest_n_packers, digest_n_indexes)) <- H.forAll gen_input_and_packed_result

      let
        indexes = coerce $ mconcat $ map snd digest_n_indexes

      -- packer to index
      for_ digest_n_packers $ \(packer_digest, packer) -> do
        possible_indexes <- H.eval $ filter ((packer_digest ==) . index_source_packer) indexes
        for_ (packer_header packer) $ \header_from_packer -> do
          void $ H.evalMaybe $ find (elem header_from_packer . index_content) possible_indexes

      -- index to packer
      for_ indexes $ \(Index source_packer_digest content_header_seq) -> do
        source_packer <- H.evalMaybe (snd <$> find ((source_packer_digest ==) . fst) digest_n_packers)
        for_ content_header_seq $ \header_from_index -> do
          void $ H.evalMaybe $ find ((header_from_index ==)) $ packer_header source_packer

    prop_sum_of_length_in_header_should_match_body :: H.Property
    prop_sum_of_length_in_header_should_match_body = H.property $ do
      (_original_input, _packer_cfg, (digest_n_packers, _digest_n_indexes)) <- H.forAll gen_input_and_packed_result
      let packers = fmap snd digest_n_packers

      -- Length in header of packer should match with body of packer
      for_ packers $ \packer ->
        sum (map header_value_length $ packer_header packer) H.=== fromIntegral (BL.length $ packer_body packer)

    prop_body_of_packers_should_be_able_to_construct_original_input :: H.Property
    prop_body_of_packers_should_be_able_to_construct_original_input = H.property $ do
      (original_inputs, _packer_cfg, (digest_n_packers, _digest_n_indexes)) <- H.forAll gen_input_and_packed_result

      BL.concat (packer_body . snd <$> digest_n_packers) H.=== mconcat original_inputs

    prop_digest_in_header_of_packer_should_match_body_content :: H.Property
    prop_digest_in_header_of_packer_should_match_body_content = H.property $ do
      (_original_input, _packer_cfg, (digest_n_packers, _digest_n_indexes)) <- H.forAll gen_input_and_packed_result

      let
        packers = fmap snd digest_n_packers

      for_ packers $ \packer -> do
        let body = packer_body packer
        for_ (packer_header packer) $ \h -> do
          let
            value_offset = fromIntegral $ header_value_offset h
            value_length = fromIntegral $ header_value_length h
            body_slice = BL.take value_length $ BL.drop value_offset body

          H.annotateShow (h, body_slice)

          -- Digest in header of packer should be correct
          compute_digest body_slice H.=== header_key h

    gen_coverage_test :: H.Property
    gen_coverage_test = H.property $ do
      (original_input, packer_cfg, (digest_n_packers, digest_n_indexes)) <- H.forAll gen_input_and_packed_result

      -- packer_cfg
      let input_bytes = fromIntegral (BL.length $ mconcat original_input)
      H.cover 5 "max byte of packer < input" $ packer_config_max_packer_bytes packer_cfg < input_bytes
      H.cover 5 "max byte of packer > input" $ packer_config_max_packer_bytes packer_cfg > input_bytes

      -- original_input
      H.cover 1 "input is empty" (BL.null $ mconcat original_input)

      -- digest_n_packers
      H.cover 1 "output zero packer" $ null digest_n_packers
      H.cover 10 "output more than 1 packer" $ length digest_n_packers > 1

      -- digest_n_indexes
      H.cover 1 "output zero index" $ null digest_n_indexes
      H.cover 10 "output more than 1 index" $ length digest_n_indexes > 1

    gen_input_and_packed_result :: H.Gen ([LazyByteString], PackerConfig, ([(Digest, Packer)], [(Digest, PackedIndexes)]))
    gen_input_and_packed_result = do
      (input, packer_cfg) <- gen_input_and_packer_config

      let packed_result =
            bimap catMaybes catMaybes . unzip $
              compute_packers_n_indexes packer_cfg input

      pure (input, packer_cfg, packed_result)

    gen_input_and_packer_config :: H.Gen ([LazyByteString], PackerConfig)
    gen_input_and_packer_config = do
      inputs <-
        Gen.list (Rng.linear 0 50) $
          Gen.frequency
            [ (5, pure BL.empty)
            , (95, fmap BL.fromStrict $ Gen.bytes $ Rng.linear 1 100)
            ]

      let length_of_inputs :: Int64 = fromIntegral $ sum $ fmap BL.length inputs

      max_packer_bytes <-
        fmap (fromIntegral . max 1 . (+ length_of_inputs) . round . (* fromIntegral length_of_inputs)) $
          Gen.double $
            Rng.constant (-10) 10
      max_index_bytes <- Gen.word64 $ Rng.linear 1 20

      pure (inputs, PackerConfig max_packer_bytes max_index_bytes)

    compute_packers_n_indexes
      :: PackerConfig
      -> [BL.ByteString]
      -> [ ( Maybe (Digest, Packer)
           , Maybe (Digest, PackedIndexes)
           )
         ]
    compute_packers_n_indexes cfg xs = runST $ do
      S.fromList xs
        & packing cfg
        & S.toList
