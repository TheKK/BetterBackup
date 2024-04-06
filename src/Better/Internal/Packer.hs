{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Packer, a format that stores multiple values in single file and could be accessed via key.
--
-- Packer always comes with index, which contains headers from multiple packers. This allows us to
-- construct complete key-to-value map (for searching) without doing too many IO request
-- (HTTP request/syscall/etc), since single index contains headers from multiple packers.
--
-- To summarize:
--
-- * multiple key value pairs construct single packer
-- * multiple headers from Packer construct single index
--
-- = Binary format of packer
--
-- [packer]: [ body | headers ]
-- [body]: [ bytes of body (raw bytes) ]
-- [header]:
--   [ length of key (w64-le)
--   | bytes of key (raw bytes)
--   | offset of value in packer (w64-le)
--   | length of value in packer (w64-le)
--   ]
-- [headers]: [ header * N | sum of length of headers (w64-le) ]
--
-- = Binary format of packed indexes
--
-- [packed_indexes]: [ packer_name | [ [ packer_name ] or [ key_in_packer ] * N ] ]
-- [packer_name]: [ 0x00 (word8) | length of packer name (w64-le) | bytes of packer name (raw bytes) ]
-- [key_in_packer]: [ 0x01 (word8) | headers ]
--
-- == Notes
-- We use TVL(Type-Length-Value) encoding here to maximun the ability of streaming index.
--
-- = Thread safety
--
-- None of IO operation here is thread-safe, please sync them with your desired choice.
module Better.Internal.Packer (
  -- * Packing operations

  -- ** Types
  PackingState,

  -- ** Create/Destroy PackingState
  withPackingState,
  mkPackingState,
  destroyPackingState,

  -- ** Put value into PackingState
  putToPack,
  unsafePutToPack,

  -- ** Extract packer/packed index from PackingState
  extractPacker,
  extractPackedIndex,

  -- * Tests
  propsPacker,
) where

import Better.Hash (Digest, digestToShortByteString, hashByteStringFold)
import Better.Hash qualified as Hash
import Better.Internal.Packer.Model qualified as Model
import Control.Applicative (Alternative ((<|>)))
import Control.Exception (Exception (toException), SomeException, bracket)
import Control.Monad (void)
import Control.Monad.Catch (MonadCatch, MonadThrow (throwM), handleAll)
import Control.Monad.Morph qualified as Morph
import Control.Monad.ST.Strict (runST)
import Control.Monad.Trans (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Monad.Trans.Resource qualified as R
import Data.ByteString qualified as BS
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy (LazyByteString)
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Lazy.Base16 qualified as LBS
import Data.ByteString.Short (ShortByteString)
import Data.ByteString.Short qualified as SBS
import Data.Foldable (fold)
import Data.Function ((&))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List (nub)
import Data.Maybe (isJust, isNothing)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Word (Word64)
import FlatParse.Basic (Parser)
import FlatParse.Basic qualified as Parser
import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Rng
import Path (Abs, Dir, File, Path, absdir, parseAbsFile, toFilePath)
import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S
import Streamly.External.ByteString qualified as S
import Streamly.FileSystem.Handle qualified as Handle
import Streamly.Internal.Data.SVar qualified as S
import Streamly.Internal.Data.Stream qualified as S
import System.Directory (removeFile)
import System.IO (Handle, IOMode (WriteMode), SeekMode (AbsoluteSeek), hClose, hSeek, hSetFileSize, hTell, openBinaryTempFile, withBinaryFile)

data PackingState = PackingState
  { ps_packer_body_handle :: !(FilePath, Handle)
  , ps_packer_headers :: !(IORef (Seq Header))
  , ps_current_packer_output_bytes :: !(IORef Word64)
  , ps_packed_index :: !(FilePath, Handle)
  }

-- | It is @bracket 'mkPackingState' 'destroyPackingState'@
withPackingState :: Path Abs Dir -> (PackingState -> IO a) -> IO a
withPackingState dir = bracket (mkPackingState dir) destroyPackingState

mkPackingState
  :: Path Abs Dir
  -- ^ Directory used for storing temp packer and packed index.
  -> IO PackingState
mkPackingState dir =
  PackingState
    <$> openBinaryTempFile (Path.toFilePath dir) "packing-packer-body-"
    <*> newIORef Seq.empty
    <*> newIORef 0
    <*> openBinaryTempFile (Path.toFilePath dir) "packing-packed-index-"

-- | Destroy packing state.
--
-- Note that this function won't extract anything even there's remaining bytes in 'PackingState'.
destroyPackingState :: PackingState -> IO ()
destroyPackingState ps = do
  hClose $ snd $ ps_packer_body_handle ps
  removeFile $ fst $ ps_packer_body_handle ps

  hClose $ snd $ ps_packed_index ps
  removeFile $ fst $ ps_packed_index ps

getCurrentPackerSize :: PackingState -> IO Word64
getCurrentPackerSize = readIORef . ps_current_packer_output_bytes

getCurrentPackedIndexSize :: PackingState -> IO Word64
getCurrentPackedIndexSize = fmap fromIntegral . hTell . snd . ps_packed_index

reset_packer_state :: PackingState -> IO ()
reset_packer_state (PackingState (_, body_h) headers_ref bytes_ref _) = do
  hSeek body_h AbsoluteSeek 0
  hSetFileSize body_h 0
  modifyIORef' headers_ref $ const Seq.empty
  modifyIORef' bytes_ref $ const 0

reset_packed_index_state :: PackingState -> IO ()
reset_packed_index_state (PackingState _ _ _ (_, packed_index_h)) = do
  hSeek packed_index_h AbsoluteSeek 0
  hSetFileSize packed_index_h 0

putToPack :: PackingState -> LazyByteString -> IO ()
putToPack ps value = unsafePutToPack ps (compute_digest value) value

-- | Unsafety: client has to ensure Digest matches LazyByteString.
--
-- Only use this version when you don't want to re-calculate Digest from input again.
unsafePutToPack :: PackingState -> Digest -> LazyByteString -> IO ()
unsafePutToPack (PackingState (_, body_h) headers_ref bytes_ref _) !value_digest !value = do
  !cur_offset <- fromIntegral <$> hTell body_h
  BB.hPutBuilder body_h body_builder
  let !header = Header value_digest cur_offset (fromIntegral $! BL.length value)
  modifyIORef' headers_ref (Seq.|> header)
  let
    !body_bytes = fromIntegral $ BL.length value
  modifyIORef' bytes_ref (+ (body_bytes + header_bytes header))
  where
    body_builder = BB.lazyByteString value

-- | When there's anything to extract, extract it to given 'Path' with corresponding Digest.
--
-- = Properties
-- == 'putToPack' at least once before
--
-- @
-- 'putToPack' ps value
-- Just d <- 'extractPacker' ps out
-- Nothing <- 'extractPacker' ps out
-- -- ... keep getting Nothing
-- @
--
-- == No 'putToPack' before
--
-- @
-- Nothing <- 'extractPacker' ps out
-- -- ... keep getting Nothing
-- @
extractPacker :: PackingState -> Path Abs File -> IO (Maybe Digest)
extractPacker s@(PackingState (_, body_h) headers_ref _ (_, packed_index_h)) out = when_has_stuff_to_extract $ withBinaryFile (Path.toFilePath out) WriteMode $ \out_h -> do
  hSeek body_h AbsoluteSeek 0

  mid_fold <-
    S.unfold Handle.chunkReader body_h
      & fmap S.fromArray
      & S.trace (BS.hPut out_h)
      & flip F.addStream Hash.hashByteStringFoldIO

  headers <- readIORef headers_ref

  mid_fold' <-
    from_builder (foldMap header_builder headers)
      & S.trace (BS.hPut out_h)
      & flip F.addStream mid_fold

  let !headers_bytes = sum $ header_bytes <$> headers
  !digest <-
    from_builder (BB.word64LE $ fromIntegral headers_bytes)
      & S.trace (BS.hPut out_h)
      & S.fold mid_fold'

  from_builder (packed_index_builder digest headers)
    & S.fold (F.drainMapM $ BS.hPut packed_index_h)

  reset_packer_state s

  pure $ Just digest
  where
    when_has_stuff_to_extract m = do
      size <- getCurrentPackerSize s
      if size /= 0 then m else pure Nothing

    {-# INLINE from_builder #-}
    from_builder = S.fromList . BL.toChunks . BB.toLazyByteString

-- | When there's anything to extract, extract it to given 'Path' with corresponding Digest.
--
-- = Properties
-- == Valid 'extractPacker' before
--
-- @
-- Just packer_digest <- 'extratPacker' ps out
-- Just index_digest <- 'extractPackedIndex' ps out
-- Nothing <- 'extractPackedIndex' ps out
-- -- ... keep getting Nothing
-- @
--
-- == No 'extractPacker' before
--
-- @
-- Nothing <- 'extractPackedIndex' ps out
-- -- ... keep getting Nothing
-- @
extractPackedIndex :: PackingState -> Path Abs File -> IO (Maybe Digest)
extractPackedIndex s@(PackingState _ _ _ (_, packed_index_h)) out = when_has_stuff_to_extract $ withBinaryFile (Path.toFilePath out) WriteMode $ \out_h -> do
  hSeek packed_index_h AbsoluteSeek 0

  !digest <-
    S.unfold Handle.chunkReader packed_index_h
      & fmap S.fromArray
      & S.trace (BS.hPut out_h)
      & S.fold Hash.hashByteStringFoldIO

  reset_packed_index_state s

  pure $ pure digest
  where
    when_has_stuff_to_extract m = do
      size <- getCurrentPackedIndexSize s
      if size /= 0 then m else pure Nothing

data Header = Header
  { header_value_digest :: !Digest
  , header_value_offset :: !Word64
  , header_value_length :: !Word64
  }
  deriving (Show, Eq)

header_builder :: Header -> BB.Builder
header_builder (Header d v_offset v_length) =
  fold
    [ BB.word64LE $! fromIntegral $ SBS.length d_sbs
    , BB.shortByteString d_sbs
    , BB.word64LE $! v_offset
    , BB.word64LE $! v_length
    ]
  where
    d_sbs = digestToShortByteString d

header_parser :: Parser SomeException Header
header_parser = do
  !value_digest <- digest_parser
  !value_off <- Parser.anyWord64le
  !value_len <- Parser.anyWord64le
  pure $ Header value_digest value_off value_len

-- | Reads out stream of Header from "headers section in packer without last 64 bits".
--
-- This would be relative lowlevel API so be sure to feed correct inputs.
packer_header_reader :: (Monad m, MonadThrow m) => S.Stream m BS.ByteString -> S.Stream m Header
packer_header_reader = flatparse_stream "packer header" header_parser
{-# INLINE packer_header_reader #-}

header_bytes :: Header -> Word64
header_bytes h =
  sum
    [ -- length of key (word64)
      8
    , -- bytes of key
      fromIntegral (SBS.length $ digestToShortByteString $ header_value_digest h)
    , -- offset of value (word64)
      8
    , -- length of value (word64)
      8
    ]

short_bytestring_parser :: Parser e ShortByteString
short_bytestring_parser = do
  !len <- Parser.anyWord64le
  !sbs <- fmap SBS.toShort $ Parser.take $ fromIntegral len
  pure sbs

digest_parser :: Parser SomeException Digest
digest_parser = do
  sbs <- short_bytestring_parser
  case Hash.digestFromShortByteString sbs of
    Just !d -> pure d
    Nothing -> Parser.err $ toException $ userError $ "invalid bytestring for constructing digest: " <> show sbs

packed_index_builder :: Foldable t => Digest -> t Header -> BB.Builder
packed_index_builder packer_digest headers =
  packer_name_bilder <> foldMap key_in_packer_builder headers
  where
    packer_name_bilder =
      fold
        [ BB.word8 0x00
        , BB.word64LE (fromIntegral $ SBS.length packer_name)
        , BB.shortByteString packer_name
        ]
    key_in_packer_builder h = BB.word8 0x01 <> header_builder h
    packer_name = digestToShortByteString packer_digest

data IndexToken
  = PackerName !ShortByteString
  | PackerHeader !Header
  deriving (Show, Eq)

packed_index_token_parser :: Parser SomeException IndexToken
packed_index_token_parser = packer_header_parser <|> packer_name_parser
  where
    packer_name_parser =
      Parser.word8 0x00 >> do
        !sbs <- short_bytestring_parser
        pure $ PackerName sbs
    packer_header_parser =
      Parser.word8 0x01 >> do
        !header <- header_parser
        pure $ PackerHeader header

-- | Reads out stream of IndexToken from "entire packed index".
--
-- This would be relative lowlevel API so be sure to feed correct inputs.
--
-- = Notes
-- This function only lexes without checking semantics.
packed_index_token_reader :: (Monad m, MonadThrow m) => S.Stream m BS.ByteString -> S.Stream m IndexToken
packed_index_token_reader = flatparse_stream "packed index token" packed_index_token_parser
{-# INLINE packed_index_token_reader #-}

-- TODO Move this to more general place.
{-# INLINE flatparse_stream #-}
flatparse_stream
  :: (Monad m, MonadThrow m, Show e)
  => String
  -- ^ Name used for displaying error message.
  -> Parser e a
  -- ^ FlatParser to be applied for parsing.
  -> S.Stream m BS.ByteString
  -- ^ Original input stream.
  -> S.Stream m a
flatparse_stream item_name parser (S.Stream step s0) = S.Stream step' s0'
  where
    s0' = (s0, BS.empty)

    {-# INLINE [0] step' #-}
    step' st (s, buf) = case Parser.runParser parser buf of
      Parser.OK !token !rest_buf -> pure $! S.Yield token (s, rest_buf)
      Parser.Fail ->
        step (S.adaptState st) s >>= \case
          S.Yield extra_buf s' ->
            let !new_buf = buf <> extra_buf
            in  pure $ S.Skip (s', new_buf)
          S.Skip s' -> pure $ S.Skip (s', buf)
          S.Stop ->
            if BS.null buf
              then pure S.Stop
              else throwM $ userError $ "unexpected EOF while parsing " <> item_name
      Parser.Err e -> throwM $ userError $ "unexpected error while parsing " <> item_name <> ": " <> show e

compute_digest :: BL.ByteString -> Digest
compute_digest bl = runST $ S.fromList (BL.toChunks bl) & S.fold hashByteStringFold

propsPacker :: H.Group
propsPacker =
  H.Group
    "packer"
    [ ("prop_tripping_packed_index_builder_n_parser", prop_tripping_packed_index_builder_n_parser)
    , ("prop_tripping_packer_header_builder_n_parser", prop_tripping_packer_header_builder_n_parser)
    , ("prop_binary_produced_by_packing_state_should_match_model", prop_binary_produced_by_packing_state_should_match_model)
    , ("prop_extract_packer", prop_extract_packer)
    , ("prop_extract_packed_index", prop_extract_packed_index)
    ]

prop_tripping_packed_index_builder_n_parser :: H.Property
prop_tripping_packed_index_builder_n_parser = H.property $ do
  fake_packer_content <- H.forAll $ Gen.bytes $ Rng.linear 0 100
  headers <- H.forAll $ Gen.list (Rng.linear 0 100) header_gen
  let
    fake_packer_digest = compute_digest $ BL.fromStrict fake_packer_content
    builder = packed_index_builder fake_packer_digest headers
    parsed_packed_index_tokens =
      runST $
        S.fromList (BL.toChunks $ BB.toLazyByteString builder)
          & packed_index_token_reader
          & S.toList
  parsed_packed_index_tokens
    H.=== (PackerName (digestToShortByteString fake_packer_digest) : fmap PackerHeader headers)
  where
    header_gen = do
      Just digest <- Hash.digestFromByteString <$> Gen.bytes (Rng.singleton Hash.digestSize)
      Header digest
        <$> Gen.word64 Rng.constantBounded
        <*> Gen.word64 Rng.constantBounded

prop_tripping_packer_header_builder_n_parser :: H.Property
prop_tripping_packer_header_builder_n_parser = H.property $ do
  input_headers <- H.forAll $ Gen.list (Rng.linear 0 100) header_gen
  let
    builder = foldMap header_builder input_headers
    parsed_headers =
      runST $
        S.fromList (BL.toChunks $ BB.toLazyByteString builder)
          & packer_header_reader
          & S.toList
  parsed_headers H.=== input_headers
  where
    header_gen = do
      Just digest <- Hash.digestFromByteString <$> Gen.bytes (Rng.singleton Hash.digestSize)
      Header digest
        <$> Gen.word64 Rng.constantBounded
        <*> Gen.word64 Rng.constantBounded

prop_binary_produced_by_packing_state_should_match_model :: H.Property
prop_binary_produced_by_packing_state_should_match_model = H.property . Morph.hoist runResourceT $ do
  inputs <- fmap nub $ H.forAll $ Gen.list (Rng.linear 0 100) $ fmap BL.fromStrict $ Gen.bytes $ Rng.linear 0 100

  -- Note: use MonadReource after forAll for correct behaviour, might be a bug.
  raw_tmp_packer_path <- alloc_tmp_file "packer"
  raw_tmp_index_path <- alloc_tmp_file "index"
  H.annotateShow (raw_tmp_packer_path, raw_tmp_index_path)

  tmp_packer_path <- H.evalIO $ Path.parseAbsFile raw_tmp_packer_path
  tmp_index_path <- H.evalIO $ Path.parseAbsFile raw_tmp_index_path
  H.annotateShow (tmp_packer_path, tmp_index_path)

  H.evalIO $ withPackingState [absdir|/tmp|] $ \ps -> do
    mapM_ (putToPack ps) inputs
    void $ extractPacker ps tmp_packer_path
    void $ extractPackedIndex ps tmp_index_path

  outs <-
    H.evalIO $
      S.fromList inputs
        & Model.packing (Model.PackerConfig maxBound maxBound)
        & S.toList
  H.annotateShow outs

  (packer_lbs_from_model, index_lbs_from_model) <- H.evalM $ case outs of
    [] -> pure ("", "")
    [(Just (_, p), Just (_, i))] -> pure (BB.toLazyByteString $ Model.packerToBuilder p, BB.toLazyByteString $ Model.packedIndexToBuilder i)
    _ -> throwM $ userError "packing shoulding produce this"
  H.annotateShow (packer_lbs_from_model, index_lbs_from_model)

  packer_lbs_from_here <- H.evalIO $ BL.fromStrict <$> BS.readFile raw_tmp_packer_path
  index_lbs_from_here <- H.evalIO $ BL.fromStrict <$> BS.readFile raw_tmp_index_path

  LBS.encodeBase16 packer_lbs_from_here H.=== LBS.encodeBase16 packer_lbs_from_model
  LBS.encodeBase16 index_lbs_from_here H.=== LBS.encodeBase16 index_lbs_from_model

prop_extract_packer :: H.Property
prop_extract_packer = H.property . Morph.hoist runResourceT $ do
  inputs <- fmap nub $ H.forAll $ Gen.list (Rng.constant 0 1) $ fmap BL.fromStrict $ Gen.bytes $ Rng.linear 0 1024

  H.cover 10 "without input" $ null inputs
  H.cover 10 "has input" $ not $ null inputs

  -- Note: use MonadReource after forAll for correct behaviour, might be a bug.
  raw_tmp_packer_path <- alloc_tmp_file "packer"
  raw_tmp_index_path <- alloc_tmp_file "index"
  H.annotateShow (raw_tmp_packer_path, raw_tmp_index_path)

  tmp_packer_path <- H.evalIO $ Path.parseAbsFile raw_tmp_packer_path
  tmp_index_path <- H.evalIO $ Path.parseAbsFile raw_tmp_index_path
  H.annotateShow (tmp_packer_path, tmp_index_path)

  ps <- liftIO $ mkPackingState [absdir|/tmp|] -- Bracket version is too hard to use in PropertyT (ResourceT IO).
  handleAll (\e -> liftIO (destroyPackingState ps) >> throwM e) $ do
    liftIO $ do
      mapM_ (putToPack ps) inputs
      void $ extractPacker ps tmp_packer_path

    if null inputs
      then do
        (H.assert . isNothing) =<< liftIO (extractPackedIndex ps tmp_index_path)
      else do
        (H.assert . isJust) =<< liftIO (extractPackedIndex ps tmp_index_path)
        (H.assert . isNothing) =<< liftIO (extractPackedIndex ps tmp_index_path)
        (H.assert . isNothing) =<< liftIO (extractPackedIndex ps tmp_index_path)
  liftIO $ destroyPackingState ps

prop_extract_packed_index :: H.Property
prop_extract_packed_index = H.property . Morph.hoist runResourceT $ do
  inputs <- fmap nub $ H.forAll $ Gen.list (Rng.constant 0 1) $ fmap BL.fromStrict $ Gen.bytes $ Rng.linear 0 1024

  H.cover 10 "without input" $ null inputs
  H.cover 10 "has input" $ not $ null inputs

  -- Note: use MonadReource after forAll for correct behaviour, might be a bug.
  raw_tmp_packer_path <- alloc_tmp_file "packer"
  H.annotateShow raw_tmp_packer_path

  tmp_packer_path <- H.evalIO $ Path.parseAbsFile raw_tmp_packer_path
  H.annotateShow tmp_packer_path

  ps <- liftIO $ mkPackingState [absdir|/tmp|] -- Bracket version is too hard to use in PropertyT (ResourceT IO).
  handleAll (\e -> liftIO (destroyPackingState ps) >> throwM e) $ do
    liftIO $ mapM_ (putToPack ps) inputs
    if null inputs
      then do
        (H.assert . isNothing) =<< liftIO (extractPacker ps tmp_packer_path)
      else do
        (H.assert . isJust) =<< liftIO (extractPacker ps tmp_packer_path)
        (H.assert . isNothing) =<< liftIO (extractPacker ps tmp_packer_path)
        (H.assert . isNothing) =<< liftIO (extractPacker ps tmp_packer_path)
  liftIO $ destroyPackingState ps

alloc_tmp_file :: (H.MonadTest m, MonadCatch m, R.MonadResource m) => String -> m FilePath
alloc_tmp_file name =
  fmap snd . H.evalM $
    R.allocate
      ( do
          (p, h) <- openBinaryTempFile "/tmp" name
          hClose h
          pure p
      )
      removeFile
