{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Criterion.Main (bench, bgroup, defaultMain, env, nf, nfAppIO, whnf, whnfAppIO)
import Criterion.Types (Benchmark)

import Data.Bits (Bits (shiftL, (.&.)), FiniteBits (countLeadingZeros))
import Data.Foldable (Foldable (foldl'), for_)
import Data.Function ((&))
import qualified Data.HashSet as HashSet
import qualified Data.Set as Set
import Data.Word (Word64, Word8)

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import Streamly.External.ByteString (toArray)

import Crypto.Hash (
  Blake2b_256,
  Blake2bp,
  Blake2s,
  Blake2sp,
  Digest,
  HashAlgorithm,
  MD5,
  SHA1,
  SHA256 (SHA256),
  SHA3_256,
  hashFinalize,
  hashInit,
  hashInitWith,
  hashUpdate,
 )

import qualified Crypto.Cipher.Types as Cipher

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS16

import qualified Better.Hash as Hash
import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr, that_aes, unsafeEncryptCtr)
import Better.Streamly.FileSystem.Chunker (defaultGearHashConfig, gearHash, gearHashConfig, gearHashWithFileUnfold)
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import Data.Functor.Identity (Identity (runIdentity))
import qualified Streamly.Internal.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.Handle as Handle
import Streamly.Internal.System.IO (defaultChunkSize)
import System.IO (IOMode (ReadMode), withFile)

import qualified System.Random as Rng
import qualified System.Random.SplitMix as Sp

import Control.Monad.ST (runST)

import Better.Internal.Streamly.Array (fastArrayAsPtrUnsafe, fastMutArrayAsPtrUnsafe)
import Control.Concurrent.STM
import qualified Data.Base16.Types as Base16
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString.Short.Base16 as BSS16
import Data.IORef
import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Mut.Type as MutArray
import qualified Streamly.Internal.Data.Array.Type as Array

import qualified Parser

newtype ArrayBA = ArrayBA {un_array_ba :: Array.Array Word8}
  deriving (Eq, Ord)

instance BA.ByteArrayAccess ArrayBA where
  length (ArrayBA arr) = Array.byteLength arr
  {-# INLINE length #-}
  withByteArray (ArrayBA arr) = Array.asPtrUnsafe (Array.castUnsafe arr)
  {-# INLINE withByteArray #-}

-- 50 MiB = 1600 * 32KiB
{-# NOINLINE input #-}
input :: [BS.ByteString]
input = replicate 1600 $ BS.concat $ replicate 1024 ("asdfjkla;sdfk;sdjl;asdjfl;aklsd;" :: BS.ByteString)

-- 50 MiB = 1600 * 32KiB
{-# NOINLINE input_array #-}
input_array :: [Array.Array Word8]
input_array = replicate 1600 $ toArray $ BS.concat $ replicate 1024 ("asdfjkla;sdfk;sdjl;asdjfl;aklsd;" :: BS.ByteString)

-- 50 MiB = 1600 * 32KiB
{-# NOINLINE input_30KiB #-}
input_30KiB :: [BS.ByteString]
input_30KiB = replicate 1707 $ BS.concat $ replicate 1024 ("asdfjkla;sdfk;sdjl;asdjfl;akl;" :: BS.ByteString)

-- 50 MiB = 1600 * 32KiB
{-# NOINLINE input_array_30KiB #-}
input_array_30KiB :: [Array.Array Word8]
input_array_30KiB = replicate 1707 $ toArray $ BS.concat $ replicate 1024 ("asdfjkla;sdfk;sdjl;asdjfl;akl;" :: BS.ByteString)

main :: IO ()
main =
  defaultMain
    [ bgroup
        "chunks"
        [ bench "gear" $
            whnfAppIO
              ( \file' ->
                  gearHash defaultGearHashConfig file'
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.drain
              )
              file
        , bench "gear-new-fd-32KiB-level1" $
            whnfAppIO
              ( \file' -> withFile file' ReadMode $ \h ->
                  S.unfold (gearHashWithFileUnfold $ gearHashConfig 1 (32 * 2 ^ (10 :: Int))) h
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.drain
              )
              file
        , bench "gear-new-fd-16KiB-level1" $
            whnfAppIO
              ( \file' -> withFile file' ReadMode $ \h ->
                  S.unfold (gearHashWithFileUnfold $ gearHashConfig 1 (16 * 2 ^ (10 :: Int))) h
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.drain
              )
              file
        , bench "gear-pure-input-IO-50MiB" $
            nfAppIO
              ( \input' ->
                  S.fromList input'
                    & Chunker.gearHashPure defaultGearHashConfig
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.drain
              )
              input
        , bench "gear-pure-input-Identity-50MiB" $
            nf
              ( \input' ->
                  S.fromList input'
                    & Chunker.gearHashPure defaultGearHashConfig
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.drain
                    & runIdentity
              )
              input
        , bench "gear-then-read-with-file" $
            whnfAppIO
              ( \file' ->
                  File.readChunks file'
                    & fmap ArrayBA
                    & Chunker.gearHashPure defaultGearHashConfig
                    & S.mapM
                      ( \(Chunker.Chunk b e) -> do
                          S.unfold File.chunkReaderFromToWith (b, e - 1, defaultChunkSize, file')
                            & S.fold F.toList
                      )
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.latest
              )
              file
        , bench "gear-then-read-with-fd" $
            whnfAppIO
              ( \file' ->
                  withFile file' ReadMode $ \fp -> do
                    gearHash defaultGearHashConfig file'
                      & S.mapM
                        ( \(Chunker.Chunk b e) -> do
                            S.unfold Handle.chunkReaderFromToWith (b, e - 1, defaultChunkSize, fp)
                              & S.fold F.toList
                        )
                      & S.mapM (\(!a) -> pure a)
                      & S.fold F.drain
              )
              file
        , bench "gear-new-fd-then-read-with-fd-32KiB-level1" $
            whnfAppIO
              ( \file' ->
                  withFile file' ReadMode $ \chunk_h ->
                    withFile file' ReadMode $ \read_h -> do
                      S.unfold (gearHashWithFileUnfold $ gearHashConfig 1 (32 * 2 ^ (10 :: Int))) chunk_h
                        & S.mapM
                          ( \(Chunker.Chunk b e) -> do
                              S.unfold Handle.chunkReaderFromToWith (b, e - 1, defaultChunkSize, read_h)
                                & S.fold F.toList
                          )
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
              )
              file
        , bench "gear-new-fd-then-read-with-fd-16KiB-level1" $
            whnfAppIO
              ( \file' ->
                  withFile file' ReadMode $ \chunk_h ->
                    withFile file' ReadMode $ \read_h -> do
                      S.unfold (gearHashWithFileUnfold $ gearHashConfig 1 (16 * 2 ^ (10 :: Int))) chunk_h
                        & S.mapM
                          ( \(Chunker.Chunk b e) -> do
                              S.unfold Handle.chunkReaderFromToWith (b, e - 1, defaultChunkSize, read_h)
                                & S.fold F.toList
                          )
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
              )
              file
        ]
    , bgroup
        "bits"
        ( let
            !mask = shiftL (maxBound :: Word64) 50
            !d = 0x12345 :: Word64
            !v = (2 :: Word64) ^ (13 :: Int)
          in
            [ bench "mask" $ whnf (\d' -> d' .&. mask == 0) d
            , bench "count_zero" $ whnf (\d' -> countLeadingZeros d' == 14) d
            , bench "less" $ whnf (< v) d
            ]
        )
    , env (pure input) $ \input' ->
        bgroup
          "hash"
          [ bench "Blake3-256-st" $
              nf
                (\i -> runST $ S.fromList i & S.fold Hash.hashByteStringFold)
                input'
          , bench "Blake3-256-io" $
              nfAppIO
                (\i -> S.fromList i & S.fold Hash.hashByteStringFoldIO)
                input'
          , bench "Blake3-256-io-non-aligned-30KiB" $
              nfAppIO
                (\i -> S.fromList i & S.fold Hash.hashByteStringFoldIO)
                input_30KiB
          , bench "sha2-256-fold" $
              nf
                (hashFinalize . foldl' hashUpdate (hashInitWith SHA256))
                input'
          , bench "sha1" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @SHA1) & runIdentity)
                input'
          , bench "md5" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @MD5) & runIdentity)
                input'
          , bench "sha3-256" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @SHA3_256) & runIdentity)
                input'
          , bench "Blake2s-256" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @(Blake2s 256)) & runIdentity)
                input'
          , bench "Blake2b-256" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @Blake2b_256) & runIdentity)
                input'
          , bench "Blake2bp-256" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @(Blake2bp 256)) & runIdentity)
                input'
          , bench "Blake2sp-256" $
              nf
                (\i -> S.fromList i & S.fold (hashByteStringFoldX @(Blake2sp 256)) & runIdentity)
                input'
          ]
    , env that_aes $ \aes ->
        env (pure input) $ \input' ->
          bgroup
            "enc-cbc"
            [ bench "single-cbc-encrypt" $
                let iv = Cipher.nullIV
                in  nf
                      (Cipher.cbcEncrypt aes iv)
                      (head input')
            , bench "single-cbc-dencrypt" $
                let iv = Cipher.nullIV
                in  nf
                      (Cipher.cbcDecrypt aes iv)
                      (head input')
            ]
    , env that_aes $ \aes ->
        env (pure input) $ \input' ->
          let input_array' = map toArray input'
          in  bgroup
                "enc-ctr"
                [ bench "single-ctrCombine" $
                    let iv = Cipher.nullIV
                    in  nf
                          (Cipher.ctrCombine aes iv)
                          (head input')
                , bench "ctrCombine-with-streamly" $
                    let iv = Cipher.nullIV
                    in  nfAppIO
                          ( \inputs ->
                              inputs
                                & fmap (Cipher.ctrCombine aes iv)
                                & S.mapM (\(!a) -> pure a)
                                & S.fold F.drain
                          )
                          (S.fromList input')
                , bench "ctrCombine-with-fmap" $
                    let iv = Cipher.nullIV
                    in  nfAppIO
                          ( \inputs -> for_ inputs $ \i -> do
                              let !ret = (\(!a) -> a) $ Cipher.ctrCombine aes iv i
                              pure ret
                          )
                          input'
                , bench "unsafeEncryptCtr" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & unsafeEncryptCtr aes Cipher.nullIV 1024
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-compact-32b" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV 32
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-compact-4k" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV (1024 * 4)
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-compact-32k" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV (1024 * 32)
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-compact-64k" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV (1024 * 64)
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-compact-128k" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV (1024 * 128)
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                , bench "encryptCtr-then-decryptCtr-compact-32k" $
                    nfAppIO
                      ( \inputs ->
                          inputs
                            & encryptCtr aes Cipher.nullIV (1024 * 32)
                            & S.mapM (\(!a) -> pure a)
                            & decryptCtr aes (1024 * 32)
                            & S.mapM (\(!a) -> pure a)
                            & S.fold F.drain
                      )
                      (S.fromList input_array')
                ]
    , env (pure input) $ \input' ->
        let input_array' = map toArray input'
        in  bgroup
              "compact"
              [ bench "16k" $
                  nfAppIO
                    ( \inputs ->
                        inputs
                          & compact (1024 * 16)
                          & S.mapM (\(!a) -> pure a)
                          & S.fold F.drain
                    )
                    (S.fromList input_array')
              , bench "32k" $
                  nfAppIO
                    ( \inputs ->
                        inputs
                          & compact (1024 * 32)
                          & S.mapM (\(!a) -> pure a)
                          & S.fold F.drain
                    )
                    (S.fromList input_array)
              , bench "32k-non-aligned" $
                  nfAppIO
                    ( \inputs ->
                        inputs
                          & compact (1024 * 32)
                          & S.mapM (\(!a) -> pure a)
                          & S.fold F.drain
                    )
                    (S.fromList input_array_30KiB)
              , bench "ref-noopt" $
                  nfAppIO
                    ( \inputs ->
                        inputs
                          & S.mapM (\(!a) -> pure a)
                          & S.fold F.drain
                    )
                    (S.fromList input')
              ]
    , bench_concurrent
    , bench_as_ptr
    , bench_base16
    , Parser.benchGroup
    ]
  where
    file = "data2"

bench_as_ptr :: Benchmark
bench_as_ptr =
  bgroup
    "as_ptr"
    [ bench "Streamly.Array" $
        whnfAppIO
          ( \array' -> Array.asPtrUnsafe array' (\ptr -> pure ())
          )
          array
    , bench "Streamly.MutArray" $
        whnfAppIO
          ( \array' -> MutArray.asPtrUnsafe array' (\ptr -> pure ())
          )
          mu_array
    , bench "fastArrayAsPtrUnsafe" $
        whnfAppIO
          ( \array' -> fastArrayAsPtrUnsafe array' (\ptr -> pure ())
          )
          array
    , bench "fastMutArrayAsPtrUnsafe" $
        whnfAppIO
          ( \array' -> fastMutArrayAsPtrUnsafe array' (\ptr -> pure ())
          )
          mu_array
    , bench "ByteString" $
        whnfAppIO
          ( \bs' -> BA.withByteArray bs' (\ptr -> pure ())
          )
          bs
    ]
  where
    array = Array.fromList (BS.unpack "sjdfklsdjfsdkfl")
    mu_array = Array.unsafeThaw array
    bs = "sjdfklsdjfsdkfl" :: BS.ByteString

bench_concurrent :: Benchmark
bench_concurrent = env (pure $ take 100000 $ Rng.randoms @Word64 (Sp.mkSMGen 123)) $ \vec' ->
  bgroup
    "concurrent"
    [ bench "modifyIORef'-set-insert" $
        nfAppIO
          ( \vec -> do
              set <- newIORef Set.empty
              for_ vec $ \v -> modifyIORef' set (Set.insert v)
              readIORef set
          )
          vec'
    , bench "atomicModifyIORef'-set-insert" $
        nfAppIO
          ( \vec -> do
              set <- newIORef Set.empty
              for_ vec $ \v -> atomicModifyIORef' set (\s -> (Set.insert v s, ()))
              readIORef set
          )
          vec'
    , bench "modifyTVar'-set-insert" $
        nfAppIO
          ( \vec -> do
              set <- newTVarIO Set.empty
              for_ vec $ \v -> atomically $ modifyTVar' set (Set.insert v)
              readTVarIO set
          )
          vec'
    , bench "modifyIORef'-hashset-insert" $
        nfAppIO
          ( \vec -> do
              set <- newIORef HashSet.empty
              for_ vec $ \v -> modifyIORef' set (HashSet.insert v)
              readIORef set
          )
          vec'
    , bench "atomicModifyIORef'-hashset-insert" $
        nfAppIO
          ( \vec -> do
              set <- newIORef HashSet.empty
              for_ vec $ \v -> atomicModifyIORef' set (\s -> (HashSet.insert v s, ()))
              readIORef set
          )
          vec'
    , bench "modifyTVar'-hashset-insert" $
        nfAppIO
          ( \vec -> do
              set <- newTVarIO HashSet.empty
              for_ vec $ \v -> atomically $ modifyTVar' set (HashSet.insert v)
              readTVarIO set
          )
          vec'
    ]

bench_base16 :: Benchmark
bench_base16 =
  bgroup
    "base16"
    [ env (pure ("asdfkj; lweqruasd fjkl;asdfjk as" :: BS.ByteString)) $ \bs ->
        bgroup
          "encode bytestring"
          [ bench "ByteArrayAccess" $ nf (BA.convertToBase @_ @BS.ByteString BA.Base16) bs
          , bench "base16-bytestring" $ nf (Base16.extractBase16 . BS16.encodeBase16') bs
          ]
    , env (pure ("asdfkj; lweqruasd fjkl;asdfjk as" :: BSS.ShortByteString)) $ \bs ->
        bgroup
          "encode shortbytestring"
          [ bench "ByteArrayAccess" $ nf (BA.convertToBase @_ @BS.ByteString BA.Base16 . BSS.fromShort) bs
          , bench "base16-bytestring" $ nf (Base16.extractBase16 . BSS16.encodeBase16') bs
          ]
    ]

hashByteStringFoldX :: (HashAlgorithm a, Monad m) => F.Fold m BS.ByteString (Digest a)
hashByteStringFoldX = hashFinalize <$> F.foldl' hashUpdate hashInit
{-# INLINE hashByteStringFoldX #-}
