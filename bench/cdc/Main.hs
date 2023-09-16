{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Criterion.Main (bench, bgroup, defaultMain, env, nf, nfAppIO, whnf, whnfAppIO)
import Criterion.Types (Benchmark)

import Data.Bits (Bits (shiftL, (.&.)), FiniteBits (countLeadingZeros))
import Data.Foldable (Foldable (foldl'), for_)
import Data.Function ((&))
import qualified Data.HashSet as HashSet
import qualified Data.Set as Set
import Data.Word (Word64)

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

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

import qualified Better.Hash as Hash
import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr, that_aes, unsafeEncryptCtr)
import Better.Streamly.FileSystem.Chunker (defaultGearHashConfig, gearHash)
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import Data.Functor.Identity (Identity (runIdentity))
import qualified Streamly.Internal.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.Handle as Handle
import Streamly.Internal.System.IO (defaultChunkSize)
import System.IO (IOMode (ReadMode), withFile)

import qualified System.Random as Rng
import qualified System.Random.SplitMix as Sp

import Control.Concurrent.STM
import qualified Data.ByteArray as BA
import Data.IORef
import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Mut.Type as MA
import qualified Streamly.Internal.Data.Array.Type as Array

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
        , bench "gear-then-read-with-file" $
            whnfAppIO
              ( \file' ->
                  File.readChunks file'
                    & fmap ArrayBA
                    & Chunker.gearHashPure defaultGearHashConfig
                    & S.mapM (\(!a) -> pure a)
                    & S.fold F.latest
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
          [ bench "sha2-256" $
              nf
                (\i -> S.fromList i & S.fold Hash.hashByteStringFold & runIdentity)
                input'
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
          bgroup
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
                  (S.fromList input')
            , bench "encryptCtr-compact-32b" $
                nfAppIO
                  ( \inputs ->
                      inputs
                        & encryptCtr aes Cipher.nullIV 32
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
                  )
                  (S.fromList input')
            , bench "encryptCtr-compact-4k" $
                nfAppIO
                  ( \inputs ->
                      inputs
                        & encryptCtr aes Cipher.nullIV (1024 * 4)
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
                  )
                  (S.fromList input')
            , bench "encryptCtr-compact-32k" $
                nfAppIO
                  ( \inputs ->
                      inputs
                        & encryptCtr aes Cipher.nullIV (1024 * 32)
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
                  )
                  (S.fromList input')
            , bench "encryptCtr-compact-64k" $
                nfAppIO
                  ( \inputs ->
                      inputs
                        & encryptCtr aes Cipher.nullIV (1024 * 64)
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
                  )
                  (S.fromList input')
            , bench "encryptCtr-compact-128k" $
                nfAppIO
                  ( \inputs ->
                      inputs
                        & encryptCtr aes Cipher.nullIV (1024 * 128)
                        & S.mapM (\(!a) -> pure a)
                        & S.fold F.drain
                  )
                  (S.fromList input')
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
                  (S.fromList input')
            ]
    , env (pure input) $ \input' ->
        bgroup
          "compact"
          [ bench "16k" $
              nfAppIO
                ( \inputs ->
                    inputs
                      & compact (1024 * 16)
                      & S.mapM (\(!a) -> pure a)
                      & S.fold F.drain
                )
                (S.fromList input')
          , bench "32k" $
              nfAppIO
                ( \inputs ->
                    inputs
                      & compact (1024 * 32)
                      & S.mapM (\(!a) -> pure a)
                      & S.fold F.drain
                )
                (S.fromList input')
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
    ]
  where
    file = "data2"

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

hashByteStringFoldX :: (HashAlgorithm a, Monad m) => F.Fold m BS.ByteString (Digest a)
hashByteStringFoldX = hashFinalize <$> F.foldl' hashUpdate hashInit
{-# INLINE hashByteStringFoldX #-}
