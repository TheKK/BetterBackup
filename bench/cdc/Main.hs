{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Criterion.Main (bench, bgroup, defaultMain, env, nf, nfAppIO, nfIO, whnf, whnfAppIO)

import Data.Bits (Bits (shiftL, (.&.)), FiniteBits (countLeadingZeros))
import Data.Foldable (for_)
import Data.Function ((&))
import Data.Word (Word64)

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import qualified Crypto.Cipher.Types as Cipher

import qualified Data.ByteString as BS

import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr, that_aes, unsafeEncryptCtr)
import Better.Streamly.FileSystem.Chunker (defaultGearHashConfig, gearHash)

-- 50 MiB
input :: [BS.ByteString]
input = replicate 1638400 ("asdfjkla;sdfk;sdjl;asdjfl;aklsd;" :: BS.ByteString)

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
    ]
  where
    file = "data2"
