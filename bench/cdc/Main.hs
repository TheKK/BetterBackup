{-# LANGUAGE BangPatterns #-}

module Main where

import Criterion.Main

import Data.Word
import Data.Bits
import Data.Function ((&))

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import Better.Streamly.FileSystem.Chunker

main :: IO ()
main = defaultMain
  [ bgroup "chunks"
      [ bench "gear" $ nfIO $ (gearHash defaultGearHashConfig file & S.fold F.length)
      ]
  , bgroup "bits" (

      let
        !mask = shiftL (maxBound :: Word64) 50
        !d = 0x12345 :: Word64
        !v = (2 :: Word64)^(13 :: Int)
      in
        [ bench "mask" $ whnf (\d' -> d' .&. mask == 0) d
        , bench "count_zero" $ whnf (\d' -> countLeadingZeros d' == 14) d
        , bench "less" $ whnf (\d' -> d' < v) d
        ]
    )
  ]
  where
    file = "data2"
