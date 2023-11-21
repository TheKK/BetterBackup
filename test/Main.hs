{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Main (
  main,
) where

import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.Hedgehog (fromGroup, testProperty)

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Hedgehog (Property, annotateShow, cover, diff, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import qualified Crypto.Cipher.Types as Cipher

import Data.Foldable (for_)
import Data.Function ((&))

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import qualified Streamly.Data.Stream as S
import Streamly.External.ByteString

import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr, that_aes)
import Better.Streamly.FileSystem.Chunker (props_distribute, props_fast_cdc)
import Better.Data.FileSystemChanges (props_filesystem_change)

main :: IO ()
main =
  defaultMain . testGroup "tests" $
    [ fromGroup props_distribute
    , fromGroup props_fast_cdc
    , fromGroup props_filesystem_change
    , testProperty "ctr enc & dec" prop_ctr_enc_dec
    , testProperty "compact" prop_compact
    ]

-- TODO Move this property to its own module.
prop_ctr_enc_dec :: Property
prop_ctr_enc_dec = property $ do
  aes <- liftIO that_aes
  let iv = Cipher.nullIV

  plain_text_chunks <-
    forAll $
      Gen.list (Range.linear 0 126) $
        Gen.frequency
          [ (10, Gen.bytes (Range.linear 0 512))
          , (1, pure "")
          ]

  batch_size <- fmap ((* Cipher.blockSize aes) . (+ 1)) $ forAll $ Gen.int (Range.linear 1 512)

  cover 3 "blocksize < 1024" $ batch_size < 1024
  cover 3 "blocksize >= 1024" $ batch_size >= 1024
  cover 3 "blocksize >= 4096" $ batch_size >= 4096

  cover 5 "contain empty bs" $ any BS.null plain_text_chunks
  cover 20 "contain non-empty bs" $ not (all BS.null plain_text_chunks)
  cover 20 "total text greater than batch_size" $ sum (BS.length <$> plain_text_chunks) > batch_size

  plain_text_chunks' <-
    liftIO $
      S.fromList plain_text_chunks
        & fmap toArray
        & encryptCtr aes iv batch_size
        & decryptCtr aes batch_size
        & S.toList

  BL.fromChunks plain_text_chunks === BL.fromChunks (fmap fromArray plain_text_chunks')

-- TODO Move this property to its own module.
prop_compact :: Property
prop_compact = property $ do
  plain_text_chunks <-
    forAll $
      Gen.list (Range.linear 1 128) $
        Gen.frequency
          [ (10, Gen.bytes (Range.linear 1 512))
          , (1, pure "")
          ]

  let total_length = sum (fmap BS.length plain_text_chunks)
  cover 30 "total length >= 1024" $ total_length >= 1024
  cover 10 "input chunks >= 30" $ length plain_text_chunks >= 30

  chunk_size <- forAll $ fmap succ $ Gen.int $ Range.linear 1 256

  cover 10 "chunk_size < total_length" $ chunk_size < total_length
  cover 1 "chunk_size >= total_length" $ chunk_size >= total_length

  compacted_chunks <- liftIO $ S.fromList plain_text_chunks & fmap toArray & compact chunk_size & S.toList
  annotateShow compacted_chunks

  mconcat plain_text_chunks === mconcat (fmap fromArray $ compacted_chunks)

  when (length compacted_chunks >= 2) $ do
    for_ (init compacted_chunks) ((chunk_size ===) . BS.length . fromArray)

    let last_chunk = last compacted_chunks
    annotateShow last_chunk
    diff (BS.length $ fromArray last_chunk) (<=) chunk_size
