{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Main (
  main,
) where

import Better.Data.FileSystemChanges (props_filesystem_change)
import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr)
import Better.Repository.Backup (props_what_to_do_with_file_and_dir)
import Better.Streamly.FileSystem.Chunker (props_distribute, props_fast_cdc)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Crypto.Cipher.AES (AES128)
import Crypto.Cipher.Types qualified as Cipher
import Crypto.Error (CryptoFailable (CryptoFailed, CryptoPassed))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BL
import Data.Foldable (for_)
import Data.Function ((&))
import Hedgehog (Property, annotateShow, cover, diff, forAll, property, (===))
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Streamly.Data.Stream qualified as S
import Streamly.External.ByteString (fromArray, toArray)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.Hedgehog (fromGroup, testProperty)

main :: IO ()
main =
  defaultMain . testGroup "tests" $
    [ fromGroup props_distribute
    , fromGroup props_fast_cdc
    , fromGroup props_filesystem_change
    , fromGroup props_what_to_do_with_file_and_dir
    , testProperty "ctr enc & dec" prop_ctr_enc_dec
    , testProperty "compact" prop_compact
    ]

gen_aes128 :: IO AES128
gen_aes128 = case Cipher.cipherInit @AES128 @BS.ByteString "just for testxxx" of
  CryptoPassed aes -> pure aes
  CryptoFailed err -> error $ show err

-- TODO Move this property to its own module.
prop_ctr_enc_dec :: Property
prop_ctr_enc_dec = property $ do
  aes <- liftIO gen_aes128
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
