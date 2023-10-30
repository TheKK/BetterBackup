{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Better.Internal.Streamly.Crypto.AES (
  encryptCtr,
  decryptCtr,
  unsafeEncryptCtr,
  unsafeDecryptCtr,
  that_aes,
  compact,
)
where

import Control.Exception (assert)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Data.Function (fix)
import Data.Word (Word8)

import Foreign (plusPtr)
import Fusion.Plugin.Types (Fuse (Fuse))

import qualified Control.Monad.State.Strict as ST

import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS

import Crypto.Cipher.AES (AES128)
import qualified Crypto.Cipher.AES as AES
import Crypto.Cipher.Types (BlockCipher (blockSize))
import qualified Crypto.Cipher.Types as Cipher
import Crypto.Error (CryptoFailable (CryptoFailed, CryptoPassed))
import Crypto.Hash.Algorithms (SHA256 (SHA256))

import qualified Crypto.KDF.PBKDF2 as PBKDF2
import qualified Streamly.Data.Array as Array
import qualified Streamly.Data.MutArray as MA
import qualified Streamly.Internal.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Mut.Type as MA
import qualified Streamly.Internal.Data.Stream as D

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA, un_array_ba), MutArrayBA (MutArrayBA))

{-# DEPRECATED that_passwd "dev only variable" #-}
that_passwd :: SBS.ShortByteString
that_passwd = "happyMeal"

{-# DEPRECATED that_salt "dev only variable" #-}
that_salt :: SBS.ShortByteString
that_salt = ";\t\222bK \217\241\139\"&3\221\&4\210-"

{-# DEPRECATED that_aes "dev only variable" #-}
that_aes :: IO AES.AES128
that_aes =
  case Cipher.cipherInit @_ @BS.ByteString (gen_key (undefined :: AES128) (SBS.fromShort that_passwd) (SBS.fromShort that_salt)) of
    CryptoPassed aes -> pure aes
    CryptoFailed err -> error $ show err

gen_key
  :: (BlockCipher cipher, BA.ByteArray ba, BA.ByteArrayAccess salt, BA.ByteArrayAccess password)
  => cipher
  -> password
  -> salt
  -> ba
gen_key ~cipher = PBKDF2.generate prf params
  where
    prf = PBKDF2.prfHMAC SHA256
    params = PBKDF2.Parameters{PBKDF2.outputLength = blockSize cipher, PBKDF2.iterCounts = 100000}

encryptCtr :: (BlockCipher cipher) => cipher -> Cipher.IV cipher -> Int -> D.Stream IO (Array.Array Word8) -> D.Stream IO (Array.Array Word8)
encryptCtr cipher iv0 batch_size = unsafeEncryptCtr cipher iv0 batch_size . compact batch_size
{-# INLINE encryptCtr #-}

-- | Unsafe: size of input @ba@ should always has size of @batch_size@ except that last chunk.
unsafeEncryptCtr :: (BlockCipher cipher) => cipher -> Cipher.IV cipher -> Int -> D.Stream IO (Array.Array Word8) -> D.Stream IO (Array.Array Word8)
unsafeEncryptCtr cipher iv0 batch_size (D.Stream inner_step inner_s0) = D.Stream step s0
  where
    iv_offset = batch_size `div` Cipher.blockSize cipher
    s0 = (inner_s0, iv0, Just iv0) -- TODO this is faster than using cons
    {-# INLINE [0] step #-}
    step ~st (s', iv, Nothing) = do
      ret <- inner_step st s'
      case ret of
        D.Yield ba next_s -> pure $ D.Yield (un_array_ba $ Cipher.ctrCombine cipher iv $ ArrayBA ba) (next_s, Cipher.ivAdd iv iv_offset, Nothing)
        D.Skip next_s -> pure $ D.Skip (next_s, iv, Nothing)
        D.Stop -> pure D.Stop
    step ~_ (s', iv, Just ivv) = pure $ D.Yield (un_array_ba $ BA.convert ivv) (s', iv, Nothing)
{-# INLINE unsafeEncryptCtr #-}

decryptCtr :: (BlockCipher cipher) => cipher -> Int -> D.Stream IO (Array.Array Word8) -> D.Stream IO (Array.Array Word8)
decryptCtr cipher batch_size = unsafeDecryptCtr cipher batch_size . compact batch_size
{-# INLINE decryptCtr #-}

{-# ANN type DecStage Fuse #-}
data DecStage cipher = DecReadIV | DecDec !(Cipher.IV cipher)

-- | Unsafe: size of input @ba@ should always has size of @batch_size@ except that last chunk.
unsafeDecryptCtr :: (BlockCipher cipher) => cipher -> Int -> D.Stream IO (Array.Array Word8) -> D.Stream IO (Array.Array Word8)
unsafeDecryptCtr cipher batch_size (D.Stream inner_step inner_s0) = D.Stream step s0
  where
    iv_len = Cipher.blockSize cipher
    iv_offset = batch_size `div` Cipher.blockSize cipher

    s0 = (inner_s0, DecReadIV)

    {-# INLINE [0] step #-}
    step ~st (s', DecReadIV) = do
      ret <- inner_step st s'
      case fmap ArrayBA ret of
        D.Yield ba next_s -> do
          when (BA.length ba < Cipher.blockSize cipher) $
            error "oh no iv not enough"

          iv <- case Cipher.makeIV (BA.take iv_len ba) of
            Nothing -> error "oh no"
            Just iv -> pure iv

          pure $
            if BA.length ba == Cipher.blockSize cipher
              then D.Skip (next_s, DecDec iv)
              else D.Yield (un_array_ba $ Cipher.ctrCombine cipher iv $ BA.drop iv_len ba) (next_s, DecDec $ Cipher.ivAdd iv (iv_offset - 1))
        D.Skip next_s -> pure $ D.Skip (next_s, DecReadIV)
        D.Stop -> pure D.Stop
    step ~st (s', DecDec iv) = do
      ret <- inner_step st s'
      case ret of
        D.Yield ba next_s -> pure $ D.Yield (un_array_ba $ Cipher.ctrCombine cipher iv $ ArrayBA ba) (next_s, DecDec $ Cipher.ivAdd iv iv_offset)
        D.Skip next_s -> pure $ D.Skip (next_s, DecDec iv)
        D.Stop -> pure D.Stop
{-# INLINE unsafeDecryptCtr #-}

data BufReadEnv ba s = BufReadEnv
  { _br_next :: !(s -> IO (D.Step s ba))
  , _br_innerStreamState :: !(Maybe s)
  , _br_buf :: !ba
  , br_bufBegin :: !Int
  }

fill_buffer :: (BA.ByteArray ba) => ST.StateT (BufReadEnv ba s) IO (BA.View ba)
fill_buffer = do
  BufReadEnv next opt_s buf begin <- ST.get
  let buf_rest_len = BA.length buf - begin
  if buf_rest_len > 0
    then pure $ BA.dropView buf begin
    else case opt_s of
      Just s0 -> flip fix s0 $ \(~rec) s -> do
        ret <- liftIO $ next s
        case ret of
          D.Yield next_buf next_s ->
            if not $ BA.null next_buf
              then do
                ST.put $ BufReadEnv next (Just next_s) next_buf 0
                pure $ BA.dropView next_buf 0
              else rec next_s
          D.Skip next_s -> rec next_s
          D.Stop -> do
            ST.put $ BufReadEnv next Nothing buf begin
            pure $ BA.takeView buf 0
      Nothing -> pure $ BA.takeView buf 0
{-# INLINE fill_buffer #-}

unsafe_copy_to_muarr :: BA.ByteArrayAccess ba => ba -> MA.MutArray Word8 -> IO (MA.MutArray Word8)
unsafe_copy_to_muarr ba buf = assert (MA.bytesFree buf >= BA.length ba) $ BA.withByteArray (MutArrayBA buf) $ \buf_ptr -> do
  BA.copyByteArrayToPtr ba (buf_ptr `plusPtr` MA.arrEnd buf)
  pure $ buf{MA.arrEnd = MA.arrEnd buf + BA.length ba}
{-# INLINE unsafe_copy_to_muarr #-}

compact :: Int -> D.Stream IO (Array.Array Word8) -> D.Stream IO (Array.Array Word8)
compact n (D.Stream inner_step inner_s0) = D.Stream step s0
  where
    s0 = (Just inner_s0, BA.empty, 0 :: Int)

    {-# INLINE [0] step #-}
    step ~st (opt_s, buf, begin) = do
      marr0 <- MA.newPinned @_ @Word8 n
      flip fix (marr0, BufReadEnv (fmap (fmap ArrayBA) . inner_step st) opt_s buf begin) $ \(~rec) (marr, buf_read_env) -> do
        let count_to_read = MA.bytesFree marr

        (arr, next_buf_read_env@(BufReadEnv _ next_opt_s next_buf next_begin)) <- ST.runStateT fill_buffer buf_read_env
        if BA.length arr /= 0
          then case BA.length arr `compare` MA.bytesFree marr of
            LT -> do
              marr' <- unsafe_copy_to_muarr arr marr
              rec (marr', next_buf_read_env{br_bufBegin = next_begin + BA.length arr})
            _ -> do
              marr' <- unsafe_copy_to_muarr (BA.takeView arr count_to_read) marr
              let item = Array.unsafeFreeze marr'
              pure $ D.Yield item (next_opt_s, next_buf, next_begin + count_to_read)
          else
            if MA.length marr /= 0
              then do
                let item = Array.unsafeFreeze marr
                pure $ D.Yield item (next_opt_s, next_buf, next_begin)
              else pure D.Stop
{-# INLINE compact #-}
