{-# LANGUAGE OverloadedStrings #-}

module Crypto (
  decryptAndVerifyAES128Key,
)
where

import Data.ByteArray qualified as BA
import Data.ByteString qualified as BS

import Control.Exception (throwIO)
import Control.Monad (unless)

import Crypto.Cipher.AES (AES128)
import Crypto.Cipher.Types (BlockCipher (blockSize))
import Crypto.Cipher.Types qualified as Cipher
import Crypto.Error (CryptoFailable (CryptoFailed, CryptoPassed))
import Crypto.Hash.Algorithms (SHA256 (SHA256))
import Crypto.KDF.PBKDF2 qualified as PBKDF2

pbkdf2
  :: (BlockCipher cipher, BA.ByteArray ba, BA.ByteArrayAccess salt, BA.ByteArrayAccess password)
  => cipher
  -> password
  -> salt
  -> ba
pbkdf2 cipher = PBKDF2.generate prf params
  where
    prf = PBKDF2.prfHMAC SHA256
    params = PBKDF2.Parameters{PBKDF2.outputLength = blockSize cipher, PBKDF2.iterCounts = 100000}

decryptAndVerifyAES128Key :: BS.ByteString -> BS.ByteString -> BS.ByteString -> BS.ByteString -> IO AES128
decryptAndVerifyAES128Key salt passwd cipher_verify secret = do
  let key_from_password = pbkdf2 (undefined :: AES128) passwd salt
  aes_from_user_password <- case Cipher.cipherInit @_ @BS.ByteString key_from_password of
    CryptoPassed aes -> pure aes
    CryptoFailed err -> error $ show err

  -- Constant time eq: for security reason.
  -- First 4 bytes (32 bits): increase complexity to brutla force user passworld. The rest bits are hidden.
  unless (BA.constEq (generate_cipher_verification_bytes aes_from_user_password) cipher_verify) $
    throwIO $
      userError "wrong password or salt"

  decrypt_secret aes_from_user_password secret

decrypt_secret :: BlockCipher cipher => cipher -> BS.ByteString -> IO AES128
decrypt_secret cipher cipher_secret =
  case Cipher.cipherInit (Cipher.ctrCombine cipher iv_for_secret cipher_secret) of
    CryptoPassed aes -> pure $! aes
    CryptoFailed err -> error $ show err

-- | Use given block cipher to encrypt plain secret.
encrypt_secret :: BlockCipher cipher => cipher -> BS.ByteString -> BS.ByteString
encrypt_secret cipher plain_secret = Cipher.ctrCombine cipher iv_for_secret plain_secret

iv_for_secret :: BlockCipher cipher => Cipher.IV cipher
iv_for_secret = Cipher.ivAdd Cipher.nullIV 99987

generate_cipher_verification_bytes :: AES128 -> BS.ByteString
generate_cipher_verification_bytes aes = BS.take 4 $ Cipher.ctrCombine aes Cipher.nullIV verification_plain_text
  where
    verification_plain_text :: BS.ByteString
    verification_plain_text = "harder, better, faster, stronger"
