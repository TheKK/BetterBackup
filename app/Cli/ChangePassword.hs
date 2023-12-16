{-# LANGUAGE OverloadedStrings #-}

module Cli.ChangePassword (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  helper,
  info,
  progDesc,
 )

import Control.Monad.Catch (bracket_, throwM, try)

import Path qualified

import Data.Foldable (fold, for_)
import Data.Function (fix)

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Data.ByteString qualified as BS

import System.Directory qualified as D
import System.IO (hGetEcho, hSetEcho, stdin)
import System.IO.Error (isUserError)

import Config (AES128Config (AES128Config), CipherConfig (CipherConfigAES128))
import Config qualified
import Crypto qualified
import LocalCache qualified

parser_info :: Options.Applicative.ParserInfo (IO ())
parser_info = Options.Applicative.info (Options.Applicative.helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ Options.Applicative.progDesc "Change password of repository"
        ]

    parser = pure go

    go = do
      cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
      config <- LocalCache.readConfig cwd

      opt_new_cipher_config <- case Config.config_cipher config of
        Config.CipherConfigNoCipher -> Nothing <$ T.putStrLn "There's no password for you to change. The repository has no encryption."
        Config.CipherConfigAES128 cfg -> Just . Config.CipherConfigAES128 <$> renew_aes128_config cfg

      for_ opt_new_cipher_config $ \new_cipher_config ->
        LocalCache.overwrite (Path.Abs cwd) $ config{Config.config_cipher = new_cipher_config}

renew_aes128_config :: AES128Config -> IO AES128Config
renew_aes128_config
  AES128Config
    { Config.aes128_salt = old_salt
    , Config.aes128_verify = old_verify
    , Config.aes128_secret = old_cipher_secret
    } = do
    old_pass <- fix $ \rec -> do
      old_pass <- do
        T.putStrLn "Please enter old password:"
        getline_from_stdin_wihout_echo
      ret <- try $ Crypto.decryptAndVerifyAES128Key old_salt old_pass old_verify old_cipher_secret
      case ret of
        Left err | isUserError err -> do
          T.putStrLn $ "Failed to verify old password: " <> T.pack (show err)
          T.putStrLn "Please try again"
          rec
        Left err -> throwM err
        Right _ -> do
          T.putStrLn "Correct password!"
          pure old_pass

    T.putStrLn "Please enter new password"
    new_pass <- ask_for_new_password_from_stdin

    new_salt <- Crypto.generateAES128KeyFromEntropy
    (new_verify, new_cipher_secret) <- Crypto.renewVerificationBytesAndAES128Key (old_salt, old_pass) (new_salt, new_pass) old_cipher_secret

    pure $!
      AES128Config
        { Config.aes128_salt = new_salt
        , Config.aes128_verify = new_verify
        , Config.aes128_secret = new_cipher_secret
        }

ask_for_new_password_from_stdin :: IO BS.ByteString
ask_for_new_password_from_stdin = do
  a <- ask_one "enter new password: "
  b <- ask_one "enter new password again: "
  if a == b
    then pure a
    else do
      T.putStrLn "entered passwords are different, please enter again"
      ask_for_new_password_from_stdin
  where
    ask_one prompt = do
      T.putStrLn prompt
      getline_from_stdin_wihout_echo

getline_from_stdin_wihout_echo :: IO BS.ByteString
getline_from_stdin_wihout_echo = do
  old_echo <- hGetEcho stdin
  bracket_ (hSetEcho stdin False) (hSetEcho stdin old_echo) BS.getLine
