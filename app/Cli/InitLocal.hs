{-# LANGUAGE OverloadedStrings #-}

module Cli.InitLocal (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Control.Exception (bracket_)

import Path qualified

import Data.Foldable (Foldable (fold))

import Data.Text.Encoding qualified as TE
import Data.Text.IO qualified as T

import Data.ByteString qualified as BS

import System.Directory qualified as D
import System.IO (hGetEcho, hSetEcho, stdin)

import Config (AES128Config (AES128Config), CipherConfig (CipherConfigAES128), Config (Config))
import Config qualified
import Crypto (encryptAndVerifyAES128Key, generateAES128KeyFromEntropy)
import LocalCache qualified
import Util.Options (absDirRead)

parser_info :: Options.Applicative.ParserInfo (IO ())
parser_info = Options.Applicative.info (Options.Applicative.helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ Options.Applicative.progDesc "Initialize with local repository"
        ]

    parser =
      go
        <$> argument
          absDirRead
          ( fold
              [ metavar "REPO_PATH"
              , help "path to store your backup"
              ]
          )

    go repo_path = do
      cache_path <- fmap Path.Abs . Path.parseAbsDir =<< D.getCurrentDirectory

      T.putStrLn "Please enter your password:"
      user_passwd <- read_passworld_from_stdin
      user_salt <- generateAES128KeyFromEntropy

      plain_secret <- generateAES128KeyFromEntropy

      (verification_bytes, cipher_secret) <- encryptAndVerifyAES128Key user_salt user_passwd plain_secret

      let
        local_repo_config = Config.Local $ Config.LocalRepoConfig repo_path
        aes_cipher_config = CipherConfigAES128 (AES128Config user_salt cipher_secret verification_bytes)

      LocalCache.initialize cache_path $ Config local_repo_config aes_cipher_config

read_passworld_from_stdin :: IO BS.ByteString
read_passworld_from_stdin = do
  a <- ask_one "enter password: "
  b <- ask_one "enter password again: "
  if a == b
    then pure a
    else do
      T.putStrLn "entered passwords are different, please enter again"
      read_passworld_from_stdin
  where
    ask_one prompt = do
      old_echo <- hGetEcho stdin
      bracket_ (hSetEcho stdin False) (hSetEcho stdin old_echo) $ do
        T.putStrLn prompt
        TE.encodeUtf8 <$> T.getLine
