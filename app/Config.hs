{-# LANGUAGE OverloadedStrings #-}

module Config (
  parseConfig,
  renderConfig,
  Config (..),
  RepoType (..),
  LocalRepoConfig (..),
  CipherConfig (..),
  AES128Config (..),
) where

import Control.Applicative ((<|>))

import Data.Bifunctor qualified as Bi
import Data.ByteString qualified as BS
import Data.Text qualified as T

import Path (Path)
import Path qualified

import Toml (TomlCodec, (.=))
import Toml qualified

parseConfig :: T.Text -> Either T.Text Config
parseConfig = Bi.first Toml.prettyTomlDecodeErrors . Toml.decode config_toml_codec

renderConfig :: Config -> T.Text
renderConfig = Toml.encode config_toml_codec

data RepoType = Local !LocalRepoConfig

data Config = Config
  { config_repoType :: !RepoType
  , config_cipher :: CipherConfig
  }

data LocalRepoConfig = LocalRepoConfig
  { local_repo_path :: !(Path Path.Abs Path.Dir)
  }
  deriving (Show)

data CipherConfig
  = CipherConfigNoCipher
  | CipherConfigAES128 AES128Config

data AES128Config = AES128Config
  { aes128_salt :: BS.ByteString
  , aes128_secret :: BS.ByteString
  , aes128_verify :: BS.ByteString
  }

config_toml_codec :: TomlCodec Config
config_toml_codec =
  Config
    <$> (repo_type_codec .= config_repoType)
    <*> (Toml.table cipher_config_codec "DANGER_ZONE.cipher" .= config_cipher)
  where
    match_local (Local c) = Just c

    repo_type_codec =
      Toml.dimatch match_local Local (Toml.table local_repo_config_codec "local")

local_repo_config_codec :: TomlCodec LocalRepoConfig
local_repo_config_codec =
  LocalRepoConfig
    <$> (abs_dir_codec "path" .= local_repo_path)

abs_dir_codec :: Toml.Key -> TomlCodec (Path Path.Abs Path.Dir)
abs_dir_codec = Toml.textBy to_text from_text
  where
    to_text = T.pack . Path.fromAbsDir

    from_text :: T.Text -> Either T.Text (Path Path.Abs Path.Dir)
    from_text t = Bi.first (const $ "'" <> t <> "'" <> " is not in format of absolute directory, which is expected") . Path.parseAbsDir . T.unpack $ t

cipher_config_codec :: TomlCodec CipherConfig
cipher_config_codec =
  (Toml.hardcoded "aes128" Toml._String "type" .= undefined *> from_aes)
    <|> (Toml.hardcoded "none" Toml._String "type" .= undefined *> from_none)
  where
    from_aes = Toml.dimatch to_aes CipherConfigAES128 aes128_config_codec
    to_aes (CipherConfigAES128 aes128) = Just aes128
    to_aes _ = Nothing

    from_none = Toml.dimatch to_none (const CipherConfigNoCipher) (pure ())
    to_none CipherConfigNoCipher = Just ()
    to_none _ = Nothing

aes128_config_codec :: TomlCodec AES128Config
aes128_config_codec =
  AES128Config
    <$> (Toml.byteStringArray "aes128.salt" .= aes128_salt)
    <*> (Toml.byteStringArray "aes128.secret" .= aes128_secret)
    <*> (Toml.byteStringArray "aes128.verify" .= aes128_verify)
