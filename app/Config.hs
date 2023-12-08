{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Config (
  parseConfig,
  renderConfig,
  Config (..),
  RepoType (..),
  LocalRepoConfig (..),
) where

import Data.Bifunctor qualified as Bi

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
  deriving (Show)

data Config = Config
  { config_repoType :: !RepoType
  }
  deriving (Show)

data LocalRepoConfig = LocalRepoConfig
  { local_repo_path :: !(Path Path.Abs Path.Dir)
  }
  deriving (Show)

config_toml_codec :: TomlCodec Config
config_toml_codec =
  Config
    <$> (repo_type_codec .= config_repoType)
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
