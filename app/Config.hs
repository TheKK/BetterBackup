{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE GADTs #-}

module Config
  ( parseConfig
  , renderConfig
  , Config(..)
  , RepoType(..)
  , LocalRepoConfig(..)
  ) where

import qualified Data.Bifunctor as Bi

import qualified Data.Text as T

import qualified Path
import Path (Path)

import Control.Applicative

import Toml (TomlCodec, (.=))
import qualified Toml

parseConfig :: T.Text -> Either T.Text Config
parseConfig = Bi.first Toml.prettyTomlDecodeErrors . Toml.decode config_toml_codec

renderConfig :: Config -> T.Text
renderConfig = Toml.encode config_toml_codec

config_toml_codec :: TomlCodec Config
config_toml_codec = Config
  <$> repo_type_codec .= config_repoType
  where
    match_local (Local c) = Just c

    repo_type_codec
      =  Toml.dimatch match_local Local (Toml.table local_repo_config_codec "local")

data RepoType = Local LocalRepoConfig
  deriving Show

data Config = Config 
 { config_repoType :: RepoType
 }
  deriving Show

data LocalRepoConfig = LocalRepoConfig
  { local_repo_path :: (Either (Path Path.Abs Path.Dir) (Path Path.Rel Path.Dir))
  }
  deriving Show

local_repo_config_codec :: TomlCodec LocalRepoConfig
local_repo_config_codec = LocalRepoConfig
  <$> Toml.match _RepoPath "path" .= local_repo_path 

-- TODO Maybe there's a better way that we could reuse Toml.text?
_RepoPath :: Toml.TomlBiMap (Either (Path Path.Abs Path.Dir) (Path Path.Rel Path.Dir)) Toml.AnyValue
_RepoPath = Toml.invert $ Toml.prism
  (Toml.AnyValue . Toml.Text . T.pack . either Path.toFilePath Path.toFilePath)
  (\v -> case v of
    Toml.AnyValue (Toml.Text p) -> case parse (T.unpack p) of
      Just p' -> Right p'
      Nothing -> Left $ Toml.ArbitraryError $ "Invalid path: " <> p
    _ -> Left $ Toml.WrongValue $ Toml.MatchError Toml.TText v
  )
  where
    parse p = (Left <$> Path.parseAbsDir p) <|> (Right <$> Path.parseRelDir p)
