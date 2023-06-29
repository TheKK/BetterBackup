{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE QuasiQuotes #-}

module LocalCache
  ( initialize
  , readConfig
  ) where

import Control.Monad (when, unless)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Path (Path, Abs, Rel, File, Dir, (</>), relfile)
import qualified Path as Path

import qualified System.Directory as D

import qualified UnliftIO as Un

import Config(Config(..))
import qualified Config

initialize :: Path.SomeBase Dir -> Config -> IO ()
initialize cache_p config = do
    repo_path <- render_to_abs_dir cache_p

    repo_exists <- D.doesDirectoryExist $ Path.toFilePath repo_path
    unless repo_exists $ do
      Un.throwString $ "path '" <> Path.toFilePath repo_path <> "' does not exist, please construct it properly first" 

    let config_path = Path.toFilePath $ repo_path </> config_filename
    config_exists <- D.doesFileExist config_path
    when config_exists $ do
      Un.throwString $ "config file '" <> config_path <> "' has already existed" 

    initialize_config repo_path config

readConfig :: Path Abs Dir -> IO Config
readConfig cwd = do
  let config_path = cwd </> config_filename
  parse_result <- Config.parseConfig <$> (T.readFile $ Path.fromAbsFile config_path)
  case parse_result of
    Left err -> Un.throwString $ T.unpack err
    Right config -> pure config

initialize_config :: Path Abs Dir -> Config -> IO ()
initialize_config repo_path = T.writeFile (Path.toFilePath $ repo_path </> config_filename) . Config.renderConfig

config_filename :: Path Rel File
config_filename = [relfile|config.toml|]

render_to_abs_dir :: Path.SomeBase Dir -> IO (Path Abs Dir)
render_to_abs_dir = \case
  Path.Abs p -> pure p
  Path.Rel p -> do
    cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
    pure $ cwd </> p
