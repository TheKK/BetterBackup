{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Strict #-}

module LocalCache (
  initialize,
  overwrite,
  readConfig,
) where

import GHC.Stack (HasCallStack)

import Control.Monad (unless, when)

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Path (Abs, Dir, File, Path, Rel, relfile, (</>))
import Path qualified

import System.Directory qualified as D

import Config (Config (..))
import Config qualified

-- | Initialize @Config@ to given path. It's an error if the file have already existed.
initialize :: Path.SomeBase Dir -> Config -> IO ()
initialize cache_p config = do
  repo_path <- render_to_abs_dir cache_p

  repo_exists <- D.doesDirectoryExist $ Path.toFilePath repo_path
  unless repo_exists $ do
    error $ "path '" <> Path.toFilePath repo_path <> "' does not exist, please construct it properly first"

  let config_path = Path.toFilePath $ repo_path </> config_filename
  config_exists <- D.doesFileExist config_path
  when config_exists $ do
    error $ "config file '" <> config_path <> "' has already existed"

  initialize_config repo_path config

-- | Overwrite @Config@ to given path. It's an error if the file have not existed yet.
overwrite :: HasCallStack => Path.SomeBase Dir -> Config -> IO ()
overwrite cache_p config = do
  repo_path <- render_to_abs_dir cache_p

  let config_path = Path.toFilePath $ repo_path </> config_filename
  config_exists <- D.doesFileExist config_path
  unless config_exists $ do
    error $ "failed to overwrite config file: '" <> config_path <> "' does not exist"

  initialize_config repo_path config

readConfig :: Path Abs Dir -> IO Config
readConfig cwd = do
  let config_path = cwd </> config_filename
  parse_result <- Config.parseConfig <$> (T.readFile $ Path.fromAbsFile config_path)
  case parse_result of
    Left err -> error $ T.unpack err
    Right config -> pure config

-- | Put @Config@ to given path. The old one WOULD be overwritten.
initialize_config :: Path Abs Dir -> Config -> IO ()
initialize_config repo_path config = do
  tmp_config_path <- Path.addExtension ".tmp" config_path
  T.writeFile (Path.toFilePath tmp_config_path) $ Config.renderConfig config
  D.renameFile (Path.toFilePath tmp_config_path) (Path.toFilePath config_path)
  where
    config_path = repo_path </> config_filename

config_filename :: Path Rel File
config_filename = [relfile|config.toml|]

render_to_abs_dir :: Path.SomeBase Dir -> IO (Path Abs Dir)
render_to_abs_dir = \case
  Path.Abs p -> pure p
  Path.Rel p -> do
    cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
    pure $ cwd </> p
