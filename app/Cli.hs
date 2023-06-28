{-# LANGUAGE TypeApplications #-}

module Cli
  ( cmds
  ) where

import Options.Applicative

import Control.Exception (Exception(displayException))

import Data.Foldable
import Data.Bifunctor (first)

import Path (Path, Abs, Dir)
import qualified Path

import Config (Config(..))
import qualified Config

import qualified LocalCache

cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Better operations"
      ]

    parser = subparser $ fold
      [ (command "init" parser_info_init)
      ]

parser_info_init :: ParserInfo (IO ())
parser_info_init = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Initialize repository"
      ]

    parser = subparser $ fold
      [ command "local" parser_info_init_local
      ]

parser_info_init_local :: ParserInfo (IO ())
parser_info_init_local = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Initialize with local repository"
      ]

    parser = LocalCache.initialize
      <$> argument some_base_dir_read (fold
            [ metavar "CACHE_PATH"
            , help "path to store your local cache"
            ])
      <*> (Config <$> p_local_repo_config)

p_local_repo_config :: Parser Config.RepoType
p_local_repo_config = (Config.Local . Config.LocalRepoConfig)
  <$> (argument abs_dir_read (metavar "REPO_PATH" <> help "path to store your backup"))

some_base_dir_read :: ReadM (Path.SomeBase Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir

abs_dir_read :: ReadM (Path Abs Dir)
abs_dir_read = eitherReader $ first displayException . Path.parseAbsDir
