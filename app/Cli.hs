{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Strict #-}

module Cli
  ( cmds
  ) where

import qualified System.Posix.Directory as P

import Options.Applicative

import Control.Exception (Exception(displayException))

import Data.Foldable
import Data.Function ((&))
import Data.Bifunctor (first)

import qualified Data.Text as T

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import Path (Path, Abs, Dir)
import qualified Path

import Config (Config(..))
import qualified Config

import qualified Better.Repository as Repo

import qualified LocalCache
import qualified Monad as M

cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Better operations"
      ]

    parser = subparser $ fold
      [ (command "init" parser_info_init)
      , (command "versions" parser_info_versions)
      , (command "backup" parser_info_backup)
      , (command "gc" parser_info_gc)
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

parser_info_versions :: ParserInfo (IO ())
parser_info_versions = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "List backuped versions"
      ]

    parser = pure go

    go = do
      hbk <- mk_hbk_from_cwd
      Repo.listVersions
        & S.morphInner (flip M.runHbkT hbk)
        & S.fold (F.drainMapM print)

parser_info_backup :: ParserInfo (IO ())
parser_info_backup = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Construct new version"
      ]

    parser = go
      <$> argument some_base_dir_read (fold
            [ metavar "BACKUP_ROOT"
            , help "directory you'd like to backup"
            ])

    go dir_to_backup = do
      hbk <- mk_hbk_from_cwd
      version <- flip M.runHbkT hbk $ do
        Repo.backup $ T.pack $ Path.fromSomeDir dir_to_backup
      print version

parser_info_gc :: ParserInfo (IO ())
parser_info_gc = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Running garbage collection to release unused data"
      ]

    parser = pure go

    go = do
      hbk <- mk_hbk_from_cwd
      flip M.runHbkT hbk $ Repo.garbageCollection

p_local_repo_config :: Parser Config.RepoType
p_local_repo_config = (Config.Local . Config.LocalRepoConfig)
  <$> (argument abs_dir_read (metavar "REPO_PATH" <> help "path to store your backup"))

some_base_dir_read :: ReadM (Path.SomeBase Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir

abs_dir_read :: ReadM (Path Abs Dir)
abs_dir_read = eitherReader $ first displayException . Path.parseAbsDir

mk_hbk_from_cwd :: Applicative m => IO (M.Hbk m)
mk_hbk_from_cwd = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  config <- LocalCache.readConfig cwd

  let
    repository = case Config.config_repoType config of
      Config.Local l -> Repo.localRepo $ Config.local_repo_path l

  pure $ M.MkHbk
    [Path.absdir|/tmp|]
    cwd
    repository
    [Path.absdir|/tmp|]
    (pure ())
