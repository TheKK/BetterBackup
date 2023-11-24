{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli (
  cmds,
) where

import Control.Parallel (par)

import Options.Applicative (
  Parser,
  ParserInfo,
  argument,
  command,
  help,
  helper,
  info,
  metavar,
  progDesc,
  subparser,
 )

import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Foldable (Foldable (fold))
import Data.Function ((&))

import qualified Data.Text as T
import qualified Data.Text.IO as T

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import qualified Streamly.Console.Stdio as Stdio

import qualified Effectful.Dispatch.Static.Unsafe as E

import Config (Config (..))
import qualified Config

import Better.Hash (Digest)
import qualified Better.Repository as Repo

import Cli.Backup (parser_info)
import Cli.GarbageCollection (parser_info)
import Cli.IntegrityCheck (parser_info)
import Cli.PatchBackup (parser_info)
import qualified Cli.Ref as Ref
import Cli.RestoreTree (parser_info)
import Cli.TreeList (parser_info)
import Cli.VersionFind (parser_info)
import Cli.Versions (parser_info)

import qualified LocalCache
import Monad (run_readonly_repo_t_from_cwd)
import Util.Options (absDirRead, digestRead, someBaseDirRead)

-- TODO add ability to put trace markers
-- TODO add ability to collect running statistics
cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Better operations"
        ]

    parser =
      subparser $
        fold
          [ command "init" parser_info_init
          , command "versions" Cli.Versions.parser_info
          , command "version" parser_info_version
          , command "tree" parser_info_tree
          , command "backup" Cli.Backup.parser_info
          , command "patch-backup" Cli.PatchBackup.parser_info
          , command "gc" Cli.GarbageCollection.parser_info
          , command "integrity-check" Cli.IntegrityCheck.parser_info
          , command "cat-chunk" parser_info_cat_chunk
          , command "cat-file" parser_info_cat_file
          , command "cat-file-chunks" parser_info_cat_file_chunks
          , command "cat-tree" parser_info_cat_tree
          , command "restore-tree" Cli.RestoreTree.parser_info
          , command "ref" Ref.cmds
          ]

parser_info_init :: ParserInfo (IO ())
parser_info_init = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Initialize repository"
        ]

    parser =
      subparser $
        fold
          [ command "local" parser_info_init_local
          ]

parser_info_version :: ParserInfo (IO ())
parser_info_version = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Version related operations"
        ]

    parser =
      subparser $
        fold
          [ command "find" Cli.VersionFind.parser_info
          ]

parser_info_tree :: ParserInfo (IO ())
parser_info_tree = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Tree related operations"
        ]

    parser =
      subparser $
        fold
          [ command "ls" Cli.TreeList.parser_info
          ]

parser_info_init_local :: ParserInfo (IO ())
parser_info_init_local = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Initialize with local repository"
        ]

    parser =
      LocalCache.initialize
        <$> argument
          someBaseDirRead
          ( fold
              [ metavar "CACHE_PATH"
              , help "path to store your local cache"
              ]
          )
        <*> (Config <$> p_local_repo_config)

parser_info_cat_chunk :: ParserInfo (IO ())
parser_info_cat_chunk = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of single chunk"
        ]

    parser =
      go
        <$> argument
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of chunk"
              ]
          )

    {-# NOINLINE go #-}
    go :: Digest -> IO ()
    go sha =
      run_readonly_repo_t_from_cwd $
        Repo.catChunk sha
          & S.fold Stdio.writeChunks

parser_info_cat_file :: ParserInfo (IO ())
parser_info_cat_file = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of single file"
        ]

    parser =
      go
        <$> argument
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of file"
              ]
          )

    {-# NOINLINE go #-}
    go :: Digest -> IO ()
    go sha =
      run_readonly_repo_t_from_cwd $ E.reallyUnsafeUnliftIO $ \un -> liftIO $ do
        Repo.catFile sha
          & S.morphInner un
          -- Use parConcatMap to open multiple chunk files concurrently.
          -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
          & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
          -- Use parEval to read from chunks concurrently.
          -- Since read is often faster than write, using parEval with buffer should reduce running time.
          & S.parEval (S.maxBuffer 30)
          & S.fold Stdio.writeChunks

parser_info_cat_file_chunks :: ParserInfo (IO ())
parser_info_cat_file_chunks = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display chunks a single file references to"
        ]

    parser =
      go
        <$> argument
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of file"
              ]
          )

    go sha = run_readonly_repo_t_from_cwd $ do
      Repo.catFile sha
        & fmap (show . Repo.chunk_name)
        & S.fold (F.drainMapM $ liftIO . putStrLn)

parser_info_cat_tree :: ParserInfo (IO ())
parser_info_cat_tree = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of tree"
        ]

    parser =
      go
        <$> argument
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of tree"
              ]
          )

    go sha = run_readonly_repo_t_from_cwd $ do
      Repo.catTree sha
        & S.fold (F.drainMapM $ liftIO . T.putStrLn . T.pack . show)

p_local_repo_config :: Parser Config.RepoType
p_local_repo_config =
  Config.Local . Config.LocalRepoConfig
    <$> argument absDirRead (metavar "REPO_PATH" <> help "path to store your backup")
