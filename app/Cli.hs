{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli (
  cmds,
) where

import Control.Parallel (par)

import Options.Applicative (
  Parser,
  ParserInfo,
  ReadM,
  argument,
  command,
  eitherReader,
  help,
  helper,
  info,
  long,
  metavar,
  progDesc,
  short,
  subparser,
  switch,
 )

import Control.Exception (Exception (displayException))
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Bifunctor (first)
import Data.Foldable (Foldable (fold), for_)
import Data.Function ((&))
import Data.List (sortOn)
import Data.String (fromString)

import qualified Data.Aeson as A

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Data.Time (UTCTime, defaultTimeLocale, formatTime, utc, utcToZonedTime)
import Data.Time.LocalTime (getCurrentTimeZone)

import qualified Data.ByteString.Base16 as BSBase16

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import qualified Streamly.Console.Stdio as Stdio

import Path (Abs, Dir, Path)
import qualified Path

import qualified Effectful.Dispatch.Static.Unsafe as E

import Config (Config (..))
import qualified Config

import Better.Hash (Digest, digestFromByteString)
import qualified Better.Repository as Repo

import Cli.Backup (parser_info)
import Cli.GarbageCollection (parser_info)
import Cli.IntegrityCheck (parser_info)
import Cli.PatchBackup (parser_info)
import qualified Cli.Ref as Ref
import Cli.RestoreTree (parser_info)
import Cli.VersionFind (parser_info)

import qualified Data.ByteString.Lazy.Char8 as BL
import qualified LocalCache
import Monad (run_readonly_repo_t_from_cwd)

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
          , command "versions" parser_info_versions
          , command "version" parser_info_version
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
          some_base_dir_read
          ( fold
              [ metavar "CACHE_PATH"
              , help "path to store your local cache"
              ]
          )
        <*> (Config <$> p_local_repo_config)

parser_info_versions :: ParserInfo (IO ())
parser_info_versions = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "List backuped versions"
        ]

    parser =
      go
        <$> switch
          ( fold
              [ long "utc"
              , help "display timestamp with timezone in UTC instead of CST"
              ]
          )
        <*> switch
          ( fold
              [ long "json"
              , short 'j'
              , help "display in JSON format"
              ]
          )

    {-# NOINLINE go #-}
    go use_utc is_json = do
      tz <- if use_utc then pure utc else getCurrentTimeZone
      let
        display_time = formatTime defaultTimeLocale "%c" . utcToZonedTime tz
        render =
          if is_json
            then render_json display_time
            else render_normally display_time

      digests_and_vs <- run_readonly_repo_t_from_cwd $ do
        Repo.listVersions & S.toList

      -- Sort on list should be acceptible in the context of "backup version".
      -- Currently I believe the number would be under 100,000 and causes no problem here.
      for_ (sortOn (Repo.ver_timestamp . snd) digests_and_vs) $ \(v_digest, v) -> do
        render v_digest v

    render_normally display_time v_digest v =
      putStrLn $
        display_time (Repo.ver_timestamp v)
          <> " - version digest ["
          <> show v_digest
          <> "] - tree root digest ["
          <> show (Repo.ver_root v)
          <> "]"

    render_json :: (UTCTime -> String) -> Digest -> Repo.Version -> IO ()
    render_json display_time v_digest v =
      BL.putStrLn . A.encode $
        A.object
          [ ("digest", A.String (T.pack $ show v_digest))
          , ("timestamp", A.String (T.pack $ display_time (Repo.ver_timestamp v)))
          , ("tree_root", A.String (T.pack $ show (Repo.ver_root v)))
          ]

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
          digest_read
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
          digest_read
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
          digest_read
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
          digest_read
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
    <$> argument abs_dir_read (metavar "REPO_PATH" <> help "path to store your backup")

some_base_dir_read :: ReadM (Path.SomeBase Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir

abs_dir_read :: ReadM (Path Abs Dir)
abs_dir_read = eitherReader $ first displayException . Path.parseAbsDir

digest_read :: ReadM Digest
digest_read = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest
