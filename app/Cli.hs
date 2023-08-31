{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

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
  metavar,
  progDesc,
  subparser,
 )

import qualified Ki.Unlifted as Ki

import UnliftIO (MonadUnliftIO)
import qualified UnliftIO.Concurrent as Un
import qualified UnliftIO.Exception as Un

import Control.Exception (Exception (displayException))
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Crypto.Hash (Digest, SHA256, digestFromByteString)

import Data.Bifunctor (first)
import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.String (fromString)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import qualified Data.ByteString.Base16 as BSBase16

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import qualified Streamly.Console.Stdio as Stdio

import Path (Abs, Dir, Path)
import qualified Path

import Config (Config (..))
import qualified Config

import qualified Better.Repository as Repo
import qualified Better.Repository.Backup as Repo
import Better.Statistics.Backup (MonadBackupStat)
import qualified Better.Statistics.Backup as BackupSt

import qualified Cli.Ref as Ref
import qualified LocalCache
import Monad (run_backup_repo_t_from_cwd, run_readonly_repo_t_from_cwd)

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
          , command "backup" parser_info_backup
          , command "gc" parser_info_gc
          , command "integrity-check" parser_info_integrity_check
          , command "cat-chunk" parser_info_cat_chunk
          , command "cat-file" parser_info_cat_file
          , command "cat-file-chunks" parser_info_cat_file_chunks
          , command "cat-tree" parser_info_cat_tree
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

    parser = pure go

    {-# NOINLINE go #-}
    go =
      run_readonly_repo_t_from_cwd $
        Repo.listVersions
          & S.fold (F.drainMapM $ liftIO . print)

parser_info_backup :: ParserInfo (IO ())
parser_info_backup = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Construct new version"
        ]

    parser =
      go
        <$> argument
          some_base_dir_read
          ( fold
              [ metavar "BACKUP_ROOT"
              , help "directory you'd like to backup"
              ]
          )

    go dir_to_backup = run_backup_repo_t_from_cwd $ do
      let
        process_reporter = forever $ do
          Un.mask_ report_backup_stat
          Un.threadDelay (1000 * 1000)

      v <- Ki.scoped $ \scope -> do
        _ <- Ki.fork scope process_reporter
        Repo.backup $ T.pack $ Path.fromSomeDir dir_to_backup

      liftIO (putStrLn "result:") >> report_backup_stat
      liftIO $ print v

report_backup_stat :: (MonadBackupStat m, MonadUnliftIO m) => m ()
report_backup_stat = do
  process_file_count <- BackupSt.readStatistics BackupSt.processedFileCount
  total_file_count <- BackupSt.readStatistics BackupSt.totalFileCount
  process_dir_count <- BackupSt.readStatistics BackupSt.processedDirCount
  total_dir_count <- BackupSt.readStatistics BackupSt.totalDirCount
  process_chunk_count <- BackupSt.readStatistics BackupSt.processedChunkCount
  upload_bytes <- BackupSt.readStatistics BackupSt.uploadedBytes
  liftIO $
    putStrLn $
      fold
        [ show process_file_count
        , "/"
        , show total_file_count
        , " files, "
        , show process_dir_count
        , "/"
        , show total_dir_count
        , " dirs, "
        , show process_chunk_count
        , " chunks, "
        , show upload_bytes
        , " bytes"
        ]

parser_info_gc :: ParserInfo (IO ())
parser_info_gc = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Running garbage collection to release unused data"
        ]

    parser = pure go

    {-# NOINLINE go #-}
    go = run_readonly_repo_t_from_cwd Repo.garbageCollection

parser_info_integrity_check :: ParserInfo (IO ())
parser_info_integrity_check = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Verify and handle corrupted data"
        ]

    parser = pure go

    {-# NOINLINE go #-}
    go = run_readonly_repo_t_from_cwd $ Repo.checksum 10

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
    go :: Digest SHA256 -> IO ()
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
    go :: Digest SHA256 -> IO ()
    go sha =
      run_readonly_repo_t_from_cwd $
        Repo.catFile sha
          -- Use parConcatMap to open multiple chunk files concurrently.
          -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
          & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . Repo.catChunk . Repo.chunk_name)
          -- Use parEval to read from chunks concurrently.
          -- Since read is often faster than write, using parEval with buffer should reduce running time.
          -- & S.mapM (\(~e) -> par e $ pure e)
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

digest_read :: ReadM (Digest SHA256)
digest_read = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString @SHA256 sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest
