{-# LANGUAGE OverloadedStrings #-}

module Cli.Backup (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  ReadM,
  argument,
  eitherReader,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import qualified Ki.Unlifted as Ki

import qualified UnliftIO.Concurrent as Un
import qualified UnliftIO.Exception as Un

import Control.Exception (Exception (displayException))
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Bifunctor (first)
import Data.Foldable (Foldable (fold))

import qualified Data.Text as T

import Path (Dir)
import qualified Path

import qualified Better.Repository.Backup as Repo

import Monad (run_backup_repo_t_from_cwd)
import Better.Statistics.Backup (MonadBackupStat)
import UnliftIO (MonadUnliftIO)
import qualified Better.Statistics.Backup as BackupSt

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
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

some_base_dir_read :: ReadM (Path.SomeBase Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir

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

