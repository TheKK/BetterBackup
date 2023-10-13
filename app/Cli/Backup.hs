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

import Control.Exception (Exception (displayException), mask_)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Control.Monad.IO.Unlift (MonadUnliftIO)
import qualified Control.Monad.IO.Unlift as Un

import Data.Bifunctor (first)
import Data.Foldable (Foldable (fold))

import qualified Data.Text as T

import Path (Dir)
import qualified Path

import qualified Better.Repository.Backup as Repo

import Monad (run_backup_repo_t_from_cwd)
import Better.Statistics.Backup (MonadBackupStat)
import qualified Better.Statistics.Backup as BackupSt
import Control.Concurrent (threadDelay)

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

    go dir_to_backup = run_backup_repo_t_from_cwd $ Un.withRunInIO $ \un -> do
      let
        process_reporter = forever $ do
          mask_ $ un report_backup_stat
          threadDelay (1000 * 1000)

      v <- Ki.scoped $ \scope -> do
        _ <- Ki.fork scope process_reporter
        un $ Repo.backup $ T.pack $ Path.fromSomeDir dir_to_backup

      putStrLn "result:" >> un report_backup_stat
      print v

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

