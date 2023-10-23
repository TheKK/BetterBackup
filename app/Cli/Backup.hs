{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}

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

import Data.Bifunctor (first)
import Data.Foldable (Foldable (fold))

import qualified Data.Text as T

import Path (Dir)
import qualified Path

import qualified Better.Repository.Backup as Repo

import Monad (run_backup_repo_t_from_cwd)
import qualified Better.Statistics.Backup as BackupSt
import Control.Concurrent (threadDelay)
import qualified Effectful as E
import Better.Statistics.Backup.Class (BackupStatistics, processedFileCount, totalFileCount, processedDirCount, totalDirCount, processedChunkCount, uploadedBytes)
import qualified Effectful.Dispatch.Static.Unsafe as EU

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

    go dir_to_backup = run_backup_repo_t_from_cwd $ EU.reallyUnsafeUnliftIO $ \un -> do
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

report_backup_stat :: (BackupStatistics E.:> es, E.IOE E.:> es) => E.Eff es ()
report_backup_stat = do
  process_file_count <- BackupSt.readStatistics processedFileCount
  total_file_count <- BackupSt.readStatistics totalFileCount
  process_dir_count <- BackupSt.readStatistics processedDirCount
  total_dir_count <- BackupSt.readStatistics totalDirCount
  process_chunk_count <- BackupSt.readStatistics processedChunkCount
  upload_bytes <- BackupSt.readStatistics uploadedBytes
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
