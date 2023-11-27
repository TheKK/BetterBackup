{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Cli.Backup (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import qualified Ki

import Control.Concurrent (threadDelay)
import Control.Exception (mask_)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Foldable (Foldable (fold))

import qualified System.Posix as P

import qualified Path

import qualified Effectful as E
import qualified Effectful.Dispatch.Static.Unsafe as EU

import qualified Better.Repository.Backup as Repo
import qualified Better.Statistics.Backup as BackupSt
import Better.Statistics.Backup.Class (BackupStatistics, newChunkCount, newDirCount, newFileCount, processedChunkCount, processedDirCount, processedFileCount, totalDirCount, totalFileCount, uploadedBytes)

import Monad (run_backup_repo_t_from_cwd)
import Util.Options (someBaseDirRead)

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
          someBaseDirRead
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

      abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory
      let
        abs_dir = case dir_to_backup of
          Path.Abs dir -> dir
          Path.Rel dir -> abs_pwd Path.</> dir

      v <- Ki.scoped $ \scope -> do
        Ki.fork_ scope process_reporter
        un $ do
          Repo.runBackup $ do
            Repo.backup_dir abs_dir

      putStrLn "result:" >> un report_backup_stat
      print v

report_backup_stat :: (BackupStatistics E.:> es, E.IOE E.:> es) => E.Eff es ()
report_backup_stat = do
  process_file_count <- BackupSt.readStatistics processedFileCount
  new_file_count <- BackupSt.readStatistics newFileCount
  total_file_count <- BackupSt.readStatistics totalFileCount
  process_dir_count <- BackupSt.readStatistics processedDirCount
  new_dir_count <- BackupSt.readStatistics newDirCount
  total_dir_count <- BackupSt.readStatistics totalDirCount
  process_chunk_count <- BackupSt.readStatistics processedChunkCount
  new_chunk_count <- BackupSt.readStatistics newChunkCount
  upload_bytes <- BackupSt.readStatistics uploadedBytes
  liftIO $
    putStrLn $
      fold
        [ "(new "
        , show new_file_count
        , ") "
        , show process_file_count
        , "/"
        , show total_file_count
        , " processed files, (new "
        , show new_dir_count
        , ") "
        , show process_dir_count
        , "/"
        , show total_dir_count
        , " processed dirs, (new "
        , show new_chunk_count
        , ") "
        , show process_chunk_count
        , " processed chunks, "
        , show upload_bytes
        , " uploaded/transfered bytes"
        ]
