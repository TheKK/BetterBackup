{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Cli.FamiliarBackup (
  parser_info,
) where

import Options.Applicative (
  Alternative (many),
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
  str,
 )

import Control.Concurrent (threadDelay)
import Control.Monad (forever, unless, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Foldable (Foldable (fold), for_)
import Data.Traversable (for)

import System.Posix qualified as P

import Path qualified

import Data.ByteString.Char8 qualified as BC

import Effectful qualified as E
import Effectful.Ki qualified as EKi

import Better.Repository.Backup (DirEntry (DirEntryDir))
import Better.Repository.Backup qualified as Repo
import Better.Statistics.Backup qualified as BackupSt
import Better.Statistics.Backup.Class (
  BackupStatistics,
  newChunkCount,
  newDirCount,
  newFileCount,
  processedChunkCount,
  processedDirCount,
  processedFileCount,
  totalDirCount,
  totalFileCount,
  uploadedBytes,
 )

import Better.Logging.Effect (logging, loggingOnSyncException)
import Control.Monad.Catch (MonadThrow (throwM), mask_)
import Katip (katipAddNamespace)
import Katip qualified as Log
import Monad (runRepositoryForBackupFromCwd)
import System.Directory qualified as D
import Util.Options (someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Construct new version in a familiar way"
        ]

    parser =
      go
        <$> argument
          someBaseDirRead
          ( fold
              [ metavar "OS_CONFIG_PATH"
              , help "path to OS config"
              ]
          )
        <*> many
          ( (,)
              <$> argument
                str
                ( fold
                    [ metavar "SHARE_NAME"
                    , help "name of share"
                    ]
                )
              <*> argument
                someBaseDirRead
                ( fold
                    [ metavar "SHARE_PATH"
                    , help "path to folder on local file system"
                    ]
                )
          )

    go :: Path.SomeBase Path.Dir -> [(String, Path.SomeBase Path.Dir)] -> IO ()
    go os_config_some_path list_of_share_and_filesystem_some_path = void $ do
      runRepositoryForBackupFromCwd $ katipAddNamespace "familiar_backup" $ do
        abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory

        let
          to_abs p = case p of
            Path.Abs dir -> dir
            Path.Rel dir -> abs_pwd Path.</> dir

          process_reporter = forever $ do
            mask_ report_backup_stat
            liftIO $ threadDelay (1000 * 1000)

        logging Log.InfoS "checking permission of pathes"
        loggingOnSyncException Log.ErrorS "checking permission of pathes failed" $
          for_ (os_config_some_path : map snd list_of_share_and_filesystem_some_path) $ \some_path -> do
            let abs_path = Path.toFilePath $ to_abs some_path

            exists <- liftIO $ D.doesDirectoryExist abs_path
            unless exists $ do
              throwM $ userError $ abs_path <> ": path does not exist"

            p <- liftIO $ D.getPermissions abs_path
            let required_permissions = [("readable" :: String, D.readable p), ("searchable", D.searchable p)]
            unless (all snd required_permissions) $ do
              throwM $ userError $ abs_path <> ": permissions are not enough: " <> show required_permissions

        (v_digest, v) <- EKi.scoped $ \scope -> do
          EKi.fork_ scope process_reporter

          logging Log.InfoS "start familiar backup"
          Repo.runRepositoryBackup $ Repo.runBackupWorkersWithTBQueue 40 8 $ do
            logging Log.InfoS $ Log.ls $ "backup os config from " <> Path.toFilePath (to_abs os_config_some_path)
            os_dir_digest <- Repo.backup_dir $ to_abs os_config_some_path

            logging Log.InfoS $ Log.ls $ "backup shares" <> show list_of_share_and_filesystem_some_path
            share_dir_digest <- do
              dir_entry_of_shares <- for list_of_share_and_filesystem_some_path $ \(share_name, share_some_path) -> do
                logging Log.InfoS $ Log.ls $ "backup share from " <> Path.toFilePath (to_abs share_some_path)
                !digest <- Repo.backup_dir $ to_abs share_some_path
                pure $! Repo.DirEntryDir digest (BC.pack share_name)
              Repo.backupDirFromList dir_entry_of_shares

            Repo.backupDirFromList
              [ DirEntryDir os_dir_digest "@OS_CONFIG"
              , DirEntryDir share_dir_digest "@SHARES"
              ]

        liftIO $ putStrLn "result:"
        report_backup_stat
        liftIO $ print v

        pure (v_digest, v)

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
