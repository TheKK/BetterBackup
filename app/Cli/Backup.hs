{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
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

import Control.Concurrent (threadDelay)
import Control.Monad (forever, replicateM_, void)
import Control.Monad.Catch (mask_)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Foldable (Foldable (fold))
import Data.Text.Lazy qualified as TL

import System.Console.ANSI qualified as Ansi
import System.Console.Terminal.Size qualified as Console
import System.Posix qualified as P

import Path qualified

import Prettyprinter qualified as PP
import Prettyprinter.Render.Text qualified as PP

import Effectful qualified as E
import Effectful.Ki qualified as EKi

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

import Data.Text.Lazy.IO qualified as TL
import Monad (runRepositoryForBackupFromCwd)
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

    go dir_to_backup = void $ runRepositoryForBackupFromCwd $ do
      let
        process_reporter = forever $ do
          mask_ report_backup_stat
          liftIO $ threadDelay (1000 * 1000)

      abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory
      let
        abs_dir = case dir_to_backup of
          Path.Abs dir -> dir
          Path.Rel dir -> abs_pwd Path.</> dir

      (v_digest, v) <- EKi.scoped $ \scope -> do
        EKi.fork_ scope process_reporter
        Repo.runRepositoryBackup $ Repo.runBackupWorkersWithTBQueue 40 8 $ do
          Repo.backup_dir abs_dir

      liftIO (putStrLn "result:") >> report_backup_stat
      liftIO $ print v

      pure (v_digest, v)

report_backup_stat :: (BackupStatistics E.:> es, E.IOE E.:> es) => E.Eff es ()
report_backup_stat =
  liftIO Console.size >>= \case
    Nothing -> pure ()
    Just (Console.Window _h w) -> do
      doc_lazy_text <- PP.renderLazy . PP.layoutPretty (PP.LayoutOptions $ PP.AvailablePerLine w 1) <$> mk_backup_stat_doc "Backup"
      let !doc_height = TL.foldl' (\(!acc) c -> if c == '\n' then acc + 1 else acc) 1 doc_lazy_text

      liftIO $ do
        -- TODO Maybe there's better way to do this.
        Ansi.clearLine >> TL.putStrLn ""
        TL.putStrLn doc_lazy_text
        replicateM_ (doc_height + 1) $ do
          Ansi.clearLine
          Ansi.cursorUpLine 1

mk_backup_stat_doc :: (BackupStatistics E.:> es, E.IOE E.:> es) => String -> E.Eff es (PP.Doc ann)
mk_backup_stat_doc name = do
  process_file_count <- BackupSt.readStatistics processedFileCount
  new_file_count <- BackupSt.readStatistics newFileCount
  total_file_count <- BackupSt.readStatistics totalFileCount
  process_dir_count <- BackupSt.readStatistics processedDirCount
  new_dir_count <- BackupSt.readStatistics newDirCount
  total_dir_count <- BackupSt.readStatistics totalDirCount
  process_chunk_count <- BackupSt.readStatistics processedChunkCount
  new_chunk_count <- BackupSt.readStatistics newChunkCount
  upload_bytes <- BackupSt.readStatistics uploadedBytes

  pure $
    PP.hang 2 $
      PP.sep
        [ PP.brackets $ PP.pretty name
        , PP.concatWith
            (PP.surround $ PP.flatAlt PP.line (PP.comma PP.<> PP.space))
            [ fold ["(new ", PP.pretty new_file_count, ") ", PP.pretty process_file_count, "/", PP.pretty total_file_count, " processed files"]
            , fold ["(new ", PP.pretty new_dir_count, ") ", PP.pretty process_dir_count, "/", PP.pretty total_dir_count, " processed dirs"]
            , fold ["(new ", PP.pretty new_chunk_count, ") ", PP.pretty process_chunk_count, " processed chunks"]
            , fold [PP.pretty upload_bytes PP.<> " uploaded/tranfered bytes"]
            ]
        ]
