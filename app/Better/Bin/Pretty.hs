{-# LANGUAGE OverloadedStrings #-}

module Better.Bin.Pretty (
  mkBackupStatisticsDoc,
) where

import Data.Foldable (Foldable (fold))

import Prettyprinter qualified as PP

import Effectful qualified as E

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

mkBackupStatisticsDoc
  :: (BackupStatistics E.:> es, E.IOE E.:> es)
  => Maybe String
  -- ^ Title to display
  -> E.Eff es (PP.Doc ann)
mkBackupStatisticsDoc opt_name = do
  process_file_count <- BackupSt.readStatistics processedFileCount
  new_file_count <- BackupSt.readStatistics newFileCount
  total_file_count <- BackupSt.readStatistics totalFileCount
  process_dir_count <- BackupSt.readStatistics processedDirCount
  new_dir_count <- BackupSt.readStatistics newDirCount
  total_dir_count <- BackupSt.readStatistics totalDirCount
  process_chunk_count <- BackupSt.readStatistics processedChunkCount
  new_chunk_count <- BackupSt.readStatistics newChunkCount
  upload_bytes <- BackupSt.readStatistics uploadedBytes

  pure
    $
    PP.hang 2 $
      PP.sep
        [ maybe PP.emptyDoc (PP.brackets . PP.pretty) opt_name
        , PP.concatWith
            (PP.surround $ PP.flatAlt PP.line (PP.comma PP.<> PP.space))
            [ fold ["(new ", PP.pretty new_file_count, ") ", PP.pretty process_file_count, "/", PP.pretty total_file_count, " processed files"]
            , fold ["(new ", PP.pretty new_dir_count, ") ", PP.pretty process_dir_count, "/", PP.pretty total_dir_count, " processed dirs"]
            , fold ["(new ", PP.pretty new_chunk_count, ") ", PP.pretty process_chunk_count, " processed chunks"]
            , fold [PP.pretty upload_bytes PP.<> " uploaded/tranfered bytes"]
            ]
        ]
