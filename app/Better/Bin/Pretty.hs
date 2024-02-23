{-# LANGUAGE OverloadedStrings #-}

module Better.Bin.Pretty (
  mkBackupStatisticsDoc,
  PrevStatistic,
  initPrevStatistic,
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
import Data.Bifunctor (first)
import Data.Word (Word64)
import Text.Printf (printf)

data PrevStatistic = PrevStatistic
  { prev_process_file_count :: Word64
  , prev_process_dir_count :: Word64
  , prev_process_chunk_count :: Word64
  , prev_upload_bytes :: Word64
  }

initPrevStatistic :: PrevStatistic
initPrevStatistic = PrevStatistic 0 0 0 0

mkBackupStatisticsDoc
  :: (BackupStatistics E.:> es, E.IOE E.:> es)
  => Maybe String
  -- ^ Title to display
  -> Maybe (PrevStatistic, Double)
  -- ^ Previous statistic and time elapse used for calculating speed
  -> E.Eff es (PP.Doc ann, PrevStatistic)
mkBackupStatisticsDoc opt_name opt_prev_stat_n_time_elapse = do
  process_file_count <- BackupSt.readStatistics processedFileCount
  new_file_count <- BackupSt.readStatistics newFileCount
  total_file_count <- BackupSt.readStatistics totalFileCount
  process_dir_count <- BackupSt.readStatistics processedDirCount
  new_dir_count <- BackupSt.readStatistics newDirCount
  total_dir_count <- BackupSt.readStatistics totalDirCount
  process_chunk_count <- BackupSt.readStatistics processedChunkCount
  new_chunk_count <- BackupSt.readStatistics newChunkCount
  upload_bytes <- BackupSt.readStatistics uploadedBytes

  let
    counting_speed_per_sec cur (prev, elapse) = fold [PP.pretty (fromIntegral (cur - prev) / elapse), "/s"]

    upload_speed_per_sec :: Word64 -> (Word64, Double) -> PP.Doc ann
    upload_speed_per_sec cur (prev, elapse) = value_doc <> "/s"
      where
        value_doc = bytes_to_unit_doc dx

        dx :: Double
        dx = fromIntegral (cur - prev) / elapse

    bytes_to_unit_doc bytes = value_doc PP.<+> unit_doc
      where
        value_doc = PP.pretty (printf "%0.2f" (bytes / (1024 ^ unit_log) :: Double) :: String)
        unit_doc = units !! unit_log

        unit_log :: Int
        unit_log
          | bytes == 0 = 0
          | otherwise = floor (logBase 1024 bytes :: Double)

        units = ["B" :: PP.Doc ann, "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"]

    counting_doc :: String -> Word64 -> Word64 -> Maybe (Word64, Double) -> Maybe Word64 -> PP.Doc ann
    counting_doc name new_count process_count opt_prev_process_count_n_dt opt_total_count =
      PP.hsep
        [ "(new " <> PP.pretty new_count <> ")"
        , PP.pretty process_count <> opt_total_doc <> " processed " <> PP.pretty name
        , opt_speed_doc
        ]
      where
        opt_total_doc = maybe PP.emptyDoc (("/" <>) . PP.pretty) opt_total_count
        opt_speed_doc = maybe PP.emptyDoc (counting_speed_per_sec process_count) opt_prev_process_count_n_dt

    uploaded_bytes_doc uploaded opt_prev_uploaded = PP.hsep [bytes_to_unit_doc $ fromIntegral uploaded, "uploaded/tranfered bytes", opt_speed_doc]
      where
        opt_speed_doc = maybe PP.emptyDoc (upload_speed_per_sec uploaded) opt_prev_uploaded

    !doc =
      PP.hang 2 $
        PP.sep
          [ maybe PP.emptyDoc (PP.brackets . PP.pretty) opt_name
          , PP.concatWith
              (PP.surround $ PP.flatAlt PP.line (PP.comma PP.<> PP.space))
              [ counting_doc "files" new_file_count process_file_count (fmap (first prev_process_file_count) opt_prev_stat_n_time_elapse) (Just total_file_count)
              , counting_doc "dirs" new_dir_count process_dir_count (fmap (first prev_process_dir_count) opt_prev_stat_n_time_elapse) (Just total_dir_count)
              , counting_doc "chunks" new_chunk_count process_chunk_count (fmap (first prev_process_chunk_count) opt_prev_stat_n_time_elapse) Nothing
              , uploaded_bytes_doc upload_bytes (fmap (first prev_upload_bytes) opt_prev_stat_n_time_elapse)
              ]
          ]

    !cur_stat = PrevStatistic process_file_count process_dir_count process_chunk_count upload_bytes

  pure $ (doc, cur_stat)
