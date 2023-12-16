{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeOperators #-}

module Cli.PatchBackup (
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

import Ki qualified

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Exception (displayException, mask_)
import Control.Monad (forever, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Char (chr, isPrint, isSpace)
import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.Void (Void)

import Streamly.Console.Stdio qualified as Stdio
import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream qualified as S

import Path (Path)
import Path qualified

import Text.Megaparsec qualified as Mega
import Text.Megaparsec.Byte qualified as Byte
import Text.Megaparsec.Byte.Lexer qualified as Lex

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BC

import Effectful qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as EU

import Better.Data.FileSystemChanges qualified as FSC
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

import Monad (runRepositoryForBackupFromCwd)
import Streamly.External.ByteString (fromArray)
import Streamly.Internal.Data.Stream.Chunked qualified as Chunked
import Util.Options (treeDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Construct new version from existed tree and series of filesystem events"
        ]

    parser =
      go
        <$> argument
          treeDigestRead
          ( fold
              [ metavar "TREE_DIGEST"
              , help "the tree you'd like to patch with"
              ]
          )

    go tree_digest = do
      fsc <- read_filesystem_chagnes_from_stdin

      void $ runRepositoryForBackupFromCwd $ EU.reallyUnsafeUnliftIO $ \un -> do
        let
          process_reporter = forever $ do
            mask_ $ un report_backup_stat
            threadDelay (1000 * 1000)

        (v_digest, v) <- Ki.scoped $ \scope -> do
          Ki.fork_ scope process_reporter

          un $ do
            Repo.runBackup $ do
              ret <- Repo.backupDirFromExistedTree fsc tree_digest [Path.reldir|.|]
              case ret of
                Just (Repo.DirEntryDir new_tree_digest _) -> pure new_tree_digest
                Just (Repo.DirEntryFile _ _) -> error "oh no, the root is now a file and but I only backup directories!"
                Nothing -> Repo.backupDirFromList [] -- Empty dir
        putStrLn "result:" >> un report_backup_stat
        print v

        pure (v_digest, v)

{-# NOINLINE read_filesystem_chagnes_from_stdin #-}
read_filesystem_chagnes_from_stdin :: IO FSC.FileSystemChanges
read_filesystem_chagnes_from_stdin = do
  S.unfold Stdio.chunkReader ()
    & Chunked.splitOn (fromIntegral $ fromEnum '\n')
    & S.filter (not . BS.null . fromArray)
    & fmap (either (error . show) id . parse_change "stdin" . fromArray)
    & S.fold the_fold

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

type MegaParser a = Mega.Parsec Void BS.ByteString a

{-# INLINE the_fold #-}
the_fold :: Monad m => F.Fold m (Path Path.Rel Path.Dir, FSC.FileSystemChange) FSC.FileSystemChanges
the_fold = F.foldl' (flip $ uncurry FSC.insert') FSC.empty

parse_change :: String -> BC.ByteString -> Either (Mega.ParseErrorBundle BC.ByteString Void) (Path Path.Rel Path.Dir, FSC.FileSystemChange)
parse_change = Mega.parse (Byte.space *> parser <* Mega.eof)
  where
    lexeme :: MegaParser a -> MegaParser a
    lexeme = Lex.lexeme (Byte.space1 <|> Mega.eof)

    keyword :: BS.ByteString -> MegaParser BS.ByteString
    keyword = Lex.symbol (Byte.space1 <|> Mega.eof)

    filepath_p :: MegaParser FilePath
    filepath_p = lexeme $ BC.unpack <$> Mega.takeWhile1P (Just "filepath") ((\c -> not (isSpace c) && isPrint c) . chr . fromIntegral)

    to_rel_dir :: MegaParser FilePath -> MegaParser (Path Path.Rel Path.Dir)
    to_rel_dir p = do
      raw <- p
      case Path.parseRelDir raw of
        Left e -> fail $ displayException e
        Right path -> pure path

    to_abs_file :: MegaParser FilePath -> MegaParser (Path Path.Abs Path.File)
    to_abs_file p = do
      raw <- p
      case Path.parseAbsFile raw of
        Left e -> fail $ displayException e
        Right path -> pure path

    parser :: MegaParser (Path Path.Rel Path.Dir, FSC.FileSystemChange)
    parser = do
      rel_path <- to_rel_dir filepath_p
      !change <- removeP <|> newP <|> freshP
      pure (rel_path, change)
      where
        removeP = FSC.IsRemoved <$ keyword "remove"
        newP = FSC.IsNew <$ keyword "new" <*> to_abs_file filepath_p
        freshP = FSC.NeedFreshBackup <$ keyword "fresh" <*> to_abs_file filepath_p
