{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Parser (
  benchGroup,
) where

import Criterion.Main (bench, bgroup, defaultMain, env, nf, nfAppIO, whnf, whnfAppIO)
import Criterion.Types (Benchmark)

import Control.Exception (displayException, mask_)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Char
import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.Functor.Identity (Identity (runIdentity))

import qualified Streamly.Console.Stdio as Stdio
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Parser as Par
import qualified Streamly.Data.Stream as S
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Internal.Data.Parser as Par

import Path (Path)
import qualified Path

import qualified Better.Repository.Backup as Repo

import Better.Hash (Digest, digestFromByteString)
import qualified Better.Internal.Repository.LowLevel as Repo
import qualified Better.Statistics.Backup as BackupSt
import Better.Statistics.Backup.Class (BackupStatistics, newChunkCount, newDirCount, newFileCount, processedChunkCount, processedDirCount, processedFileCount, totalDirCount, totalFileCount, uploadedBytes)
import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BSS
import Data.Word (Word8)
import Streamly.External.ByteString (fromArray, toArray)
import qualified Streamly.Internal.Data.Stream.Chunked as Chunked

import Text.Megaparsec (Parsec)
import qualified Text.Megaparsec as Mega
import qualified Text.Megaparsec.Byte as Byte
import qualified Text.Megaparsec.Byte.Lexer as Lex

benchGroup :: Benchmark
benchGroup =
  bgroup
    "parser"
    [ megaparsec_parser_banch 10000
    , megaparsec_parser_banch 100000
    , megaparsec_parser_banch 1000000
    , megaparsec_parser_banch'
    -- streamly_parser_bench, -- It takes too long to compile, so only uncomment it when it's needed.
    ]

type MegaParser a = Parsec Void BS.ByteString a

megaparsec_parser_banch :: Int -> Benchmark
megaparsec_parser_banch num =
  bench ("megaparsec-parse-then-fold-" <> show num) $
    whnfAppIO
      ( \input ->
            & fmap fromArray
            & S.filter (not . BS.null)
            & Chunked.splitOn (fromIntegral $ fromEnum '\n')
            & fmap fromArray
            & S.filter (not . BS.null)
            & S.mapM ((pure $!) . either (error . show) id . running)
            & S.fold the_fold
      )
      (_input num)

megaparsec_parser_banch' :: Benchmark
megaparsec_parser_banch' =
  bench "megaparsec-parse" $
    whnfAppIO
      ( \input ->
            & fmap fromArray
            & S.filter (not . BS.null)
            & Chunked.splitOn (fromIntegral $ fromEnum '\n')
            & fmap fromArray
            & S.filter (not . BS.null)
            & S.mapM ((pure $!) . either (error . show) id . running)
            & S.fold F.drain
      )
      (_input 10000)

{-# INLINE the_fold #-}
the_fold :: Monad m => F.Fold m (Path Path.Rel Path.Dir, Repo.FileSystemChange) Repo.FileSystemChanges
the_fold = F.foldl' (flip $ uncurry Repo.insertFileSystemChange) Repo.emptyFileSystemChanges

lexeme :: MegaParser a -> MegaParser a
lexeme = Lex.lexeme Byte.space1

keyword :: BS.ByteString -> MegaParser BS.ByteString
keyword = Lex.symbol Byte.space1

keyword' :: BS.ByteString -> MegaParser BS.ByteString
keyword' = Lex.symbol Byte.space

filepath_p :: MegaParser FilePath
filepath_p = BC.unpack <$> Mega.takeWhile1P (Just "filepath") ((\c -> isAlphaNum c || c == '/') . chr . fromIntegral)

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

parser :: MegaParser (Path Path.Rel Path.Dir, Repo.FileSystemChange)
parser = do
  rel_path <- to_rel_dir $ lexeme filepath_p
  !change <- (removeP <|> newP <|> freshP)
  pure (rel_path, change)
  where
    removeP = Repo.IsRemoved <$ keyword' "remove"
    newP = Repo.IsNew <$ keyword "new" <*> to_abs_file filepath_p
    freshP = Repo.NeedFreshBackup <$ keyword "fresh" <*> to_abs_file filepath_p

running = Mega.parse parser "src"

streamly_parser_bench :: Benchmark
streamly_parser_bench =
  bench "streamly" $
    whnf
      ( \input ->
          S.fromList input
            & S.concatMap (S.fromList . BS.unpack)
            & parse_changes
            & fmap (either (error . show) id)
            & S.fold the_fold
            & runIdentity
      )
      (_input 10000)
  where
    {-# INLINE the_fold #-}
    the_fold :: Monad m => F.Fold m (Path Path.Rel Path.Dir, Repo.FileSystemChange) Repo.FileSystemChanges
    the_fold = F.foldl' (flip $ uncurry Repo.insertFileSystemChange) Repo.emptyFileSystemChanges

    {-# INLINE lexeme #-}
    lexeme :: (Monad m) => Par.Parser Word8 m b -> Par.Parser Word8 m b
    lexeme p = p <* consume_spaces_and_newlines

    {-# INLINE consume_spaces_and_newlines #-}
    consume_spaces_and_newlines :: (Monad m) => Par.Parser Word8 m ()
    consume_spaces_and_newlines =
      Par.takeWhile
        (\c -> c == fromIntegral (fromEnum ' ') || c == fromIntegral (fromEnum '\n'))
        F.drain

    {-# INLINE keyword #-}
    keyword :: (Monad m) => BSS.ShortByteString -> Par.Parser Word8 m [Word8]
    keyword as = lexeme $ Par.listEq $ BSS.unpack as

    {-# INLINE filepath_p #-}
    filepath_p :: (Monad m) => Par.Parser Word8 m FilePath
    filepath_p = lexeme (BC.unpack . BS.pack <$> Par.takeWhile1 (\c -> c /= fromIntegral (fromEnum ' ') && c /= fromIntegral (fromEnum '\n')) F.toList)

    {-# INLINE to_rel_dir #-}
    to_rel_dir :: (Monad m) => Par.Parser Word8 m FilePath -> Par.Parser Word8 m (Path Path.Rel Path.Dir)
    to_rel_dir p = do
      raw <- p
      case Path.parseRelDir raw of
        Left e -> Par.die $ displayException e
        Right path -> pure path

    {-# INLINE to_abs_file #-}
    to_abs_file :: (Monad m) => Par.Parser Word8 m FilePath -> Par.Parser Word8 m (Path Path.Abs Path.File)
    to_abs_file p = do
      raw <- p
      case Path.parseAbsFile raw of
        Left e -> Par.die $ displayException e
        Right path -> pure path

    {-# INLINE parser #-}
    parser :: (Monad m) => Par.Parser Word8 m (Path Path.Rel Path.Dir, Repo.FileSystemChange)
    parser = do
      rel_path <- to_rel_dir filepath_p
      !change <- removeP <|> newP <|> freshP
      pure (rel_path, change)
      where
        {-# INLINE removeP #-}
        removeP = Repo.IsRemoved <$ keyword "remove"
        {-# INLINE newP #-}
        newP = Repo.IsNew <$ keyword "new" <*> to_abs_file filepath_p
        {-# INLINE freshP #-}
        freshP = Repo.NeedFreshBackup <$ keyword "fresh" <*> to_abs_file filepath_p

    {-# INLINE parse_changes #-}
    parse_changes :: (Monad m) => S.Stream m Word8 -> S.Stream m (Either Par.ParseError (Path Path.Rel Path.Dir, Repo.FileSystemChange))
    parse_changes = S.parseMany parser . S.dropWhile (\c -> c == fromIntegral (fromEnum ' ') || c == fromIntegral (fromEnum '\n'))

{-# NOINLINE _input #-}
_input :: Int -> [BS.ByteString]
_input num = BL.toChunks $ BB.toLazyByteString $ mconcat $ flip map [1 :: Int .. num] $ \i -> BB.byteString "a/b/c/" <> BB.byteString (BC.pack (show i)) <> BB.byteString " new /fs/x/y/z\n"
