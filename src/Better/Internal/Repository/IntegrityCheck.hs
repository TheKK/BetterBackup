{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.IntegrityCheck (
  checksum,
) where

import Prelude hiding (read)

import Data.Function ((&))

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Path (Path, (</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Control.Monad.Catch (MonadThrow)

import Better.Internal.Repository.LowLevel (
  catChunk,
  folder_chunk,
  folder_file,
  folder_tree,
  listFolderFiles,
  s2d,
 )

import Better.Hash (hashArrayFoldIO)
import Better.Repository.Class (MonadRepository (..))
import Data.Maybe (fromJust, isJust)
import Data.Word (Word8)
import qualified Streamly.Data.Array as Array
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Control.Monad.IO.Unlift as Un

{-# INLINE checksum #-}
checksum :: (MonadThrow m, Un.MonadUnliftIO m, MonadRepository m) => Int -> m ()
checksum n = do
  S.fromList [folder_tree, folder_file]
    & S.concatMap
      ( \p ->
          listFolderFiles p
            & fmap (\f -> (Path.fromRelFile f, p </> f))
      )
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \(expected_sha, f) -> do
          actual_sha <-
            read f
              & S.fold (F.morphInner liftIO hashArrayFoldIO)
          if show actual_sha == expected_sha
            then pure Nothing
            else pure $ Just (f, actual_sha)
      )
    & S.filter (isJust)
    & fmap (fromJust)
    & S.fold
      ( F.foldMapM $ \(invalid_f, actual_sha) ->
          liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.fromRelFile invalid_f) <> ", " <> T.pack (show actual_sha)
      )

  listFolderFiles folder_chunk
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \chunk_path -> do
          expected_sha <- s2d $ Path.fromRelFile chunk_path
          actual_sha <- catChunk expected_sha & S.fold (F.morphInner liftIO hashArrayFoldIO)
          if expected_sha == actual_sha
            then pure Nothing
            else
              let
                !actual_sha_str = T.pack $ show actual_sha
              in
                pure $ Just (folder_chunk </> chunk_path, actual_sha_str)
      )
    & S.filter (isJust)
    & fmap (fromJust)
    & S.fold
      ( F.foldMapM $ \(invalid_f, actual_sha) ->
          liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.toFilePath invalid_f) <> ", checksum: " <> actual_sha
      )

read
  :: (MonadIO m, MonadRepository m)
  => Path Path.Rel Path.File
  -> S.Stream m (Array.Array Word8)
read p = S.concatEffect $ do
  f <- mkRead
  pure $ f p
