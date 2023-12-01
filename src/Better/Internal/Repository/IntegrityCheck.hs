{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.IntegrityCheck (
  checksum,
) where

import Prelude hiding (read)

import Data.Function ((&))

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Path ((</>))
import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Effectful qualified as E

import Better.Internal.Repository.LowLevel (
  catChunk,
  folder_chunk,
  folder_file,
  folder_tree,
  folder_version,
  listFolderFiles,
  read,
  s2d,
 )

import Better.Hash (ChunkDigest (UnsafeMkChunkDigest), hashArrayFoldIO)
import Better.Repository.Class qualified as E

import Control.Monad.IO.Class (liftIO)

import Data.Coerce (coerce)
import Data.Maybe (fromJust, isJust)

import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

checksum :: (E.Repository E.:> es) => Int -> E.Eff es ()
checksum n = E.reallyUnsafeUnliftIO $ \un -> do
  S.fromList [folder_version, folder_tree, folder_file]
    & S.concatMap
      ( \p ->
          listFolderFiles p
            & fmap (\f -> (Path.fromRelFile f, p </> f))
      )
    & S.morphInner un
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \(expected_sha, f) -> do
          actual_sha <-
            un $
              read f
                & S.fold (F.morphInner E.unsafeEff_ hashArrayFoldIO)
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
    & S.morphInner un
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \chunk_path -> do
          expected_sha <- fmap UnsafeMkChunkDigest <$> s2d $ Path.fromRelFile chunk_path
          actual_sha <- un $ catChunk expected_sha & S.fold (F.morphInner E.unsafeEff_ hashArrayFoldIO)
          if coerce expected_sha == actual_sha
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
