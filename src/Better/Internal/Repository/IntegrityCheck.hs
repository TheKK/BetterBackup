{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeOperators #-}

module Better.Internal.Repository.IntegrityCheck (
  checksum,
) where

import Prelude hiding (read)

import Data.Function ((&))

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Path ((</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import qualified Effectful as E

import Better.Internal.Repository.LowLevel (
  catChunk,
  folder_chunk,
  folder_file,
  folder_tree,
  listFolderFiles,
  s2d, read,
 )

import Better.Hash (hashArrayFoldIO)
import qualified Better.Repository.Class as E

import Data.Maybe (fromJust, isJust)
import Data.Word (Word8)
import qualified Streamly.Data.Array as Array
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Control.Monad.IO.Unlift as Un
import qualified Effectful.Dispatch.Static.Unsafe as E
import qualified Effectful.Dispatch.Static as E

checksum :: (E.Repository E.:> es) => Int -> E.Eff es ()
checksum n = E.reallyUnsafeUnliftIO $ \un -> do
  S.fromList [folder_tree, folder_file]
    & S.concatMap
      ( \p ->
          listFolderFiles p
            & fmap (\f -> (Path.fromRelFile f, p </> f))
      )
    & S.morphInner un
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \(expected_sha, f) -> do
          actual_sha <- un $
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
          expected_sha <- s2d $ Path.fromRelFile chunk_path
          actual_sha <- un $ catChunk expected_sha & S.fold (F.morphInner E.unsafeEff_ hashArrayFoldIO)
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
