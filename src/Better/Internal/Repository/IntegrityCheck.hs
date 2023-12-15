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
import Effectful.Dispatch.Static qualified as E

import Better.Internal.Repository.LowLevel (
  folder_chunk,
  folder_file,
  folder_tree,
  folder_version,
  listFolderFiles,
  s2d,
 )

import Better.Hash (hashArrayFoldIO)
import Better.Repository.Class qualified as E

import Control.Monad.IO.Class (liftIO)

import Data.Coerce (coerce)
import Data.Maybe (fromJust, isJust)

import Better.Internal.Repository.LowLevel qualified as Repo
import Better.Internal.Streamly.Crypto.AES (decryptCtr)

checksum :: (E.Repository E.:> es) => Int -> E.Eff es ()
checksum n = do
  -- TODO Use traverse & worker pipeline to reuse worker.
  check_encrypted n folder_version
  check_plaintext n folder_tree
  check_encrypted n folder_file
  check_encrypted n folder_chunk

check_plaintext :: (E.Repository E.:> es) => Int -> Path.Path Path.Rel Path.Dir -> E.Eff es ()
check_plaintext n folder = E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
  listFolderFiles folder
    & S.morphInner un
    & S.parMapM
      (S.maxBuffer (n + 1) . S.eager True)
      ( \chunk_path -> do
          expected_sha <- s2d $ Path.fromRelFile chunk_path
          actual_sha <-
            Repo.read (folder </> chunk_path)
              & S.morphInner un
              & S.fold hashArrayFoldIO
          if coerce expected_sha == actual_sha
            then pure Nothing
            else
              let
                !actual_sha_str = T.pack $ show actual_sha
              in
                pure $ Just (folder </> chunk_path, actual_sha_str)
      )
    & S.filter isJust
    & fmap fromJust
    & S.fold
      ( F.foldMapM $ \(invalid_f, actual_sha) ->
          liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.toFilePath invalid_f) <> ", checksum: " <> actual_sha
      )

check_encrypted :: (E.Repository E.:> es) => Int -> Path.Path Path.Rel Path.Dir -> E.Eff es ()
check_encrypted n folder = do
  aes <- Repo.getAES
  E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
    listFolderFiles folder
      & S.morphInner un
      & S.parMapM
        (S.maxBuffer (n + 1) . S.eager True)
        ( \chunk_path -> do
            expected_sha <- s2d $ Path.fromRelFile chunk_path
            actual_sha <-
              Repo.read (folder </> chunk_path)
                & S.morphInner un
                & decryptCtr aes (1024 * 32)
                & S.fold hashArrayFoldIO
            if expected_sha == actual_sha
              then pure Nothing
              else
                let
                  !actual_sha_str = T.pack $ show actual_sha
                in
                  pure $ Just (folder </> chunk_path, actual_sha_str)
        )
      & S.filter isJust
      & fmap fromJust
      & S.fold
        ( F.foldMapM $ \(invalid_f, actual_sha) ->
            liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.toFilePath invalid_f) <> ", checksum: " <> actual_sha
        )
