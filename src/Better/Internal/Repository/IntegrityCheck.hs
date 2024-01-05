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

import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (fromJust, isJust)

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E

import Control.Monad.IO.Class (liftIO)

import Better.Hash (ChunkDigest (..), FileDigest (..), TreeDigest (..), VersionDigest (..), hashArrayFoldIO)
import Better.Internal.Repository.LowLevel (
  listChunkDigests,
  listFileDigests,
  listTreeDigests,
  listVersionDigests,
  pathOfChunk,
  pathOfFile,
  pathOfTree,
  pathOfVersion,
 )
import Better.Internal.Repository.LowLevel qualified as Repo
import Better.Internal.Streamly.Crypto.AES (decryptCtr)
import Better.Repository.Class qualified as E

checksum :: (E.Repository E.:> es) => Int -> E.Eff es ()
checksum n = do
  aes <- Repo.getAES
  E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
    S.fromList
      [ fmap (\d -> (pathOfVersion d, coerce d)) listVersionDigests
      , fmap (\d -> (pathOfTree d, coerce d)) listTreeDigests
      , fmap (\d -> (pathOfFile d, coerce d)) listFileDigests
      , fmap (\d -> (pathOfChunk d, coerce d)) listChunkDigests
      ]
      & S.concatMap id
      & S.morphInner un
      & S.parMapM
        (S.maxBuffer (n + 1) . S.eager True)
        ( \(path, expected_digest) -> do
            actual_sha <-
              Repo.read path
                & S.morphInner un
                & decryptCtr aes (1024 * 32)
                & S.fold hashArrayFoldIO
            if expected_digest == actual_sha
              then pure Nothing
              else
                let
                  !actual_sha_str = T.pack $ show actual_sha
                in
                  pure $ Just (path, actual_sha_str)
        )
      & S.filter isJust
      & fmap fromJust
      & S.fold
        ( F.foldMapM $ \(invalid_f, actual_sha) ->
            liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.toFilePath invalid_f) <> ", checksum: " <> actual_sha
        )
