{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.GarbageCollection (
  garbageCollection,
) where

import Prelude hiding (read)

import Data.Foldable (for_)
import Data.Function (fix, (&))

import qualified Data.Set as Set

import UnliftIO (
  MonadIO (..),
  MonadUnliftIO,
  modifyIORef',
  newIORef,
  readIORef,
  tryIO,
 )
import qualified UnliftIO.STM as Un

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString.UTF8 as UTF8

import Path ((</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Better.Repository.Class (MonadRepository (..))

import Control.Monad.Catch (MonadCatch)

import Better.Internal.Repository.LowLevel

garbageCollection :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
garbageCollection = gc_tree >> gc_file >> gc_chunk
  where
    gc_tree :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_tree = do
      -- tree level
      all_tree_file_set <-
        listFolderFiles folder_tree
          & fmap (UTF8.fromString . Path.fromRelFile)
          & S.parEval (S.maxBuffer 20 . S.eager True)
          & S.fold F.toSet

      traverse_set <- newIORef all_tree_file_set
      to_set <- Un.newTVarIO Set.empty

      listVersions
        & fmap ver_root
        & S.parEval (S.eager True . S.maxBuffer 20)
        & S.mapM (\sha -> Un.atomically $ Un.modifyTVar' to_set (Set.insert sha))
        & S.fold F.drain

      fix $ \(~cont) -> do
        to <- Un.atomically $ Un.readTVar to_set
        case Set.lookupMin to of
          Nothing -> pure ()
          Just e -> do
            Un.atomically $ Un.modifyTVar' to_set (Set.delete e)
            visited <- not . Set.member (d2b e) <$> readIORef traverse_set
            if visited
              then cont
              else do
                modifyIORef' traverse_set (Set.delete $ d2b e)
                traversed_set' <- readIORef traverse_set
                catTree e
                  & S.parEval (S.eager True . S.maxBuffer 20)
                  & S.mapMaybe (either (Just . tree_sha) (const Nothing))
                  & S.filter (flip Set.member traversed_set' . d2b)
                  & S.fold (F.drainMapM $ \s -> Un.atomically $ Un.modifyTVar' to_set (Set.insert s))
                cont

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete tree: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_tree </> rel_dead_file])

    gc_file :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_file = do
      -- tree level
      all_file_file_set <-
        listFolderFiles folder_file
          & fmap (UTF8.fromString . Path.fromRelFile)
          & S.fold F.toSet

      traverse_set <- newIORef all_file_file_set

      listFolderFiles folder_tree
        & S.concatMapM
          ( \tree_path -> do
              tree_digest <- t2d $ T.pack $ Path.fromRelFile $ tree_path
              pure $
                catTree tree_digest
                  & S.mapMaybe (either (const Nothing) (Just . file_sha))
          )
        & S.mapM
          ( \file_digest -> do
              modifyIORef' traverse_set $ Set.delete (d2b file_digest)
          )
        & S.fold F.drain

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete file: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_file </> rel_dead_file])

    gc_chunk :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_chunk = do
      all_file_file_set <-
        listFolderFiles folder_chunk
          & fmap (UTF8.fromString . Path.fromRelFile)
          & S.fold F.toSet

      traverse_set <- newIORef all_file_file_set

      listFolderFiles folder_file
        & S.concatMapM
          ( \tree_path -> do
              file_digest <- t2d $ T.pack $ Path.fromRelFile $ tree_path
              pure $
                catFile file_digest
                  & fmap chunk_name
          )
        & S.mapM
          ( \file_digest -> do
              modifyIORef' traverse_set $ Set.delete (d2b file_digest)
          )
        & S.fold F.drain

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete chunk: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_chunk </> rel_dead_file])
