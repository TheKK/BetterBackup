{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.GarbageCollection (
  garbageCollection,
) where

import Prelude hiding (read)

import Data.Foldable (for_)
import Data.Function (fix, (&))

import Debug.Trace (traceMarker, traceMarkerIO)

import qualified Data.Set as Set

import UnliftIO (
  MonadIO (..),
  MonadUnliftIO,
  replicateConcurrently_,
  tryIO,
 )
import qualified UnliftIO.Exception as Un
import qualified UnliftIO.STM as Un

import Path ((</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Control.Monad (unless, void, when)
import Control.Monad.Catch (MonadCatch)

import Crypto.Hash (Digest, SHA256)

import Better.Internal.Repository.LowLevel (
  FFile (file_sha),
  Object (chunk_name),
  Tree (tree_sha),
  Version (ver_root),
  catFile,
  catTree,
  folder_chunk,
  folder_file,
  folder_tree,
  listFolderFiles,
  listVersions,
  s2d,
 )

import Better.Repository.Class (MonadRepository (..))

{-# INLINE garbageCollection #-}
garbageCollection :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
garbageCollection = gc_tree >>= gc_file >>= gc_chunk
  where
    {-# INLINE [2] gc_tree #-}
    gc_tree :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m (Un.TVar (Set.Set (Digest SHA256)))
    gc_tree = Debug.Trace.traceMarker "gc_tree" $ do
      (traversal_queue, live_tree_set) <- do
        trees <-
          listVersions
            & fmap ver_root
            & S.fold F.toSet

        q <- Un.newTQueueIO
        Un.atomically $ for_ trees $ Un.writeTQueue q

        set <- Un.newTVarIO trees

        pure (q, set)

      live_file_set <- Un.newTVarIO Set.empty

      let worker_num = 10 :: Int
      live_worker_num <- Un.newTVarIO worker_num
      waiting_num <- Un.newTVarIO (0 :: Int)
      replicateConcurrently_ worker_num $ do
        fix $ \(~cont) -> do
          opt_to_traverse <- Un.bracket_ (Un.atomically $ Un.modifyTVar' waiting_num succ) (Un.atomically $ Un.modifyTVar' waiting_num pred) $ Un.atomically $ do
            opt_to_traverse' <- Un.tryReadTQueue traversal_queue
            case opt_to_traverse' of
              Just _ -> pure opt_to_traverse'
              Nothing -> do
                waiting <- Un.readTVar waiting_num
                live_num <- Un.readTVar live_worker_num
                if waiting == live_num
                  then do
                    Un.modifyTVar' live_worker_num pred
                    pure Nothing
                  else Un.retrySTM

          case opt_to_traverse of
            Just e -> do
              catTree e
                & S.mapM
                  ( \case
                      Left dir -> do
                        let dir_sha = tree_sha dir
                        Un.atomically $ do
                          prev_size <- Set.size <$> Un.readTVar live_tree_set
                          Un.modifyTVar' live_tree_set $ Set.insert dir_sha
                          cur_size <- Set.size <$> Un.readTVar live_tree_set

                          let is_new_item = prev_size /= cur_size
                          when is_new_item $ do
                            Un.writeTQueue traversal_queue dir_sha
                            Un.modifyTVar' live_tree_set . Set.insert $ dir_sha
                      Right file -> Un.atomically $ do
                        Un.modifyTVar' live_file_set $ Set.insert $ file_sha file
                  )
                & S.fold F.drain
              cont
            Nothing -> pure ()

      -- Now marked_set should be filled with live nodes.
      n <- Set.size <$> Un.readTVarIO live_tree_set
      liftIO $ putStrLn $ "size of live tree set: " <> show n

      -- TODO compact it
      liftIO $ Debug.Trace.traceMarkerIO "gc_tree/delete/begin"
      listFolderFiles folder_tree
        & S.parMapM
          (S.eager True . S.maxBuffer 10)
          ( \rel_tree_file -> tryIO $ do
              tree_sha' <- s2d $ Path.fromRelFile rel_tree_file
              exist <- Set.member tree_sha' <$> Un.atomically (Un.readTVar live_tree_set)
              unless exist $ void $ tryIO $ do
                Un.atomically $ Un.modifyTVar' live_tree_set $ Set.delete tree_sha'
                liftIO $ putStrLn $ "delete tree: " <> Path.toFilePath rel_tree_file
                removeFiles [folder_tree </> rel_tree_file]
          )
        & S.fold F.drain
      liftIO $ Debug.Trace.traceMarkerIO "gc_tree/delete/end"

      pure live_file_set

    {-# INLINE [2] gc_file #-}
    gc_file :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => Un.TVar (Set.Set (Digest SHA256)) -> m (Un.TVar (Set.Set (Digest SHA256)))
    gc_file live_file_set = Debug.Trace.traceMarker "gc_file" $ do
      live_chunk_set <-
        listFolderFiles folder_file
          & S.parMapM
            (S.eager True . S.maxBuffer 10)
            ( \rel_file -> do
                file_sha' <- s2d $ Path.fromRelFile rel_file
                exist <- Set.member file_sha' <$> Un.atomically (Un.readTVar live_file_set)
                unless exist $ void $ tryIO $ do
                  Un.atomically $ Un.modifyTVar' live_file_set $ Set.delete file_sha'
                  liftIO $ putStrLn $ "delete file: " <> Path.toFilePath rel_file
                  removeFiles [folder_file </> rel_file]

                pure (catFile file_sha' & fmap chunk_name)
            )
          & S.concatMap id
          & S.fold F.toSet

      Un.newTVarIO live_chunk_set

    {-# INLINE [2] gc_chunk #-}
    gc_chunk :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => Un.TVar (Set.Set (Digest SHA256)) -> m ()
    gc_chunk live_chunk_set = Debug.Trace.traceMarker "gc_chunk" $ do
      listFolderFiles folder_chunk
        & S.parMapM
          (S.eager True . S.maxBuffer 10)
          ( \rel_chunk -> do
              chunk_sha' <- s2d $ Path.fromRelFile rel_chunk
              exist <- Set.member chunk_sha' <$> Un.atomically (Un.readTVar live_chunk_set)
              unless exist $ void $ tryIO $ do
                Un.atomically $ Un.modifyTVar' live_chunk_set $ Set.delete chunk_sha'
                liftIO $ putStrLn $ "delete chunk: " <> Path.toFilePath rel_chunk
                removeFiles [folder_file </> rel_chunk]
          )
        & S.fold F.drain
