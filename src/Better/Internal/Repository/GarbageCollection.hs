{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.GarbageCollection (
  garbageCollection,
) where

import Prelude hiding (read)

import Data.Foldable (for_)
import Data.Function (fix, (&))

import Debug.Trace (traceMarker, traceMarkerIO)

import Data.Set qualified as Set

import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import System.IO.Error (tryIOError)

import Control.Monad (unless, void, when)

import Effectful qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

import Control.Concurrent.Async (replicateConcurrently_)
import Control.Concurrent.STM (
  atomically,
  modifyTVar',
  newTQueueIO,
  newTVarIO,
  readTVar,
  readTVarIO,
  retry,
  tryReadTQueue,
  writeTQueue,
 )
import Control.Exception (bracket_)

import Better.Hash (ChunkDigest (), FileDigest ())
import Better.Internal.Repository.LowLevel (
  FFile (file_sha),
  Object (chunk_name),
  Tree (tree_sha),
  Version (ver_root),
  catFile,
  catTree,
  listChunkDigests,
  listFileDigests,
  listTreeDigests,
  listVersions,
  pathOfChunk,
  pathOfFile,
  pathOfTree,
  removeFiles,
 )
import Better.Repository.Class qualified as E

{-# NOINLINE garbageCollection #-}
garbageCollection :: (E.Repository E.:> es) => E.Eff es ()
garbageCollection = gc_tree >>= gc_file >>= gc_chunk
  where
    {-# INLINE [2] gc_tree #-}
    gc_tree :: (E.Repository E.:> es) => E.Eff es (Set.Set FileDigest)
    gc_tree = Debug.Trace.traceMarker "gc_tree" $ E.reallyUnsafeUnliftIO $ \un -> do
      (traversal_queue, live_tree_set) <- do
        trees <-
          un $
            listVersions
              & fmap (ver_root . snd)
              & S.fold F.toSet

        q <- newTQueueIO
        atomically $ for_ trees $ writeTQueue q

        set <- newTVarIO trees

        pure (q, set)

      live_file_set <- newTVarIO Set.empty

      let worker_num = 10 :: Int
      live_worker_num <- newTVarIO worker_num
      waiting_num <- newTVarIO (0 :: Int)
      replicateConcurrently_ worker_num $ do
        fix $ \cont -> do
          opt_to_traverse <- bracket_ (atomically $ modifyTVar' waiting_num succ) (atomically $ modifyTVar' waiting_num pred) $ atomically $ do
            opt_to_traverse' <- tryReadTQueue traversal_queue
            case opt_to_traverse' of
              Just _ -> pure opt_to_traverse'
              Nothing -> do
                waiting <- readTVar waiting_num
                live_num <- readTVar live_worker_num
                if waiting == live_num
                  then do
                    modifyTVar' live_worker_num pred
                    pure Nothing
                  else retry

          case opt_to_traverse of
            Just e -> do
              catTree e
                & S.morphInner un
                & S.mapM
                  ( \case
                      Left dir -> do
                        let dir_sha = tree_sha dir
                        atomically $ do
                          prev_size <- Set.size <$> readTVar live_tree_set
                          modifyTVar' live_tree_set $ Set.insert dir_sha
                          cur_size <- Set.size <$> readTVar live_tree_set

                          let is_new_item = prev_size /= cur_size
                          when is_new_item $ do
                            writeTQueue traversal_queue dir_sha
                            modifyTVar' live_tree_set . Set.insert $ dir_sha
                      Right file -> atomically $ do
                        modifyTVar' live_file_set $ Set.insert $ file_sha file
                  )
                & S.fold F.drain
              cont
            Nothing -> pure ()

      -- Now marked_set should be filled with live nodes.

      live_tree_set' <- readTVarIO live_tree_set
      putStrLn $ "size of live tree set: " <> show (Set.size live_tree_set')

      -- TODO compact it
      Debug.Trace.traceMarkerIO "gc_tree/delete/begin"
      listTreeDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 100)
          ( \tree_digest -> tryIOError $ do
              let exist = Set.member tree_digest live_tree_set'
              unless exist $ void $ tryIOError $ do
                let tree_path = pathOfTree tree_digest
                putStrLn $ "delete tree: " <> Path.fromRelFile tree_path
                un $ removeFiles [tree_path]
          )
        & S.fold F.drain
      Debug.Trace.traceMarkerIO "gc_tree/delete/end"

      readTVarIO live_file_set

    {-# INLINE [2] gc_file #-}
    gc_file :: (E.Repository E.:> es) => Set.Set FileDigest -> E.Eff es (Set.Set ChunkDigest)
    gc_file live_file_set = E.reallyUnsafeUnliftIO $ \un -> Debug.Trace.traceMarker "gc_file" $ do
      listFileDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 100)
          ( \file_digest -> do
              let still_alive = Set.member file_digest live_file_set

              if still_alive
                then do
                  pure $! (catFile file_digest & S.morphInner un & fmap chunk_name)
                else do
                  let file_path = pathOfFile file_digest
                  putStrLn $ "delete file: " <> Path.fromRelFile file_path
                  void $ tryIOError $ un $ removeFiles [file_path]
                  pure S.nil
          )
        & S.concatMap id
        & S.fold F.toSet

    {-# INLINE [2] gc_chunk #-}
    gc_chunk :: (E.Repository E.:> es) => Set.Set ChunkDigest -> E.Eff es ()
    gc_chunk live_chunk_set = E.reallyUnsafeUnliftIO $ \un -> Debug.Trace.traceMarker "gc_chunk" $ do
      listChunkDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 100)
          ( \chunk_digest -> do
              let exist = Set.member chunk_digest live_chunk_set
              unless exist $ void $ tryIOError $ do
                let chunk_path = pathOfChunk chunk_digest
                putStrLn $ "delete chunk: " <> Path.fromRelFile chunk_path
                un $ removeFiles [chunk_path]
          )
        & S.fold F.drain
