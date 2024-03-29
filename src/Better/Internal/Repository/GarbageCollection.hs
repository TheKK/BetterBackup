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
import Better.Logging.Effect (logging)
import Better.Logging.Effect qualified as E
import Better.Repository.Class qualified as E
import Control.Applicative ((<|>))
import Control.Concurrent.Async (replicateConcurrently_)
import Control.Concurrent.STM (
  atomically,
  check,
  modifyTVar',
  newTQueueIO,
  newTVarIO,
  readTQueue,
  readTVar,
  readTVarIO,
  retry,
  tryReadTQueue,
  writeTQueue,
 )
import Control.Exception (bracket_)
import Control.Monad (unless, void, when)
import Data.Foldable (for_)
import Data.Function (fix, (&))
import Data.Set qualified as Set
import Data.Word (Word32)
import Debug.Trace (traceMarker, traceMarkerIO)
import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Katip.Core qualified as L
import Path qualified
import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S
import System.IO.Error (tryIOError)
import Prelude hiding (read)

{-# NOINLINE garbageCollection #-}
garbageCollection :: (E.Repository E.:> es, E.Logging E.:> es) => E.Eff es ()
garbageCollection = gc_tree >>= gc_file >>= gc_chunk
  where
    {-# INLINE [2] gc_tree #-}
    gc_tree :: (E.Repository E.:> es, E.Logging E.:> es) => E.Eff es (Set.Set FileDigest)
    gc_tree = Debug.Trace.traceMarker "gc_tree" $ E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
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

      num_of_working_thread_tvar <- newTVarIO (0 :: Word32)
      let
        take_job = modifyTVar' num_of_working_thread_tvar succ
        done_job = modifyTVar' num_of_working_thread_tvar pred
        running_bracket = bracket_ (pure ()) (atomically done_job)
        no_one_is_running = check . (0 ==) =<< readTVar num_of_working_thread_tvar

      let worker_num = 10 :: Int
      replicateConcurrently_ worker_num $ do
        fix $ \cont -> do
          opt_to_traverse <- atomically $ do
            (Just <$> (take_job >> readTQueue traversal_queue)) <|> (Nothing <$ no_one_is_running)

          case opt_to_traverse of
            Just e -> running_bracket $ do
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
                      Right file -> atomically $ do
                        modifyTVar' live_file_set $ Set.insert $ file_sha file
                  )
                & S.fold F.drain
              cont
            Nothing -> pure ()

      -- Now marked_set should be filled with live nodes.

      live_tree_set' <- readTVarIO live_tree_set
      un $ logging L.InfoS $ L.ls @String "size of live tree set: " <> L.ls (show (Set.size live_tree_set'))

      -- TODO compact it
      Debug.Trace.traceMarkerIO "gc_tree/delete/begin"
      listTreeDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 8)
          ( \tree_digest -> tryIOError $ do
              let exist = Set.member tree_digest live_tree_set'
              unless exist $ void $ un $ do
                let tree_path = pathOfTree tree_digest
                logging L.DebugS $ L.ls @String "delete tree: " <> L.ls (Path.fromRelFile tree_path)
                removeFiles [tree_path]
          )
        & S.fold F.drain
      Debug.Trace.traceMarkerIO "gc_tree/delete/end"

      readTVarIO live_file_set

    {-# INLINE [2] gc_file #-}
    gc_file :: (E.Repository E.:> es, E.Logging E.:> es) => Set.Set FileDigest -> E.Eff es (Set.Set ChunkDigest)
    gc_file live_file_set = E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> Debug.Trace.traceMarker "gc_file" $ do
      un $ logging L.InfoS $ L.ls @String "size of live file set: " <> L.ls (show (Set.size live_file_set))

      listFileDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 8)
          ( \file_digest -> do
              let still_alive = Set.member file_digest live_file_set

              if still_alive
                then do
                  pure $! (catFile file_digest & S.morphInner un & fmap chunk_name)
                else do
                  let file_path = pathOfFile file_digest
                  void $ tryIOError $ un $ do
                    logging L.DebugS $ L.ls @String "delete file: " <> L.ls (Path.fromRelFile file_path)
                    removeFiles [file_path]
                  pure S.nil
          )
        & S.concatMap id
        & S.fold F.toSet

    {-# INLINE [2] gc_chunk #-}
    gc_chunk :: (E.Repository E.:> es, E.Logging E.:> es) => Set.Set ChunkDigest -> E.Eff es ()
    gc_chunk live_chunk_set = E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> Debug.Trace.traceMarker "gc_chunk" $ do
      un $ logging L.InfoS $ L.ls @String "size of live chunk set: " <> L.ls (show (Set.size live_chunk_set))

      listChunkDigests
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 16)
          ( \chunk_digest -> do
              let exist = Set.member chunk_digest live_chunk_set
              unless exist $ void $ tryIOError $ un $ do
                let chunk_path = pathOfChunk chunk_digest
                logging L.DebugS $ L.ls @String "delete chunk: " <> L.ls (Path.fromRelFile chunk_path)
                removeFiles [chunk_path]
          )
        & S.fold F.drain
