{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeOperators #-}

module Better.Internal.Repository.GarbageCollection (
  garbageCollection,
) where

import Prelude hiding (read)

import Data.Foldable (for_)
import Data.Function (fix, (&))

import Debug.Trace (traceMarker, traceMarkerIO)

import qualified Data.Set as Set

import Path ((</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Control.Monad (unless, void, when)

import qualified Effectful as E
import qualified Effectful.Dispatch.Static.Unsafe as E

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
  s2d, removeFiles,
 )

import Better.Hash (Digest)
import Control.Concurrent.STM
import Control.Concurrent.Async (replicateConcurrently_)
import Control.Exception (bracket_)
import System.IO.Error (tryIOError)
import qualified Better.Repository.Class as E

garbageCollection :: (E.Repository E.:> es) => E.Eff es ()
garbageCollection = gc_tree >>= gc_file >>= gc_chunk
  where
    {-# INLINE [2] gc_tree #-}
    gc_tree :: (E.Repository E.:> es) => E.Eff es (Set.Set Digest)
    gc_tree = Debug.Trace.traceMarker "gc_tree" $ E.reallyUnsafeUnliftIO $ \un -> do
      (traversal_queue, live_tree_set) <- do
        trees <- un $
          listVersions
            & fmap ver_root
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
      n <- Set.size <$> readTVarIO live_tree_set
      putStrLn $ "size of live tree set: " <> show n

      -- TODO compact it
      Debug.Trace.traceMarkerIO "gc_tree/delete/begin"
      listFolderFiles folder_tree
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 10)
          ( \rel_tree_file -> tryIOError $ do
              tree_sha' <- s2d $ Path.fromRelFile rel_tree_file
              exist <- Set.member tree_sha' <$> atomically (readTVar live_tree_set)
              unless exist $ void $ tryIOError $ do
                putStrLn $ "delete tree: " <> Path.toFilePath rel_tree_file
                un $ removeFiles [folder_tree </> rel_tree_file]
          )
        & S.fold F.drain
      Debug.Trace.traceMarkerIO "gc_tree/delete/end"

      atomically $ readTVar live_file_set

    {-# INLINE [2] gc_file #-}
    gc_file :: (E.Repository E.:> es) => Set.Set Digest -> E.Eff es (Set.Set Digest)
    gc_file live_file_set = E.reallyUnsafeUnliftIO $ \un -> Debug.Trace.traceMarker "gc_file" $ do
      listFolderFiles folder_file
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 10)
          ( \rel_file -> do
              file_sha' <- s2d $ Path.fromRelFile rel_file
              let still_alive = Set.member file_sha' live_file_set

              if still_alive
                then do
                  pure $! (catFile file_sha' & S.morphInner un & fmap chunk_name)
                else do
                  putStrLn $ "delete file: " <> Path.toFilePath rel_file
                  void $ tryIOError $ un $ removeFiles [folder_file </> rel_file]
                  pure S.nil
          )
        & S.concatMap id
        & S.fold F.toSet

    {-# INLINE [2] gc_chunk #-}
    gc_chunk :: (E.Repository E.:> es) => Set.Set Digest -> E.Eff es ()
    gc_chunk live_chunk_set = E.reallyUnsafeUnliftIO $ \un -> Debug.Trace.traceMarker "gc_chunk" $ do
      listFolderFiles folder_chunk
        & S.morphInner un
        & S.parMapM
          (S.eager True . S.maxBuffer 10)
          ( \rel_chunk -> do
              chunk_sha' <- s2d $ Path.fromRelFile rel_chunk
              let exist = Set.member chunk_sha' live_chunk_set
              unless exist $ void $ tryIOError $ do
                putStrLn $ "delete chunk: " <> Path.toFilePath rel_chunk
                un $ removeFiles [folder_chunk </> rel_chunk]
          )
        & S.fold F.drain
