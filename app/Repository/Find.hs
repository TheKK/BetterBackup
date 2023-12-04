{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Repository.Find (
  findTree,
  searchDirInTree,
  searchFileInTree,
  searchItemInTree,
) where

import Control.Monad (when, (<$!>), (<=<))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (ExceptT, except, runExceptT, throwE)

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream qualified as S

import System.FilePath qualified as FP

import Data.Foldable (foldlM)
import Data.Function ((&))
import Data.Word (Word64)

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Path (Path)
import Path qualified

import Effectful qualified as E

import Better.Hash (FileDigest, TreeDigest)
import Better.Repository qualified as Repo
import Better.Repository.Class (Repository)

findTree :: (Repository E.:> es, E.IOE E.:> es) => Bool -> Maybe Word64 -> TreeDigest -> Maybe (Path.SomeBase Path.Dir) -> E.Eff es ()
findTree show_digest opt_depth tree_root opt_somebase_dir = do
  tree_root' <- case opt_somebase_dir of
    Just somebase_dir -> either error pure =<< searchDirInTree tree_root somebase_dir
    Nothing -> pure tree_root

  echo_tree opt_depth "/" tree_root'
  where
    {-# NOINLINE echo_tree #-}
    echo_tree :: (Repository E.:> es, E.IOE E.:> es) => Maybe Word64 -> T.Text -> TreeDigest -> E.Eff es ()
    echo_tree opt_cur_depth parent digest = do
      let opt_tree_digest = if show_digest then T.pack ("[" <> show digest <> "][d] ") else ""
      liftIO $ T.putStrLn $ opt_tree_digest <> parent

      when (opt_cur_depth /= Just 0) $
        Repo.catTree digest
          & S.mapM
            ( \case
                Left tree -> do
                  echo_tree (pred <$!> opt_cur_depth) (parent <> Repo.tree_name tree <> "/") (Repo.tree_sha tree)
                Right file -> do
                  let opt_file_digest = if show_digest then T.pack ("[" <> show (Repo.file_sha file) <> "][f] ") else ""
                  liftIO $ T.putStrLn $ opt_file_digest <> parent <> Repo.file_name file
            )
          & S.fold F.drain

searchFileInTree :: (Repository E.:> es) => TreeDigest -> Path.SomeBase Path.File -> E.Eff es (Either String FileDigest)
searchFileInTree root_digest somebase =
  searchItemInTree root_digest (Path.fromAbsFile abs_file_path) >>= \case
    Left err_msg -> error err_msg
    Right (Left tree_digest) -> error $ "'" <> Path.toFilePath abs_file_path <> " [" <> show tree_digest <> "] is not an file but directory"
    Right (Right file_digest) -> pure $! Right $! file_digest
  where
    abs_file_path :: Path Path.Abs Path.File
    abs_file_path = case somebase of
      Path.Abs abs_file -> abs_file
      Path.Rel rel_file -> [Path.absdir|/|] Path.</> rel_file

searchDirInTree :: (Repository E.:> es) => TreeDigest -> Path.SomeBase Path.Dir -> E.Eff es (Either String TreeDigest)
searchDirInTree root_digest somebase =
  searchItemInTree root_digest (Path.fromAbsDir abs_dir_path) >>= \case
    Left err_msg -> error err_msg
    Right (Left tree_digest) -> pure $! Right $! tree_digest
    Right (Right file_digest) -> error $ "'" <> Path.toFilePath abs_dir_path <> " [" <> show file_digest <> "] is not an directory but file"
  where
    abs_dir_path :: Path Path.Abs Path.Dir
    abs_dir_path = case somebase of
      Path.Abs abs_dir -> abs_dir
      Path.Rel rel_dir -> [Path.absdir|/|] Path.</> rel_dir

searchItemInTree :: (Repository E.:> es) => TreeDigest -> FilePath -> E.Eff es (Either String (Either TreeDigest FileDigest))
searchItemInTree !root_digest path = runExceptT $ snd <$> foldlM step (["/"], Left root_digest) path_segments
  where
    -- Make everything abs dir, then split them into ["/", "a", "b"] and drop the "/" part.
    path_segments = filter (/= "/") $ FP.splitDirectories path

    {-# INLINE step #-}
    step :: (Repository E.:> es) => ([FilePath], Either TreeDigest FileDigest) -> FilePath -> ExceptT String (E.Eff es) ([FilePath], Either TreeDigest FileDigest)
    step (traversed_pathes, !tree_or_file_digest) path_segment = do
      !digest <- case tree_or_file_digest of
        Left tree_digest -> pure tree_digest
        Right file_digest -> throwE $ "'" <> FP.joinPath traversed_pathes <> "' [" <> show file_digest <> "] is not an directory but file"

      !next_digest <-
        (except <=< lift) $
          Repo.catTree digest
            & S.mapMaybe
              ( \case
                  Left tree ->
                    if Repo.tree_name tree == T.pack path_segment
                      then Just $! Left $! Repo.tree_sha tree
                      else Nothing
                  Right file ->
                    if Repo.file_name file == T.pack path_segment
                      then Just $! Right $! Repo.file_sha file
                      else Nothing
              )
            & S.fold F.one
            & fmap
              ( \case
                  Nothing -> Left $ "unable to find directory '" <> path_segment <> "' under '" <> FP.joinPath traversed_pathes <> "' [" <> show digest <> "] in backup tree"
                  Just d -> Right d
              )
      pure (traversed_pathes <> [path_segment], next_digest)
