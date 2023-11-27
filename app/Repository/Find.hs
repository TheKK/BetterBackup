{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeOperators #-}

module Repository.Find (
  findTree,
) where

import Control.Monad (when, (<$!>))
import Control.Monad.IO.Class (MonadIO (liftIO))

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream as S

import qualified System.FilePath as FP

import Data.Foldable (foldlM)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Word (Word64)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Path (Path)
import qualified Path

import qualified Effectful as E

import Better.Hash (Digest)
import qualified Better.Repository as Repo
import Better.Repository.Class (Repository)

findTree :: (Repository E.:> es, E.IOE E.:> es) => Bool -> Maybe Word64 -> Digest -> Maybe (Path.SomeBase Path.Dir) -> E.Eff es ()
findTree show_digest opt_depth tree_root opt_somebase_dir = do
  tree_root' <- case opt_somebase_dir of
    Just somebase_dir -> search_dir_in_tree tree_root somebase_dir
    Nothing -> pure tree_root

  echo_tree opt_depth "/" tree_root'
  where
    {-# NOINLINE echo_tree #-}
    echo_tree :: (Repository E.:> es, E.IOE E.:> es) => Maybe Word64 -> T.Text -> Digest -> E.Eff es ()
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

search_dir_in_tree :: (Repository E.:> es) => Digest -> Path.SomeBase Path.Dir -> E.Eff es Digest
search_dir_in_tree root_digest somebase = do
  -- Make everything abs dir, then split them into ["/", "a", "b"] and drop the "/" part.
  let dir_segments = tail $ FP.splitDirectories $ Path.fromAbsDir abs_dir_path
  go root_digest dir_segments
  where
    abs_dir_path :: Path Path.Abs Path.Dir
    abs_dir_path = case somebase of
      Path.Abs abs_dir -> abs_dir
      Path.Rel rel_dir -> [Path.absdir|/|] Path.</> rel_dir

    go :: (Repository E.:> es) => Digest -> [FilePath] -> E.Eff es Digest
    go digest path_segments = snd <$> foldlM step (["/"], digest) path_segments

    step :: (Repository E.:> es) => ([FilePath], Digest) -> FilePath -> E.Eff es ([FilePath], Digest)
    step (traversed_pathes, !digest) path = do
      !next_digest <-
        Repo.catTree digest
          & S.mapMaybe
            ( \case
                Left tree ->
                  if Repo.tree_name tree == T.pack path
                    then Just $! Repo.tree_sha tree
                    else Nothing
                Right _file -> Nothing
            )
          & S.fold F.one
          & fmap
            ( fromMaybe . error $
                "unable to find directory '" <> path <> "' under '" <> FP.joinPath traversed_pathes <> "' [" <> show digest <> "] in backup tree"
            )
      pure (traversed_pathes <> [path], next_digest)