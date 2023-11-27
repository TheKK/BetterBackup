{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Better.Data.FileSystemChanges (
  FileSystemChanges,
  FileSystemChange (..),
  empty,
  insert',
  submap,
  hasDescendants,
  lookupDir,
  lookupFile,
  toList,

  -- * Tests
  props_filesystem_change,
) where

import GHC.Generics (Generic)

import Control.Monad (replicateM)

import System.FilePath (dropTrailingPathSeparator)

import Data.Coerce (coerce)
import Data.Foldable (for_)
import Data.Function ((&))
import Data.List (foldl', intercalate)
import Data.Traversable (for)

import qualified Data.Set as Set

import Data.Trie (Trie)
import qualified Data.Trie as Trie

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC

import Control.DeepSeq (NFData)

import Path (Path, (</>))
import qualified Path

import qualified Hedgehog as H
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

-- | Information about what changes after last backup.
newtype FileSystemChanges = FileSystemChanges {un_filesystem_changes :: Trie FileSystemChange}
  deriving (Show, Eq)

-- | What happends to these file/directory.
data FileSystemChange
  = IsRemoved
  | IsNew (Path Path.Abs Path.File)
  | NeedFreshBackup (Path Path.Abs Path.File)
  deriving (Eq, Show, Generic, NFData)

empty :: FileSystemChanges
empty = FileSystemChanges Trie.empty

-- | Insert FileSystemChange to storage strictly.
insert' :: Path Path.Rel Path.Dir -> FileSystemChange -> FileSystemChanges -> FileSystemChanges
insert' !p !change (FileSystemChanges fsc) =
  -- The rules are simple here:
  -- - First, when the path has parent in exitsed tree, discard this path
  -- - Second, when the path has any child in existed tree, discard these children
  --
  -- These rules hold because all change status of parent would mask change status of children
  -- currently, so it's safe to remove them from the tree to safe space. In fact it's also safe
  -- to keep them in the tree since we won't traverse to children when we found change status
  -- on parent.
  FileSystemChanges $!
    if has_changes_on_parent
      then fsc
      else -- Trie.deleteSubmap would delete p_bs itself,
        Trie.insert path_bs change $ Trie.deleteSubmap path_bs fsc
  where
    path_bs = convert_path_to_key p

    has_changes_on_parent = case Trie.minMatch fsc path_bs of
      Just (_, _, rest) | not (BS.null rest) -> True
      _ -> False

lookupFile :: Path Path.Rel Path.File -> FileSystemChanges -> Maybe FileSystemChange
lookupFile p = Trie.lookup path_bs . coerce
  where
    path_bs = BC.pack $ Path.toFilePath p

lookupDir :: Path Path.Rel Path.Dir -> FileSystemChanges -> Maybe FileSystemChange
lookupDir p = Trie.lookup path_bs . coerce
  where
    path_bs = convert_path_to_key p

submap :: Path Path.Rel Path.Dir -> FileSystemChanges -> FileSystemChanges
submap p = FileSystemChanges . Trie.submap path_bs . coerce
  where
    path_bs = convert_path_to_key p

hasDescendants :: Path Path.Rel Path.Dir -> FileSystemChanges -> Bool
hasDescendants p = not . Trie.null . un_filesystem_changes . submap p

toList :: FileSystemChanges -> [(BC.ByteString, FileSystemChange)]
toList = Trie.toList . coerce

-- Use this function to handle "./" related issue. We use empty path ("") to represent root here.
convert_path_to_key :: Path Path.Rel Path.Dir -> BC.ByteString
convert_path_to_key p = if p == [Path.reldir|.|] then "" else BC.pack $ dropTrailingPathSeparator $ Path.toFilePath p

props_filesystem_change :: H.Group
props_filesystem_change =
  H.Group
    "FileSystemChanges"
    [ ("prop_order_of_insertion_between_child_and_parent_does_not_matter", prop_order_of_insertion_between_child_and_parent_does_not_matter)
    , ("prop_existed_parent_should_mask_all_child_changes", prop_existed_parent_should_filter_out_all_child_changes)
    , ("prop_insert_parent_should_remove_all_child_changes", prop_insert_parent_should_remove_all_child_changes)
    , ("prop_lookup_after_lookup", prop_lookup_def)
    , ("prop_submap_def", prop_submap_def)
    , ("prop_hasDescendants_def", prop_hasDescendants_def)
    ]
  where
    prop_order_of_insertion_between_child_and_parent_does_not_matter :: H.Property
    prop_order_of_insertion_between_child_and_parent_does_not_matter = H.property $ do
      parent_pathes <- H.forAll $ path_segments_gen $ Range.linear 0 10
      parent_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : parent_pathes)
      parent_change <- H.forAll filesystem_change_gen
      H.annotateShow (parent_rel_dir, parent_change)

      child_pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10
      child_rel_dir <- H.evalIO $ fmap (parent_rel_dir Path.</>) $ Path.parseRelDir $ intercalate "/" child_pathes
      child_change <- H.forAll filesystem_change_gen
      H.annotateShow (child_rel_dir, child_change)

      H.cover 60 "change are not identical" $ parent_change /= child_change

      H.annotate "order of insersion between child and parent won't affect the result"
      insert' child_rel_dir child_change (insert' parent_rel_dir parent_change empty)
        H.=== insert' parent_rel_dir parent_change (insert' child_rel_dir child_change empty)

    prop_existed_parent_should_filter_out_all_child_changes :: H.Property
    prop_existed_parent_should_filter_out_all_child_changes = H.property $ do
      parent_pathes <- H.forAll $ path_segments_gen $ Range.linear 0 10
      parent_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : parent_pathes)
      H.annotateShow parent_rel_dir

      parent_change <- H.forAll filesystem_change_gen

      let fsc = empty & insert' parent_rel_dir parent_change

      child_pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10
      child_rel_dir <- H.evalIO $ fmap (parent_rel_dir Path.</>) $ Path.parseRelDir $ intercalate "/" child_pathes
      H.annotateShow child_rel_dir

      child_change <- H.forAll filesystem_change_gen

      H.annotate "parent path would filter out all following children"
      fsc H.=== insert' child_rel_dir child_change fsc

    prop_insert_parent_should_remove_all_child_changes :: H.Property
    prop_insert_parent_should_remove_all_child_changes = H.property $ do
      parent_pathes <- H.forAll $ path_segments_gen $ Range.linear 0 10
      parent_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : parent_pathes)
      H.annotateShow parent_rel_dir

      parent_change <- H.forAll filesystem_change_gen

      child_pathes_changes <- H.forAll $ Gen.list (Range.linear 1 100) $ do
        child_pathes <- path_segments_gen $ Range.linear 1 10
        change <- filesystem_change_gen
        pure (child_pathes, change)

      child_changes <- for child_pathes_changes $ \(pathes, change) -> do
        child_rel_path <- H.evalIO $ fmap (parent_rel_dir Path.</>) $ Path.parseRelDir $ intercalate "/" pathes
        pure (child_rel_path, change)
      H.annotateShow child_changes

      let fsc_with_children =
            foldl'
              (\fsc' (rel_dir, change) -> insert' rel_dir change fsc')
              empty
              child_changes
      H.annotateShow fsc_with_children

      H.annotate "parent path would discard all of its children"
      insert' parent_rel_dir parent_change fsc_with_children
        H.=== insert' parent_rel_dir parent_change empty

    prop_lookup_def :: H.Property
    prop_lookup_def = H.property $ do
      path_segments <-
        H.forAll $
          Gen.frequency
            [ (10, path_segments_gen $ Range.linear 1 10)
            , (1, pure [])
            ]

      H.cover 1 "./" $ null path_segments

      rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : path_segments)
      H.annotateShow rel_dir

      change <- H.forAll filesystem_change_gen

      let fsc = insert' rel_dir change empty
      H.annotateShow fsc

      lookupDir rel_dir fsc H.=== Just change

    prop_submap_def :: H.Property
    prop_submap_def = H.property $ do
      parent_path_segments <-
        H.forAll $
          Gen.frequency
            [ (10, path_segments_gen $ Range.linear 1 10)
            , (1, pure [])
            ]

      H.cover 1 "./" $ null parent_path_segments

      parent_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : parent_path_segments)
      H.annotateShow parent_rel_dir

      set_size <- H.forAll $ Gen.int $ Range.linear 1 100

      children_rel_dir_set <- fmap Set.fromList <$> replicateM set_size $ do
        -- We only generate one level of child to avoid overlapping parents between them.
        child_path_segments <- H.forAll $ path_segments_gen $ Range.constant 1 1
        fmap (parent_rel_dir </>) <$> H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : child_path_segments)
      H.annotateShow children_rel_dir_set

      H.cover 90 "has more than 1 child" $ Set.size children_rel_dir_set > 1

      change <- H.forAll filesystem_change_gen

      let fsc = foldl' (\acc rel_dir -> insert' rel_dir change acc) empty children_rel_dir_set
      H.annotateShow fsc

      H.annotate "All children in parent/child should exist in submap of parent"
      for_ children_rel_dir_set $ \rel_dir -> do
        lookupDir rel_dir (submap parent_rel_dir fsc) H.=== Just change

    prop_hasDescendants_def :: H.Property
    prop_hasDescendants_def = H.property $ do
      parent_path_segments <-
        H.forAll $
          Gen.frequency
            [ (10, path_segments_gen $ Range.linear 1 10)
            , (1, pure [])
            ]

      H.cover 1 "./" $ null parent_path_segments

      parent_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : parent_path_segments)
      H.annotateShow parent_rel_dir

      child_path_segments <- H.forAll $ path_segments_gen $ Range.constant 1 10
      child_rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" ("." : child_path_segments)
      H.annotateShow child_rel_dir

      change <- H.forAll filesystem_change_gen

      let fsc = insert' (parent_rel_dir </> child_rel_dir) change empty
      H.annotateShow fsc

      H.annotate "Children should be descendants of parent"
      hasDescendants parent_rel_dir fsc H.=== True

    path_segments_gen :: H.Range Int -> H.Gen [String]
    path_segments_gen length_range = Gen.list length_range $ replicateM 2 Gen.alphaNum

    filesystem_change_gen :: H.Gen FileSystemChange
    filesystem_change_gen = do
      random_abs_path <- either (error . show) id . Path.parseAbsFile . ('/' :) <$> Gen.list (Range.constant 4 10) Gen.alphaNum
      Gen.element [IsNew random_abs_path, NeedFreshBackup random_abs_path, IsRemoved]
