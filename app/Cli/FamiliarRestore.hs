{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeOperators #-}

module Cli.FamiliarRestore (
  parser_info,
) where

import Options.Applicative (
  Alternative (some),
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
  str,
 )

import Control.Monad (unless)
import Control.Monad.Catch (throwM)
import Control.Monad.IO.Class (MonadIO (liftIO))

import System.Posix qualified as P

import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S
import System.Directory qualified as D

import Data.Bifunctor (second)
import Data.Foldable (Foldable (fold), for_)
import Data.Function ((&))
import Data.Map.Strict qualified as Map
import Data.Text qualified as T

import Katip (katipAddNamespace)
import Katip qualified as Log

import Better.Hash (TreeDigest)
import Better.Logging.Effect (logging, loggingOnSyncException)
import Better.Repository qualified as Repo

import Monad (run_readonly_repo_t_from_cwd)
import Repository.Find (searchDirInTree)
import Repository.Restore (restoreTreeInBFS, runParallelRestore)
import Util.Options (someBaseDirRead, treeDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Restore backup data in a familiar way"
        ]

    parser =
      go
        <$> argument
          treeDigestRead
          ( fold
              [ metavar "DIGEST"
              , help "digest of tree which you'd like to restore from"
              ]
          )
        <*> argument
          someBaseDirRead
          ( fold
              [ metavar "OS_CONFIG_PATH"
              , help "path for restoring OS config (for demonstration purpose)"
              ]
          )
        <*> some
          ( (,)
              <$> argument
                str
                ( fold
                    [ metavar "SHARE_NAME"
                    , help "name of share to be restored"
                    ]
                )
              <*> argument
                someBaseDirRead
                ( fold
                    [ metavar "DEST"
                    , help "path to local file system for restoring share"
                    ]
                )
          )

    go :: TreeDigest -> Path.SomeBase Path.Dir -> [(String, Path.SomeBase Path.Dir)] -> IO ()
    go tree_digest os_config_some_path list_of_share_and_filesystem_some_path = run_readonly_repo_t_from_cwd $ katipAddNamespace "familiar_restore" $ do
      abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory

      let
        to_abs p = case p of
          Path.Abs dir -> dir
          Path.Rel dir -> abs_pwd Path.</> dir

        share_name_to_restore_path_map = Map.fromList $ fmap (second to_abs) list_of_share_and_filesystem_some_path

      logging Log.InfoS "checking permission of pathes"
      loggingOnSyncException Log.ErrorS "checking permission of pathes failed" $
        for_ (os_config_some_path : map snd list_of_share_and_filesystem_some_path) $ \some_path -> do
          let abs_path = Path.toFilePath $ to_abs some_path

          exists <- liftIO $ D.doesDirectoryExist abs_path
          unless exists $ do
            throwM $ userError $ abs_path <> ": path does not exist"

          p <- liftIO $ D.getPermissions abs_path
          let required_permissions = [("writable" :: String, D.writable p), ("searchable", D.searchable p)]
          unless (all snd required_permissions) $ do
            throwM $ userError $ abs_path <> ": permissions are not enough: " <> show required_permissions
      logging Log.InfoS "checking permission of pathes [done]"

      -- We don't have read OS in fact, just leave it here for tree validation.
      _os_config_tree_digest <- fmap (either error id) $ searchDirInTree tree_digest $ Path.Rel [Path.reldir|@OS_CONFIG|]
      shares_folder_tree_digest <- fmap (either error id) $ searchDirInTree tree_digest $ Path.Rel [Path.reldir|@SHARES|]

      required_share_name_and_tree_digest_and_local_path_set <-
        Repo.catTree shares_folder_tree_digest
          & S.mapMaybeM
            ( \case
                Left (Repo.Tree name digest) ->
                  case Map.lookup (T.unpack name) share_name_to_restore_path_map of
                    Just local_fs_path -> pure $ Just (T.unpack name, digest, local_fs_path)
                    Nothing -> pure Nothing
                Right (Repo.FFile name _digest) -> do
                  logging Log.WarningS $ "got file under folder of backuped shares: " <> Log.ls (show name)
                  pure Nothing
            )
          & S.fold F.toSet

      runParallelRestore (S.maxBuffer 20) $ do
        for_ required_share_name_and_tree_digest_and_local_path_set $ \(share_name, digest, local_fs_path) -> do
          logging Log.InfoS $ "restore " <> Log.ls share_name
          restoreTreeInBFS local_fs_path digest
          logging Log.InfoS $ "restore " <> Log.ls share_name <> " [done]"
