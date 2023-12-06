{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeOperators #-}

module Cli.FamiliarListShareFolders (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Control.Monad.IO.Class (MonadIO (liftIO))

import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.Text.IO qualified as T

import Katip qualified as Log

import Better.Hash (TreeDigest)
import Better.Logging.Effect (logging)
import Better.Repository qualified as Repo

import Monad (runReadonlyRepositoryFromCwd)
import Repository.Find (searchDirInTree)
import Util.Options (treeDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "List backed up share folders in a familiar tree format"
        ]

    parser =
      go
        <$> argument
          treeDigestRead
          ( fold
              [ metavar "DIGEST"
              , help "digest of tree which you'd like to inspect"
              ]
          )

    go :: TreeDigest -> IO ()
    go tree_digest = runReadonlyRepositoryFromCwd $ do
      shares_folder_tree_digest <- fmap (either error id) $ searchDirInTree tree_digest $ Path.Rel [Path.reldir|@SHARES|]

      Repo.catTree shares_folder_tree_digest
        & S.mapM
          ( \case
              Left (Repo.Tree name _digest) -> liftIO $ T.putStrLn name
              Right (Repo.FFile name _digest) -> logging Log.WarningS $ "got file under folder of backuped shares: " <> Log.ls (show name)
          )
        & S.fold F.drain
