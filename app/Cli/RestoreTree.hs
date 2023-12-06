module Cli.RestoreTree (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  flag,
  help,
  helper,
  info,
  long,
  metavar,
  progDesc,
 )

import Control.Exception (throwIO)
import Control.Monad (unless)

import Data.Foldable (Foldable (fold))

import Streamly.Data.Stream.Prelude qualified as S

import System.Directory qualified as D

import Path qualified

import Better.Hash (TreeDigest)

import Monad (runReadonlyRepositoryFromCwd)
import Repository.Restore (restoreTreeInBFS, restoreTreeInDFS, runParallelRestore)
import Util.Options (treeDigestRead, someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Restore tree to selected empty directory"
        ]

    parser =
      go
        <$> flag
          BFS
          DFS
          ( fold
              [ help "Use DFS instead of BFS to traverse backup tree while restoring."
              , long "dfs"
              ]
          )
        <*> argument
          treeDigestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of tree"
              ]
          )
        <*> argument
          someBaseDirRead
          ( fold
              [ metavar "PATH"
              , help "restore destination, must exist and be empty"
              ]
          )

    {-# NOINLINE go #-}
    go :: TraverseMethod -> TreeDigest -> Path.SomeBase Path.Dir -> IO ()
    go traverse_method sha some_dir = do
      abs_out_path <- case some_dir of
        Path.Abs abs_dir -> pure abs_dir
        Path.Rel rel_dir -> do
          abs_cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
          pure $ abs_cwd Path.</> rel_dir

      out_path_exists <- D.doesDirectoryExist (Path.fromAbsDir abs_out_path)
      unless out_path_exists $ throwIO $ userError $ "given path is not a directory: " <> Path.fromAbsDir abs_out_path

      out_path_is_empty <- null <$> D.listDirectory (Path.fromAbsDir abs_out_path)
      unless out_path_is_empty $ throwIO $ userError $ "given directory is not empty: " <> Path.fromAbsDir abs_out_path

      runReadonlyRepositoryFromCwd $ do
        runParallelRestore (S.maxBuffer 20 . S.eager True) $ do
          case traverse_method of
            BFS -> restoreTreeInBFS abs_out_path sha
            DFS -> restoreTreeInDFS abs_out_path sha

data TraverseMethod = DFS | BFS
