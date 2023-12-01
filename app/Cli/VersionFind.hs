{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli.VersionFind (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  auto,
  help,
  helper,
  info,
  long,
  metavar,
  option,
  optional,
  progDesc,
  short,
  switch,
 )

import qualified Path

import Data.Foldable
import Data.Word (Word64)

import Better.Hash (VersionDigest)
import qualified Better.Repository as Repo

import Monad (run_readonly_repo_t_from_cwd)
import Repository.Find (findTree)
import Util.Options (versionDigestRead, someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "find tree"
        ]

    parser =
      go
        <$> switch
          ( fold
              [ help "display digest"
              , long "digest"
              ]
          )
        <*> optional
          ( option
              auto
              ( fold
                  [ help "depth to traverse (default: unlimit)"
                  , long "depth"
                  , short 'd'
                  ]
              )
          )
        <*> argument
          versionDigestRead
          ( fold
              [ help "SHA of tree"
              , metavar "SHA"
              ]
          )
        <*> optional
          ( argument
              someBaseDirRead
              ( fold
                  [ help "starting path to traversing, default to root"
                  , metavar "ROOT"
                  ]
              )
          )

    go :: Bool -> Maybe Word64 -> VersionDigest -> Maybe (Path.SomeBase Path.Dir) -> IO ()
    go show_digest opt_depth version_digest opt_somebase_dir = run_readonly_repo_t_from_cwd $ do
      tree_root <- Repo.ver_root <$> Repo.catVersion version_digest
      findTree show_digest opt_depth tree_root opt_somebase_dir
