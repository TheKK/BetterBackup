{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Cli.TreeList (
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
  progDesc,
  short,
  showDefault,
  switch,
  value,
 )

import qualified Path

import Data.Foldable (Foldable (fold))
import Data.Word (Word64)

import Better.Hash (Digest)

import Monad (run_readonly_repo_t_from_cwd)
import Repository.Find (findTree)
import Util.Options (digestRead, someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "list content of tree"
        ]

    parser =
      go
        <$> switch
          ( fold
              [ help "display digest"
              , long "digest"
              ]
          )
        <*> option
          auto
          ( fold
              [ help "depth to traverse"
              , metavar "NUM"
              , long "depth"
              , short 'd'
              , value 1
              , showDefault
              ]
          )
        <*> argument
          digestRead
          ( fold
              [ help "SHA of tree"
              , metavar "SHA"
              ]
          )
        <*> argument
          someBaseDirRead
          ( fold
              [ help "starting path to traverse"
              , metavar "ROOT"
              , value (Path.Rel [Path.reldir|.|])
              , showDefault
              ]
          )

    go :: Bool -> Word64 -> Digest -> Path.SomeBase Path.Dir -> IO ()
    go !show_digest !depth !tree_root !opt_somebase_dir = run_readonly_repo_t_from_cwd $ do
      findTree show_digest (Just depth) tree_root (Just opt_somebase_dir)
