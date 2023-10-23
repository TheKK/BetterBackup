{-# LANGUAGE OverloadedStrings #-}

module Cli.IntegrityCheck (
  parser_info,
) where

import Options.Applicative (ParserInfo, helper, info, progDesc)

import Data.Foldable (Foldable (fold))

import qualified Better.Repository as Repo

import Monad (run_readonly_repo_t_from_cwd)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Verify and handle corrupted data"
        ]

    parser = pure go

    {-# NOINLINE go #-}
    go = run_readonly_repo_t_from_cwd $ Repo.checksum 10

