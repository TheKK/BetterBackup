{-# LANGUAGE OverloadedStrings #-}

module Cli.GarbageCollection (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  helper,
  info,
  progDesc,
 )

import Data.Foldable (Foldable (fold))

import qualified Better.Repository as Repo

import Monad (runReadonlyRepositoryFromCwd)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Running garbage collection to release unused data"
        ]

    parser = pure go

    {-# NOINLINE go #-}
    go = runReadonlyRepositoryFromCwd Repo.garbageCollection
