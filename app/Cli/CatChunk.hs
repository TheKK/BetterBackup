module Cli.CatChunk (
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

import Data.Foldable (Foldable (fold))
import Data.Function ((&))

import Streamly.Data.Stream.Prelude qualified as S

import Streamly.Console.Stdio qualified as Stdio

import Better.Hash (Digest)
import Better.Repository qualified as Repo

import Monad (run_readonly_repo_t_from_cwd)
import Util.Options (digestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of single chunk"
        ]

    parser =
      go
        <$> argument
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of chunk"
              ]
          )

    {-# NOINLINE go #-}
    go :: Digest -> IO ()
    go sha =
      run_readonly_repo_t_from_cwd $
        Repo.catChunk sha
          & S.fold Stdio.writeChunks
