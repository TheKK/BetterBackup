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

import Better.Hash (ChunkDigest)
import Better.Repository qualified as Repo

import Monad (runReadonlyRepositoryFromCwd)
import Util.Options (chunkDigestRead)

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
          chunkDigestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of chunk"
              ]
          )

    {-# NOINLINE go #-}
    go :: ChunkDigest -> IO ()
    go sha =
      runReadonlyRepositoryFromCwd $
        Repo.catChunk sha
          & S.fold Stdio.writeChunks
