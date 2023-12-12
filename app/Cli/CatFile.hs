module Cli.CatFile (
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

import Control.Monad.IO.Class (liftIO)
import Control.Parallel (par)

import Data.Foldable (Foldable (fold))
import Data.Function ((&))

import Streamly.Console.Stdio qualified as Stdio
import Streamly.Data.Stream.Prelude qualified as S

import Better.Hash (FileDigest)
import Better.Repository qualified as Repo

import Effectful.Dispatch.Static.Unsafe qualified as E

import Monad (runReadonlyRepositoryFromCwd)
import Util.Options (fileDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of single file"
        ]

    parser =
      go
        <$> argument
          fileDigestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of file"
              ]
          )

    {-# NOINLINE go #-}
    go :: FileDigest -> IO ()
    go sha =
      runReadonlyRepositoryFromCwd $ E.reallyUnsafeUnliftIO $ \un -> liftIO $ do
        Repo.catFile sha
          & S.morphInner un
          -- Use parConcatMap to open multiple chunk files concurrently.
          -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
          & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
          -- Use parEval to read from chunks concurrently.
          -- Since read is often faster than write, using parEval with buffer should reduce running time.
          & S.parEval (S.maxBuffer 30)
          & S.fold Stdio.writeChunks
