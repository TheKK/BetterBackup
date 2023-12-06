module Cli.CatFileChunks (
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

import Data.Foldable (Foldable (fold))
import Data.Function ((&))

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Better.Repository qualified as Repo

import Monad (runReadonlyRepositoryFromCwd)
import Util.Options (fileDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display chunks a single file references to"
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

    go sha = runReadonlyRepositoryFromCwd $ do
      Repo.catFile sha
        & fmap (show . Repo.chunk_name)
        & S.fold (F.drainMapM $ liftIO . putStrLn)
