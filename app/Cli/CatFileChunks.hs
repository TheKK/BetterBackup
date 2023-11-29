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

import Monad (run_readonly_repo_t_from_cwd)
import Util.Options (digestRead)

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
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of file"
              ]
          )

    go sha = run_readonly_repo_t_from_cwd $ do
      Repo.catFile sha
        & fmap (show . Repo.chunk_name)
        & S.fold (F.drainMapM $ liftIO . putStrLn)
