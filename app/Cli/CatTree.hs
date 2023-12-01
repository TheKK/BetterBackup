module Cli.CatTree (
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

import Data.Text qualified as T
import Data.Text.IO qualified as T

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Better.Repository qualified as Repo

import Monad (run_readonly_repo_t_from_cwd)
import Util.Options (treeDigestRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Display content of tree"
        ]

    parser =
      go
        <$> argument
          treeDigestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of tree"
              ]
          )

    go sha = run_readonly_repo_t_from_cwd $ do
      Repo.catTree sha
        & S.fold (F.drainMapM $ liftIO . T.putStrLn . T.pack . show)
