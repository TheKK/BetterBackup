module Cli.InitLocal (
  parser_info,
) where

import Options.Applicative (
  Parser,
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Data.Foldable (Foldable (fold))

import Config (Config (Config))
import Config qualified
import LocalCache qualified
import Util.Options (absDirRead, someBaseDirRead)

parser_info :: Options.Applicative.ParserInfo (IO ())
parser_info = Options.Applicative.info (Options.Applicative.helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ Options.Applicative.progDesc "Initialize with local repository"
        ]

    parser =
      LocalCache.initialize
        <$> Options.Applicative.argument
          someBaseDirRead
          ( fold
              [ Options.Applicative.metavar "CACHE_PATH"
              , Options.Applicative.help "path to store your local cache"
              ]
          )
        <*> (Config <$> p_local_repo_config)

p_local_repo_config :: Parser Config.RepoType
p_local_repo_config =
  Config.Local . Config.LocalRepoConfig
    <$> argument absDirRead (metavar "REPO_PATH" <> help "path to store your backup")
