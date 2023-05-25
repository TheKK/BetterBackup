{-# LANGUAGE TypeApplications #-}

module Cli
  (
  ) where

import Options.Applicative

import Data.Foldable

g :: IO Int
g = execParser $ info (helper <*> cmds) mempty

cmds
  =   (subparser $ command "init" parser_info_init)

parser_info_init = info (helper <*> parser) infoMod
  where
    infoMod = fold
      [ progDesc "Initialize repository"
      ]

    parser
      =  flag () () (short 'f' <> help "oh f")
      *> argument (str @String) (metavar "string" <> help "yes")
      *> argument auto (metavar "int" <> help "no")
