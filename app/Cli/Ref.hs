{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Cli.Ref
  ( cmds,
  )
where

import qualified Cli.Ref.Cat as Cat
import Data.Foldable (Foldable (fold))
import Options.Applicative (ParserInfo, command, helper, info, progDesc, subparser)

cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Reference of operations"
        ]

    parser =
      subparser $
        fold
          [ command "cat" Cat.parserInfo
          ]
