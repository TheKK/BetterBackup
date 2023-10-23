{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Cli.Ref.Cat
  ( parserInfo
  )
where

import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Options.Applicative (ParserInfo, argument, helper, info, progDesc, str)
import qualified Streamly.Console.Stdio as Stdio
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.FileSystem.File as File

parserInfo :: ParserInfo (IO ())
parserInfo = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "cat command to test maximum speed of Haskell code"
        ]

    parser = go <$> argument str (fold [])

    {-# NOINLINE go #-}
    go path = File.readChunks path & S.fold Stdio.writeChunks
