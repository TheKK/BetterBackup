{-# LANGUAGE Strict #-}

module Main (
  main,
) where

import qualified Options.Applicative as O

import Control.Monad (join)

import Data.Foldable (fold)

import qualified System.Console.Terminal.Size as Term

import qualified Cli

main :: IO ()
main = do
  term_column_perf <- maybe mempty (O.columns . Term.width) <$> Term.size
  let
    parser_prefs =
      O.prefs $
        fold
          [ term_column_perf
          , O.showHelpOnError
          , O.showHelpOnEmpty
          , O.multiSuffix "..."
          , O.noBacktrack
          ]
  join $ O.customExecParser parser_prefs Cli.cmds
