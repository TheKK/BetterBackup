{-# LANGUAGE Strict #-}

module Main (
  main,
) where

import qualified Options.Applicative as O
import qualified System.Console.Terminal.Size as Term

import qualified Cli
import Control.Monad (join)

main :: IO ()
main = do
  term_column_perf <- maybe mempty (O.columns . Term.width) <$> Term.size
  join $ O.customExecParser (O.prefs $ term_column_perf <> O.showHelpOnError <> O.showHelpOnEmpty) Cli.cmds
