{-# LANGUAGE Strict #-}

module Main
  ( main
  ) where

import qualified Options.Applicative as O
import Control.Monad (join)
import qualified Cli

main :: IO ()
main = join $ O.customExecParser (O.prefs $ O.showHelpOnError <> O.showHelpOnEmpty) Cli.cmds
