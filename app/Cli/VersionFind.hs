{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli.VersionFind (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  ReadM,
  argument,
  eitherReader,
  help,
  helper,
  info,
  long,
  metavar,
  progDesc,
  short,
  switch,
 )

import Control.Monad.IO.Class (MonadIO (liftIO))

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream as S

import qualified Data.ByteString.Base16 as BSBase16

import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.String (fromString)

import qualified Data.Text as T
import qualified Data.Text.IO as T

import Better.Hash (Digest, digestFromByteString)
import qualified Better.Repository as Repo

import Monad (run_readonly_repo_t_from_cwd)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "find tree"
        ]

    parser =
      go
        <$> switch
          ( fold
              [ help "display digest"
              , long "show-digest"
              , short 'd'
              ]
          )
        <*> argument
          digest_read
          ( fold
              [ help "SHA of tree"
              , metavar "SHA"
              ]
          )

    go :: Bool -> Digest -> IO ()
    go show_digest version_digest = run_readonly_repo_t_from_cwd $ do
      root <- Repo.ver_root <$> Repo.catVersion version_digest
      echo_tree "/" root
      where
        {-# NOINLINE echo_tree #-}
        echo_tree parent digest = do
          let opt_tree_digest = if show_digest then T.pack ("[" <> show digest <> "][d] ") else ""
          liftIO $ T.putStrLn $ opt_tree_digest <> parent

          Repo.catTree digest
            & S.mapM
              ( \case
                  Left tree -> do
                    echo_tree (parent <> Repo.tree_name tree <> "/") (Repo.tree_sha tree)
                  Right file -> do
                    let opt_file_digest = if show_digest then T.pack ("[" <> show (Repo.file_sha file) <> "][f] ") else ""
                    liftIO $ T.putStrLn $ opt_file_digest <> parent <> Repo.file_name file
              )
            & S.fold F.drain

-- TODO This is a copy-paste snippt.
digest_read :: ReadM Digest
digest_read = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest
