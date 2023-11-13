{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Cli.RestoreTree (
  parser_info,
) where

import Control.Parallel (par)

import Options.Applicative (
  ParserInfo,
  ReadM,
  argument,
  eitherReader,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Control.Exception (Exception (displayException), throwIO)
import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Bifunctor (Bifunctor (first))
import Data.Foldable (Foldable (fold))
import Data.Function ((&))
import Data.String (fromString)

import qualified Data.Text as T

import qualified Data.ByteString.Base16 as BSBase16

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.FileSystem.Handle as Handle

import qualified Effectful as E
import qualified Effectful.Dispatch.Static.Unsafe as E

import qualified System.Directory as D
import System.IO (IOMode (WriteMode), withBinaryFile)
import qualified System.Posix as P

import qualified Path

import Monad (run_readonly_repo_t_from_cwd)

import Better.Hash (Digest, digestFromByteString)
import qualified Better.Repository as Repo
import Better.Repository.Class (Repository)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Restore tree to selected empty directory"
        ]

    parser =
      go
        <$> argument
          digest_read
          ( fold
              [ metavar "SHA"
              , help "SHA of tree"
              ]
          )
        <*> argument
          some_base_dir_read
          ( fold
              [ metavar "PATH"
              , help "restore destination, must exist and be empty"
              ]
          )

    {-# NOINLINE go #-}
    go :: Digest -> Path.SomeBase Path.Dir -> IO ()
    go sha some_dir = do
      abs_out_path <- case some_dir of
        Path.Abs abs_dir -> pure abs_dir
        Path.Rel rel_dir -> do
          abs_cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
          pure $ abs_cwd Path.</> rel_dir

      out_path_exists <- D.doesDirectoryExist (Path.fromAbsDir abs_out_path)
      unless out_path_exists $ throwIO $ userError $ "given path is not a directory: " <> Path.fromAbsDir abs_out_path

      out_path_is_empty <- null <$> D.listDirectory (Path.fromAbsDir abs_out_path)
      unless out_path_is_empty $ throwIO $ userError $ "given directory is not empty: " <> Path.fromAbsDir abs_out_path

      -- XXX Well, using name of Repo.Tree here is not very ideal since it breaks the meaning of name.
      run_readonly_repo_t_from_cwd $ restore_tree $! Repo.Tree (T.pack $ Path.fromAbsDir abs_out_path) sha

    -- TODO Copy from cat-file, should have only one copy.
    {-# NOINLINE restore_file #-}
    restore_file :: (Repository E.:> es, E.IOE E.:> es) => Repo.FFile -> E.Eff es ()
    restore_file f = E.reallyUnsafeUnliftIO $ \un -> withBinaryFile (T.unpack $ Repo.file_name f) WriteMode $ \h -> liftIO $ do
      Repo.catFile (Repo.file_sha f)
        & S.morphInner un
        -- Use parConcatMap to open multiple chunk files concurrently.
        -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
        & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
        -- Use parEval to read from chunks concurrently.
        -- Since read is often faster than write, using parEval with buffer should reduce running time.
        & S.parEval (S.maxBuffer 30)
        & S.fold (Handle.writeChunks h)

    {-# NOINLINE restore_tree #-}
    restore_tree :: (Repository E.:> es, E.IOE E.:> es) => Repo.Tree -> E.Eff es ()
    restore_tree t = do
      let tree_path = T.unpack $ Repo.tree_name t
      -- TODO Permission should be backed up as well.
      liftIO $ D.createDirectoryIfMissing False tree_path
      E.reallyUnsafeUnliftIO $ \un ->
        D.withCurrentDirectory tree_path $
          un $
            Repo.catTree (Repo.tree_sha t)
              & S.mapM (either restore_tree restore_file)
              & S.fold F.drain

digest_read :: ReadM Digest
digest_read = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest

some_base_dir_read :: ReadM (Path.SomeBase Path.Dir)
some_base_dir_read = eitherReader $ first displayException . Path.parseSomeDir
