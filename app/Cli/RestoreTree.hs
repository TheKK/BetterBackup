{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}

module Cli.RestoreTree (
  parser_info,
) where

import Control.Parallel (par)

import Options.Applicative (
  Alternative ((<|>)),
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import qualified Ki

import Control.Concurrent.STM (STM, TBQueue, atomically, newTBQueueIO, readTBQueue, writeTBQueue)
import Control.Exception (throwIO)
import Control.Monad (unless, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Numeric.Natural (Natural)

import Data.Foldable (Foldable (fold))
import Data.Function ((&))

import qualified Data.Text as T

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.FileSystem.Handle as Handle

import qualified Effectful as E
import qualified Effectful.Dispatch.Static.Unsafe as E

import qualified System.Directory as D
import System.IO (IOMode (WriteMode), withBinaryFile)

import Path (Path)
import qualified Path

import Monad (run_readonly_repo_t_from_cwd)
import Util.Options (digestRead, someBaseDirRead)

import Better.Hash (Digest)
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
          digestRead
          ( fold
              [ metavar "SHA"
              , help "SHA of tree"
              ]
          )
        <*> argument
          someBaseDirRead
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

      run_readonly_repo_t_from_cwd $ do
        mk_producer_and_workers
          50
          (S.maxBuffer 50 . S.eager True)
          (\tbq -> traverse_tree_and_produce_jobs tbq abs_out_path sha)
          ( \case
              RestoreTree abs_dir_path digest -> restore_dir abs_dir_path digest
              RestoreFile abs_file_path digest -> restore_file abs_file_path digest
          )

    {-# NOINLINE traverse_tree_and_produce_jobs #-}
    traverse_tree_and_produce_jobs :: (Repository E.:> es, E.IOE E.:> es) => TBQueue RestoreJob -> Path Path.Abs Path.Dir -> Digest -> E.Eff es ()
    traverse_tree_and_produce_jobs tbq abs_out_path tree_digest = do
      liftIO $ do
        D.createDirectoryIfMissing False $ Path.fromAbsDir abs_out_path
        atomically $ writeTBQueue tbq $ RestoreTree abs_out_path tree_digest

      E.reallyUnsafeUnliftIO $ \un ->
        Repo.catTree tree_digest
          & S.morphInner un
          & S.mapM -- TODO parMapM like backup
            ( either
                ( \(Repo.Tree name digest) -> do
                    rel_subtree_path <- Path.parseRelDir $ T.unpack name
                    un $ traverse_tree_and_produce_jobs tbq (abs_out_path Path.</> rel_subtree_path) digest
                )
                ( \(Repo.FFile name digest) -> do
                    rel_file_path <- Path.parseRelFile $ T.unpack name
                    atomically $ writeTBQueue tbq $ RestoreFile (abs_out_path Path.</> rel_file_path) digest
                )
            )
          & S.fold F.drain

    {-# NOINLINE restore_file #-}
    restore_file :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.File -> Digest -> E.Eff es ()
    restore_file abs_out_path digest = E.reallyUnsafeUnliftIO $ \un -> withBinaryFile (Path.fromAbsFile abs_out_path) WriteMode $ \h -> do
      Repo.catFile digest
        & S.morphInner un
        -- Use parConcatMap to open multiple chunk files concurrently.
        -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
        & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
        -- Use parEval to read from chunks concurrently.
        -- Since read is often faster than write, using parEval with buffer should reduce running time.
        & S.parEval (S.maxBuffer 30)
        & S.fold (Handle.writeChunks h)

    {-# NOINLINE restore_dir #-}
    restore_dir :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.Dir -> Digest -> E.Eff es ()
    restore_dir _ _ = do
      -- TODO Permission should be backed up as well.
      pure ()

data RestoreJob = RestoreTree (Path Path.Abs Path.Dir) Digest | RestoreFile (Path Path.Abs Path.File) Digest
  deriving (Show)

mk_producer_and_workers :: (E.IOE E.:> es) => Natural -> (S.Config -> S.Config) -> (TBQueue e -> E.Eff es a) -> (e -> E.Eff es ()) -> E.Eff es a
mk_producer_and_workers n par_map_config emitter worker = E.reallyUnsafeUnliftIO $ \un -> do
  tbq <- newTBQueueIO n

  Ki.scoped $ \scope -> do
    emitter_thread <- Ki.fork scope $ un $ emitter tbq
    reciver_thread <- Ki.fork scope $ un $ reciver tbq worker (void $ Ki.await emitter_thread)
    emitter_ret <- atomically $ Ki.await emitter_thread
    () <- atomically $ Ki.await reciver_thread
    pure emitter_ret
  where
    reciver :: (E.IOE E.:> es) => TBQueue e -> (e -> E.Eff es ()) -> STM () -> E.Eff es ()
    reciver tbq' worker' emitter_stopped = E.reallyUnsafeUnliftIO $ \un -> do
      S.unfoldrM
        (\() -> atomically $ (Just . (,()) <$> readTBQueue tbq') <|> (Nothing <$ emitter_stopped))
        ()
        & S.parMapM par_map_config (un . worker')
        & S.fold F.drain
