{-# LANGUAGE LambdaCase #-}

module Cli.RestoreTree (
  parser_info,
) where

import Options.Applicative (
  Alternative ((<|>)),
  ParserInfo,
  argument,
  flag,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Ki qualified

import Control.Concurrent.STM (STM, TBQueue, atomically, check, modifyTVar', newTBQueueIO, newTQueueIO, newTVarIO, readTBQueue, readTQueue, readTVar, writeTBQueue, writeTQueue)
import Control.Exception (bracket_, throwIO)
import Control.Monad (replicateM_, unless, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Numeric.Natural (Natural)

import Data.Foldable (Foldable (fold))
import Data.Function (fix, (&))
import Data.Word (Word32)

import Data.Text qualified as T

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Effectful qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

import System.Directory qualified as D

import Path (Path)
import Path qualified

import Monad (run_readonly_repo_t_from_cwd)
import Repository.Restore (restoreDirMeta, restoreFile)
import Util.Options (digestRead, someBaseDirRead)

import Better.Hash (Digest)
import Better.Repository qualified as Repo
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
        <$> flag
          BFS
          DFS
          ( fold
              [ help "Use DFS instead of BFS to traverse backup tree while restoring."
              ]
          )
        <*> argument
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
    go :: TraverseMethod -> Digest -> Path.SomeBase Path.Dir -> IO ()
    go traverse_method sha some_dir = do
      abs_out_path <- case some_dir of
        Path.Abs abs_dir -> pure abs_dir
        Path.Rel rel_dir -> do
          abs_cwd <- Path.parseAbsDir =<< D.getCurrentDirectory
          pure $ abs_cwd Path.</> rel_dir

      out_path_exists <- D.doesDirectoryExist (Path.fromAbsDir abs_out_path)
      unless out_path_exists $ throwIO $ userError $ "given path is not a directory: " <> Path.fromAbsDir abs_out_path

      out_path_is_empty <- null <$> D.listDirectory (Path.fromAbsDir abs_out_path)
      unless out_path_is_empty $ throwIO $ userError $ "given directory is not empty: " <> Path.fromAbsDir abs_out_path

      let
        traverse_tree_and_produce_jobs = case traverse_method of
          BFS -> traverse_tree_and_produce_jobs_bfs
          DFS -> traverse_tree_and_produce_jobs_dfs

      run_readonly_repo_t_from_cwd $ do
        mk_producer_and_workers
          50
          (S.maxBuffer 50 . S.eager True)
          (\tbq -> traverse_tree_and_produce_jobs tbq abs_out_path sha)
          ( \case
              RestoreTree abs_dir_path digest -> restoreDirMeta abs_dir_path digest
              RestoreFile abs_file_path digest -> restoreFile abs_file_path digest
          )

data TraverseMethod = DFS | BFS

{-# NOINLINE traverse_tree_and_produce_jobs_dfs #-}
traverse_tree_and_produce_jobs_dfs :: (Repository E.:> es, E.IOE E.:> es) => TBQueue RestoreJob -> Path Path.Abs Path.Dir -> Digest -> E.Eff es ()
traverse_tree_and_produce_jobs_dfs tbq abs_out_path tree_digest = do
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
                un $ traverse_tree_and_produce_jobs_dfs tbq (abs_out_path Path.</> rel_subtree_path) digest
            )
            ( \(Repo.FFile name digest) -> do
                rel_file_path <- Path.parseRelFile $ T.unpack name
                atomically $ writeTBQueue tbq $ RestoreFile (abs_out_path Path.</> rel_file_path) digest
            )
        )
      & S.fold F.drain

{-# NOINLINE traverse_tree_and_produce_jobs_bfs #-}
traverse_tree_and_produce_jobs_bfs :: (Repository E.:> es, E.IOE E.:> es) => TBQueue RestoreJob -> Path Path.Abs Path.Dir -> Digest -> E.Eff es ()
traverse_tree_and_produce_jobs_bfs restore_job_tbq abs_root_path root_digest = do
  E.reallyUnsafeUnliftIO $ \un -> Ki.scoped $ \scope -> do
    tree_digest_tq <- newTQueueIO
    atomically $ writeTQueue tree_digest_tq (abs_root_path, root_digest)

    num_of_working_thread_tvar <- newTVarIO (0 :: Word32)
    let
      take_job = modifyTVar' num_of_working_thread_tvar succ
      done_job = modifyTVar' num_of_working_thread_tvar pred
      running_bracket = bracket_ (pure ()) (atomically done_job)
      no_one_is_running = check . (0 ==) =<< readTVar num_of_working_thread_tvar

    replicateM_ 4 {- CORE/2 -} $ Ki.fork scope $ fix $ \rec -> do
      opt_digest <- atomically $ do
        (Just <$> (take_job >> readTQueue tree_digest_tq)) <|> (Nothing <$ no_one_is_running)

      case opt_digest of
        Just (parent_abs_path, parent_digest) -> do
          running_bracket $ do
            D.createDirectoryIfMissing False $ Path.fromAbsDir parent_abs_path
            atomically $ writeTBQueue restore_job_tbq $ RestoreTree parent_abs_path parent_digest

            un $
              Repo.catTree parent_digest
                & S.mapM
                  ( either
                      ( \(Repo.Tree name child_digest) -> do
                          rel_subtree_path <- Path.parseRelDir $ T.unpack name
                          liftIO $ atomically $ writeTQueue tree_digest_tq (parent_abs_path Path.</> rel_subtree_path, child_digest)
                      )
                      ( \(Repo.FFile name digest) -> do
                          rel_file_path <- Path.parseRelFile $ T.unpack name
                          liftIO $ atomically $ writeTBQueue restore_job_tbq $ RestoreFile (parent_abs_path Path.</> rel_file_path) digest
                      )
                  )
                & S.fold F.drain
          rec
        Nothing -> pure ()
    atomically $ Ki.awaitAll scope

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
