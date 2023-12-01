{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module Repository.Restore (
  -- * Effect
  RepositoryRestore,

  -- * Effect evaluator
  runParallelRestore,

  -- * Restor operations
  restoreTreeInDFS,
  restoreTreeInBFS,
) where

import Ki qualified

import Control.Applicative ((<|>))
import Control.Concurrent.STM (
  STM,
  TBQueue,
  atomically,
  check,
  modifyTVar',
  newTBQueueIO,
  newTQueueIO,
  newTVarIO,
  readTBQueue,
  readTQueue,
  readTVar,
  writeTBQueue,
  writeTQueue,
 )
import Control.Exception (bracket_)
import Control.Monad (replicateM_, void)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Parallel (par)

import Data.Function (fix, (&))
import Data.Word (Word32)

import Data.Text qualified as T

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import System.Directory qualified as D
import System.IO (IOMode (WriteMode), withBinaryFile)

import Path (Path)
import Path qualified

import Streamly.FileSystem.Handle qualified as Handle

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

import Better.Hash (TreeDigest, FileDigest)
import Better.Repository qualified as Repo
import Better.Repository.Class (Repository)

data RepositoryRestore :: E.Effect

type instance E.DispatchOf RepositoryRestore = 'E.Static 'E.NoSideEffects

newtype instance E.StaticRep RepositoryRestore = RepositoryRestoreRep (TBQueue RestoreJob)

-- | Files and directories would be restore parallely.
runParallelRestore
  :: (E.IOE E.:> es, Repository E.:> es)
  => (S.Config -> S.Config)
  -- ^ Control how 'parMapM' behaves
  -> E.Eff (RepositoryRestore : es) a
  -> E.Eff es a
runParallelRestore par_map_config emitter = E.withEffToIO (E.ConcUnlift E.Ephemeral E.Unlimited) $ \un -> do
  -- TODO Can't tell how large this should be so just pick one randomly.
  tbq <- newTBQueueIO 20

  Ki.scoped $ \scope -> do
    emitter_thread <- Ki.fork scope $ un $ E.evalStaticRep (RepositoryRestoreRep tbq) emitter
    reciver_thread <- Ki.fork scope $ un $ reciver tbq (void $ Ki.await emitter_thread)
    emitter_ret <- atomically $ Ki.await emitter_thread
    () <- atomically $ Ki.await reciver_thread
    pure emitter_ret
  where
    reciver :: (E.IOE E.:> es, Repository E.:> es) => TBQueue RestoreJob -> STM () -> E.Eff es ()
    reciver tbq' emitter_stopped = E.reallyUnsafeUnliftIO $ \un -> do
      S.unfoldrM
        (\() -> atomically $ (Just . (,()) <$> readTBQueue tbq') <|> (Nothing <$ emitter_stopped))
        ()
        & S.parMapM par_map_config (un . worker)
        & S.fold F.drain

    worker :: (E.IOE E.:> es, Repository E.:> es) => RestoreJob -> E.Eff es ()
    worker = \case
      RestoreTree abs_dir_path digest -> restoreDirMeta abs_dir_path digest
      RestoreFile abs_file_path digest -> restoreFile abs_file_path digest

restoreTreeInDFS :: (Repository E.:> es, RepositoryRestore E.:> es, E.IOE E.:> es) => Path Path.Abs Path.Dir -> TreeDigest -> E.Eff es ()
restoreTreeInDFS abs_out_path tree_digest = do
  RepositoryRestoreRep tbq <- E.getStaticRep

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
                un $ restoreTreeInDFS (abs_out_path Path.</> rel_subtree_path) digest
            )
            ( \(Repo.FFile name digest) -> do
                rel_file_path <- Path.parseRelFile $ T.unpack name
                atomically $ writeTBQueue tbq $ RestoreFile (abs_out_path Path.</> rel_file_path) digest
            )
        )
      & S.fold F.drain

restoreTreeInBFS :: (Repository E.:> es, RepositoryRestore E.:> es, E.IOE E.:> es) => Path Path.Abs Path.Dir -> TreeDigest -> E.Eff es ()
restoreTreeInBFS abs_root_path root_digest = do
  RepositoryRestoreRep restore_job_tbq <- E.getStaticRep

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

data RestoreJob = RestoreTree (Path Path.Abs Path.Dir) TreeDigest | RestoreFile (Path Path.Abs Path.File) FileDigest
  deriving (Show)

-- | Restore meta of existed directory.
--
-- The parent path must exists before calling this function. And it's fine if the file has not existed yet.
-- If the file exists then its content and meta would be overwritten after this function.
{-# NOINLINE restoreFile #-}
restoreFile :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.File -> FileDigest -> E.Eff es ()
restoreFile abs_out_path digest = E.reallyUnsafeUnliftIO $ \un -> withBinaryFile (Path.fromAbsFile abs_out_path) WriteMode $ \h -> do
  Repo.catFile digest
    & S.morphInner un
    -- Use parConcatMap to open multiple chunk files concurrently.
    -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
    & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
    -- Use parEval to read from chunks concurrently.
    -- Since read is often faster than write, using parEval with buffer should reduce running time.
    & S.parEval (S.maxBuffer 30)
    & S.fold (Handle.writeChunks h)

-- | Restore meta of existed directory.
--
-- Might throws exception If directory does not exist.
restoreDirMeta :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.Dir -> TreeDigest -> E.Eff es ()
restoreDirMeta _ _ = do
  -- TODO Permission should be backed up as well.
  pure ()
