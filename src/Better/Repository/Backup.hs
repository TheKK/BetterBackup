{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}

module Better.Repository.Backup (
  -- * Effects
  RepositoryBackup,
  BackupWorkers,

  -- * Effect handlers
  runRepositoryBackup,
  runBackupWorkersWithTBQueue,

  -- * Backup functions
  backup_dir,
  backupDirWithoutCollectingDirAndFileStatistics,
  DirEntry (..),
  backupDirFromList,
  backupFileFromBuilder,

  -- * Backup from existed tree
  backupDirFromExistedTree,

  -- * Tests
  props_what_to_do_with_file_and_dir,
) where

import Prelude hiding (read)

import Numeric.Natural (Natural)

import Data.Coerce (coerce)
import Data.Either (fromRight)
import Data.Function (fix, (&))
import Data.List (inits, intercalate)
import Data.Maybe (fromMaybe)
import Data.Time (getCurrentTime)
import Data.Word (Word8)

import StmContainers.Set qualified as STMSet

import Data.Text qualified as T

import Data.ByteString qualified as BS
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Builder.Extra qualified as BB
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL

import Data.Foldable (for_)

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad (guard, replicateM, unless, when)
import Control.Monad.Catch (finally, mask, onException, throwM)
import Control.Monad.IO.Class (liftIO)

import Control.Concurrent.STM (TBQueue, TVar, atomically, modifyTVar', newTBQueueIO, newTVarIO, readTBQueue, readTVar, readTVarIO, writeTBQueue)

import Streamly.Data.Array qualified as Array
import Streamly.External.ByteString (toArray)
import Streamly.Internal.Data.Array.Type qualified as Array (byteLength)

import Streamly.Internal.System.IO (defaultChunkSize)

import System.Directory qualified as D
import System.Environment (lookupEnv)
import System.FilePath qualified as FP
import System.IO (IOMode (ReadMode, WriteMode), withBinaryFile)
import System.Posix qualified as P

import Path (Path, (</>))
import Path qualified

import Crypto.Cipher.AES (AES128)
import Crypto.Cipher.AES qualified as Cipher
import Crypto.Cipher.Types (ivAdd)
import Crypto.Cipher.Types qualified as Cipher
import Crypto.Random.Entropy qualified as CRE

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S

import Control.Concurrent.QSem (QSem, newQSem, signalQSem, waitQSem)
import Control.Concurrent.STM.TSem (TSem, newTSem, signalTSem, waitTSem)
import Control.Monad.ST.Strict (stToIO)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))

import Effectful qualified as E
import Effectful qualified as EU
import Effectful.Dispatch.Static qualified as E
import Effectful.Ki (StructuredConcurrency)
import Effectful.Ki qualified as EKi

import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range

import Better.Data.FileSystemChanges qualified as FSC
import Better.Hash (
  Digest,
  FileDigest (UnsafeMkFileDigest),
  TreeDigest (UnsafeMkTreeDigest),
  digestToByteString,
  finalize,
  hashArrayFoldIO,
  hashByteArrayAccess',
  hashByteStringFoldIO,
 )
import Better.Hash qualified as Hash
import Better.Internal.Repository.LowLevel (addChunk', addDir', addFile', d2b, doesTreeExists)
import Better.Internal.Repository.LowLevel qualified as Repo
import Better.Internal.Streamly.Array (chunkReaderFromToWith)
import Better.Logging.Effect qualified as E
import Better.Repository.BackupCache.Class (BackupCache)
import Better.Repository.BackupCache.LevelDB qualified as BackupCacheLevelDB
import Better.Repository.Class qualified as E
import Better.Statistics.Backup qualified as BackupSt
import Better.Statistics.Backup.Class (BackupStatistics)
import Better.Statistics.Backup.Class qualified as BackupSt
import Better.Streamly.FileSystem.Chunker qualified as Chunker
import Better.Streamly.FileSystem.Dir qualified as Dir
import Better.TempDir qualified as Tmp
import Better.TempDir.Class (Tmp)

-- | Effect for generating a new version of backup.
data RepositoryBackup :: E.Effect

type instance E.DispatchOf RepositoryBackup = 'E.Static 'E.NoSideEffects
data instance E.StaticRep RepositoryBackup = RepositoryBackupRep
  { repobackuprep_unique_tree_gate :: forall es. Digest -> E.Eff es () -> E.Eff es ()
  , repobackuprep_unique_file_gate :: forall es. Digest -> E.Eff es () -> E.Eff es ()
  , repobackuprep_unique_chunk_gate :: forall es. Digest -> E.Eff es () -> E.Eff es ()
  , repobackuprep_tvar_iv :: TVar (Cipher.IV AES128)
  , repobackuprep_scope :: !EKi.Scope
  , repobackuprep_forkfns :: !ForkFns
  }

newtype ForAllGate = ForAllGate (forall es2. Digest -> EU.Eff es2 () -> EU.Eff es2 ())

-- | Provide env to run functions which needs capability, RepositoryWrite.
runRepositoryBackup
  :: (E.Repository E.:> es, BackupStatistics E.:> es, StructuredConcurrency E.:> es, E.IOE E.:> es)
  => E.Eff (RepositoryBackup : es) TreeDigest
  -> E.Eff es (Hash.VersionDigest, Repo.Version)
runRepositoryBackup m = do
  ForAllGate unique_chunk_gate <- mk_uniq_gate
  ForAllGate unique_tree_gate <- mk_uniq_gate
  ForAllGate unique_file_gate <- mk_uniq_gate
  tvar_iv <- E.unsafeEff_ $ newTVarIO =<< init_iv

  !root_digest <- EKi.scoped $ \scope -> do
    fork_fns <- do
      sem_for_dir <- E.unsafeEff_ mk_sem_for_dir
      sem_for_file <- E.unsafeEff_ mk_sem_for_file
      pure $ ForkFns (fork_or_wait sem_for_file scope) (fork_or_run_directly sem_for_dir scope)

    E.evalStaticRep (RepositoryBackupRep unique_tree_gate unique_file_gate unique_chunk_gate tvar_iv scope fork_fns) m

  -- We don't increase iv since it's the end of ctr here.
  iv <- E.unsafeEff_ $ readTVarIO tvar_iv
  now <- E.unsafeEff_ getCurrentTime

  let !v = Repo.Version now root_digest

  (written_bytes, v_digest) <- Repo.addVersion iv v
  BackupSt.modifyStatistic' BackupSt.uploadedBytes (+ written_bytes)

  pure (v_digest, v)
  where
    mk_uniq_gate :: EU.Eff es1 ForAllGate
    mk_uniq_gate = do
      running_set <- E.unsafeEff_ $ STMSet.newIO @Digest
      pure $ ForAllGate $ \hash' m' -> do
        !other_is_running <- E.unsafeEff_ $ atomically $ do
          !exist <- STMSet.lookup hash' running_set
          unless exist $ STMSet.insert hash' running_set
          pure exist
        unless other_is_running $
          m' `finally` E.unsafeEff_ (atomically $ STMSet.delete hash' running_set)

-- | Effect for worker pool which is used for backup.
--
-- Although BackupWorkers always comes with RepositoryBackup, make them separated give us more opportunity
-- to create different implementation.
data BackupWorkers :: E.Effect

type instance E.DispatchOf BackupWorkers = 'E.Static 'E.NoSideEffects
newtype instance E.StaticRep BackupWorkers = BackupWorkersRep (UploadTask -> IO ())

runBackupWorkersWithTBQueue
  :: ( RepositoryBackup E.:> es
     , BackupStatistics E.:> es
     , BackupCache E.:> es
     , StructuredConcurrency E.:> es
     , E.IOE E.:> es
     , E.Repository E.:> es
     )
  => Natural
  -> Int
  -> E.Eff (BackupWorkers : es) a
  -> E.Eff es a
runBackupWorkersWithTBQueue q_buffer worker_num m = do
  RepositoryBackupRep unique_tree_gate unique_file_gate unique_chunk_gate tvar_iv _ _ <- E.getStaticRep

  connectProducerAndComsumers
    q_buffer
    worker_num
    (\tbq -> E.evalStaticRep (BackupWorkersRep (atomically . writeTBQueue tbq)) m)
    ( \case
        UploadTree dir_hash file_name' file_size -> (`finally` E.unsafeEff_ (D.removeFile $ Path.fromAbsFile file_name')) $ do
          unique_tree_gate dir_hash $ do
            added <- do
              iv <- E.unsafeEff_ $ retrive_iv_for_bytes file_size tvar_iv
              addDir' dir_hash file_name' iv
            when added $ do
              BackupSt.modifyStatistic' BackupSt.newDirCount (+ 1)

          BackupSt.modifyStatistic' BackupSt.processedDirCount (+ 1)
        UploadFile file_hash file_name' opt_st file_size -> (`finally` E.unsafeEff_ (D.removeFile $ Path.fromAbsFile file_name')) $ do
          unique_file_gate file_hash $ do
            added <- do
              iv <- E.unsafeEff_ $ retrive_iv_for_bytes file_size tvar_iv
              addFile' file_hash file_name' iv
            when added $ do
              BackupSt.modifyStatistic' BackupSt.newFileCount (+ 1)

          BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
          for_ opt_st $ \st -> BackupCacheLevelDB.saveCurrentFileHash st file_hash
        FindNoChangeFile file_hash st -> do
          BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
          BackupCacheLevelDB.saveCurrentFileHash st file_hash
        UploadChunk chunk_hash chunks chunk_size -> do
          unique_chunk_gate chunk_hash $ do
            iv <- E.unsafeEff_ $ retrive_iv_for_bytes chunk_size tvar_iv
            !added_bytes <- addChunk' chunk_hash chunks iv
            when (added_bytes > 0) $ do
              BackupSt.modifyStatistic' BackupSt.uploadedBytes (+ added_bytes)
              BackupSt.modifyStatistic' BackupSt.newChunkCount (+ 1)

          BackupSt.modifyStatistic' BackupSt.processedChunkCount (+ 1)
    )

init_iv :: IO (Cipher.IV Cipher.AES128)
init_iv = do
  ent <- CRE.getEntropy @BS.ByteString (Cipher.blockSize (undefined :: Cipher.AES128))
  case Cipher.makeIV ent of
    Just iv -> pure $! iv
    Nothing -> error "failed to generate iv"

-- | Backup specified directory.
--
-- This function would collect statistics of directories and files concurrently, which is what you want most of the time.
-- if you need more control, consider using 'backupDirWithoutCollectingDirAndFileStatistics'.
backup_dir
  :: ( E.Logging E.:> es
     , E.Repository E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , BackupCache E.:> es
     , Tmp E.:> es
     , BackupStatistics E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => Path Path.Abs Path.Dir
  -> E.Eff es TreeDigest
backup_dir abs_dir = EKi.scoped $ \scope -> do
  stat_async <- EKi.fork scope $ collect_dir_and_file_statistics abs_dir
  !root_digest <- backupDirWithoutCollectingDirAndFileStatistics abs_dir
  E.unsafeEff_ $ atomically $ EKi.await stat_async
  pure root_digest

data UploadTask
  = -- | User MUST ensure that all content matches to same tree for correct result.
    UploadTree {-# UNPACK #-} !Digest !(Path Path.Abs Path.File) !P.FileOffset
  | -- | User MUST ensure that all content matches to same file for correct result.
    UploadFile {-# UNPACK #-} !Digest !(Path Path.Abs Path.File) !(Maybe P.FileStatus) !P.FileOffset
  | UploadChunk {-# UNPACK #-} !Digest ![Array.Array Word8] !P.FileOffset
  | FindNoChangeFile {-# UNPACK #-} !Digest !P.FileStatus

tree_content_from_dir_entry :: DirEntry -> BS.ByteString
tree_content_from_dir_entry file_or_dir =
  let
    !name = case file_or_dir of
      DirEntryFile _ n -> n
      DirEntryDir _ n -> n
    !t = case file_or_dir of
      DirEntryFile _ _ -> "file"
      DirEntryDir _ _ -> "dir"
    !byteshash = d2b $ case file_or_dir of
      DirEntryFile digest _ -> coerce digest
      DirEntryDir digest _ -> coerce digest
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, byteshash, BS.singleton 0x0a]

tree_content_of_tree :: Path Path.Rel Path.Dir -> TreeDigest -> BS.ByteString
tree_content_of_tree dir_path hash' =
  let
    !name = dir_name' dir_path
    !t = "dir"
    !byteshash = d2b $ coerce hash'
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, byteshash, BS.singleton 0x0a]
  where
    dir_name' :: Path r Path.Dir -> BS.ByteString
    dir_name' = BC.pack . FP.dropTrailingPathSeparator . Path.toFilePath . Path.dirname

tree_content_of_file :: Path Path.Rel Path.File -> FileDigest -> BS.ByteString
tree_content_of_file file_path hash' =
  let
    !name = file_name' file_path
    !t = "file"
    !byteshash = d2b $ coerce hash'
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, byteshash, BS.singleton 0x0a]
  where
    file_name' :: Path r Path.File -> BS.ByteString
    file_name' = BC.pack . Path.toFilePath . Path.filename

collect_dir_and_file_statistics
  :: (E.Repository E.:> es, BackupStatistics E.:> es, E.IOE E.:> es)
  => Path Path.Abs Path.Dir
  -> E.Eff es ()
collect_dir_and_file_statistics rel_tree_name = do
  Dir.readEither rel_tree_name
    & S.morphInner liftIO
    & S.mapM
      ( either
          (const $ BackupSt.modifyStatistic' BackupSt.totalFileCount (+ 1))
          (collect_dir_and_file_statistics . (rel_tree_name </>))
      )
    & S.fold F.drain
  BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

-- TODO Should use our MonadReader instance to store this kind of information.
{-# NOINLINE mk_sem_for_dir #-}
mk_sem_for_dir :: IO TSem
mk_sem_for_dir = atomically . newTSem . fromMaybe 4 . (readMaybe =<<) =<< lookupEnv "CORE"

{-# NOINLINE mk_sem_for_file #-}
mk_sem_for_file :: IO QSem
mk_sem_for_file = newQSem . fromMaybe 4 . (readMaybe =<<) =<< lookupEnv "CORE2"

fork_or_wait :: (StructuredConcurrency E.:> es) => QSem -> EKi.Scope -> E.Eff es a -> E.Eff es (E.Eff es a)
fork_or_wait s scope io = do
  a <- mask $ \restore -> do
    E.unsafeEff_ $ waitQSem s
    EKi.fork scope (restore io `finally` E.unsafeEff_ (signalQSem s)) `onException` E.unsafeEff_ (signalQSem s)
  pure $! E.unsafeEff_ $ atomically $ EKi.await a

fork_or_run_directly :: (StructuredConcurrency E.:> es) => TSem -> EKi.Scope -> E.Eff es a -> E.Eff es (E.Eff es a)
fork_or_run_directly s = \scope io -> mask $ \restore -> do
  to_fork <- E.unsafeEff_ $ atomically $ (True <$ waitTSem s) <|> pure False
  if to_fork
    then do
      -- XXX We never know if we should call `signalTSem` for Ki.fork since we don't know if `finally` on
      -- `io` would be run or not when exception (both sync and async) was thrown.
      a <-
        EKi.fork scope (restore io `finally` E.unsafeEff_ (atomically $ signalTSem s))
          `onException` E.unsafeEff_ (atomically $ signalTSem s)
      pure $! restore $ E.unsafeEff_ $ atomically $ EKi.await a
    else do
      r <- restore io
      pure $! pure r

data ForkFns
  = ForkFns
      (forall es a. StructuredConcurrency E.:> es => E.Eff es a -> E.Eff es (E.Eff es a))
      (forall es a. StructuredConcurrency E.:> es => E.Eff es a -> E.Eff es (E.Eff es a))

-- | Backup specified directory, and only do backup.
--
-- NOTE: Normally you'll only need 'backup_dir' unless you have special needs.
--
-- This function WON'T collect statistics of directories and files concurrently, which gives you more control.
backupDirWithoutCollectingDirAndFileStatistics
  :: ( E.Repository E.:> es
     , BackupCache E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , Tmp E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => Path Path.Abs Path.Dir
  -> E.Eff es TreeDigest
backupDirWithoutCollectingDirAndFileStatistics rel_tree_name = do
  BackupWorkersRep push_job <- E.getStaticRep
  backupEnv <- E.getStaticRep @RepositoryBackup
  let
    (ForkFns file_fork_or_not dir_fork_or_not) = repobackuprep_forkfns backupEnv

  Tmp.withEmptyTmpFile $ \file_name' -> EU.withSeqEffToIO $ \seq_un -> do
    (!dir_hash, !file_size) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
      Dir.readEither rel_tree_name
        & S.morphInner E.unsafeEff_
        & S.mapM
          ( \case
              Left f -> file_fork_or_not $ fmap (tree_content_of_file f) $ backup_file $ rel_tree_name </> f
              Right d -> dir_fork_or_not $ fmap (tree_content_of_tree d) $ backupDirWithoutCollectingDirAndFileStatistics $ rel_tree_name </> d
          )
        & S.parSequence (S.ordered True)
        & S.trace (E.unsafeEff_ . BC.hPut fd)
        & S.fold
          ( F.tee
              (F.morphInner E.unsafeEff_ hashByteStringFoldIO)
              $ F.lmap (fromIntegral . BS.length) F.sum
          )
        & E.withUnliftStrategy (E.ConcUnlift E.Ephemeral E.Unlimited)
        & seq_un

    liftIO $ push_job $ UploadTree dir_hash file_name' file_size

    pure $ UnsafeMkTreeDigest dir_hash

data DirEntry
  = DirEntryFile {-# UNPACK #-} !FileDigest {-# UNPACK #-} !BS.ByteString
  | DirEntryDir {-# UNPACK #-} !TreeDigest {-# UNPACK #-} !BS.ByteString

backupDirFromList :: (BackupWorkers E.:> es, BackupStatistics E.:> es, Tmp E.:> es, E.IOE E.:> es) => [DirEntry] -> E.Eff es TreeDigest
backupDirFromList inputs = Tmp.withEmptyTmpFile $ \file_name' -> do
  BackupWorkersRep push_job <- E.getStaticRep

  BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

  (!dir_hash, !file_size) <- liftIO $ withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    S.fromList inputs
      & fmap tree_content_from_dir_entry
      & S.trace (BC.hPut fd)
      & S.fold (F.tee hashByteStringFoldIO $ F.lmap (fromIntegral . BS.length) F.sum)

  liftIO $ push_job $ UploadTree dir_hash file_name' file_size

  pure $ UnsafeMkTreeDigest dir_hash

backup_file
  :: (E.Repository E.:> es, BackupWorkers E.:> es, BackupCache E.:> es, Tmp E.:> es, E.IOE E.:> es)
  => Path Path.Abs Path.File
  -> E.Eff es FileDigest
backup_file rel_file_name = do
  BackupWorkersRep push_job <- E.getStaticRep

  st <- liftIO $ P.getFileStatus $ Path.fromAbsFile rel_file_name
  to_scan <- BackupCacheLevelDB.tryReadingCacheHash st
  case to_scan of
    Just cached_digest -> do
      liftIO $ push_job $ FindNoChangeFile cached_digest st
      pure $! UnsafeMkFileDigest cached_digest
    Nothing -> Tmp.withEmptyTmpFile $ \file_name' -> E.withEffToIO (E.ConcUnlift E.Ephemeral E.Unlimited) $ \conc_un -> do
      (!file_hash, !file_size) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd ->
        withBinaryFile (Path.fromAbsFile rel_file_name) ReadMode $ \fdd ->
          Chunker.gearHash Chunker.defaultGearHashConfig (Path.fromAbsFile rel_file_name)
            & S.mapM
              ( \(Chunker.Chunk b e) -> do
                  S.unfold chunkReaderFromToWith (b, e - 1, defaultChunkSize, fdd)
                    & S.fold F.toList
              )
            & S.parMapM (S.ordered True . S.maxBuffer 8) (conc_un . backup_chunk)
            & S.trace (BS.hPut fd)
            & S.fold (F.tee hashByteStringFoldIO $ F.lmap (fromIntegral . BS.length) F.sum)

      push_job $ UploadFile file_hash file_name' (Just st) file_size
      pure $ UnsafeMkFileDigest file_hash

backupFileFromBuilder
  :: (E.Repository E.:> es, BackupWorkers E.:> es, BackupStatistics E.:> es, Tmp E.:> es, E.IOE E.:> es)
  => BB.Builder
  -> E.Eff es Digest
backupFileFromBuilder builder = do
  BackupWorkersRep push_job <- E.getStaticRep

  BackupSt.modifyStatistic' BackupSt.totalFileCount (+ 1)

  let lbs = BB.toLazyByteStringWith (BB.untrimmedStrategy BB.defaultChunkSize BB.defaultChunkSize) BL.empty builder
  lbs_tvar <- E.unsafeEff_ $ newTVarIO lbs

  Tmp.withEmptyTmpFile $ \file_name' -> E.withEffToIO (E.ConcUnlift E.Ephemeral E.Unlimited) $ \conc_un -> do
    (!file_hash, !file_size) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd ->
      S.fromList (BL.toChunks lbs)
        & Chunker.gearHashPure Chunker.defaultGearHashConfig
        & S.mapM
          ( \(Chunker.Chunk b e) -> atomically $ do
              chunk <- fmap toArray . BL.toChunks . BL.take (fromIntegral $ e - b) <$> readTVar lbs_tvar
              modifyTVar' lbs_tvar (BL.drop (fromIntegral $ e - b))
              -- Keep the thunk (chunk) and let parMapM evalutes them parallelly.
              pure chunk
          )
        & S.parMapM (S.ordered True . S.maxBuffer 16) (conc_un . backup_chunk)
        & S.trace (BS.hPut fd)
        & S.fold (F.tee hashByteStringFoldIO $ F.lmap (fromIntegral . BS.length) F.sum)

    push_job $ UploadFile file_hash file_name' Nothing file_size
    pure file_hash

backup_chunk :: (E.Repository E.:> es, BackupWorkers E.:> es) => [Array.Array Word8] -> E.Eff es BS.ByteString
backup_chunk chunks = do
  BackupWorkersRep push_job <- E.getStaticRep

  E.unsafeEff_ $ do
    (!chunk_hash, !chunk_length) <-
      S.fromList chunks
        & S.fold (F.tee hashArrayFoldIO (fromIntegral <$> F.lmap Array.byteLength F.sum))

    push_job $ UploadChunk chunk_hash chunks chunk_length

    pure $! digestToByteString chunk_hash

connectProducerAndComsumers
  :: (StructuredConcurrency E.:> es)
  => Natural
  -- ^ buffer size of bounded queue
  -> Int
  -- ^ number of workers
  -> (TBQueue e -> E.Eff es a)
  -> (e -> E.Eff es ())
  -> E.Eff es a
connectProducerAndComsumers q_size worker_num putter go = do
  tbq <- E.unsafeEff_ $ newTBQueueIO q_size

  EKi.scoped $ \scope -> do
    thread_putter <- EKi.fork scope $ putter tbq
    thread_solvers <- replicateM worker_num $ EKi.fork scope $ fix $ \rec -> do
      e <- E.unsafeEff_ $ atomically $ (Just <$> readTBQueue tbq) <|> (Nothing <$ EKi.await thread_putter)
      case e of
        Just v -> go v >> rec
        Nothing -> pure ()

    ret_putter <- E.unsafeEff_ $ atomically $ EKi.await thread_putter
    E.unsafeEff_ $ mapM_ (atomically . EKi.await) thread_solvers

    pure ret_putter

data TreeWhatNow
  = TreeIsIntact
  | TreeIsRemoved
  | -- | This tree is lost and we have no idea how to patch it, so just backup it again.
    -- Note that it might not exist nor be a directory now.
    TreeShouldBeBackedUpAgain (Path Path.Abs Path.File)
  | -- | There're changes in this tree so keep traversing and you'll find out.
    -- Also there might be new element in this tree which are stored in the list.
    TreeShouldKeepTraversing [(Path Path.Rel Path.File, Path Path.Abs Path.File)] -- TODO Use Path Abs File
  deriving (Eq, Show)

data FileWhatNow
  = FileIsIntact
  | FileIsRemoved
  | -- | This file is lost and we have no idea how to patch it, so just backup it again.
    -- Note that it might not exist nor be a file now.
    FileShouldBeBackedUpAgain (Path Path.Abs Path.File)
  deriving (Eq, Show)

what_to_do_with_this_tree :: FSC.FileSystemChanges -> Path Path.Rel Path.Dir -> E.Eff es TreeWhatNow
what_to_do_with_this_tree fsc p = do
  case FSC.lookupDir p fsc of
    Just FSC.IsRemoved -> pure TreeIsRemoved
    Just (FSC.IsNew abs_path) -> pure $ TreeShouldBeBackedUpAgain abs_path -- Not possible case actually
    Just (FSC.NeedFreshBackup abs_path) -> pure $ TreeShouldBeBackedUpAgain abs_path
    Nothing ->
      if FSC.hasDescendants p fsc
        then pure $ TreeShouldKeepTraversing new_entries
        else pure TreeIsIntact
  where
    new_entries = do
      (raw_rel_path, FSC.IsNew local_abs_path) <- FSC.toList $ FSC.submap p fsc
      rel_file <- Path.parseRelFile $ BC.unpack raw_rel_path
      guard $ Path.parent rel_file == p
      stripped_rel_file_path <- Path.stripProperPrefix p rel_file
      pure (stripped_rel_file_path, local_abs_path)

-- Keep it returning Eff in case that we use IO operation to implement it in the future.
what_to_do_with_this_file :: FSC.FileSystemChanges -> Path Path.Rel Path.File -> E.Eff es FileWhatNow
what_to_do_with_this_file fsc p = do
  case FSC.lookupFile p fsc of
    Just FSC.IsRemoved -> pure FileIsRemoved
    Just (FSC.IsNew abs_path) -> pure $! FileShouldBeBackedUpAgain abs_path
    Just (FSC.NeedFreshBackup abs_path) -> pure $! FileShouldBeBackedUpAgain abs_path
    Nothing -> pure FileIsIntact

-- TODO Currently we doesn't update total file/dir count.
backupDirFromExistedTree
  :: ( E.Logging E.:> es
     , E.Repository E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , BackupCache E.:> es
     , Tmp E.:> es
     , BackupStatistics E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => FSC.FileSystemChanges
  -> TreeDigest
  -> Path Path.Rel Path.Dir
  -> E.Eff es (Maybe DirEntry)
backupDirFromExistedTree fsc digest rel_dir_path_in_tree = do
  tree_exists <- doesTreeExists digest
  unless tree_exists $ do
    throwM $ userError $ show digest <> ": tree does not exist in backup"

  what_now <- what_to_do_with_this_tree fsc rel_dir_path_in_tree
  case what_now of
    TreeIsIntact -> pure $! Just $! DirEntryDir digest direntry_name
    TreeIsRemoved -> pure Nothing
    TreeShouldBeBackedUpAgain abs_fs_path -> try_backing_up_file_or_dir rel_dir_path_in_tree abs_fs_path
    TreeShouldKeepTraversing possible_new_entries ->
      fmap (`DirEntryDir` direntry_name) <$> keep_traversing_existed_tree fsc digest rel_dir_path_in_tree possible_new_entries
  where
    direntry_name = rel_path_to_direntry_name rel_dir_path_in_tree

backup_file_from_existed_tree
  :: ( E.Logging E.:> es
     , E.Repository E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , BackupCache E.:> es
     , Tmp E.:> es
     , BackupStatistics E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => FSC.FileSystemChanges
  -> FileDigest
  -> Path Path.Rel Path.File
  -> E.Eff es (Maybe DirEntry)
backup_file_from_existed_tree fsc digest rel_file_path_in_tree = do
  what_now <- what_to_do_with_this_file fsc rel_file_path_in_tree
  case what_now of
    FileIsIntact -> pure $! Just $! DirEntryFile digest direntry_name
    FileIsRemoved -> pure Nothing
    FileShouldBeBackedUpAgain abs_fs_path -> try_backing_up_file_or_dir rel_file_path_in_tree abs_fs_path
  where
    direntry_name = rel_path_to_direntry_name rel_file_path_in_tree

keep_traversing_existed_tree
  :: ( E.Logging E.:> es
     , E.Repository E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , BackupCache E.:> es
     , Tmp E.:> es
     , BackupStatistics E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => FSC.FileSystemChanges
  -> TreeDigest
  -> Path Path.Rel Path.Dir
  -> [(Path Path.Rel Path.File, Path Path.Abs Path.File)]
  -> E.Eff es (Maybe TreeDigest)
keep_traversing_existed_tree fsc digest_of_existed_tree rel_dir_path_in_tree possible_new_entries = do
  BackupWorkersRep push_job <- E.getStaticRep

  let
    -- It's fine to use origin fsc, but using submap should makes lookup faster to traverse down.
    -- The assumptions are:
    --   - time to run submap is identical to single lookup
    --   - submap use very few memory
    sub_fsc = FSC.submap rel_dir_path_in_tree fsc

  liftIO $ print (rel_dir_path_in_tree, sub_fsc)

  -- Pair with UploadTree.
  BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

  fmap Just $ Tmp.withEmptyTmpFile $ \file_name' -> EU.withSeqEffToIO $ \seq_un -> do
    (!dir_hash, !file_size) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> seq_un $ do
      (!mid_hasher, !mid_file_size) <-
        -- Traverse existed part.
        Repo.catTree digest_of_existed_tree
          & S.mapM
            ( \case
                Left d -> do
                  rel_dir <- Path.parseRelDir $ T.unpack $ Repo.tree_name d
                  backupDirFromExistedTree sub_fsc (Repo.tree_sha d) (rel_dir_path_in_tree </> rel_dir)
                Right f -> do
                  rel_file <- Path.parseRelFile $ T.unpack $ Repo.file_name f
                  backup_file_from_existed_tree sub_fsc (Repo.file_sha f) (rel_dir_path_in_tree </> rel_file)
            )
          & S.catMaybes
          & fmap tree_content_from_dir_entry
          & S.trace (liftIO . BC.hPut fd)
          & S.fold
            ( F.tee
                (F.morphInner (liftIO . stToIO) $ hashByteArrayAccess' $ Hash.init Nothing)
                (F.lmap (fromIntegral . BS.length) F.sum)
            )

      -- Traverse added part.
      (!digest, !rest_file_size) <-
        S.fromList possible_new_entries
          & S.mapM
            ( \(entry_rel_path_in_tree, entry_abs_path_on_filesystem) -> do
                try_backing_up_file_or_dir entry_rel_path_in_tree entry_abs_path_on_filesystem
            )
          & S.catMaybes
          & fmap tree_content_from_dir_entry
          & S.trace (liftIO . BC.hPut fd)
          & S.fold
            ( F.tee
                (F.morphInner (liftIO . stToIO) $ F.rmapM finalize $ hashByteArrayAccess' mid_hasher)
                (F.lmap (fromIntegral . BS.length) F.sum)
            )

      pure (digest, mid_file_size + rest_file_size)

    liftIO $ push_job $ UploadTree dir_hash file_name' file_size

    pure $ UnsafeMkTreeDigest dir_hash

try_backing_up_file_or_dir
  :: ( E.Logging E.:> es
     , E.Repository E.:> es
     , RepositoryBackup E.:> es
     , BackupWorkers E.:> es
     , BackupCache E.:> es
     , Tmp E.:> es
     , BackupStatistics E.:> es
     , E.IOE E.:> es
     , StructuredConcurrency E.:> es
     )
  => Path Path.Rel t
  -> Path Path.Abs Path.File
  -> E.Eff es (Maybe DirEntry)
try_backing_up_file_or_dir rel_tree_path abs_filesystem_path = runMaybeT $ do
  path_exists <- liftIO $ D.doesPathExist raw_abs_filesystem_path
  guard path_exists

  is_dir <- liftIO $ P.isDirectory <$> P.getFileStatus raw_abs_filesystem_path
  MaybeT $!
    if is_dir
      then do
        abs_dir <- Path.parseAbsDir raw_abs_filesystem_path
        -- We don't need modifyStatistic' for dir since backup_dir do that for us.
        Just . (`DirEntryDir` direntry_name) <$> backup_dir abs_dir
      else do
        BackupSt.modifyStatistic' BackupSt.totalFileCount (+ 1)
        -- TODO rename backup_file to backup_file_without_collecting_statistic or something.
        Just . (`DirEntryFile` direntry_name) <$> backup_file abs_filesystem_path
  where
    direntry_name = rel_path_to_direntry_name rel_tree_path
    raw_abs_filesystem_path = Path.fromAbsFile abs_filesystem_path

-- | Returns and increases @IV@ in a thread safe manger.
--
-- The goal is to not use same @IV@ for ALL of the encrypted message during single backup session.
retrive_iv_for_bytes :: P.FileOffset -> TVar (Cipher.IV AES128) -> IO (Cipher.IV Cipher.AES128)
retrive_iv_for_bytes chunk_length tvar_iv = do
  atomically $ do
    !iv_to_use <- readTVar tvar_iv
    modifyTVar' tvar_iv (`ivAdd` iv_offset)
    -- TODO workaround to seq iv currently. Should be fixed by package owner in next version.
    pure $! seq (iv_to_use == iv_to_use) iv_to_use
  where
    iv_offset = ceiling @Double $ fromIntegral chunk_length / fromIntegral iv_length
    iv_length = Cipher.blockSize (undefined :: AES128)

-- | Run dirname or file name.
--
-- examples:
--   extract_dirname_or_filename [reldir|a/b/c|] == "c/"
--   extract_dirname_or_filename [reldir|a/b|] == "b/"
--   extract_dirname_or_filename [reldir|a|] == "a/"
--   extract_dirname_or_filename [reldir|.|] == "./"

--   extract_dirname_or_filename [relfile|a/b/c|] == "c"
--   extract_dirname_or_filename [relfile|a/b|] == "b"
--   extract_dirname_or_filename [relfile|a|] == "a"
dirname_or_filename :: Path Path.Rel t -> Path Path.Rel t
dirname_or_filename p = fromRight p $ Path.stripProperPrefix (Path.parent p) p

rel_path_to_direntry_name :: Path Path.Rel t -> BS.ByteString
rel_path_to_direntry_name = BC.pack . FP.dropTrailingPathSeparator . Path.toFilePath . dirname_or_filename

props_what_to_do_with_file_and_dir :: H.Group
props_what_to_do_with_file_and_dir =
  H.Group
    "FileSystemChanges"
    [ ("prop_semantics_between_filesystem_change_and_what_to_do_with_file", prop_def_what_to_do_with_this_file)
    , ("prop_semantics_between_filesystem_change_and_what_to_do_with_dir", prop_def_what_to_do_with_this_tree)
    , ("prop_insersion_should_be_observable", prop_insersion_should_be_observable)
    , ("prop_keep_traversing", prop_keep_traversing_from_root_to_leaf)
    ]
  where
    prop_insersion_should_be_observable :: H.Property
    prop_insersion_should_be_observable = H.property $ do
      pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10

      rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" pathes
      rel_file <- H.evalIO $ Path.parseRelFile $ intercalate "/" pathes
      H.annotateShow (rel_dir, rel_file)

      change <- H.forAll filesystem_change_gen

      let fsc = FSC.empty & FSC.insert' rel_dir change
      H.annotateShow fsc

      let what_now = E.runPureEff $ what_to_do_with_this_tree fsc rel_dir
      what_now H.=== case change of
        FSC.IsNew abs_file_path -> TreeShouldBeBackedUpAgain abs_file_path
        FSC.IsRemoved -> TreeIsRemoved
        FSC.NeedFreshBackup abs_file_path -> TreeShouldBeBackedUpAgain abs_file_path

      let parent_what_now = E.runPureEff $ what_to_do_with_this_tree fsc $ Path.parent rel_dir
      parent_what_now H.=== case change of
        FSC.IsNew abs_file_path ->
          let stripped_rel_file = either (error . show) id $ Path.stripProperPrefix (Path.parent rel_dir) rel_file
          in  TreeShouldKeepTraversing [(stripped_rel_file, abs_file_path)]
        FSC.IsRemoved -> TreeShouldKeepTraversing []
        FSC.NeedFreshBackup _ -> TreeShouldKeepTraversing []

    prop_def_what_to_do_with_this_file :: H.Property
    prop_def_what_to_do_with_this_file = H.property $ do
      pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10

      rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" pathes
      rel_file <- H.evalIO $ Path.parseRelFile $ intercalate "/" pathes
      H.annotateShow (rel_dir, rel_file)

      change <- H.forAll filesystem_change_gen

      let fsc = FSC.empty & FSC.insert' rel_dir change
      H.annotateShow fsc

      H.annotate "when filesystem change is empty, everything should be intact"
      let empty_what_now = E.runPureEff (what_to_do_with_this_file FSC.empty rel_file)
      empty_what_now H.=== FileIsIntact

      H.annotate "when filesystem has change, it should report expected answer"
      let what_now = E.runPureEff (what_to_do_with_this_file fsc rel_file)
      what_now H.=== case change of
        FSC.IsNew abs_fs_path -> FileShouldBeBackedUpAgain abs_fs_path
        FSC.NeedFreshBackup abs_fs_path -> FileShouldBeBackedUpAgain abs_fs_path
        FSC.IsRemoved -> FileIsRemoved

    prop_def_what_to_do_with_this_tree :: H.Property
    prop_def_what_to_do_with_this_tree = H.property $ do
      pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10

      rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" pathes
      H.annotateShow rel_dir

      change <- H.forAll filesystem_change_gen

      fsc <- H.eval $ FSC.empty & FSC.insert' rel_dir change
      H.annotateShow fsc

      H.annotate "when filesystem change is empty, everything should be intact"
      let empty_what_now = E.runPureEff (what_to_do_with_this_tree FSC.empty rel_dir)
      empty_what_now H.=== TreeIsIntact

      H.annotate "when filesystem has change, it should report correct answer"
      let what_now = E.runPureEff (what_to_do_with_this_tree fsc rel_dir)
      what_now H.=== case change of
        FSC.IsNew abs_fs_path -> TreeShouldBeBackedUpAgain abs_fs_path
        FSC.NeedFreshBackup abs_fs_path -> TreeShouldBeBackedUpAgain abs_fs_path
        FSC.IsRemoved -> TreeIsRemoved

    prop_keep_traversing_from_root_to_leaf :: H.Property
    prop_keep_traversing_from_root_to_leaf = H.property $ do
      pathes <- H.forAll $ path_segments_gen $ Range.linear 1 10

      rel_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" pathes
      rel_file <- H.evalIO $ Path.parseRelFile $ intercalate "/" pathes
      H.annotateShow (rel_dir, rel_file)

      change <- H.forAll filesystem_change_gen

      let fsc = FSC.empty & FSC.insert' rel_dir change
      H.annotateShow fsc

      H.annotate "All prefixes should tell you to keep traversing"
      for_ (["."] : init (tail $ inits pathes)) $ \prefix_segs -> do
        prefix_dir <- H.evalIO $ Path.parseRelDir $ intercalate "/" prefix_segs
        H.annotateShow prefix_dir

        let what_now = E.runPureEff $ what_to_do_with_this_tree fsc prefix_dir
        what_now H.=== case change of
          FSC.IsNew abs_fs_path
            | Path.parent rel_dir == prefix_dir ->
                let stripped_rel_file = either (error . show) id $ Path.stripProperPrefix (Path.parent rel_dir) rel_file
                in  TreeShouldKeepTraversing [(stripped_rel_file, abs_fs_path)]
          _ -> TreeShouldKeepTraversing []

    path_segments_gen :: H.Range Int -> H.Gen [String]
    path_segments_gen length_range = Gen.list length_range $ replicateM 2 Gen.alphaNum

    filesystem_change_gen :: H.Gen FSC.FileSystemChange
    filesystem_change_gen = Gen.element [FSC.IsNew random_abs_path, FSC.NeedFreshBackup random_abs_path, FSC.IsRemoved]
      where
        random_abs_path = [Path.absfile|/aabb|]
