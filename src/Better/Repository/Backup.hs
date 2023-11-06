{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Better.Repository.Backup (
  backup,

  -- * Backup related functions
  DirEntry (..),
  backup_dir_from_list,
) where

import Prelude hiding (read)

import Numeric.Natural (Natural)

import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Word (Word8)

import qualified StmContainers.Set as STMSet

import qualified Ki

import qualified Data.Text as T

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Short as BSS

import Data.Foldable (for_)

import Data.Time (getCurrentTime)

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad (unless, when)
import Control.Monad.IO.Class (liftIO)

import Control.Monad.IO.Unlift (MonadUnliftIO)
import qualified Control.Monad.IO.Unlift as Un

import Control.Concurrent.STM (TBQueue, TVar, atomically, modifyTVar', newTBQueueIO, newTVarIO, readTBQueue, readTVar, writeTBQueue)

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Type as Array (byteLength)

import Streamly.Internal.System.IO (defaultChunkSize)

import qualified Streamly.FileSystem.File as File

import qualified System.Directory as D
import qualified System.Posix.Files as P

import Path (Path, (</>))
import qualified Path

import qualified Crypto.Cipher.AES as Cipher
import Crypto.Cipher.Types (ivAdd)
import qualified Crypto.Cipher.Types as Cipher
import qualified Crypto.Random.Entropy as CRE

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Control.Concurrent.QSem (QSem, newQSem, signalQSem, waitQSem)
import Control.Concurrent.STM.TSem (TSem, newTSem, signalTSem, waitTSem)
import Control.Exception (finally, mask, onException)

import System.Environment (lookupEnv)
import System.IO (IOMode (ReadMode, WriteMode), withBinaryFile)

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as E
import qualified Effectful.Dispatch.Static.Unsafe as E
import qualified Effectful.Dispatch.Static.Unsafe as EU

import Better.Hash (Digest, digestToBase16ShortByteString, hashArrayFoldIO, hashByteStringFoldIO)
import Better.Internal.Repository.LowLevel (Version (Version), addBlob', addDir', addFile', addVersion, d2b)
import Better.Internal.Streamly.Array (ArrayBA (ArrayBA, un_array_ba), chunkReaderFromToWith)
import Better.Internal.Streamly.Crypto.AES (encryptCtr, that_aes)
import qualified Better.Logging.Effect as E
import Better.Repository.BackupCache.Class (BackupCache)
import qualified Better.Repository.BackupCache.LevelDB as BackupCacheLevelDB
import Better.Repository.Class (RepositoryWrite)
import qualified Better.Repository.Class as E
import qualified Better.Statistics.Backup as BackupSt
import Better.Statistics.Backup.Class (BackupStatistics)
import qualified Better.Statistics.Backup.Class as BackupSt
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import qualified Better.Streamly.FileSystem.Dir as Dir
import qualified Better.TempDir as Tmp
import Better.TempDir.Class (Tmp)

data Ctr = Ctr
  { _ctr_aes :: Cipher.AES128
  , _ctr_iv :: (TVar (Cipher.IV Cipher.AES128))
  }

init_iv :: IO (Cipher.IV Cipher.AES128)
init_iv = do
  ent <- CRE.getEntropy @BS.ByteString (Cipher.blockSize (undefined :: Cipher.AES128))
  case Cipher.makeIV ent of
    Just iv -> pure $! iv
    Nothing -> error "failed to generate iv"

init_ctr :: IO Ctr
init_ctr = do
  aes <- that_aes

  iv <- newTVarIO =<< init_iv
  pure $! Ctr aes iv

backup :: (E.Logging E.:> es, E.Repository E.:> es, BackupCache E.:> es, Tmp E.:> es, BackupStatistics E.:> es, E.IOE E.:> es) => T.Text -> E.Eff es Version
backup dir = do
  rel_dir <- liftIO $ Path.parseRelDir $ T.unpack dir

  root_digest <- run_backup $ do
    RepositoryWriteRep scope _ _ _ <- E.getStaticRep @RepositoryWrite

    stat_async <- E.reallyUnsafeUnliftIO $ \un -> Ki.fork scope (un $ collect_dir_and_file_statistics rel_dir)
    root_digest <- backup_dir rel_dir
    liftIO $ atomically $ Ki.await stat_async
    pure root_digest

  -- TODO getCurrentTime inside addVersion for better interface.
  now <- liftIO getCurrentTime
  let !v = Version now root_digest
  addVersion v
  return v

data instance E.StaticRep RepositoryWrite = RepositoryWriteRep {-# UNPACK #-} !Ki.Scope {-# UNPACK #-} !ForkFns {-# UNPACK #-} !(TBQueue UploadTask) {-# UNPACK #-} !Ctr

-- TODO Extract part of this function into a separated effect.
run_backup
  :: (E.Logging E.:> es, E.Repository E.:> es, BackupCache E.:> es, Tmp E.:> es, BackupStatistics E.:> es, E.IOE E.:> es)
  => E.Eff (RepositoryWrite : es) a
  -> E.Eff es a
run_backup m = EU.reallyUnsafeUnliftIO $ \un -> do
  let
    mk_uniq_gate = do
      running_set <- STMSet.newIO @Digest
      pure $ \ !hash' !m -> do
        !other_is_running <- atomically $ do
          !exist <- STMSet.lookup hash' running_set
          unless exist $ STMSet.insert hash' running_set
          pure exist
        unless other_is_running $
          m `finally` atomically (STMSet.delete hash' running_set)

  unique_chunk_gate <- mk_uniq_gate
  unique_tree_gate <- mk_uniq_gate
  unique_file_gate <- mk_uniq_gate

  ctr <- init_ctr

  Ki.scoped $ \scope -> do
    fork_fns <- do
      sem_for_dir <- mk_sem_for_dir
      sem_for_file <- mk_sem_for_file
      pure $ ForkFns (fork_or_wait sem_for_file scope) (fork_or_run_directly sem_for_dir scope)

    (ret, ()) <-
      withEmitUnfoldr
        20
        (\tbq -> un $ E.evalStaticRep (RepositoryWriteRep scope fork_fns tbq ctr) m)
        ( \s ->
            s
              & S.parMapM
                (S.maxBuffer 100 . S.eager True)
                ( \case
                    UploadTree dir_hash file_name' -> do
                      unique_tree_gate dir_hash $
                        do
                          added <- un (addDir' dir_hash (File.readChunks (Path.fromAbsFile file_name'))) `finally` D.removeFile (Path.fromAbsFile file_name')
                          when added $ do
                            un $ BackupSt.modifyStatistic' BackupSt.newDirCount (+ 1)
                      un $ BackupSt.modifyStatistic' BackupSt.processedDirCount (+ 1)
                    UploadFile file_hash file_name' opt_st -> do
                      unique_file_gate file_hash $
                        do
                          added <- un (addFile' file_hash (File.readChunks (Path.fromAbsFile file_name'))) `finally` D.removeFile (Path.fromAbsFile file_name')
                          when added $ do
                            un $ BackupSt.modifyStatistic' BackupSt.newFileCount (+ 1)
                      un $ do
                        BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                        for_ opt_st $ \st -> BackupCacheLevelDB.saveCurrentFileHash st file_hash
                    FindNoChangeFile file_hash st -> un $ do
                      BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                      BackupCacheLevelDB.saveCurrentFileHash st file_hash
                    UploadChunk chunk_hash chunk_stream -> do
                      unique_chunk_gate chunk_hash $ un $ do
                        !added_bytes <- fromIntegral <$> addBlob' chunk_hash (S.morphInner liftIO chunk_stream)
                        when (added_bytes > 0) $ do
                          BackupSt.modifyStatistic' BackupSt.uploadedBytes (+ added_bytes)
                          BackupSt.modifyStatistic' BackupSt.newChunkCount (+ 1)
                      un $ BackupSt.modifyStatistic' BackupSt.processedChunkCount (+ 1)
                )
              & S.fold F.drain
        )
    -- TODO how should we handle scope at the end of run_backup?
    atomically $ Ki.awaitAll scope
    pure ret

data Tree = Tree
  { tree_name :: {-# UNPACK #-} !T.Text
  , tree_sha :: {-# UNPACK #-} !Digest
  }
  deriving (Show)

data FFile = FFile
  { file_name :: {-# UNPACK #-} !T.Text
  , file_sha :: {-# UNPACK #-} !Digest
  }
  deriving (Show)

data UploadTask
  = UploadTree !Digest !(Path Path.Abs Path.File)
  | UploadFile !Digest !(Path Path.Abs Path.File) !(Maybe P.FileStatus)
  | UploadChunk !Digest !(S.Stream IO (Array.Array Word8))
  | FindNoChangeFile !Digest !P.FileStatus

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
      DirEntryFile digest _ -> digest
      DirEntryDir digest _ -> digest
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, byteshash, BS.singleton 0x0a]

tree_content :: Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir) -> Digest -> BS.ByteString
tree_content file_or_dir hash' =
  let
    !name = either file_name' dir_name' file_or_dir
    !t = either (const "file") (const "dir") file_or_dir
    !byteshash = d2b hash'
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, byteshash, BS.singleton 0x0a]
  where
    file_name' :: Path r Path.File -> BS.ByteString
    file_name' = BC.pack . Path.toFilePath . Path.filename

    dir_name' :: Path r Path.Dir -> BS.ByteString
    dir_name' = BC.pack . init . Path.toFilePath . Path.dirname

collect_dir_and_file_statistics
  :: (E.Repository E.:> es, BackupStatistics E.:> es, E.IOE E.:> es)
  => Path Path.Rel Path.Dir
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

fork_or_wait :: QSem -> Ki.Scope -> IO a -> IO (IO a)
fork_or_wait s = \scope io -> do
  a <- mask $ \restore -> do
    waitQSem s
    Ki.fork scope (restore io `finally` signalQSem s) `onException` signalQSem s
  pure $! atomically $ Ki.await a

fork_or_run_directly :: TSem -> Ki.Scope -> IO a -> IO (IO a)
fork_or_run_directly s = \scope io -> mask $ \restore -> do
  to_fork <- atomically $ (True <$ waitTSem s) <|> pure False
  if to_fork
    then do
      -- XXX We never know if we should call `signalTSem` for Ki.fork since we don't know if `finally` on
      -- `io` would be run or not when exception (both sync and async) was thrown.
      a <- Ki.fork scope (restore io `finally` atomically (signalTSem s)) `onException` atomically (signalTSem s)
      pure $! restore $ atomically $ Ki.await a
    else do
      r <- restore io
      pure $! pure r

data ForkFns
  = ForkFns
      (IO Digest -> IO (IO Digest))
      (IO Digest -> IO (IO Digest))

backup_dir :: (RepositoryWrite E.:> es, BackupCache E.:> es, Tmp E.:> es, E.IOE E.:> es) => Path Path.Rel Path.Dir -> E.Eff es Digest
backup_dir rel_tree_name = Tmp.withEmptyTmpFile $ \file_name' -> EU.reallyUnsafeUnliftIO $ \un -> do
  RepositoryWriteRep _ (ForkFns file_fork_or_not dir_fork_or_not) tbq _ <- un $ E.getStaticRep @RepositoryWrite
  !dir_hash <- liftIO $ withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    Dir.readEither rel_tree_name
      & S.mapM
        ( \fod ->
            fmap (tree_content fod) <$> case fod of
              Left f -> file_fork_or_not $ un $ backup_file $ rel_tree_name </> f
              Right d -> dir_fork_or_not $ un $ backup_dir $ rel_tree_name </> d
        )
      & S.parSequence (S.ordered True)
      & S.trace (BC.hPut fd)
      & S.fold hashByteStringFoldIO

  liftIO $ atomically $ writeTBQueue tbq $ UploadTree dir_hash file_name'

  pure dir_hash

data DirEntry = DirEntryFile Digest BS.ByteString | DirEntryDir Digest BS.ByteString

backup_dir_from_list :: (RepositoryWrite E.:> es, BackupStatistics E.:> es, Tmp E.:> es, E.IOE E.:> es) => [DirEntry] -> E.Eff es Digest
backup_dir_from_list inputs = Tmp.withEmptyTmpFile $ \file_name' -> do
  RepositoryWriteRep _ _ tbq _ <- E.getStaticRep @RepositoryWrite

  BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

  !dir_hash <- liftIO $ withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    S.fromList inputs
      & fmap tree_content_from_dir_entry
      & S.trace (BC.hPut fd)
      & S.fold hashByteStringFoldIO

  liftIO $ atomically $ writeTBQueue tbq $ UploadTree dir_hash file_name'

  pure dir_hash

backup_file :: (RepositoryWrite E.:> es, BackupCache E.:> es, Tmp E.:> es, E.IOE E.:> es) => Path Path.Rel Path.File -> E.Eff es Digest
backup_file rel_file_name = do
  RepositoryWriteRep _ _ tbq _ <- E.getStaticRep @RepositoryWrite

  st <- liftIO $ P.getFileStatus $ Path.fromRelFile rel_file_name
  to_scan <- BackupCacheLevelDB.tryReadingCacheHash st
  case to_scan of
    Just cached_digest -> do
      liftIO $ atomically $ writeTBQueue tbq $ FindNoChangeFile cached_digest st
      pure $! cached_digest
    Nothing -> Tmp.withEmptyTmpFile $ \file_name' -> E.reallyUnsafeUnliftIO $ \un -> do
      !file_hash <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd ->
        withBinaryFile (Path.fromRelFile rel_file_name) ReadMode $ \fdd ->
          Chunker.gearHash Chunker.defaultGearHashConfig (Path.fromRelFile rel_file_name)
            & S.mapM
              ( \(Chunker.Chunk b e) -> do
                  S.unfold chunkReaderFromToWith (b, e - 1, defaultChunkSize, fdd)
                    & S.fold F.toList
              )
            & S.parMapM (S.ordered True . S.maxBuffer 8) (un . backup_chunk)
            & S.trace (BS.hPut fd)
            & S.fold hashByteStringFoldIO

      atomically $ writeTBQueue tbq $ UploadFile file_hash file_name' (Just st)
      pure file_hash

backup_chunk :: (RepositoryWrite E.:> es, E.IOE E.:> es) => [Array.Array Word8] -> E.Eff es BS.ByteString
backup_chunk chunk = E.reallyUnsafeUnliftIO $ \un -> do
  RepositoryWriteRep _ _ tbq ctr <- un $ E.getStaticRep @RepositoryWrite

  (!chunk_hash, !chunk_length) <-
    S.fromList chunk
      & S.fold (F.tee hashArrayFoldIO (F.lmap Array.byteLength F.sum))

  let
    encrypted_chunk_stream = S.concatEffect $ do
      let (Ctr aes tvar_iv) = ctr

      !iv <- atomically $ do
        iv_to_use <- readTVar tvar_iv
        modifyTVar' tvar_iv (`ivAdd` chunk_length)
        pure $! seq (iv_to_use == iv_to_use) iv_to_use

      pure $!
        S.fromList chunk
          & encryptCtr aes iv (1024 * 32)

  atomically $ writeTBQueue tbq $ UploadChunk chunk_hash encrypted_chunk_stream

  pure $! BSS.fromShort $ (digestToBase16ShortByteString chunk_hash) `BSS.snoc` 0x0a

withEmitUnfoldr :: MonadUnliftIO m => Natural -> (TBQueue e -> m a) -> (S.Stream m e -> m b) -> m (a, b)
withEmitUnfoldr q_size putter go = Un.withRunInIO $ \un -> do
  tbq <- newTBQueueIO q_size

  Ki.scoped $ \scope -> do
    thread_putter <- Ki.fork scope $ un $ putter tbq

    let
      f = do
        e <- atomically $ (Just <$> readTBQueue tbq) <|> (Nothing <$ Ki.await thread_putter)
        case e of
          Just v -> pure (Just (v, ()))
          Nothing -> pure Nothing

    thread_solver <- Ki.fork scope $ un $ go $ S.morphInner liftIO $ S.unfoldrM (\() -> f) ()

    ret_putter <- atomically $ Ki.await thread_putter
    ret_solver <- atomically $ Ki.await thread_solver

    pure (ret_putter, ret_solver)
