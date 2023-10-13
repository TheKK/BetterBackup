{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Repository.Backup (
  backup,
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

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad (unless)
import Control.Monad.Catch (MonadCatch, MonadMask)
import Control.Monad.IO.Class (MonadIO, liftIO)

import Control.Monad.IO.Unlift (MonadUnliftIO)
import qualified Control.Monad.IO.Unlift as Un

import Control.Concurrent.STM (atomically, TVar, modifyTVar', newTVarIO, readTBQueue, TBQueue, readTVar, writeTBQueue, newTBQueueIO)

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

import Better.Hash (Digest, digestToBase16ByteString, digestToBase16ShortByteString, hashArrayFoldIO, hashByteStringFoldIO)
import Better.Internal.Repository.LowLevel (addBlob', addDir', addFile', addVersion)
import Better.Internal.Streamly.Array (ArrayBA (ArrayBA, un_array_ba), chunkReaderFromToWith)
import Better.Internal.Streamly.Crypto.AES (encryptCtr, that_aes)
import Better.Repository.BackupCache.Class (MonadBackupCache)
import qualified Better.Repository.BackupCache.Class as BackupCache
import Better.Repository.Class (MonadRepository (mkListFolderFiles))
import Better.Repository.Types (Version (..))
import qualified Better.Statistics.Backup as BackupSt
import Better.Statistics.Backup.Class (MonadBackupStat)
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import qualified Better.Streamly.FileSystem.Dir as Dir
import Better.TempDir.Class (MonadTmp (..))

data Ctr = Ctr
  { _ctr_aes :: {-# UNPACK #-} !Cipher.AES128
  , _ctr_iv :: {-# UNPACK #-} !(TVar (Cipher.IV Cipher.AES128))
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

data Tree = Tree
  { tree_name :: {-# UNPACK #-} !T.Text
  , tree_sha :: {-# UNPACK #-} !(Digest)
  }
  deriving (Show)

data FFile = FFile
  { file_name :: {-# UNPACK #-} !T.Text
  , file_sha :: {-# UNPACK #-} !(Digest)
  }
  deriving (Show)

listFolderFiles
  :: (MonadIO m, MonadRepository m)
  => Path Path.Rel Path.Dir
  -> S.Stream m (Path Path.Rel Path.File)
listFolderFiles d = S.concatEffect $ do
  f <- mkListFolderFiles
  pure $ f d

nextBackupVersionId :: (MonadIO m, MonadCatch m, MonadRepository m) => m Integer
nextBackupVersionId = do
  listFolderFiles folder_version
    & S.mapMaybe (readMaybe @Integer . Path.fromRelFile)
    & S.fold F.maximum
    & fmap (succ . fromMaybe 0)

data UploadTask
  = UploadTree {-# UNPACK #-} !Digest !(Path Path.Abs Path.File)
  | UploadFile {-# UNPACK #-} !Digest !(Path Path.Abs Path.File) !P.FileStatus
  | UploadChunk {-# UNPACK #-} !Digest !(S.Stream IO (Array.Array Word8))
  | FindNoChangeFile {-# UNPACK #-} !Digest !P.FileStatus

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

{-# INLINE collect_dir_and_file_statistics #-}
collect_dir_and_file_statistics
  :: (MonadBackupStat m, MonadCatch m, MonadUnliftIO m)
  => Path Path.Rel Path.Dir
  -> m ()
collect_dir_and_file_statistics rel_tree_name = Un.withRunInIO $ \un -> do
  Dir.readEither rel_tree_name
    & S.mapM
      ( un
          . either
            (const $ BackupSt.modifyStatistic' BackupSt.totalFileCount (+ 1))
            (collect_dir_and_file_statistics . (rel_tree_name </>))
      )
    & S.fold F.drain
  un $ BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

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
      {-# UNPACK #-} !(IO (Digest) -> IO (IO (Digest)))
      {-# UNPACK #-} !(IO (Digest) -> IO (IO (Digest)))

{-# INLINE backup_dir #-}
backup_dir :: (MonadBackupCache m, MonadTmp m, MonadUnliftIO m) => Ctr -> ForkFns -> TBQueue UploadTask -> Path Path.Rel Path.Dir -> m (Digest)
backup_dir ctr fork_fns@(ForkFns file_fork_or_not dir_fork_or_not) tbq rel_tree_name = withEmptyTmpFile $ \file_name' -> Un.withRunInIO $ \un -> do
  (!dir_hash, ()) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    Dir.readEither rel_tree_name
      & S.mapM
        ( \fod ->
            fmap (tree_content fod) <$> case fod of
              Left f -> fmap liftIO $ file_fork_or_not $ un $ backup_file ctr tbq $ rel_tree_name </> f
              Right d -> fmap liftIO $ dir_fork_or_not $ un $ backup_dir ctr fork_fns tbq $ rel_tree_name </> d
        )
      & S.parSequence (S.ordered True)
      & S.fold (F.tee hashByteStringFoldIO (F.drainMapM $ BC.hPut fd))

  atomically $ writeTBQueue tbq $ UploadTree dir_hash file_name'

  pure dir_hash

{-# INLINE backup_file #-}
backup_file :: (MonadBackupCache m, MonadTmp m, MonadUnliftIO m) => Ctr -> TBQueue UploadTask -> Path Path.Rel Path.File -> m (Digest)
backup_file ctr tbq rel_file_name = Un.withRunInIO $ \un -> do
  st <- P.getFileStatus $ Path.fromRelFile rel_file_name
  to_scan <- un $ BackupCache.tryReadingCacheHash st
  case to_scan of
    Just cached_digest -> do
      atomically $ writeTBQueue tbq $ FindNoChangeFile cached_digest st
      pure $! cached_digest
    Nothing -> un $ withEmptyTmpFile $ \file_name' -> liftIO $ do
      (!file_hash, _) <- withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd ->
        withBinaryFile (Path.fromRelFile rel_file_name) ReadMode $ \fdd ->
          Chunker.gearHash Chunker.defaultGearHashConfig (Path.fromRelFile rel_file_name)
            & S.mapM
              ( \(Chunker.Chunk b e) -> do
                  S.unfold chunkReaderFromToWith (b, e - 1, defaultChunkSize, fdd)
                    & S.fold F.toList
              )
            & S.parMapM (S.ordered True . S.maxBuffer 8) (backup_chunk ctr tbq)
            & S.fold (F.tee hashByteStringFoldIO (F.drainMapM $ BS.hPut fd))

      atomically $ writeTBQueue tbq $ UploadFile file_hash file_name' st
      pure file_hash

backup_chunk :: Ctr -> TBQueue UploadTask -> [Array.Array Word8] -> IO BS.ByteString
backup_chunk ctr tbq chunk = do
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
          & fmap ArrayBA
          & encryptCtr aes iv (1024 * 32)
          & fmap un_array_ba

  atomically $ writeTBQueue tbq $ UploadChunk chunk_hash encrypted_chunk_stream

  pure $! BSS.fromShort $ (digestToBase16ShortByteString chunk_hash) `BSS.snoc` 0x0a

{-# INLINE backup #-}
backup
  :: (MonadBackupCache m, MonadBackupStat m, MonadRepository m, MonadTmp m, MonadMask m, MonadUnliftIO m)
  => T.Text
  -> m Version
backup dir = Un.withRunInIO $ \un -> do
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
  rel_dir <- Path.parseRelDir $ T.unpack dir

  (root_hash, _) <- Ki.scoped $ \scope -> do
    fork_fns <- do
      sem_for_dir <- mk_sem_for_dir
      sem_for_file <- mk_sem_for_file
      pure $ ForkFns (fork_or_wait sem_for_file scope) (fork_or_run_directly sem_for_dir scope)

    _ <- Ki.fork scope (un $ collect_dir_and_file_statistics rel_dir)
    ret <-
      withEmitUnfoldr
        20
        (\tbq -> un $ backup_dir ctr fork_fns tbq rel_dir)
        ( \s ->
            s
              & S.parMapM
                (S.maxBuffer 100 . S.eager True)
                ( \case
                    UploadTree dir_hash file_name' -> do
                      unique_tree_gate dir_hash $
                        do
                          un (addDir' dir_hash (File.readChunks (Path.fromAbsFile file_name')))
                          `finally` D.removeFile (Path.fromAbsFile file_name')
                      un $ BackupSt.modifyStatistic' BackupSt.processedDirCount (+ 1)
                    UploadFile file_hash file_name' st -> do
                      unique_file_gate file_hash $
                        do
                          un (addFile' file_hash (File.readChunks (Path.fromAbsFile file_name')))
                          `finally` D.removeFile (Path.fromAbsFile file_name')
                      un $ BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                      un $ BackupCache.saveCurrentFileHash st file_hash
                    FindNoChangeFile file_hash st -> do
                      un $ BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                      un $ BackupCache.saveCurrentFileHash st file_hash
                    UploadChunk chunk_hash chunk_stream -> do
                      unique_chunk_gate chunk_hash $ do
                        added_bytes <- un $ addBlob' chunk_hash (S.morphInner liftIO chunk_stream)
                        un $ BackupSt.modifyStatistic' BackupSt.uploadedBytes (+ fromIntegral added_bytes)
                      un $ BackupSt.modifyStatistic' BackupSt.processedChunkCount (+ 1)
                )
              & S.fold F.drain
        )
    atomically $ Ki.awaitAll scope
    pure ret

  version_id <- un nextBackupVersionId
  un $ addVersion version_id root_hash
  return $ Version version_id root_hash

d2b :: Digest -> BS.ByteString
d2b = digestToBase16ByteString

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

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]
