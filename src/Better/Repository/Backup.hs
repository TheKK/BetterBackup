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

import qualified Data.HashSet as HashSet
import qualified Data.Set as Set

import UnliftIO (
  IOMode (ReadMode, WriteMode),
  MonadIO (..),
  MonadUnliftIO,
  TBQueue,
  TVar,
  atomically,
  finally,
  hClose,
  modifyTVar',
  newTBQueueIO,
  newTVarIO,
  onException,
  readTBQueue,
  readTVar,
  tryIO,
  writeTBQueue,
 )
import qualified UnliftIO.Directory as Un
import qualified UnliftIO.IO.File as Un

import qualified Ki.Unlifted as Ki

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BC

import qualified Data.ByteString.UTF8 as UTF8

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad (unless, when, (<=<), (>=>))
import Control.Monad.Catch (MonadCatch, MonadMask)

import Data.Functor.Identity (Identity (runIdentity))

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe)
import qualified Streamly.Internal.Data.Array.Type as Array (byteLength)
import Streamly.Internal.System.IO (defaultChunkSize)

import qualified Streamly.FileSystem.File as File

import qualified System.Posix.Files as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash (Digest, SHA256)

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S

import Better.Hash (hashArrayFoldIO, hashByteStringFoldIO)
import Better.Repository.Class (
  MonadRepository (fileExists, mkListFolderFiles, mkPutFileFold),
 )
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import qualified Better.Streamly.FileSystem.Dir as Dir
import Better.TempDir.Class (MonadTmp (..))

import Better.Repository.BackupCache.Class (MonadBackupCache)
import qualified Better.Repository.BackupCache.Class as BackupCache

import qualified Better.Statistics.Backup as BackupSt
import Better.Statistics.Backup.Class (MonadBackupStat)

import Better.Repository.Types (Version (..))

import Better.Internal.Streamly.Crypto.AES (compact, encryptCtr, that_aes)
import Data.ByteArray (ByteArrayAccess (..))
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA

import Better.Internal.Repository.LowLevel (addBlob', addDir', addFile', addVersion)
import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM.TSem (TSem, newTSem, signalTSem, waitTSem)
import qualified Crypto.Cipher.AES as Cipher
import Crypto.Cipher.Types (ivAdd)
import qualified Crypto.Cipher.Types as Cipher
import qualified Crypto.Random.Entropy as CRE
import qualified Streamly.Internal.FileSystem.Handle as Handle
import System.Environment (lookupEnv)
import qualified System.Posix.Fcntl as P
import qualified System.Posix.IO as P
import qualified System.Posix.IO.ByteString as PB
import qualified System.Posix.Temp as P
import UnliftIO (bracketOnError)
import qualified UnliftIO as Un
import qualified UnliftIO.Async as Un
import qualified UnliftIO.Concurrent as Un
import qualified UnliftIO.Exception as Un

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
  , tree_sha :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

data FFile = FFile
  { file_name :: {-# UNPACK #-} !T.Text
  , file_sha :: {-# UNPACK #-} !(Digest SHA256)
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

newtype ArrayBA a = ArrayBA {un_array_ba :: Array.Array a}
  deriving (Eq, Ord, Monoid, Semigroup)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) = Array.asPtrUnsafe (Array.castUnsafe arr)

instance BA.ByteArray (ArrayBA Word8) where
  allocRet n f = do
    (ret, bs) <- BA.allocRet n f
    pure (ret, ArrayBA $ Array.fromListN n $ BS.unpack bs)

data UploadTask
  = UploadTree !(Digest SHA256) !(Path Path.Abs Path.File)
  | UploadFile !(Digest SHA256) !(Path Path.Abs Path.File) !(Path Path.Rel Path.File)
  | UploadChunk !(Digest SHA256) !(S.Stream IO (BS.ByteString))
  | FindNoChangeFile !(Digest SHA256) !P.FileStatus

tree_content :: Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir) -> Digest SHA256 -> BS.ByteString
tree_content file_or_dir hash' =
  let
    !name = either file_name' dir_name' file_or_dir
    !t = either (const "file") (const "dir") file_or_dir
    !dd = d2b hash'
  in
    BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, dd, BS.singleton 0x0a]
  where
    file_name' :: Path r Path.File -> BS.ByteString
    file_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . Path.toFilePath . Path.filename

    dir_name' :: Path r Path.Dir -> BS.ByteString
    dir_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . init . Path.toFilePath . Path.dirname

collect_dir_and_file_statistics
  :: (MonadBackupStat m, MonadCatch m, MonadUnliftIO m)
  => Path Path.Rel Path.Dir
  -> m ()
collect_dir_and_file_statistics rel_tree_name = do
  Dir.readEither rel_tree_name
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
mk_sem_for_file :: IO TSem
mk_sem_for_file = atomically . newTSem . fromMaybe 4 . (readMaybe =<<) =<< lookupEnv "CORE2"

fork_or_run_directly :: MonadUnliftIO m => TSem -> Ki.Scope -> m a -> m (m a)
fork_or_run_directly s = \scope io -> do
  to_fork <- atomically $ (True <$ waitTSem s) <|> pure False
  if to_fork
    then do
      -- XXX We never know if we should call `signalTSem` for Ki.fork since we don't know if `finally` on
      -- `io` would be run or not when exception (both sync and async) was thrown.
      -- a <- Un.mask_ $ Ki.fork scope (io `finally` atomically (signalTSem s)) `onException` atomically (signalTSem s)
      -- pure $ atomically $ Ki.await a
      a <- Un.async (io `finally` atomically (signalTSem s)) `onException` atomically (signalTSem s)
      pure $ Un.wait a
    else do
      r <- io
      pure $! pure r

only_fork_to_run :: MonadUnliftIO m => TSem -> Ki.Scope -> m a -> m (m a)
only_fork_to_run s = \scope io -> do
  atomically $ waitTSem s
  a <- Un.async (io `finally` atomically (signalTSem s)) `onException` atomically (signalTSem s)
  pure $ Un.wait a

data ForkFns m
  = ForkFns
      !(Ki.Scope -> m (Digest SHA256) -> m (m (Digest SHA256)))
      !(Ki.Scope -> m (Digest SHA256) -> m (m (Digest SHA256)))

{-# INLINE backup_dir #-}
backup_dir :: (MonadBackupCache m, MonadTmp m, MonadMask m, MonadUnliftIO m) => Ctr -> ForkFns m -> TBQueue UploadTask -> Path Path.Rel Path.Dir -> m (Digest SHA256)
backup_dir ctr fork_fns@(ForkFns file_fork_or_not dir_fork_or_not) tbq rel_tree_name = ww $ \(file_name'', fd) -> do
  unlift <- Un.askUnliftIO
  file_name' <- Path.parseAbsFile file_name''
  (dir_hash, ()) <- do
    -- TODO Maybe we don't need to create new scoped in every level of directory?
    -- Ki.scoped $ \scope -> do
    Dir.readEither rel_tree_name
      & S.mapM
        ( \fod ->
            fmap (tree_content fod) <$> case fod of
              Left !f -> file_fork_or_not undefined $ do
                st < liftIO (P.getFileStatus $ Path.fromRelFile $ rel_tree_name </> f)
                to_scan <- BackupCache.tryReadingCacheHash st
                case to_scan of
                  Just !cached_digest -> do
                    atomically $ writeTBQueue tbq $ FindNoChangeFile cached_digest st
                    pure $! cached_digest
                  Nothing -> do
                    liftIO $ backup_file ctr tbq $! rel_tree_name </> f
              Right !d -> do
                dir_fork_or_not undefined $ backup_dir ctr fork_fns tbq $! rel_tree_name </> d
        )
      & S.parSequence (S.ordered True)
      & S.fold (F.tee hashByteStringFoldIO (F.drainMapM $ liftIO . BC.hPut fd))
  hClose fd
  atomically $ writeTBQueue tbq $ UploadTree dir_hash file_name'

  pure $! dir_hash

{-# INLINE backup_file #-}
backup_file :: Ctr -> TBQueue UploadTask -> Path Path.Rel Path.File -> IO (Digest SHA256)
backup_file ctr tbq rel_file_name = do
  ww $ \(file_name'', fd) -> do
    (!file_hash, ()) <- do
      Un.withBinaryFile (Path.fromRelFile rel_file_name) ReadMode $ \fdd ->
        Chunker.gearHash Chunker.defaultGearHashConfig (Path.fromRelFile rel_file_name)
          & S.mapM
            ( \(Chunker.Chunk b e) -> do
                -- ret <- BS.hGet fdd  (e - b)
                -- pure [ret]
                S.unfold Handle.chunkReaderFromToWith (b, e - 1, defaultChunkSize, fdd)
                  & fmap (BA.convert @_ @BS.ByteString . ArrayBA)
                  & S.mapM (pure $!)
                  & S.fold F.toList
            )
          & S.parMapM (S.ordered True . S.maxBuffer 8) (backup_chunk ctr tbq)
          & S.fold (F.tee hashByteStringFoldIO (F.drainMapM $ liftIO . BC.hPut fd))

    hClose fd
    file_name' <- Path.parseAbsFile file_name''
    atomically $ writeTBQueue tbq $ UploadFile file_hash file_name' rel_file_name
    pure $! file_hash

{-# INLINE backup_chunk #-}
backup_chunk :: Ctr -> TBQueue UploadTask -> [BS.ByteString] -> IO UTF8.ByteString
backup_chunk ctr tbq chunk = do
  (!chunk_hash, !chunk_length) <-
    S.fromList chunk
      & S.fold (F.tee hashByteStringFoldIO (F.lmap BS.length F.sum))

  let
    encrypted_chunk_stream = S.concatEffect $ do
      let (Ctr aes tvar_iv) = ctr

      iv <- atomically $ do
        iv_to_use <- readTVar tvar_iv
        modifyTVar' tvar_iv (`ivAdd` chunk_length)
        pure $! seq (iv_to_use == iv_to_use) iv_to_use

      pure $!
        S.fromList chunk
          & encryptCtr aes iv (1024 * 32)

  atomically $ writeTBQueue tbq $ UploadChunk chunk_hash encrypted_chunk_stream
  pure $! d2b chunk_hash `BS.snoc` 0x0a

{-# INLINE backup #-}
backup
  :: (MonadBackupCache m, MonadBackupStat m, MonadRepository m, MonadTmp m, MonadMask m, MonadUnliftIO m)
  => T.Text
  -> m Version
backup dir = Un.runInUnboundThread $ do
  let
    mk_uniq_gate = do
      running_set <- newTVarIO $ HashSet.empty @(BS.ByteString)
      pure $ \(!hash') (!m) -> do
        let hash'' = BA.convert hash'
        other_is_running <- atomically $ do
          is_running <- HashSet.member hash'' <$> readTVar running_set
          if is_running
            then pure True
            else do
              modifyTVar' running_set (HashSet.insert hash'')
              pure False
        unless other_is_running $
          m `finally` atomically (modifyTVar' running_set (HashSet.delete hash''))

  fork_fns <- do
    sem_for_dir <- liftIO mk_sem_for_dir
    sem_for_file <- liftIO mk_sem_for_file
    pure $ ForkFns (fork_or_run_directly sem_for_file) (fork_or_run_directly sem_for_dir)

  unique_chunk_gate <- mk_uniq_gate
  unique_tree_gate <- mk_uniq_gate
  unique_file_gate <- mk_uniq_gate

  ctr <- liftIO init_ctr
  rel_dir <- Path.parseRelDir $ T.unpack dir

  worker_n <- do
    s <- liftIO $ lookupEnv "WORKER"
    pure $! (fromMaybe 4) $ readMaybe =<< s

  (root_hash, _) <- Ki.scoped $ \scope -> do
    _ <- Ki.fork scope (collect_dir_and_file_statistics rel_dir)
    ret <-
      withEmitUnfoldr
        50
        (\tbq -> backup_dir ctr fork_fns tbq rel_dir)
        ( \s ->
            s
              & S.parMapM
                (S.maxBuffer worker_n . S.eager True)
                ( \case
                    UploadTree dir_hash file_name' -> do
                      unique_tree_gate dir_hash $ do
                        content <- liftIO $ BS.readFile $ Path.fromAbsFile file_name'
                        addDir' dir_hash (S.fromPure content)
                          `finally` Un.removeFile (Path.fromAbsFile file_name')
                      BackupSt.modifyStatistic' BackupSt.processedDirCount (+ 1)
                    UploadFile file_hash file_name' rel_file_name -> do
                      unique_file_gate file_hash $ do
                        content <- liftIO $ BS.readFile $ Path.fromAbsFile file_name'
                        addFile' file_hash (S.fromPure content)
                          `finally` Un.removeFile (Path.fromAbsFile file_name')
                      BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                      st <- liftIO $ P.getFileStatus $ Path.fromRelFile rel_file_name
                      BackupCache.saveCurrentFileHash st file_hash
                    FindNoChangeFile file_hash st -> do
                      BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
                      BackupCache.saveCurrentFileHash st file_hash
                    UploadChunk chunk_hash chunk_stream -> do
                      unique_chunk_gate chunk_hash $ do
                        added_bytes <- addBlob' chunk_hash (S.morphInner liftIO chunk_stream)
                        BackupSt.modifyStatistic' BackupSt.uploadedBytes (+ fromIntegral added_bytes)
                      BackupSt.modifyStatistic' BackupSt.processedChunkCount (+ 1)
                )
              & S.fold F.drain
        )
    atomically $ Ki.awaitAll scope
    pure ret

  version_id <- nextBackupVersionId
  addVersion version_id root_hash
  return $ Version version_id root_hash

d2b :: Digest SHA256 -> BS.ByteString
d2b = BA.convertToBase BA.Base16

withEmitUnfoldr :: MonadUnliftIO m => Natural -> (TBQueue e -> m a) -> (S.Stream m e -> m b) -> m (a, b)
withEmitUnfoldr q_size putter go = do
  tbq <- newTBQueueIO q_size

  Ki.scoped $ \scope -> do
    thread_putter <- Ki.fork scope $ putter tbq

    let
      f = do
        e <- atomically $ (Just <$> readTBQueue tbq) <|> (Nothing <$ Ki.await thread_putter)
        case e of
          Just v -> pure (Just (v, ()))
          Nothing -> pure Nothing

    thread_solver <- Ki.fork scope $ go $ S.unfoldrM (\() -> f) ()

    ret_putter <- atomically $ Ki.await thread_putter
    ret_solver <- atomically $ Ki.await thread_solver

    pure (ret_putter, ret_solver)

folder_chunk :: Path Path.Rel Path.Dir
folder_chunk = [Path.reldir|chunk|]

folder_file :: Path Path.Rel Path.Dir
folder_file = [Path.reldir|file|]

folder_tree :: Path Path.Rel Path.Dir
folder_tree = [Path.reldir|tree|]

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]

{-# INLINE ww #-}
ww :: MonadUnliftIO m => ((FilePath, Un.Handle) -> m c) -> m c
ww run = do
  bracketOnError
    (liftIO $ P.mkstemp "/tmp/ggg-")
    ( \(name, h) -> do
        _ <- tryIO $ hClose h
        Un.removeFile name
    )
    (run)
