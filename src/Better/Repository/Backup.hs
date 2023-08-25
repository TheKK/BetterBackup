{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC
  -ddump-simpl
  -ddump-to-file
  -dsuppress-all
  -Wmissed-specialisations
  -Wall-missed-specialisations
#-}

module Better.Repository.Backup
  ( backup
  ) where

import Prelude hiding (read)

import Numeric.Natural

import Data.Word
import Data.Function
import Data.Maybe (fromMaybe)

import qualified Data.Set as Set

import UnliftIO
import qualified UnliftIO.IO.File as Un
import qualified UnliftIO.Directory as Un

import qualified Ki.Unlifted as Ki

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BC

import qualified Data.ByteString.UTF8 as UTF8

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Catch (MonadCatch, MonadMask)

import Data.Functor.Identity

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Type as Array (byteLength)
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe)
import Streamly.Internal.System.IO (defaultChunkSize)

import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.File as File

import qualified System.Posix.Files as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import Better.Hash
import Better.TempDir
import Better.Repository.Class
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import qualified Better.Streamly.FileSystem.Dir as Dir

import Better.Repository.BackupCache.Class (MonadBackupCache)
import qualified Better.Repository.BackupCache.Class as BackupCache

import Better.Statistics.Backup (MonadBackupStat)
import qualified Better.Statistics.Backup as BackupSt

import Better.Repository.Types (Version(..))

import Data.ByteArray (ByteArrayAccess(..))
import qualified Data.ByteArray.Encoding as BA

data Tree
  = Tree
  { tree_name :: {-# UNPACK #-} !T.Text
  , tree_sha :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

data FFile
  = FFile
  { file_name :: {-# UNPACK #-} !T.Text
  , file_sha :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

listFolderFiles :: (MonadIO m, MonadRepository m)
                => Path Path.Rel Path.Dir -> S.Stream m (Path Path.Rel Path.File)
listFolderFiles d = S.concatEffect $ do
  f <- mkListFolderFiles
  pure $ f d

{-# INLINE addBlob' #-}
addBlob' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m Bool -- ^ True if we actually upload this chunk, otherwise False.
addBlob' digest chunks = do
  file_name' <- Path.parseRelFile $ show digest
  let f = folder_chunk </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)
  pure $! not exist

{-# INLINE addFile' #-}
addFile' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m ()
addFile' digest chunks = do
  file_name' <- Path.parseRelFile $ show digest
  let f = folder_file </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

{-# INLINE addDir' #-}
addDir' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m ()
addDir' digest chunks = do
  file_name' <- Path.parseRelFile $ show digest
  let f = folder_tree </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

addVersion :: (MonadIO m, MonadCatch m, MonadRepository m)
  => Integer -> Digest SHA256 -> m ()
addVersion v_id v_root = do
  ver_id_file_name <- Path.parseRelFile $ show v_id
  let f = folder_version </> ver_id_file_name
  putFileFold <- mkPutFileFold
  liftIO $
    S.fromPure (Array.fromList $ BS.unpack $ TE.encodeUtf8 $ T.pack $ show v_root)
      & S.fold (putFileFold f)

nextBackupVersionId :: (MonadIO m, MonadCatch m, MonadRepository m) => m Integer
nextBackupVersionId = do
  listFolderFiles folder_version
    & S.mapMaybe (readMaybe @Integer . Path.fromRelFile)
    & S.fold F.maximum
    & fmap (succ . fromMaybe 0)

newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp

data UploadTask
  = UploadTree {-# UNPACK #-}  !(Digest SHA256) {-# UNPACK #-} !(Path Path.Abs Path.File)
  | UploadFile {-# UNPACK #-}  !(Digest SHA256) {-# UNPACK #-} !(Path Path.Abs Path.File) !(Path Path.Rel Path.File)
  | UploadChunk {-# UNPACK #-} !(Digest SHA256) [(Array.Array Word8)]
  | FindNoChangeFile !(Digest SHA256) !(Path Path.Rel Path.File)

tree_content :: Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir) -> Digest SHA256 -> BS.ByteString
tree_content file_or_dir hash' =
  let
    name = either file_name' dir_name' file_or_dir
    t = either (const "file") (const "dir") file_or_dir
  in BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, d2b hash', BS.singleton 0x0a]
  where
    file_name' :: Path r Path.File -> BS.ByteString
    file_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . Path.toFilePath . Path.filename

    dir_name' :: Path r Path.Dir -> BS.ByteString
    dir_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . init . Path.toFilePath . Path.dirname

collect_dir_and_file_statistics
  :: (MonadBackupStat m, MonadCatch m, MonadUnliftIO m)
  => Path Path.Rel Path.Dir -> m ()
collect_dir_and_file_statistics rel_tree_name = do
  Dir.readEither rel_tree_name
    & S.mapM (either
        (const $ BackupSt.modifyStatistic' BackupSt.totalFileCount (+ 1))
        (collect_dir_and_file_statistics . (rel_tree_name </>))
    )
    & S.fold F.drain
  BackupSt.modifyStatistic' BackupSt.totalDirCount (+ 1)

backup_dir :: (MonadBackupCache m, MonadTmp m, MonadMask m, MonadUnliftIO m) => TBQueue UploadTask -> Path Path.Rel Path.Dir -> m (Digest SHA256)
backup_dir tbq rel_tree_name = withEmptyTmpFile $ \file_name' -> do
  (dir_hash, ()) <- Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    Dir.readEither rel_tree_name
      -- TODO reduce memory/thread overhead
      & S.parMapM (S.ordered True . S.eager True . S.maxBuffer 2) (\fod -> do
          sub_hash <- either (backup_file tbq . (rel_tree_name </>)) (backup_dir tbq . (rel_tree_name </>)) fod
          pure $ tree_content fod sub_hash
        )
      & S.fold (F.tee hashByteStringFold (F.morphInner liftIO $ F.drainMapM $ BC.hPut fd))

  atomically $ writeTBQueue tbq $ UploadTree dir_hash file_name'

  pure dir_hash

backup_file :: (MonadBackupCache m, MonadTmp m, MonadMask m, MonadUnliftIO m) => TBQueue UploadTask -> Path Path.Rel Path.File -> m (Digest SHA256)
backup_file tbq rel_file_name = do
  st <- liftIO (P.getFileStatus $ Path.fromRelFile rel_file_name)
  to_scan <- BackupCache.tryReadingCacheHash st
  case to_scan of
    Just cached_digest -> do
      atomically $ writeTBQueue tbq $ FindNoChangeFile cached_digest rel_file_name
      pure cached_digest

    Nothing -> withEmptyTmpFile $ \file_name' -> liftIO $ do
      (file_hash, _) <- Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd ->
        Chunker.gearHash Chunker.defaultGearHashConfig (Path.fromRelFile rel_file_name)
          & S.parMapM (S.ordered True . S.eager True . S.maxBuffer 10) (\(Chunker.Chunk b e) -> do
              S.unfold File.chunkReaderFromToWith (b, (e - 1), defaultChunkSize, (Path.fromRelFile rel_file_name))
                & S.fold F.toList
            )
          & S.parMapM (S.ordered True . S.eager True . S.maxBuffer 7) (backup_chunk tbq)
          & S.fold (F.tee hashByteStringFold (F.drainMapM $ BC.hPut fd))

      atomically $ writeTBQueue tbq $ UploadFile file_hash file_name' rel_file_name
      pure file_hash

backup_chunk :: TBQueue UploadTask -> [Array.Array Word8] -> IO (UTF8.ByteString)
backup_chunk tbq chunk = do
  let chunk_hash = runIdentity (S.fromList chunk & S.fold hashArrayFold)
  atomically $ writeTBQueue tbq $ UploadChunk chunk_hash chunk
  pure $! d2b chunk_hash `BS.snoc` 0x0a

{-# INLINEABLE backup #-}
backup :: (MonadBackupCache m, MonadBackupStat m, MonadRepository m, MonadTmp m, MonadMask m, MonadUnliftIO m)
       => T.Text -> m Version
backup dir = do
  let
    mk_uniq_gate = do
      running_set <- newTVarIO $ Set.empty @(Digest SHA256)
      pure $ \hash' m -> do
        other_is_running <- atomically $ stateTVar running_set $ \set ->
          (Set.member hash' set, Set.insert hash' set)
        unless other_is_running $
          m `finally` (atomically $ modifyTVar running_set (Set.delete hash'))

  unique_chunk_gate <- mk_uniq_gate
  unique_tree_gate <- mk_uniq_gate
  unique_file_gate <- mk_uniq_gate

  rel_dir <- Path.parseRelDir $ T.unpack dir

  (root_hash, _) <- Ki.scoped $ \scope -> do
    _ <- Ki.fork scope (collect_dir_and_file_statistics rel_dir)
    ret <- withEmitUnfoldr 50 (\tbq -> backup_dir tbq rel_dir)
      (\s -> s
         & S.parMapM (S.maxBuffer 20 . S.eager True) (\case
           UploadTree dir_hash file_name' -> do
             unique_tree_gate dir_hash $ do
               addDir' dir_hash (File.readChunks (Path.fromAbsFile file_name'))
               `finally` Un.removeFile (Path.fromAbsFile file_name')
             BackupSt.modifyStatistic' BackupSt.processedDirCount (+ 1)

           UploadFile file_hash file_name' rel_file_name -> do
             unique_file_gate file_hash $ do
               addFile' file_hash (File.readChunks (Path.fromAbsFile file_name'))
               `finally` Un.removeFile (Path.fromAbsFile file_name')
             BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
             st <- liftIO $ P.getFileStatus $ Path.fromRelFile rel_file_name
             BackupCache.saveCurrentFileHash st file_hash

           FindNoChangeFile file_hash rel_file_name -> do
             BackupSt.modifyStatistic' BackupSt.processedFileCount (+ 1)
             st <- liftIO $ P.getFileStatus $ Path.fromRelFile rel_file_name
             BackupCache.saveCurrentFileHash st file_hash

           UploadChunk chunk_hash chunk -> do
             unique_chunk_gate chunk_hash $ do
               do_work <- addBlob' chunk_hash (S.fromList chunk)
               when do_work $
                 BackupSt.modifyStatistic' BackupSt.uploadedBytes $
                   (+ (fromIntegral $ sum $ fmap Array.byteLength chunk))
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
