{-# LANGUAGE Strict #-}
{-# LANGUAGE LambdaCase #-}
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

module Better.Repository
   -- * Write
  ( initRepositoryStructure
  , backup
  , addBlob'
  , addFile'
  , addDir'
  , addVersion
  , nextBackupVersionId
  -- * Read
  , catVersion
  , tryCatingVersion
  , catTree
  , catFile
  , catChunk
  , getChunkSize
  , listFolderFiles
  , listVersions
  , checksum
  -- * Deletion
  , garbageCollection
  -- * Repositories
  , localRepo
  -- * Monad
  , MonadRepository
  , TheMonadRepository(..)
  -- * Types
  , Repository

  -- * Version
  , Version(..)

  , Tree(..)
  , FFile(..)
  , Object(..)
  ) where

import Prelude hiding (read)

import Numeric.Natural

import Data.Word
import Data.Function
import Data.Foldable
import Data.Maybe (fromMaybe, isJust, fromJust)

import qualified Data.Set as Set

import UnliftIO
import qualified UnliftIO.IO.File as Un
import qualified UnliftIO.Directory as Un

import qualified Ki.Unlifted as Ki

import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BC

import qualified Data.ByteString.UTF8 as UTF8

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Catch (MonadCatch, MonadThrow, MonadMask, throwM, handleIf)

import Data.Functor.Identity

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array.Type as Array (byteLength)
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe)
import Streamly.Internal.System.IO (defaultChunkSize)

import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.File as File
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

import System.IO.Error (isDoesNotExistError)
import System.Posix.Types (FileOffset)

import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import qualified Capability.Reader as C

import Better.Hash
import Better.TempDir
import Better.Repository.Class
import qualified Better.Streamly.FileSystem.Chunker as Chunker
import qualified Better.Streamly.FileSystem.Dir as Dir

import Better.Repository.BackupCache.Class (MonadBackupCache)
import qualified Better.Repository.BackupCache.Class as BackupCache

import Better.Statistics.Backup (MonadBackupStat)
import qualified Better.Statistics.Backup as BackupSt

import Data.ByteArray (ByteArrayAccess(..))
import qualified Data.ByteArray.Encoding as BA

data Repository = Repository
   { _repo_putFile :: Path Path.Rel Path.File -> F.Fold IO (Array.Array Word8) ()
   , _repo_removeFiles :: [Path Path.Rel Path.File] -> IO ()
   -- TODO File mode?
   , _repo_createDirectory :: Path Path.Rel Path.Dir -> IO ()
   , _repo_fileExists :: Path Path.Rel Path.File -> IO Bool
   , _repo_fileSize :: Path Path.Rel Path.File -> IO FileOffset
   , _repo_read :: Path Path.Rel Path.File -> S.Stream IO (Array.Array Word8)
   , _repo_listFolderFiles :: Path Path.Rel Path.Dir -> S.Stream IO (Path Path.Rel Path.File)
   }

data Version
  = Version
  { ver_id :: {-# UNPACK #-} !Integer
  , ver_root :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

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

data Object
  = Object
  { chunk_name :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

newtype TheMonadRepository m a = TheMonadRepository (m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask)

instance (C.HasReader "repo" Repository m, MonadIO m) => MonadRepository (TheMonadRepository m) where
  mkPutFileFold = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_putFile
    pure $ F.morphInner liftIO . f

  removeFiles files = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_removeFiles
    liftIO $ f files

  createDirectory d = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_createDirectory
    liftIO $ f d

  fileExists file = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_fileExists
    liftIO $ f file

  fileSize file = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_fileSize
    liftIO $ f file

  mkRead = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_read
    pure $ S.morphInner liftIO . f

  mkListFolderFiles = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_listFolderFiles
    pure $ S.morphInner liftIO . f

listFolderFiles :: (MonadIO m, MonadRepository m)
                => Path Path.Rel Path.Dir -> S.Stream m (Path Path.Rel Path.File)
listFolderFiles d = S.concatEffect $ do
  f <- mkListFolderFiles
  pure $ f d

read :: (MonadIO m, MonadRepository m)
                => Path Path.Rel Path.File -> S.Stream m (Array.Array Word8)
read p = S.concatEffect $ do
  f <- mkRead
  pure $ f p

listVersions :: (MonadThrow m, MonadIO m, MonadRepository m) => S.Stream m Version
listVersions =
  listFolderFiles folder_version
    & S.mapM (\v -> do
      v' <- liftIO $ readIO $ Path.fromRelFile v
      catVersion v'
    )

tryCatingVersion :: (MonadThrow m, MonadIO m, MonadRepository m) => Integer -> m (Maybe Version)
tryCatingVersion vid = do
  path_vid <- Path.parseRelFile $ show vid
  -- TODO using fileExists could cause issue of TOCTOU,
  -- but trying to get "file does not exist" info from catVersion require all Repository throws
  -- specific exception, which is also hard to achieve.
  exists <- fileExists $ folder_version </> path_vid
  if exists
  then Just <$> catVersion vid
  else pure Nothing

  -- hbk <- C.asks @"hbk" hbk_path
  -- let ver_path = T.unpack $ hbk <> "/version"
  -- pure $ Dir.readFiles ver_path
  --   & S.mapM (\v -> do
  --     optSha <- fmap (BS.decode . BS.pack)
  --       $ S.toList
  --       $ File.read (ver_path <> "/" <> v)
  --     v' <- liftIO $ readIO v

  --     sha <- case optSha of
  --       Left err -> throwM $ userError $ "invalid sha of version, " <> err
  --       Right sha -> pure $ sha

  --     digest <- case digestFromByteString @SHA256 sha of
  --       Nothing -> throwM $ userError "invalid sha of version"
  --       Just digest -> pure digest

  --     pure $ Version v' digest
  --   )

localRepo :: Path Path.Abs Path.Dir -> Repository
localRepo root = Repository
  (File.writeChunks . Path.fromAbsFile . (root </>))
  (mapM_ $ (handleIf isDoesNotExistError (const $ pure ())) . Un.removeFile . Path.fromAbsFile . (root </>))
  (flip P.createDirectory 700 . Path.fromAbsDir . (root </>))
  (P.fileExist . Path.fromAbsFile . (root </>))
  (fmap P.fileSize . P.getFileStatus . Path.fromAbsFile . (root </>))
  (File.readChunks . Path.fromAbsFile . (root </>))
  (S.mapM Path.parseRelFile . Dir.read . (root </>))

folder_chunk :: Path Path.Rel Path.Dir
folder_chunk = [Path.reldir|chunk|]

folder_file :: Path Path.Rel Path.Dir
folder_file = [Path.reldir|file|]

folder_tree :: Path Path.Rel Path.Dir
folder_tree = [Path.reldir|tree|]

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]

initRepositoryStructure :: (MonadRepository m) => m ()
initRepositoryStructure = do pure ()
  -- mapM_ createDirectory $
  --   [ [Path.reldir| |]
  --   , folder_chunk
  --   , folder_file
  --   , folder_tree
  --   , folder_version
  --   ]
  --
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

cat_stuff_under :: (Show a, MonadThrow m, MonadIO m, MonadRepository m)
                => Path Path.Rel Path.Dir -> a -> S.Stream m (Array.Array Word8)
cat_stuff_under folder stuff = S.concatEffect $ do
  read_blob <- mkRead
  liftIO $ do
    stuff_path <- Path.parseRelFile $ show stuff
    pure $ read_blob (folder </> stuff_path)
{-# INLINE cat_stuff_under #-}

catFile :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest SHA256 -> S.Stream m Object
catFile sha = cat_stuff_under folder_file sha
  & S.unfoldMany Array.reader
  & US.decodeLatin1
  & US.lines (fmap T.pack F.toList)
  & S.mapM parse_file_content
  where
    parse_file_content :: (MonadThrow m, Applicative m) => T.Text -> m Object
    parse_file_content = fmap Object . t2d
{-# INLINE catFile #-}

catChunk :: (MonadThrow m, MonadIO m, MonadRepository m)
         => Digest SHA256 -> S.Stream m (Array.Array Word8)
catChunk = cat_stuff_under folder_chunk
{-# INLINE catChunk #-}

getChunkSize :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest SHA256 -> m FileOffset
getChunkSize sha = do
  sha_path <- Path.parseRelFile $ show sha
  fileSize $ folder_chunk </> sha_path
{-# INLINE getChunkSize #-}

catVersion :: (MonadThrow m, MonadIO m, MonadRepository m) => Integer -> m Version
catVersion vid = do
  optSha <- cat_stuff_under folder_version vid
    & fmap (BB.toLazyByteString . foldMap BB.word8 . Array.toList)
    & S.fold F.mconcat
    & fmap (BS.decode . BS.toStrict)

  sha <- case optSha of
    Left err -> throwM $ userError $ "invalid sha of version, " <> err
    Right sha -> pure $ sha

  digest <- case digestFromByteString @SHA256 sha of
    Nothing -> throwM $ userError "invalid sha of version"
    Just digest -> pure digest

  pure $ Version vid digest
{-# INLINE catVersion #-}

catTree :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest SHA256 -> S.Stream m (Either Tree FFile)
catTree tree_sha' = cat_stuff_under folder_tree tree_sha'
  & US.decodeUtf8Chunks
  & US.lines (fmap T.pack F.toList)
  & S.mapM parse_tree_content
  where
    parse_tree_content :: MonadThrow m => T.Text -> m (Either Tree FFile)
    parse_tree_content buf = case T.splitOn " " buf of
      ["dir", name, sha] -> do
        digest <- t2d sha
        pure $ Left $ Tree name digest
      ["file", name, sha] -> do
        digest <- t2d sha
        pure $ Right $ FFile name digest
      _ -> throwM $ userError $ "invalid dir content: " <> T.unpack buf

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

checksum :: (MonadThrow m, MonadUnliftIO m, MonadRepository m) => Int -> m ()
checksum n = do
  S.fromList [folder_tree, folder_file, folder_chunk]
    & S.concatMap (\p ->
      listFolderFiles p
        & fmap (\f -> (Path.fromRelFile f, p </> f))
    )
    & S.parMapM (S.maxBuffer (n + 1) . S.maxThreads n . S.eager True) (\(expected_sha, f) -> do
         actual_sha <- read f
           & S.parEval (S.ordered True . S.eager True . S.maxBuffer 2)
           & S.fold hashArrayFold
         pure $
           if show actual_sha == expected_sha
           then Nothing
           else Just (f, actual_sha)
       )
    & S.filter (isJust)
    & fmap (fromJust)
    & S.trace (\(invalid_f, expect_sha) ->
        liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.fromRelFile invalid_f) <> ", " <> T.pack (show expect_sha))
    & S.fold F.drain

garbageCollection :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
garbageCollection = gc_tree >> gc_file >> gc_chunk
  where
    gc_tree :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_tree = do
      -- tree level
      all_tree_file_set <- listFolderFiles folder_tree
        & fmap (UTF8.fromString . Path.fromRelFile)
        & S.fold F.toSet

      traverse_set <- newIORef all_tree_file_set
      to_set <- newIORef Set.empty

      listVersions
        & fmap (ver_root)
        & S.fold (F.drainMapM $ \sha -> modifyIORef' to_set (Set.insert sha))

      fix $ \(~cont) -> do
        to <- readIORef to_set
        case Set.lookupMin to of
          Nothing -> pure ()
          Just e -> do
            modifyIORef' to_set (Set.delete e)
            visited <- not . Set.member (d2b e) <$> readIORef traverse_set
            if visited
              then cont
              else do
                modifyIORef' traverse_set (Set.delete $ d2b e)
                traversed_set' <- readIORef traverse_set
                catTree e
                  & S.mapMaybe (either (Just . tree_sha) (const Nothing))
                  & S.filter (flip Set.member traversed_set' . d2b)
                  & S.fold (F.drainMapM $ \s -> modifyIORef' to_set (Set.insert s))
                cont

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete tree: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_tree </> rel_dead_file])

    gc_file :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_file = do
      -- tree level
      all_file_file_set <- listFolderFiles folder_file
        & fmap (UTF8.fromString . Path.fromRelFile)
        & S.fold F.toSet

      traverse_set <- newIORef all_file_file_set

      listFolderFiles folder_tree
        & S.concatMapM (\tree_path -> do
            tree_digest <- t2d $ T.pack $ Path.fromRelFile $ tree_path
            pure $ catTree tree_digest
              & S.mapMaybe (either (const Nothing) (Just . file_sha))
          )
        & S.mapM (\file_digest -> do
            modifyIORef' traverse_set $ Set.delete (d2b file_digest)
            )
        & S.fold F.drain

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete file: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_file </> rel_dead_file])

    gc_chunk :: (MonadCatch m, MonadRepository m, MonadUnliftIO m) => m ()
    gc_chunk = do
      all_file_file_set <- listFolderFiles folder_chunk
        & fmap (UTF8.fromString . Path.fromRelFile)
        & S.fold F.toSet

      traverse_set <- newIORef all_file_file_set

      listFolderFiles folder_file
        & S.concatMapM (\tree_path -> do
            file_digest <- t2d $ T.pack $ Path.fromRelFile $ tree_path
            pure $ catFile file_digest
              & fmap chunk_name
          )
        & S.mapM (\file_digest -> do
            modifyIORef' traverse_set $ Set.delete (d2b file_digest)
            )
        & S.fold F.drain

      s <- readIORef traverse_set
      for_ s $ \dead_file -> tryIO $ do
        liftIO $ putStrLn $ "delete chunk: " <> show dead_file
        rel_dead_file <- Path.parseRelFile $ T.unpack $ TE.decodeUtf8 dead_file
        removeFiles ([folder_chunk </> rel_dead_file])

t2d :: MonadThrow m => T.Text -> m (Digest SHA256)
t2d sha = do
  sha_decoded <- case BS.decode $ TE.encodeUtf8 sha of
    Left err ->
      throwM $ userError $ "invalid sha: " <> T.unpack sha <> ", " <> err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString @SHA256 sha_decoded of
    Nothing -> throwM $ userError $ "invalid sha: " <> T.unpack sha
    Just digest -> pure digest

  pure digest

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
