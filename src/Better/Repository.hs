{-# LANGUAGE Strict #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

module Better.Repository
   -- * Write
  ( initRepositoryStructure
  , backup
  , addBlob
  , addBlob'
  , addFile
  , addFile'
  , addDir
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
  , Version(..)

  -- * Version
  , Version
  , ver_id
  , ver_root

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

import Data.Set (Set)
import qualified Data.Set as Set

import UnliftIO
import qualified UnliftIO.IO.File as Un
import qualified UnliftIO.Directory as Un

import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL

import qualified Data.ByteString.UTF8 as UTF8

import Text.Read (readMaybe)

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Catch (MonadCatch, MonadThrow, MonadMask, throwM, handleIf)

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe) 

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Unfold as U
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

import System.IO.Error (isDoesNotExistError)
import System.Posix.Types (FileOffset(..))

import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P
import qualified System.Posix.Temp as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash

import GHC.Generics

import qualified Streamly.Data.Array as Array

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import qualified Capability.Reader as C
import qualified Capability.Source as C

import qualified Path
import Path (Path)

import Data.Word
import Data.Coerce (coerce)

import Control.Monad.IO.Class
import Data.ByteArray (ByteArrayAccess(..))

import Better.Hash
import Better.TempDir
import Better.Repository.Class
import qualified Better.Streamly.FileSystem.Dir as Dir

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
  { ver_id :: {-# UNPACK #-} Integer
  , ver_root :: {-# UNPACK #-} Digest SHA256
  }
  deriving (Show)

data Tree
  = Tree 
  { tree_name :: {-# UNPACK #-} T.Text
  , tree_sha :: {-# UNPACK #-} Digest SHA256
  }
  deriving (Show)

data FFile
  = FFile
  { file_name :: {-# UNPACK #-} T.Text
  , file_sha :: {-# UNPACK #-} Digest SHA256
  }
  deriving (Show)

data Object
  = Object
  { chunk_name :: {-# UNPACK #-} Digest SHA256
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

listVersions :: (MonadThrow m, MonadIO m, MonadRepository m)
	      => S.Stream m Version
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

put_file_in_repo :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Path Path.Rel Path.Dir
  -> S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
put_file_in_repo subdir chunks = do
  chunk_hash <- chunks & S.fold hashArrayFold
  file_name <- Path.parseRelFile $ show chunk_hash
  let f = subdir </> file_name
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)
  pure chunk_hash 
{-# INLINE put_file_in_repo #-}

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
addBlob' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m ()
addBlob' digest chunks = do
  file_name <- Path.parseRelFile $ show digest
  let f = folder_chunk </> file_name
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

addFile' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m ()
addFile' digest chunks = do
  file_name <- Path.parseRelFile $ show digest
  let f = folder_file </> file_name
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

addDir' :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
  -> S.Stream m (Array.Array Word8)
  -> m ()
addDir' digest chunks = do
  file_name <- Path.parseRelFile $ show digest
  let f = folder_tree </> file_name
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

addBlob :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addBlob = put_file_in_repo folder_chunk
{-# INLINE addBlob #-}

addFile :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addFile = put_file_in_repo folder_file
{-# INLINE addFile #-}

addDir :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addDir = put_file_in_repo folder_tree
{-# INLINE addDir #-}

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
  stuff_path <- Path.parseRelFile $ show stuff
  read_blob <- mkRead
  pure $ read_blob (folder </> stuff_path)
{-# INLINE cat_stuff_under #-}

catFile :: (MonadThrow m, MonadIO m, MonadRepository m)
	=> Digest SHA256 -> S.Stream m Object
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
catChunk sha = cat_stuff_under folder_chunk sha
{-# INLINE catChunk #-}

getChunkSize :: (MonadThrow m, MonadIO m, MonadRepository m)
	=> Digest SHA256 -> m FileOffset
getChunkSize sha = do
  sha_path <- Path.parseRelFile $ show sha
  fileSize $ folder_chunk </> sha_path
{-# INLINE getChunkSize #-}

catVersion :: (MonadThrow m, MonadIO m, MonadRepository m)
	=> Integer -> m Version
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

catTree :: (MonadThrow m, MonadIO m, MonadRepository m)
	=> Digest SHA256 -> S.Stream m (Either Tree FFile)
catTree sha = cat_stuff_under folder_tree sha
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

tree_content :: Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir) -> Digest SHA256 -> BS.ByteString
tree_content file_or_dir hash' =
  let
    name = either file_name' dir_name' file_or_dir
    t = either (const "file") (const "dir") file_or_dir
  in BS.concat [t, BS.singleton 0x20, name, BS.singleton 0x20, d2b hash', BS.singleton 0x0a]
  where
    file_name' :: Path r Path.File -> BS.ByteString
    file_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . Path.toFilePath . Path.filename
    {-# INLINE file_name' #-}

    dir_name' :: Path r Path.Dir -> BS.ByteString
    dir_name' = BS.toStrict . BB.toLazyByteString . BB.stringUtf8 . init . Path.toFilePath . Path.dirname
    {-# INLINE dir_name' #-}

h_dir :: (MonadRepository m, MonadTmp m, MonadCatch m, MonadIO m, MonadUnliftIO m) => TBQueue (m ()) -> Path Path.Rel Path.Dir -> m (Digest SHA256)
h_dir tbq rel_tree_name = withEmptyTmpFile $ \file_name' -> do
  (dir_hash, ()) <- Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    Dir.readEither rel_tree_name
      & S.parMapM (S.ordered True . S.eager True . S.maxBuffer 2) (\fod ->do
          hash <- either (h_file tbq . (rel_tree_name </>)) (h_dir tbq . (rel_tree_name </>)) fod
          pure $! tree_content fod hash
        )
      & S.fold (F.tee hashByteStringFold (F.drainMapM $ liftIO . BC.hPutStr fd))

  atomically $ writeTBQueue tbq $ do
    addDir' dir_hash (File.readChunks (Path.fromAbsFile file_name'))
    `finally`
      Un.removeFile (Path.fromAbsFile file_name')

  pure dir_hash 

h_file :: (MonadRepository m, MonadTmp m, MonadCatch m, MonadIO m, MonadUnliftIO m) => TBQueue (m ()) -> Path Path.Rel Path.File -> m (Digest SHA256)
h_file tbq rel_file_name = withEmptyTmpFile $ \file_name' -> do
  (file_hash, ()) <- Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    File.readChunksWith (1024 * 1024) (Path.fromRelFile rel_file_name)
      & S.filter ((0 /=) . Array.length)
      & S.parMapM (S.ordered True . S.eager True . S.maxBuffer 5) (\chunk -> do
        chunk_hash <- S.fromPure chunk & S.fold hashArrayFold
        atomically $ writeTBQueue tbq $ do
          addBlob' chunk_hash (S.fromPure chunk)
        pure $! d2b chunk_hash `BS.snoc` 0x0a
      )
      & S.fold (F.tee hashByteStringFold (F.drainMapM $ liftIO . BC.hPutStr fd))

  atomically $ writeTBQueue tbq $ do
    addFile' file_hash (File.readChunks (Path.fromAbsFile file_name'))
    `finally`
      Un.removeFile (Path.fromAbsFile file_name')

  pure file_hash

backup :: (MonadRepository m, MonadTmp m, MonadCatch m, MonadIO m, MonadUnliftIO m)
       => T.Text -> m Version
backup dir = do
  rel_dir <- Path.parseRelDir $ T.unpack dir
  (root_hash, _) <- withEmitUnfoldr 50 (\tbq -> h_dir tbq rel_dir)
    $ (\s -> s 
         & S.parSequence (S.maxBuffer 50 . S.eager True)
         & S.fold F.drain
      )
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
{-# INLINE d2b #-}

withEmitUnfoldr :: MonadUnliftIO m => Natural -> (TBQueue e -> m a) -> (S.Stream m e -> m b) -> m (a, b)
withEmitUnfoldr q_size putter go = do
  tbq <- newTBQueueIO q_size

  withAsync (putter tbq) $ \h -> do
    let 
      f = do
        e <- atomically $ (Just <$> readTBQueue tbq) <|> (Nothing <$ waitSTM h)
        case e of
          Just v -> pure (Just (v, ()))
	  Nothing -> pure Nothing

    link h
    ret_b <- go $ S.unfoldrM (\() -> f) ()
    ret_a <- wait h
    pure (ret_a, ret_b)
