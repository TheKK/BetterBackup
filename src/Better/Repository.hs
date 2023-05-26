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
  , catTree
  , catFile
  , catChunk
  , listFolderFiles
  , listVersions
  , checksum
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

import Data.Word
import Data.Function
import Data.Foldable
import Data.Maybe (fromMaybe, isJust, fromJust)

import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL

import Text.Read (readMaybe)

import Control.Monad
import Control.Monad.Reader
import Control.Monad.Catch
import Control.Monad.Trans.Control (MonadBaseControl)

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe) 

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Unfold as U
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

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
import Better.Repository.Class
import qualified Better.Streamly.FileSystem.Dir as Dir

data Repository = Repository
   { _repo_putFile :: Path Path.Rel Path.File -> F.Fold IO (Array.Array Word8) ()
   -- TODO File mode?
   , _repo_createDirectory :: Path Path.Rel Path.Dir -> IO () 
   , _repo_fileExists :: Path Path.Rel Path.File -> IO Bool
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
  , tree_sha :: {-# UNPACK #-} T.Text -- TODO To Digest
  }
  deriving (Show)

data FFile
  = FFile
  { file_name :: {-# UNPACK #-} T.Text
  , file_sha :: {-# UNPACK #-} T.Text -- TODO To Digest
  }
  deriving (Show)

data Object
  = Object
  { object_name :: {-# UNPACK #-} T.Text -- TODO To Digest
  }
  deriving (Show)

newtype TheMonadRepository m a = TheMonadRepository (m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask)

instance (C.HasReader "repo" Repository m, MonadIO m) => MonadRepository (TheMonadRepository m) where
  mkPutFileFold = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_putFile
    pure $ F.morphInner liftIO . f

  createDirectory d = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_createDirectory
    liftIO $ f d

  fileExists file = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_fileExists
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
  -- (tmp_name, tmp_fd) <- P.mkstemp (Path.fromAbsDir $ hbk_path </> [Path.reldir|version|])
  --hClose tmp_fd
  (File.writeChunks . Path.fromAbsFile . (root </>))
  (flip P.createDirectory 700 . Path.fromAbsDir . (root </>))
  (P.fileExist . Path.fromAbsFile . (root </>))
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
    parse_file_content :: Applicative m => T.Text -> m Object
    parse_file_content = pure . Object
{-# INLINE catFile #-}

catChunk :: (MonadThrow m, MonadIO m, MonadRepository m)
	=> Digest SHA256 -> S.Stream m (Array.Array Word8)
catChunk sha = cat_stuff_under folder_chunk sha
{-# INLINE catChunk #-}

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
  & S.unfoldMany Array.reader
  & US.decodeLatin1
  & US.lines (fmap T.pack F.toList)
  & S.mapM parse_tree_content
  where
    parse_tree_content :: MonadThrow m => T.Text -> m (Either Tree FFile)
    parse_tree_content buf = case T.splitOn " " buf of
      ["dir", name, sha] -> pure $ Left $ Tree name sha
      ["file", name, sha] -> pure $ Right $ FFile name sha
      _ -> throwM $ userError $ "invalid dir content: " <> T.unpack buf


newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp

checksum :: (MonadIO m, MonadCatch m, MonadRepository m, MonadBaseControl IO m) => Int -> m ()
checksum n = do
  S.fromList [folder_tree, folder_file, folder_chunk] 
    & S.concatMap (\p ->
      listFolderFiles p
        & fmap (\f -> (Path.fromRelFile f, p </> f))
    )
    & S.parMapM (S.maxBuffer 50 . S.maxThreads n . S.eager True) (\(expected_sha, f) -> do
         actual_sha <- read f
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
