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

module Better.Internal.Repository.LowLevel (
  -- * Write
  initRepositoryStructure,
  addBlob',
  addFile',
  addDir',
  addVersion,
  nextBackupVersionId,

  -- * Read
  catVersion,
  tryCatingVersion,
  catTree,
  catFile,
  catChunk,
  getChunkSize,
  listFolderFiles,
  listVersions,

  -- * Repositories
  localRepo,

  -- * Monad
  MonadRepository,
  TheMonadRepository (..),

  -- * Types
  Repository,
  module Better.Repository.Types,

  -- * Version
  Version (..),
  Tree (..),
  FFile (..),
  Object (..),

  -- * Constants
  folder_tree,
  folder_file,
  folder_chunk,
  folder_version,

  -- * Utils
  d2b,
  t2d,
  s2d,
) where

import Data.Function ( (&) )
import Data.Word ( Word8, Word32 )

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Data.Maybe (fromMaybe)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS16
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BSC

import qualified Data.ByteString.Lazy.Base16 as BL16

import qualified Data.ByteString.Short as BShort

import Text.Read (readMaybe)

import Control.Monad ( unless )
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow, handleIf, throwM)

import qualified Streamly.Data.Array as Array

import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.Unicode.Stream as US

import System.IO.Error (isDoesNotExistError)
import System.Posix.Types (FileOffset)

import qualified System.Posix.Directory as P
import qualified System.Posix.Files as P

import Path (Path, (</>))
import qualified Path

import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Internal.Data.Fold as F

import Control.Exception (mask_, onException)

import qualified Capability.Reader as C

import Better.Repository.Class ( MonadRepository(..) )
import qualified Better.Streamly.FileSystem.Dir as Dir

import Better.Repository.Types (Version (..))

import Better.Internal.Streamly.Crypto.AES (decryptCtr, that_aes)

import Better.Internal.Streamly.Array (ArrayBA (ArrayBA, un_array_ba), fastArrayAsPtrUnsafe)
import qualified Streamly.Internal.Data.Array.Type as Array
import System.IO (hPutBuf, openBinaryFile, IOMode (..), hClose)
import Better.Hash (Digest, digestFromByteString, digestToBase16ByteString)
import qualified System.Directory as D
import Control.Monad.IO.Class
import qualified Control.Monad.IO.Unlift as Un
import Unsafe.Coerce (unsafeCoerce)

data Repository = Repository
  { _repo_putFile :: Path Path.Rel Path.File -> F.Fold IO (Array.Array Word8) ()
  , _repo_removeFiles :: [Path Path.Rel Path.File] -> IO ()
  , -- TODO File mode?
    _repo_createDirectory :: Path Path.Rel Path.Dir -> IO ()
  , _repo_fileExists :: Path Path.Rel Path.File -> IO Bool
  , _repo_fileSize :: Path Path.Rel Path.File -> IO FileOffset
  , _repo_read :: Path Path.Rel Path.File -> S.Stream IO (Array.Array Word8)
  , _repo_listFolderFiles :: Path Path.Rel Path.Dir -> S.Stream IO (Path Path.Rel Path.File)
  }

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

data Object = Object
  { chunk_name :: {-# UNPACK #-} !Digest
  }
  deriving (Show)

newtype TheMonadRepository m a = TheMonadRepository (m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask)

instance (C.HasReader "repo" Repository m, MonadIO m) => MonadRepository (TheMonadRepository m) where
  {-# INLINE mkPutFileFold #-}
  mkPutFileFold = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_putFile
    pure $ F.morphInner liftIO . f

  {-# INLINE removeFiles #-}
  removeFiles files = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_removeFiles
    liftIO $ f files

  {-# INLINE createDirectory #-}
  createDirectory d = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_createDirectory
    liftIO $ f d

  {-# INLINE fileExists #-}
  fileExists file = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_fileExists
    liftIO $ f file

  {-# INLINE fileSize #-}
  fileSize file = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_fileSize
    liftIO $ f file

  {-# INLINE mkRead #-}
  mkRead = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_read
    pure $ S.morphInner liftIO . f

  {-# INLINE mkListFolderFiles #-}
  mkListFolderFiles = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_listFolderFiles
    pure $ S.morphInner liftIO . f

{-# INLINE listFolderFiles #-}
listFolderFiles
  :: (MonadIO m, MonadRepository m)
  => Path Path.Rel Path.Dir
  -> S.Stream m (Path Path.Rel Path.File)
listFolderFiles d = S.concatEffect $ do
  f <- mkListFolderFiles
  pure $! f d

{-# INLINE listVersions #-}
listVersions :: (MonadThrow m, MonadIO m, MonadRepository m) => S.Stream m Version
listVersions =
  listFolderFiles folder_version
    & S.mapM
      ( \v -> do
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
--

{-# INLINE write_chunk #-}
write_chunk :: FilePath -> F.Fold IO (Array.Array Word8) ()
write_chunk path = F.Fold step initial extract
  where
    {-# INLINE [0] initial #-}
    initial = mask_ $ do
      hd <- openBinaryFile path WriteMode
      pure $! F.Partial hd

    {-# INLINE [0] step #-}
    step hd arr = (`onException` (hClose hd)) $ do
      fastArrayAsPtrUnsafe arr $ \ptr -> hPutBuf hd ptr (Array.byteLength arr)
      pure $! F.Partial hd

    {-# INLINE [0] extract #-}
    extract hd = mask_ $ do
      hClose hd

localRepo :: Path Path.Abs Path.Dir -> Repository
localRepo root =
  Repository
    (write_chunk . Path.fromAbsFile . (root </>))
    (mapM_ $ (handleIf isDoesNotExistError (const $ pure ())) . D.removeFile . Path.fromAbsFile . (root </>))
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
addBlob'
  :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest
  -> S.Stream m (Array.Array Word8)
  -> m Word32
  -- ^ Bytes written
addBlob' digest chunks = do
  file_name' <- Path.parseRelFile $ show digest
  let f = folder_chunk </> file_name'
  exist <- fileExists f
  if exist
    then pure 0
    else do
      putFileFold <- mkPutFileFold
      ((), !len) <- chunks & S.fold (F.tee (putFileFold f) (fromIntegral <$> F.lmap Array.byteLength F.sum))
      pure len

{-# INLINE addFile' #-}
addFile'
  :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest
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
addDir'
  :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest
  -> S.Stream m (Array.Array Word8)
  -> m ()
addDir' digest chunks = do
  file_name' <- Path.parseRelFile $ show digest
  let f = folder_tree </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)

{-# INLINE addVersion #-}
addVersion
  :: (MonadIO m, MonadCatch m, MonadRepository m)
  => Integer
  -> Digest
  -> m ()
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

cat_stuff_under
  :: (Show a, MonadThrow m, MonadIO m, MonadRepository m)
  => Path Path.Rel Path.Dir
  -> a
  -> S.Stream m (Array.Array Word8)
cat_stuff_under folder stuff = S.concatEffect $ do
  read_blob <- mkRead
  liftIO $ do
    stuff_path <- Path.parseRelFile $ show stuff
    pure $ read_blob (folder </> stuff_path)
{-# INLINE cat_stuff_under #-}

catFile :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest -> S.Stream m Object
catFile sha =
  cat_stuff_under folder_file sha
    & US.decodeUtf8Chunks
    & US.lines (fmap T.pack F.toList)
    & S.mapM parse_file_content
  where
    parse_file_content :: (MonadThrow m, Applicative m) => T.Text -> m Object
    parse_file_content = fmap Object . t2d
{-# INLINE catFile #-}

catChunk
  :: (Un.MonadUnliftIO m, MonadThrow m, MonadIO m, MonadRepository m)
  => Digest
  -> S.Stream m (Array.Array Word8)
catChunk digest = S.concatEffect $ do
  aes <- liftIO that_aes
  Un.withRunInIO $ \unlift_io ->
    pure $
      cat_stuff_under folder_chunk digest
        & S.morphInner unlift_io
        & (fmap un_array_ba . decryptCtr aes (32 * 1024) . fmap ArrayBA)
        & S.morphInner liftIO
{-# INLINE catChunk #-}

getChunkSize :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest -> m FileOffset
getChunkSize sha = do
  sha_path <- Path.parseRelFile $ show sha
  fileSize $ folder_chunk </> sha_path
{-# INLINE getChunkSize #-}

catVersion :: (MonadThrow m, MonadIO m, MonadRepository m) => Integer -> m Version
catVersion vid = do
  optSha <-
    cat_stuff_under folder_version vid
      & fmap (BB.shortByteString . BShort.pack . Array.toList)
      & S.fold F.mconcat
      & fmap (BL16.decodeBase16Untyped . BB.toLazyByteString)

  sha <- case optSha of
    Left err -> throwM $ userError $ "invalid sha of version, " <> T.unpack err
    Right sha -> pure $ sha

  digest <- case digestFromByteString $ BS.toStrict sha of
    Nothing -> throwM $ userError "invalid sha of version"
    Just digest -> pure digest

  pure $ Version vid digest
{-# INLINE catVersion #-}

catTree :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest -> S.Stream m (Either Tree FFile)
catTree tree_sha' =
  cat_stuff_under folder_tree tree_sha'
    & US.decodeUtf8Chunks
    & US.lines (fmap T.pack F.toList)
    & S.mapM parse_tree_content
  where
    {-# INLINE [0] parse_tree_content #-}
    parse_tree_content :: MonadThrow m => T.Text -> m (Either Tree FFile)
    parse_tree_content buf = case T.splitOn " " buf of
      ["dir", name, sha] -> do
        digest <- t2d sha
        pure $ Left $ Tree name digest
      ["file", name, sha] -> do
        digest <- t2d sha
        pure $ Right $ FFile name digest
      _ -> throwM $ userError $ "invalid dir content: " <> T.unpack buf
{-# INLINE catTree #-}

{-# INLINEABLE t2d #-}
t2d :: MonadThrow m => T.Text -> m (Digest)
t2d sha = do
  sha_decoded <- case BS16.decodeBase16Untyped $ TE.encodeUtf8 sha of
    Left err ->
      throwM $ userError $ "invalid sha: " <> T.unpack sha <> ", " <> T.unpack err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString sha_decoded of
    Nothing -> throwM $ userError $ "invalid sha: " <> T.unpack sha
    Just digest -> pure digest

  pure digest

{-# INLINEABLE s2d #-}
s2d :: MonadThrow m => String -> m (Digest)
s2d sha = do
  sha_decoded <- case BS16.decodeBase16Untyped $ BSC.pack sha of
    Left err ->
      throwM $ userError $ "invalid sha: " <> sha <> ", " <> T.unpack err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString sha_decoded of
    Nothing -> throwM $ userError $ "invalid sha: " <> sha
    Just digest -> pure digest

  pure digest

d2b :: Digest -> BS.ByteString
d2b = digestToBase16ByteString
