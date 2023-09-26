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

module Better.Internal.Repository.LowLevel
   -- * Write
  ( initRepositoryStructure
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
  -- * Repositories
  , localRepo
  -- * Monad
  , MonadRepository
  , TheMonadRepository(..)
  -- * Types
  , Repository
  , module Better.Repository.Types

  -- * Version
  , Version(..)

  , Tree(..)
  , FFile(..)
  , Object(..)

  -- * Constants
  , folder_tree
  , folder_file
  , folder_chunk
  , folder_version

  -- * Utils
  , d2b
  , t2d
  , s2d
  ) where

import Prelude hiding (read)

import Data.Word
import Data.Function
import Data.Foldable
import Data.Maybe (fromMaybe, isJust, fromJust)

import qualified Data.Set as Set

import UnliftIO
import qualified UnliftIO.Directory as Un
import qualified UnliftIO.STM as Un

import qualified Data.Text as T
import qualified Data.Text.Encoding.Base16 as T16
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Base16 as BS16
import Data.ByteString.Internal (c2w)
import qualified Data.ByteString.Builder as BB

import qualified Data.ByteString.Lazy.Base16 as BL16

import qualified Data.ByteString.Short as BShort
import qualified Data.ByteString.Short.Base16 as BShort16

import qualified Data.ByteString.UTF8 as UTF8

import Text.Read (readMaybe)

import Control.Monad
import Control.Monad.Catch (MonadCatch, MonadThrow, MonadMask, throwM, handleIf)

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe, unsafeFreeze)

import qualified Streamly.FileSystem.File as File
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
import qualified Streamly.Data.Unfold as U

import qualified Capability.Reader as C

import Better.Hash
import Better.Repository.Class
import qualified Better.Streamly.FileSystem.Dir as Dir

import Better.Repository.Types (Version(..))

import Data.ByteArray (ByteArrayAccess(..))
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteArray as BA
import Better.Internal.Streamly.Crypto.AES (decryptCtr, that_aes)

import qualified Crypto.Cipher.AES as Cipher
import qualified Crypto.Cipher.Types as Cipher
import Crypto.Error (CryptoFailable (CryptoFailed, CryptoPassed))
import qualified Crypto.Hash.Algorithms as Hash
import qualified Crypto.KDF.PBKDF2 as PBKDF2
import qualified Control.Monad.IO.Unlift as S
import qualified Control.Monad.IO.Unlift as Un
import qualified Streamly.Internal.Data.Array.Mut.Type as MA

pass :: BS.ByteString
pass = "passwordd"

salt :: BS.ByteString
salt = "rJ,!)-w\253\213\\"

pbkdf :: BS.ByteString
pbkdf = PBKDF2.generate (PBKDF2.prfHMAC Hash.SHA256) param pass salt
  where
    param =
      PBKDF2.Parameters
        { PBKDF2.outputLength = Cipher.blockSize (undefined :: Cipher.AES128)
        , PBKDF2.iterCounts = 4000
        }

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

{-# INLINE listFolderFiles #-}
listFolderFiles :: (MonadIO m, MonadRepository m)
                => Path Path.Rel Path.Dir -> S.Stream m (Path Path.Rel Path.File)
listFolderFiles d = S.concatEffect $ do
  f <- mkListFolderFiles
  pure $! f d

read :: (MonadIO m, MonadRepository m)
                => Path Path.Rel Path.File -> S.Stream m (Array.Array Word8)
read p = S.concatEffect $ do
  f <- mkRead
  pure $ f p

{-# INLINE listVersions #-}
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
addBlob'
  :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Digest SHA256
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
      fmap snd $ chunks & S.fold (F.tee (putFileFold f) (F.lmap (fromIntegral . Array.length) F.sum))

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

{-# INLINE addVersion #-}
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
  & US.decodeUtf8Chunks
  & US.lines (fmap T.pack F.toList)
  & S.mapM parse_file_content
  where
    parse_file_content :: (MonadThrow m, Applicative m) => T.Text -> m Object
    parse_file_content = fmap Object . t2d
{-# INLINE catFile #-}

instance BA.ByteArray (ArrayBA Word8) where
  allocRet n f = do
    ma <- MA.newPinned @_ @Word8 n
    MA.asPtrUnsafe (MA.castUnsafe ma) $ \p -> do
      ret <- f p
      pure (ret, ArrayBA $ Array.unsafeFreeze $ ma { MA.arrEnd = MA.arrBound ma })

catChunk :: (MonadUnliftIO m, MonadThrow m, MonadIO m, MonadRepository m)
         => Digest SHA256 -> S.Stream m (Array.Array Word8)
catChunk digest = S.concatEffect $ do
  aes <- liftIO that_aes
  withRunInIO $ \unlift_io -> pure $
    cat_stuff_under folder_chunk digest
      & S.morphInner unlift_io
      & (fmap un_array_ba . decryptCtr aes (32 * 1024) . fmap ArrayBA)
      & S.morphInner liftIO
{-# INLINE catChunk #-}

getChunkSize :: (MonadThrow m, MonadIO m, MonadRepository m) => Digest SHA256 -> m FileOffset
getChunkSize sha = do
  sha_path <- Path.parseRelFile $ show sha
  fileSize $ folder_chunk </> sha_path
{-# INLINE getChunkSize #-}

catVersion :: (MonadThrow m, MonadIO m, MonadRepository m) => Integer -> m Version
catVersion vid = do
  optSha <- cat_stuff_under folder_version vid
    & fmap (BB.shortByteString . BShort.pack . Array.toList)
    & S.fold F.mconcat
    & fmap (BL16.decodeBase16Untyped . BB.toLazyByteString)

  sha <- case optSha of
    Left err -> throwM $ userError $ "invalid sha of version, " <> T.unpack err
    Right sha -> pure $ sha

  digest <- case digestFromByteString @SHA256 $ BS.toStrict sha of
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
    {-# INLINE[0] parse_tree_content #-}
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

newtype ArrayBA a = ArrayBA { un_array_ba :: Array.Array a }
  deriving (Eq, Ord, Monoid, Semigroup)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp

checksum :: (MonadThrow m, MonadUnliftIO m, MonadRepository m) => Int -> m ()
checksum n = do
  S.fromList [folder_tree, folder_file]
    & S.concatMap (\p ->
      listFolderFiles p
        & fmap (\f -> (Path.fromRelFile f, p </> f))
    )
    & S.parMapM (S.maxBuffer (n + 1) . S.eager True) (\(expected_sha, f) -> do
         actual_sha <- read f
           & S.fold hashArrayFoldIO
         if show actual_sha == expected_sha
           then pure Nothing
           else pure $ Just (f, actual_sha)
       )
    & S.filter (isJust)
    & fmap (fromJust)
    & S.fold (F.foldMapM $ \(invalid_f, expect_sha) ->
        liftIO $ T.putStrLn $ "invalid file: " <> T.pack (Path.fromRelFile invalid_f) <> ", " <> T.pack (show expect_sha))

  listFolderFiles folder_chunk
    & S.parMapM (S.maxBuffer (n + 1) . S.eager True) (\chunk_path -> do
         expected_sha <- s2d $ Path.fromRelFile chunk_path
         actual_sha <- catChunk expected_sha & S.fold hashArrayFoldIO
         if expected_sha == actual_sha
           then pure Nothing
           else
             let
               !expected_sha_str = T.pack $ show expected_sha
               !actual_sha_str = T.pack $ show actual_sha
             in
               pure $ Just (expected_sha_str, actual_sha_str)
       )
    & S.filter (isJust)
    & fmap (fromJust)
    & S.fold (F.foldMapM $ \(expected_sha, actual_sha) ->
        liftIO $ T.putStrLn $ "invalid file: " <> actual_sha <> ", checksum: " <> expected_sha)
{-# INLINE checksum #-}

{-# INLINEABLE t2d #-}
t2d :: MonadThrow m => T.Text -> m (Digest SHA256)
t2d sha = do
  sha_decoded <- case BS16.decodeBase16Untyped $ TE.encodeUtf8 sha of
    Left err ->
      throwM $ userError $ "invalid sha: " <> T.unpack sha <> ", " <> T.unpack err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString @SHA256 sha_decoded of
    Nothing -> throwM $ userError $ "invalid sha: " <> T.unpack sha
    Just digest -> pure digest

  pure digest

{-# INLINEABLE s2d #-}
s2d :: MonadThrow m => String -> m (Digest SHA256)
s2d sha = do
  sha_decoded <- case BS16.decodeBase16Untyped $ BSC.pack sha of
    Left err ->
      throwM $ userError $ "invalid sha: " <> sha <> ", " <> T.unpack err
    Right sha' -> pure $ sha'

  digest <- case digestFromByteString @SHA256 sha_decoded of
    Nothing -> throwM $ userError $ "invalid sha: " <> sha
    Just digest -> pure digest

  pure digest

d2b :: Digest SHA256 -> BS.ByteString
d2b = BA.convertToBase BA.Base16
