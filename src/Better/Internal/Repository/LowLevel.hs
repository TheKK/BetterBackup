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
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Internal.Repository.LowLevel (
  -- * Effectful
  runRepository,

  -- * Write
  removeFiles,
  addBlob',
  addFile',
  addDir',
  addVersion,
  nextBackupVersionId,

  -- * Read
  read,
  catVersion,
  tryCatingVersion,
  catTree,
  catFile,
  catChunk,
  getChunkSize,
  listFolderFiles,
  listVersions,
  doesVersionExists,
  doesTreeExists,
  doesFileExists,
  doesChunkExists,

  -- * Repositories
  localRepo,

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

import Prelude hiding (read)

import Data.Function ((&))
import Data.Word (Word32, Word8)

import Data.Text qualified as T
import Data.Text.Encoding qualified as TE

import Data.Maybe (fromMaybe)

import Data.Base16.Types qualified as Base16

import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as BS16
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Lazy.Base16 qualified as BL16
import Data.ByteString.Short qualified as BShort
import Data.ByteString.Short.Base16 qualified as BSS16

import Codec.Binary.UTF8.String qualified as UTF8

import Data.Binary qualified as Bin

import Data.ByteArray qualified as BA

import Text.Read (readMaybe)

import Control.Monad (unless, (<=<))
import Control.Monad.Catch (MonadThrow, handleIf, throwM)

import Streamly.Data.Array qualified as Array

import Streamly.FileSystem.File qualified as File
import Streamly.Internal.Unicode.Stream qualified as US

import System.IO (IOMode (..), hClose, hPutBuf, openBinaryFile)
import System.IO.Error (isDoesNotExistError)
import System.IO.Unsafe (unsafePerformIO)

import System.Directory qualified as D

import System.Posix.Directory qualified as P
import System.Posix.Files qualified as P
import System.Posix.Types (FileOffset)

import Path (Path, (</>))
import Path qualified

import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S
import Streamly.Internal.Data.Array.Type qualified as Array
import Streamly.Internal.Data.Fold qualified as F

import Control.Exception (mask_, onException)

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

import Better.Hash (ChunkDigest (..), Digest, FileDigest (..), TreeDigest (UnsafeMkTreeDigest), VersionDigest (UnsafeMkVersionDigest), digestFromByteString, digestToBase16ByteString, digestToBase16ShortByteString, hashByteStringFoldIO)
import Better.Internal.Streamly.Array (ArrayBA (ArrayBA, un_array_ba), fastArrayAsPtrUnsafe)
import Better.Internal.Streamly.Array qualified as BetterArray
import Better.Internal.Streamly.Crypto.AES (decryptCtr, that_aes)
import Better.Repository.Class qualified as E
import Better.Repository.Types (Version (..))
import Better.Streamly.FileSystem.Dir qualified as Dir
import Data.Coerce (coerce)

data Repository = Repository
  { _repo_putFile :: (Path Path.Rel Path.File -> F.Fold IO (Array.Array Word8) ())
  , _repo_removeFiles :: ([Path Path.Rel Path.File] -> IO ())
  , -- TODO File mode?
    _repo_createDirectory :: (Path Path.Rel Path.Dir -> IO ())
  , _repo_fileExists :: (Path Path.Rel Path.File -> IO Bool)
  , _repo_fileSize :: (Path Path.Rel Path.File -> IO FileOffset)
  , _repo_read :: (Path Path.Rel Path.File -> S.Stream IO (Array.Array Word8))
  , _repo_listFolderFiles :: (Path Path.Rel Path.Dir -> S.Stream IO (Path Path.Rel Path.File))
  }

data Tree = Tree
  { tree_name :: {-# UNPACK #-} !T.Text
  , tree_sha :: {-# UNPACK #-} !TreeDigest
  }
  deriving (Show)

data FFile = FFile
  { file_name :: {-# UNPACK #-} !T.Text
  , file_sha :: {-# UNPACK #-} !FileDigest
  }
  deriving (Show)

data Object = Object
  { chunk_name :: {-# UNPACK #-} !ChunkDigest
  }
  deriving (Show)

{-# INLINE listFolderFiles #-}
listFolderFiles
  :: (E.Repository E.:> es)
  => Path Path.Rel Path.Dir
  -> S.Stream (E.Eff es) (Path Path.Rel Path.File)
listFolderFiles = mkListFolderFiles

listVersions :: (E.Repository E.:> es) => S.Stream (E.Eff es) (VersionDigest, Version)
listVersions =
  listFolderFiles folder_version
    -- TODO Maybe we could skip invalid version files.
    & S.mapM
      ( \version_file_path -> do
          !version_digest <- fmap UnsafeMkVersionDigest <$> s2d $ Path.fromRelFile version_file_path
          !version <- catVersion version_digest
          pure (version_digest, version)
      )

does_that_exist :: (E.Repository E.:> es) => Path Path.Rel Path.Dir -> Digest -> E.Eff es Bool
does_that_exist that_folder digest = do
  path_digest <- Path.parseRelFile $ show digest
  exists <- fileExists $ that_folder </> path_digest
  pure $! exists

doesVersionExists :: (E.Repository E.:> es) => VersionDigest -> E.Eff es Bool
doesVersionExists = does_that_exist folder_version . coerce

doesTreeExists :: (E.Repository E.:> es) => TreeDigest -> E.Eff es Bool
doesTreeExists = does_that_exist folder_tree . coerce

doesFileExists :: (E.Repository E.:> es) => FileDigest -> E.Eff es Bool
doesFileExists = does_that_exist folder_file . coerce

doesChunkExists :: (E.Repository E.:> es) => ChunkDigest -> E.Eff es Bool
doesChunkExists = does_that_exist folder_chunk . coerce

tryCatingVersion :: (E.Repository E.:> es) => VersionDigest -> E.Eff es (Maybe Version)
tryCatingVersion digest = do
  path_digest <- Path.parseRelFile $ show digest
  -- TODO using fileExists could cause issue of TOCTOU,
  -- but trying to get "file does not exist" info from catVersion require all Repository throws
  -- specific exception, which is also hard to achieve.
  exists <- fileExists $ folder_version </> path_digest
  if exists
    then Just <$> catVersion digest
    else pure Nothing

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
    local_write
    local_remove_file
    (flip P.createDirectory 700 . Path.fromAbsDir . (root </>))
    local_exist
    (fmap P.fileSize . P.getFileStatus . Path.fromAbsFile . (root </>))
    (BetterArray.readChunks . Path.fromAbsFile . (root </>))
    (S.mapM Path.parseRelFile . Dir.read . (root </>))
  where
    {-# NOINLINE local_write #-}
    local_write = write_chunk . Path.fromAbsFile . (root </>)

    {-# NOINLINE local_remove_file #-}
    local_remove_file = mapM_ $ (handleIf isDoesNotExistError (const $ pure ())) . D.removeFile . Path.fromAbsFile . (root </>)

    {-# NOINLINE local_exist #-}
    local_exist = P.fileExist . Path.fromAbsFile . (root </>)

newtype instance E.StaticRep E.Repository = RepositoryRep Repository

runRepository :: (E.IOE E.:> es, E.IOE E.:> es) => Repository -> E.Eff (E.Repository : es) a -> E.Eff es a
runRepository = E.evalStaticRep . RepositoryRep

mkPutFileFold :: (E.Repository E.:> es) => E.Eff es (Path Path.Rel Path.File -> F.Fold (E.Eff es) (Array.Array Word8) ())
mkPutFileFold = do
  RepositoryRep repo <- E.getStaticRep
  pure $! F.morphInner E.unsafeEff_ . _repo_putFile repo

removeFiles :: (E.Repository E.:> es) => [Path Path.Rel Path.File] -> E.Eff es ()
removeFiles files = do
  RepositoryRep repo <- E.getStaticRep
  E.unsafeEff_ $ _repo_removeFiles repo files

createDirectory :: (E.Repository E.:> es) => Path Path.Rel Path.Dir -> E.Eff es ()
createDirectory d = do
  RepositoryRep repo <- E.getStaticRep
  E.unsafeEff_ $ _repo_createDirectory repo d

fileExists :: (E.Repository E.:> es) => Path Path.Rel Path.File -> E.Eff es Bool
fileExists d = do
  RepositoryRep repo <- E.getStaticRep
  E.unsafeEff_ $ _repo_fileExists repo d

fileSize :: (E.Repository E.:> es) => Path Path.Rel Path.File -> E.Eff es FileOffset
fileSize d = do
  RepositoryRep repo <- E.getStaticRep
  E.unsafeEff_ $ _repo_fileSize repo d

{-# INLINE read #-}
read :: (E.Repository E.:> es) => Path Path.Rel Path.File -> S.Stream (E.Eff es) (Array.Array Word8)
read f = S.concatEffect $ do
  RepositoryRep repo <- E.getStaticRep
  pure $! S.morphInner E.unsafeEff_ $ _repo_read repo f

{-# INLINE mkListFolderFiles #-}
mkListFolderFiles :: (E.Repository E.:> es) => Path Path.Rel Path.Dir -> S.Stream (E.Eff es) (Path Path.Rel Path.File)
mkListFolderFiles d = S.concatEffect $ do
  RepositoryRep repo <- E.getStaticRep
  pure $! S.morphInner E.unsafeEff_ . _repo_listFolderFiles repo $ d

folder_chunk :: Path Path.Rel Path.Dir
folder_chunk = [Path.reldir|chunk|]

folder_file :: Path Path.Rel Path.Dir
folder_file = [Path.reldir|file|]

folder_tree :: Path Path.Rel Path.Dir
folder_tree = [Path.reldir|tree|]

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]

addBlob'
  :: (E.Repository E.:> es)
  => Digest
  -> S.Stream (E.Eff es) (Array.Array Word8)
  -> E.Eff es Word32
  -- ^ Bytes written
addBlob' digest chunks = do
  file_name' <- Path.parseRelFile $ digest_to_base16_filepath digest
  let f = folder_chunk </> file_name'
  exist <- fileExists f
  if exist
    then pure 0
    else do
      putFileFold <- mkPutFileFold
      ((), !len) <- chunks & S.fold (F.tee (putFileFold f) (fromIntegral <$> F.lmap Array.byteLength F.sum))
      pure len

addFile'
  :: (E.Repository E.:> es)
  => Digest
  -> S.Stream (E.Eff es) (Array.Array Word8)
  -> E.Eff es Bool
  -- ^ Dir added.
addFile' digest chunks = do
  file_name' <- Path.parseRelFile $ digest_to_base16_filepath digest
  let f = folder_file </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)
  pure $! not exist

addDir'
  :: (E.Repository E.:> es)
  => Digest
  -> S.Stream (E.Eff es) (Array.Array Word8)
  -> E.Eff es Bool
  -- ^ Dir added.
addDir' digest chunks = do
  file_name' <- Path.parseRelFile $ digest_to_base16_filepath digest
  let f = folder_tree </> file_name'
  exist <- fileExists f
  unless exist $ do
    putFileFold <- mkPutFileFold
    chunks & S.fold (putFileFold f)
  pure $! not exist

addVersion
  :: (E.Repository E.:> es)
  => Version
  -> E.Eff es ()
addVersion v = do
  -- It should be safe to store entire version bytes here since Version is relative small (under hundred of bytes)
  let version_bytes = BS.toStrict $ Bin.encode v
  version_digest <- E.unsafeEff_ $ S.fromPure version_bytes & S.fold hashByteStringFoldIO
  version_digest_filename <- Path.parseRelFile $ show version_digest
  let f = folder_version </> version_digest_filename
  putFileFold <- mkPutFileFold
  S.fromPure (BetterArray.un_array_ba $ BA.convert version_bytes)
    & S.fold (putFileFold f)

nextBackupVersionId :: (E.Repository E.:> es) => E.Eff es Integer
nextBackupVersionId = do
  listFolderFiles folder_version
    & S.mapMaybe (readMaybe @Integer . Path.fromRelFile)
    & S.fold F.maximum
    & fmap (succ . fromMaybe 0)

cat_stuff_under
  :: (Show a, E.Repository E.:> es)
  => Path Path.Rel Path.Dir
  -> a
  -> S.Stream (E.Eff es) (Array.Array Word8)
cat_stuff_under folder stuff = S.concatEffect $ do
  stuff_path <- E.unsafeEff_ $ Path.parseRelFile $ show stuff
  pure $! read (folder </> stuff_path)
{-# INLINE cat_stuff_under #-}

catFile :: (E.Repository E.:> es) => FileDigest -> S.Stream (E.Eff es) Object
catFile sha = S.concatEffect $ E.reallyUnsafeUnliftIO $ \un -> do
  pure $!
    cat_stuff_under folder_file sha
      & S.morphInner un
      & US.decodeUtf8Chunks
      & US.lines (fmap T.pack F.toList)
      & S.mapM parse_file_content
      & S.morphInner E.unsafeEff_
  where
    parse_file_content :: (MonadThrow m, Applicative m) => T.Text -> m Object
    parse_file_content = fmap (Object . UnsafeMkChunkDigest) . t2d

catChunk
  :: (E.Repository E.:> es)
  => ChunkDigest
  -> S.Stream (E.Eff es) (Array.Array Word8)
catChunk digest = S.concatEffect $ E.reallyUnsafeUnliftIO $ \un -> do
  pure $!
    cat_stuff_under folder_chunk digest
      & S.morphInner un
      & decryptCtr aes (32 * 1024)
      & S.morphInner E.unsafeEff_
{-# INLINE catChunk #-}

{-# NOINLINE aes #-}
-- TODO Move this to EffectRep.
aes = unsafePerformIO that_aes

getChunkSize :: (E.Repository E.:> es) => ChunkDigest -> E.Eff es FileOffset
getChunkSize sha = do
  sha_path <- E.unsafeEff_ $ Path.parseRelFile $ show sha
  fileSize $ folder_chunk </> sha_path

catVersion :: (E.Repository E.:> es) => VersionDigest -> E.Eff es Version
catVersion digest = do
  decode_result <-
    cat_stuff_under folder_version digest
      & fmap (BB.shortByteString . BShort.pack . Array.toList)
      & S.fold F.mconcat
      & fmap (Bin.decodeOrFail @Version . BB.toLazyByteString)

  case decode_result of
    Left (_, _, err_msg) -> throwM $ userError $ "failed to decode version " <> show digest <> ": " <> err_msg
    Right (remain_bytes, _, v) -> do
      unless (BL.null remain_bytes) $ throwM $ userError $ "failed to decode version " <> show digest <> ": there're remaining bytes"
      pure v

catTree :: (E.Repository E.:> es) => TreeDigest -> S.Stream (E.Eff es) (Either Tree FFile)
catTree tree_sha' = S.concatEffect $ E.reallyUnsafeUnliftIO $ \un -> do
  pure $!
    cat_stuff_under folder_tree tree_sha'
      & S.morphInner un
      & US.decodeUtf8Chunks
      & US.lines (fmap T.pack F.toList)
      & S.mapM parse_tree_content
      & S.morphInner E.unsafeEff_
  where
    {-# INLINE [0] parse_tree_content #-}
    parse_tree_content :: MonadThrow m => T.Text -> m (Either Tree FFile)
    parse_tree_content buf = case T.splitOn " " buf of
      ["dir", name, sha] -> do
        digest <- UnsafeMkTreeDigest <$> t2d sha
        pure $ Left $ Tree name digest
      ["file", name, sha] -> do
        digest <- UnsafeMkFileDigest <$> t2d sha
        pure $ Right $ FFile name digest
      _ -> throwM $ userError $ "invalid dir content: " <> T.unpack buf

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

digest_to_base16_filepath :: Digest -> FilePath
digest_to_base16_filepath = map (toEnum . fromIntegral) . BShort.unpack . digestToBase16ShortByteString

d2b :: Digest -> BS.ByteString
d2b = digestToBase16ByteString
