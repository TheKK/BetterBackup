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

  -- * Getter
  getAES,

  -- * Write
  createDirectory,
  removeFiles,
  addChunk',
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

  -- * Path of item
  pathOfVersion,
  pathOfTree,
  pathOfFile,
  pathOfChunk,

  -- ** Item traversal
  listVersionDigests,
  listTreeDigests,
  listFileDigests,
  listChunkDigests,
  listVersions,

  -- * Utils
  d2b,
  t2d,
  s2d,
  initRepositoryStructure,
) where

import Better.Hash (
  ChunkDigest (..),
  Digest,
  FileDigest (..),
  TreeDigest (UnsafeMkTreeDigest),
  VersionDigest (UnsafeMkVersionDigest),
  digestFromByteString,
  digestSize,
  digestToBase16ByteString,
  hashByteStringFoldIO,
 )
import Better.Hash qualified as Hash
import Better.Internal.Streamly.Array (fastArrayAsPtrUnsafe)
import Better.Internal.Streamly.Array qualified as BetterArray
import Better.Internal.Streamly.Crypto.AES (compact, decryptCtr, encryptCtr)
import Better.Posix.File qualified as BF
import Better.Repository.Class qualified as E
import Better.Repository.Types (Version (..))
import Better.Streamly.FileSystem.Dir qualified as Dir
import Control.Exception (mask_, onException)
import Control.Monad (unless, void, when)
import Control.Monad.Catch (MonadThrow, handleIf, throwM)
import Crypto.Cipher.AES (AES128)
import Crypto.Cipher.Types qualified as Cipher
import Data.Binary qualified as Bin
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as BS16
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short qualified as BShort
import Data.Coerce (coerce)
import Data.Foldable (for_)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Word (Word32, Word64, Word8)
import Effectful (IOE, (:>))
import Effectful qualified as E
import Effectful.Concurrent qualified as E
import Effectful.Concurrent.Async qualified as E
import Effectful.Dispatch.Static qualified as E
import GHC.Stack (HasCallStack)
import Path (Path, (</>))
import Path qualified
import Streamly.Data.Array qualified as Array
import Streamly.Data.Fold qualified as F
import Streamly.Data.Stream.Prelude qualified as S
import Streamly.External.ByteString (fromArray, toArray)
import Streamly.External.ByteString.Lazy qualified as S
import Streamly.Internal.Data.Array.Type qualified as Array
import Streamly.Internal.Data.Fold qualified as F
import Streamly.Internal.System.IO (defaultChunkSize)
import Streamly.Internal.Unicode.Stream qualified as US
import System.Directory qualified as D
import System.FilePath qualified as FP
import System.IO.Error (isAlreadyExistsError, isDoesNotExistError)
import System.Posix.Directory qualified as P
import System.Posix.Files qualified as P
import System.Posix.IO qualified as P
import System.Posix.Types (FileOffset)
import Text.Printf (printf)
import Text.Read (readMaybe)
import Unsafe.Coerce (unsafeCoerce)
import Prelude hiding (read)

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

{-# INLINE listVersions #-}
listVersions :: (E.Repository E.:> es) => S.Stream (E.Eff es) (VersionDigest, Version)
listVersions =
  listVersionDigests
    & S.mapM
      ( \version_digest -> do
          !version <- catVersion version_digest
          pure (version_digest, version)
      )

{-# INLINE listVersionDigests #-}
listVersionDigests :: (E.Repository E.:> es) => S.Stream (E.Eff es) VersionDigest
listVersionDigests =
  listFolderFiles folder_version
    -- TODO Maybe we could report invalid version files.
    & S.mapMaybe (s2d . Path.fromRelFile)
    & fmap UnsafeMkVersionDigest

pathOfVersion :: VersionDigest -> Path Path.Rel Path.File
pathOfVersion d = folder_version </> d_path
  where
    !d_path = fromMaybe (error "impossible") $ Path.parseRelFile (show d)

{-# INLINE listTreeDigests #-}
listTreeDigests :: (E.Repository E.:> es) => S.Stream (E.Eff es) TreeDigest
listTreeDigests = list_digest_under_no_sharding UnsafeMkTreeDigest folder_tree

pathOfTree :: TreeDigest -> Path Path.Rel Path.File
pathOfTree = (folder_tree </>) . no_sharding_for_digest . coerce

{-# INLINE listFileDigests #-}
listFileDigests :: (E.Repository E.:> es) => S.Stream (E.Eff es) FileDigest
listFileDigests = list_digest_under_no_sharding UnsafeMkFileDigest folder_file

pathOfFile :: FileDigest -> Path Path.Rel Path.File
pathOfFile = (folder_file </>) . no_sharding_for_digest . coerce

{-# INLINE listChunkDigests #-}
listChunkDigests :: (E.Repository E.:> es) => S.Stream (E.Eff es) ChunkDigest
listChunkDigests = list_digest_under_two_layer_sharding UnsafeMkChunkDigest folder_chunk

pathOfChunk :: ChunkDigest -> Path Path.Rel Path.File
pathOfChunk = (folder_chunk </>) . two_layer_sharding_for_digest . coerce

{-# INLINE list_digest_under_no_sharding #-}
list_digest_under_no_sharding
  :: (E.Repository E.:> es)
  => (Digest -> a)
  -> Path Path.Rel Path.Dir
  -> S.Stream (E.Eff es) a
list_digest_under_no_sharding mk_digest folder =
  listFolderFiles folder
    & S.mapMaybe (s2d . Path.fromRelFile)
    & fmap mk_digest

{-# INLINE list_digest_under_two_layer_sharding #-}
list_digest_under_two_layer_sharding
  :: (E.Repository E.:> es)
  => (Digest -> a)
  -> Path Path.Rel Path.Dir
  -> S.Stream (E.Eff es) a
list_digest_under_two_layer_sharding mk_digest folder =
  S.fromList dirs
    & S.concatMap listFolderFiles
    & S.mapMaybe (s2d . Path.fromRelFile)
    & fmap mk_digest
  where
    dirs = do
      l1 <- one_level_dirs
      l2 <- one_level_dirs
      pure $! folder </> l1 </> l2
    one_level_dirs = fmap (fromMaybe (error "impossible") . Path.parseRelDir . to_hex) [0 .. 255]

    to_hex :: Word32 -> String
    to_hex = printf "%02x"

doesVersionExists :: (E.Repository E.:> es) => VersionDigest -> E.Eff es Bool
doesVersionExists = fileExists . pathOfVersion

doesTreeExists :: (E.Repository E.:> es) => TreeDigest -> E.Eff es Bool
doesTreeExists = fileExists . pathOfTree

doesFileExists :: (E.Repository E.:> es) => FileDigest -> E.Eff es Bool
doesFileExists = fileExists . pathOfFile

doesChunkExists :: (E.Repository E.:> es) => ChunkDigest -> E.Eff es Bool
doesChunkExists = fileExists . pathOfChunk

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
      !fd <- P.createFile path 0o755
      pure $! F.Partial fd

    {-# INLINE [0] step #-}
    step !fd !arr = (`onException` P.closeFd fd) $ do
      -- Using Fd for writing cost less CPU time than using Handle (hPutBuf).
      void $ fastArrayAsPtrUnsafe arr $ \ptr -> P.fdWriteBuf fd ptr $! fromIntegral $! Array.byteLength arr
      pure $! F.Partial fd

    {-# INLINE [0] extract #-}
    extract !fd = mask_ $ do
      P.closeFd fd

localRepo :: Path Path.Abs Path.Dir -> Repository
localRepo root =
  Repository
    local_write
    local_remove_file
    (handleIf isAlreadyExistsError (const $ pure ()) . flip P.createDirectory 0o777 . Path.fromAbsDir . (root </>))
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

data instance E.StaticRep E.Repository = RepositoryRep {-# UNPACK #-} !Repository {-# UNPACK #-} !AES128

runRepository :: (E.IOE E.:> es, E.IOE E.:> es) => Repository -> AES128 -> E.Eff (E.Repository : es) a -> E.Eff es a
runRepository repo cipher = E.evalStaticRep $ RepositoryRep repo cipher

getAES :: (E.Repository E.:> es) => E.Eff es AES128
getAES = do
  RepositoryRep _ aes <- E.getStaticRep
  pure $! aes

mkPutFileFold :: (E.Repository E.:> es) => E.Eff es (Path Path.Rel Path.File -> F.Fold (E.Eff es) (Array.Array Word8) ())
mkPutFileFold = do
  RepositoryRep repo _ <- E.getStaticRep
  pure $! F.morphInner E.unsafeEff_ . _repo_putFile repo

removeFiles :: (E.Repository E.:> es) => [Path Path.Rel Path.File] -> E.Eff es ()
removeFiles files = do
  RepositoryRep repo _ <- E.getStaticRep
  E.unsafeEff_ $ _repo_removeFiles repo files

createDirectory :: (E.Repository E.:> es) => Path Path.Rel Path.Dir -> E.Eff es ()
createDirectory d = do
  RepositoryRep repo _ <- E.getStaticRep
  E.unsafeEff_ $ _repo_createDirectory repo d

fileExists :: (E.Repository E.:> es) => Path Path.Rel Path.File -> E.Eff es Bool
fileExists d = do
  RepositoryRep repo _ <- E.getStaticRep
  E.unsafeEff_ $ _repo_fileExists repo d

fileSize :: (E.Repository E.:> es) => Path Path.Rel Path.File -> E.Eff es FileOffset
fileSize d = do
  RepositoryRep repo _ <- E.getStaticRep
  E.unsafeEff_ $ _repo_fileSize repo d

{-# INLINE read #-}
read :: (E.Repository E.:> es) => Path Path.Rel Path.File -> S.Stream (E.Eff es) (Array.Array Word8)
read f = S.concatEffect $ do
  RepositoryRep repo _ <- E.getStaticRep
  pure $! S.morphInner E.unsafeEff_ $ _repo_read repo f

{-# INLINE mkListFolderFiles #-}
mkListFolderFiles :: (E.Repository E.:> es) => Path Path.Rel Path.Dir -> S.Stream (E.Eff es) (Path Path.Rel Path.File)
mkListFolderFiles d = S.concatEffect $ do
  RepositoryRep repo _ <- E.getStaticRep
  pure $! S.morphInner E.unsafeEff_ . _repo_listFolderFiles repo $ d

folder_chunk :: Path Path.Rel Path.Dir
folder_chunk = [Path.reldir|chunk|]

folder_file :: Path Path.Rel Path.Dir
folder_file = [Path.reldir|file|]

folder_tree :: Path Path.Rel Path.Dir
folder_tree = [Path.reldir|tree|]

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]

addChunk'
  :: (E.Repository E.:> es)
  => Digest
  -> [Array.Array Word8]
  -> Cipher.IV AES128
  -> E.Eff es Word64
  -- ^ Bytes written
addChunk' digest chunks iv = do
  let !f = pathOfChunk $ UnsafeMkChunkDigest digest
  exist <- fileExists f
  if exist
    then pure 0
    else do
      aes <- getAES
      putFileFold <- mkPutFileFold
      ((), !len) <-
        S.fromList chunks
          & encryptCtr aes iv (1024 * 32)
          -- Do compact here so that impl of Repository doesn't need to care about buffering.
          -- TODO In the other hand, they can't do desired buffering.
          & compact (1024 * 32)
          & S.morphInner E.unsafeEff_
          & S.fold
            ( F.tee
                (putFileFold f)
                (fromIntegral <$> F.lmap Array.byteLength F.sum)
            )
      pure len

-- | Add single 'file' into repository.
--
-- Unsafety:
--   - user must ensure that @Digest@ and file at @Path@ are matched
--   - user must ensure that @Cipher.IV@ is not reused and won't be reused by other operations
addFile'
  :: (E.Repository E.:> es, E.IOE E.:> es)
  => Digest
  -> Path Path.Abs Path.File
  -> Cipher.IV AES128
  -> E.Eff es Bool
  -- ^ Dir added.
addFile' digest file_path iv = do
  let f = pathOfFile $ UnsafeMkFileDigest digest
  exist <- fileExists f
  unless exist $ do
    aes <- getAES
    putFileFold <- mkPutFileFold

    -- Optmization: `Handle` costs more CPU time and memory (perhaps for buffering).
    BF.withFile file_path P.ReadOnly P.defaultFileFlags $ \read_fd -> do
      BetterArray.fdReadChunksWith defaultChunkSize read_fd
        & fmap toArray
        & encryptCtr aes iv (1024 * 32)
        -- Do compact here so that impl of Repository doesn't need to care about buffering.
        -- TODO In the other hand, they can't do desired buffering.
        & compact (1024 * 32)
        & S.morphInner E.unsafeEff_
        & S.fold (putFileFold f)
  pure $! not exist

-- | Add single 'tree' into repository.
--
-- Unsafety:
--   - user must ensure that @Digest@ and file at @Path@ are matched
--   - user must ensure that @Cipher.IV@ is not reused and won't be reused by other operations
addDir'
  :: (E.IOE E.:> es, E.Repository E.:> es)
  => Digest
  -> Path Path.Abs Path.File
  -> Cipher.IV AES128
  -> E.Eff es Bool
  -- ^ Dir added.
addDir' digest file_path iv = do
  let f = pathOfTree $ UnsafeMkTreeDigest digest
  exist <- fileExists f
  unless exist $ do
    aes <- getAES
    putFileFold <- mkPutFileFold

    -- Optmization: `Handle` costs more CPU time and memory (perhaps for buffering).
    BF.withFile file_path P.ReadOnly P.defaultFileFlags $ \read_fd -> do
      BetterArray.fdReadChunksWith defaultChunkSize read_fd
        & fmap toArray
        & encryptCtr aes iv (1024 * 32)
        -- Do compact here so that impl of Repository doesn't need to care about buffering.
        -- TODO In the other hand, they can't do desired buffering.
        & compact (1024 * 32)
        & S.morphInner E.unsafeEff_
        & S.fold (putFileFold f)
  pure $! not exist

-- | Use @Binary Version@ to encode given Version into byte string, then store them.
addVersion
  :: (E.Repository E.:> es)
  => Cipher.IV AES128
  -> Version
  -> E.Eff es (Word64, Hash.VersionDigest)
  -- ^ Written bytes and digest of created version
addVersion iv v = do
  RepositoryRep _ aes <- E.getStaticRep

  -- It should be safe to store entire version bytes here since Version is relative small (under hundred of bytes)
  let version_bytes = Bin.encode v

  version_digest <-
    S.toChunks version_bytes
      & fmap fromArray
      & S.fold hashByteStringFoldIO
      & E.unsafeEff_

  let f = pathOfVersion $ UnsafeMkVersionDigest version_digest

  putFileFold <- mkPutFileFold

  written_bytes <- do
    S.toChunks version_bytes
      & encryptCtr aes iv (1024 * 32)
      -- Do compact here so that impl of Repository doesn't need to care about buffering.
      -- TODO In the other hand, they can't do desired buffering.
      & compact (1024 * 32)
      & S.morphInner E.unsafeEff_
      & S.tap (putFileFold f)
      & S.fold (F.lmap (fromIntegral . Array.length) F.sum)

  pure (written_bytes, Hash.UnsafeMkVersionDigest version_digest)

nextBackupVersionId :: (E.Repository E.:> es) => E.Eff es Integer
nextBackupVersionId = do
  listFolderFiles folder_version
    & S.mapMaybe (readMaybe @Integer . Path.fromRelFile)
    & S.fold F.maximum
    & fmap (succ . fromMaybe 0)

assertSizeOfFile :: (HasCallStack, E.Repository E.:> es) => FileDigest -> E.Eff es ()
assertSizeOfFile digest = do
  aes <- getAES
  let iv_length = Cipher.blockSize aes
  file_size <- getFileSize digest
  unless ((file_size - fromIntegral iv_length) `mod` fromIntegral digestSize == 0) $ do
    error $ "size of 'file', " <> show digest <> ", is incorrect: " <> show file_size

catFile :: (HasCallStack, E.Repository E.:> es) => FileDigest -> S.Stream (E.Eff es) Object
catFile digest = S.concatEffect $ do
  assertSizeOfFile digest

  aes <- getAES
  E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
    pure $!
      read (pathOfFile digest)
        & S.morphInner un
        & decryptCtr aes (1024 * 32)
        & compact digestSize
        & S.mapM (parse_file_content . fromArray)
        & S.morphInner E.unsafeEff_
  where
    parse_file_content :: BS.ByteString -> IO Object
    parse_file_content bs = case digestFromByteString bs of
      Just d -> pure $! Object $ UnsafeMkChunkDigest d
      Nothing -> error $ "failed to parse digest from bytestring: length = " <> show (BS.length bs)

catChunk
  :: (E.Repository E.:> es)
  => ChunkDigest
  -> S.Stream (E.Eff es) (Array.Array Word8)
catChunk digest = S.concatEffect $ do
  RepositoryRep _ aes <- E.getStaticRep
  E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
    pure $!
      read (pathOfChunk digest)
        & S.morphInner un
        & decryptCtr aes (32 * 1024)
        & S.morphInner E.unsafeEff_
{-# INLINE catChunk #-}

getChunkSize :: (E.Repository E.:> es) => ChunkDigest -> E.Eff es FileOffset
getChunkSize = fileSize . pathOfChunk

getFileSize :: (E.Repository E.:> es) => FileDigest -> E.Eff es FileOffset
getFileSize = fileSize . pathOfFile

catVersion :: (E.Repository E.:> es) => VersionDigest -> E.Eff es Version
catVersion digest = do
  RepositoryRep _ aes <- E.getStaticRep

  encrypted_bytes <-
    read (pathOfVersion digest)
      & fmap (BB.shortByteString . BShort.pack . Array.toList)
      & S.fold F.mconcat
      & fmap (BS.toStrict . BB.toLazyByteString)

  let
    -- Length of IV and block cipher are identical.
    iv_len = Cipher.blockSize aes
    (iv_bytes, cipher_version_bytes) = BS.splitAt iv_len encrypted_bytes

  iv <- case Cipher.makeIV iv_bytes of
    Nothing -> error "failed to read iv from raw bytes of version"
    Just iv -> pure iv

  let decode_result = Bin.decodeOrFail @Version $ BL.fromStrict $ Cipher.ctrCombine aes iv cipher_version_bytes

  case decode_result of
    Left (_, _, err_msg) -> throwM $ userError $ "failed to decode version " <> show digest <> ": " <> err_msg
    Right (remain_bytes, _, v) -> do
      unless (BL.null remain_bytes) $ throwM $ userError $ "failed to decode version " <> show digest <> ": there're remaining bytes"
      pure v

catTree :: (E.Repository E.:> es) => TreeDigest -> S.Stream (E.Eff es) (Either Tree FFile)
catTree tree_sha' = S.concatEffect $ do
  aes <- getAES
  E.unsafeConcUnliftIO E.Ephemeral E.Unlimited $ \un -> do
    pure $!
      read (pathOfTree tree_sha')
        & S.morphInner un
        & decryptCtr aes (1024 * 32)
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

d2b :: Digest -> BS.ByteString
d2b = digestToBase16ByteString

initRepositoryStructure :: (E.Repository :> es, IOE :> es) => E.Eff es ()
initRepositoryStructure = do
  -- TODO Move this to initizliaztion of repository itself.
  -- It makes more sense that repo exists before calling initRepositoryStructure.
  createDirectory [Path.reldir|.|]
  E.runConcurrent $
    E.mapConcurrently_
      id
      -- TODO Assign proper layer for folder of version, tree and file.
      [ mk_sharding_folder_for 0 folder_version
      , mk_sharding_folder_for 0 folder_tree
      , mk_sharding_folder_for 0 folder_file
      , mk_sharding_folder_for 2 folder_chunk
      ]
  where
    mk_sharding_folder_for (!n :: Word32) !folder = do
      createDirectory folder

      when (n > 0) $ do
        for_ [0 .. 255 :: Word32] $ \layer -> do
          sharding_layer <- Path.parseRelDir $ to_hex layer
          mk_sharding_folder_for (n - 1) $ folder </> sharding_layer

    to_hex :: Word32 -> String
    to_hex = printf "%02x"

no_sharding_for_digest :: Digest -> Path Path.Rel Path.File
no_sharding_for_digest d = d_path
  where
    -- Unsafe: ensure `show d` always produces valid Path Rel File.
    !d_path = unsafeCoerce d_str
    d_str = show d

-- About unsafeCoerce:
--   Using unsafeCoerce to replace parseRelFile could reward you about 10 sec CPU time (from 300s to 290s)
--   while backing up 13GiB (6 large file), without noticeable change on elapsed time though.
two_layer_sharding_for_digest :: Digest -> Path Path.Rel Path.File
two_layer_sharding_for_digest d = fromMaybe (error "impossible case") $ Path.parseRelFile raw_path
  where
    raw_path = l1_path FP.</> l2_path FP.</> d_path
    !l1_path = take 2 d_str
    !l2_path = take 2 $ drop 2 d_str
    !d_path = d_str
    d_str = show d
