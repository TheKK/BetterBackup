{-# LANGUAGE Strict #-}
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
  ( initRepositoryStructure
  , addBlob
  , addFile
  , addDir
  , addVersion
  , nextBackupVersionId
  , localRepo
  , Repository
  , MonadRepository
  , TheMonadRepository(..)
  ) where

import Data.Word
import Data.Function
import Data.Maybe (fromMaybe)

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import qualified Data.ByteString as BS

import Text.Read (readMaybe)

import Control.Monad
import Control.Monad.Reader
import Control.Monad.Catch

import qualified Streamly.Data.Array as Array
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.Dir as Dir

import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P
import qualified System.Posix.Temp as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash

import Better.Hash

import Better.Repository.Class
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

data Repository = Repository
   { _repo_putFile :: Path Path.Rel Path.File -> F.Fold IO (Array.Array Word8) ()
   -- TODO File mode?
   , _repo_createDirectory :: Path Path.Rel Path.Dir -> IO () 
   , _repo_fileExists :: Path Path.Rel Path.File -> IO Bool
   , _repo_listFolderFiles :: Path Path.Rel Path.Dir -> S.Stream IO (Path Path.Rel Path.File)
   }

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

  mkListFolderFiles = TheMonadRepository $ do
    f <- C.asks @"repo" _repo_listFolderFiles
    pure $ S.morphInner liftIO . f

localRepo :: Path Path.Abs Path.Dir -> Repository
localRepo root = Repository 
  -- (tmp_name, tmp_fd) <- P.mkstemp (Path.fromAbsDir $ hbk_path </> [Path.reldir|version|])
  --hClose tmp_fd
  (File.writeChunks . Path.fromAbsFile . (root </>))
  (liftIO . flip P.createDirectory 700 . Path.fromAbsDir . (root </>))
  (liftIO . P.fileExist . Path.fromAbsFile . (root </>))
  (S.mapM Path.parseRelFile . Dir.read . Path.fromAbsDir . (root </>))

put_file_in_repo :: (MonadCatch m, MonadIO m, MonadRepository m)
  => Path Path.Rel Path.Dir
  -> S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
put_file_in_repo subdir chunks = do
  chunk_hash <- chunks & S.fold hashArrayFold
  file_name <- Path.parseRelFile $ show chunk_hash
  let f = subdir </> file_name
  exist <- fileExists f
  putFileFold <- mkPutFileFold
  unless exist $ 
    chunks & S.fold (putFileFold f)
  pure chunk_hash 

folder_chunk :: Path Path.Rel Path.Dir
folder_chunk = [Path.reldir|obj|]

folder_file :: Path Path.Rel Path.Dir
folder_file = [Path.reldir|file|]

folder_tree :: Path Path.Rel Path.Dir
folder_tree = [Path.reldir|tree|]

folder_version :: Path Path.Rel Path.Dir
folder_version = [Path.reldir|version|]

initRepositoryStructure :: (MonadRepository m) => m ()
initRepositoryStructure = do
  mapM_ createDirectory $
    [ [Path.reldir| |]
    , folder_chunk
    , folder_file
    , folder_tree
    , folder_version
    ]

addBlob :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addBlob = put_file_in_repo folder_chunk

addFile :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addFile = put_file_in_repo folder_file

addDir :: (MonadCatch m, MonadIO m, MonadRepository m)
  => S.Stream m (Array.Array Word8)
  -> m (Digest SHA256)
addDir = put_file_in_repo folder_tree

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
  listFolderFiles <- mkListFolderFiles
  liftIO $ listFolderFiles folder_version
    & S.mapMaybe (readMaybe @Integer . Path.fromRelFile)
    & S.fold F.maximum
    & fmap (succ . fromMaybe 0)
