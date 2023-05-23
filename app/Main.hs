{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Strict #-}

module Main where

import Katip

import System.Environment

import GHC.Stack
import GHC.Generics
import Debug.Trace

import Data.Time

import Data.Function
import Data.Char
import Data.List
import Data.Kind
import Data.Word
import Data.IORef
import Data.Maybe
import Data.Bifunctor
import Data.Foldable
import GHC.TypeLits
import System.IO

import Text.Read

import Control.Monad.IO.Unlift
import Control.Arrow

import qualified Path
import Path (Path(..), (</>))

import qualified System.Posix.IO as P
import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P
import qualified System.Posix.Temp as P

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Base16 as BS

import qualified Capability.Reader as C
import qualified Capability.Source as C

import Control.Monad
import Control.Monad.Reader
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Base
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async

import Crypto.Hash
import Crypto.Hash.Algorithms

import qualified Data.List.NonEmpty as NE

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe) 

import qualified Streamly.Internal.FileSystem.Event.Linux as L
import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Internal.Data.Stream.Time as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Internal.FileSystem.Dir as Dir
import qualified Streamly.Console.Stdio as Stdio
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

import           Control.Applicative
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import Better.Repository
import Better.Repository.Class
import qualified Better.Repository as Repo

import Data.ByteArray (ByteArrayAccess(..))

newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp

data Hbk m = MkHbk
  { hbk_path :: {-# UNPACK #-} !T.Text
  , hbk_cwd :: {-# UNPACK #-} !(Path Path.Abs Path.Dir)
  , hbk_objExists :: Digest SHA256 -> m Bool
  , hbk_fileExists :: Digest SHA256 -> m Bool
  , hbk_dirExists :: Digest SHA256 -> m Bool
  , hbk_repo :: Repository
  }
  deriving (Generic)

type MonadBackup m = (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m, MonadRepository m)

newtype HbkT m a = HbkT { runHbkT :: Hbk (HbkT m) -> m a }
  deriving (Generic)
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadIO
    , MonadCatch
    , MonadThrow
    , MonadMask
    , MonadBase m'
    , MonadBaseControl m'
    ) via (ReaderT (Hbk (HbkT m)) m)
  deriving
    ( C.HasSource "hbk" (Hbk (HbkT m)) 
    , C.HasReader "hbk" (Hbk (HbkT m)) 
    ) via (C.MonadReader (ReaderT (Hbk (HbkT m)) m))
  deriving
    ( C.HasSource "repo" Repository
    , C.HasReader "repo" Repository
    ) via C.Rename "hbk_repo"
         (C.Field "hbk_repo" ()
	 (C.MonadReader
	 (ReaderT (Hbk (HbkT m)) m)))
  deriving 
    ( MonadRepository
    ) via TheMonadRepository
         (C.Rename "hbk_repo"
         (C.Field "hbk_repo" ()
	 (C.MonadReader
	 (ReaderT (Hbk (HbkT m)) m))))

touch_file :: (C.HasReader "hbk" (Hbk m) m, MonadIO m) => T.Text -> m T.Text
touch_file file_name = do
  hbk <- C.asks @"hbk" hbk_path
  liftIO $ do
    (tmp_name, tmp_fd) <- P.mkstemp (T.unpack $ hbk <> "/tmp/")
    hClose tmp_fd
    T.writeFile tmp_name ""
    pure $ T.pack tmp_name

append_file :: T.Text -> Digest SHA256 -> IO ()
append_file file_name chunk_hash = do
  let f = file_name
  T.appendFile (T.unpack f) (T.pack $ show chunk_hash <> "\n")

try_touching_dir :: MonadIO m => Hbk m -> m T.Text
try_touching_dir hbk = liftIO $ do
  (tmp_name, tmp_fd) <- P.mkstemp (T.unpack $ hbk_path hbk <> "/tmp/")
  hClose tmp_fd
  T.writeFile tmp_name ""
  pure $ T.pack tmp_name

dir_content :: Either T.Text T.Text -> Digest SHA256 -> BS.ByteString
dir_content file_or_dir hash' =
  let
    name = either file_name' file_name' file_or_dir
    t = either (const "dir") (const "file") file_or_dir
  in TE.encodeUtf8 $ t <> " " <> name <> " " <> (T.pack $ show hash' <> "\n")

data Version
  = Version
  { ver_id :: {-# UNPACK #-} Integer
  , ver_root :: {-# UNPACK #-} Digest SHA256
  }
  deriving (Show)

data Dir
  = Dir 
  { dir_name :: {-# UNPACK #-} T.Text
  , dir_sha :: {-# UNPACK #-} T.Text -- TODO To Digest
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

hash_fold :: (HashAlgorithm alg, ByteArrayAccess ba, Monad m)
	  => F.Fold m ba (Digest alg)
hash_fold = fmap hashFinalize (F.foldl' hashUpdate hashInit)

file_name' :: T.Text -> T.Text
file_name' = last . T.splitOn "/"

h_dir :: (MonadBackup m) => T.Text -> m (Digest SHA256)
h_dir dir_name = do
  hbk <- C.ask @"hbk"
  let hbb = hbk_path hbk

  tmp_dir_file <- try_touching_dir hbk
  fmap (bimap T.pack T.pack) (Dir.readEitherPaths $ T.unpack dir_name)
    & S.mapM (\fod ->do
        hash <- either h_dir h_file fod
	pure $ dir_content fod hash
      )
    & S.trace (liftIO . BS.appendFile (T.unpack tmp_dir_file))
    & S.fold F.drain

  cwd <- C.asks @"hbk" hbk_cwd
  rel_dir_name <- Path.parseRelFile $ T.unpack tmp_dir_file

  File.readChunks (Path.fromAbsFile $ cwd </> rel_dir_name)
    & Repo.addDir

h_file :: (MonadBackup m) => T.Text -> m (Digest SHA256)
h_file file_name = do
  file_name' <- touch_file file_name
  File.readChunksWith (1024 * 1024) (T.unpack file_name)
    & S.filter ((0 /=) . Array.length)
    & S.mapM (\chunk -> do
      chunk_hash <- Repo.addBlob $ S.fromPure chunk
      pure chunk_hash
    )
    & S.trace (liftIO . append_file file_name')
    & S.fold F.drain

  cwd <- C.asks @"hbk" hbk_cwd
  rel_file_name' <- Path.parseRelFile $ T.unpack file_name'
  File.readChunks (Path.fromAbsFile $ cwd </> rel_file_name')
    & Repo.addFile

backup :: (MonadBackup m) => T.Text -> m Version
backup dir = do
  version_id <- Repo.nextBackupVersionId
  root_hash <- h_dir dir
  Repo.addVersion version_id root_hash
  return $ Version version_id root_hash

init_bkp :: (C.HasReader "hbk" (Hbk m) m, MonadIO m) => m ()
init_bkp = do
  hbk <- C.asks @"hbk" hbk_path
  exist <- liftIO $ P.fileExist $ T.unpack hbk
  liftIO $ unless exist $ do
    for_ ["", "obj", "file", "dir", "version", "tmp"] $ \d ->
      P.createDirectory (T.unpack $ hbk <> "/" <> d) 700

list_versions :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	      => S.Stream m Version
list_versions = S.concatEffect $ do
  hbk <- C.asks @"hbk" hbk_path
  let ver_path = T.unpack $ hbk <> "/version"
  pure $ Dir.readFiles ver_path
    & S.mapM (\v -> do
      optSha <- fmap (BS.decode . BS.pack)
        $ S.toList
	$ File.read (ver_path <> "/" <> v)
      v' <- liftIO $ readIO v

      sha <- case optSha of
	Left err -> throwM $ userError $ "invalid sha of version, " <> err
	Right sha -> pure $ sha

      digest <- case digestFromByteString @SHA256 sha of
	Nothing -> throwM $ userError "invalid sha of version"
	Just digest -> pure digest

      pure $ Version v' digest
    )

cat_dir :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	=> T.Text -> S.Stream m (Either Dir FFile)
cat_dir sha = S.concatEffect $ do
  hbk <- C.asks @"hbk" hbk_path
  let dir_path = T.unpack $ (hbk <> "/dir/") <> sha
  pure $ File.read dir_path
    & US.decodeLatin1
    & US.lines (fmap T.pack F.toList)
    & S.mapM parse_dir_content

parse_dir_content :: MonadThrow m => T.Text -> m (Either Dir FFile)
parse_dir_content buf = case T.splitOn " " buf of
  ["dir", name, sha] -> pure $ Left $ Dir name sha
  ["file", name, sha] -> pure $ Right $ FFile name sha
  _ -> throwM $ userError $ "invalid dir content: " <> T.unpack buf

cat_file :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	=> T.Text -> S.Stream m Object
cat_file sha = S.concatEffect $ do
  hbk <- C.asks @"hbk" hbk_path
  let dir_path = T.unpack $ (hbk <> "/file/") <> sha
  pure $ File.read dir_path
    & US.decodeLatin1
    & US.lines (fmap T.pack F.toList)
    & S.mapM parse_file_content
{-# INLINE cat_file #-}

parse_file_content :: Applicative m => T.Text -> m Object
parse_file_content = pure . Object

cat_object :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	=> T.Text -> S.Stream m (Array.Array Word8)
cat_object sha = S.concatEffect $ do
  hbk <- C.asks @"hbk" hbk_path
  let dir_path = T.unpack $ (hbk <> "/obj/") <> sha
  pure $ File.readChunks dir_path
{-# INLINE cat_object #-}

checksum :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	=> m ()
checksum = do
  hbk <- C.asks @"hbk" hbk_path

  let
    check p = Dir.readFiles (T.unpack p)
      & fmap (\f -> (f, T.unpack p <> "/" <> f))
      & S.mapMaybeM (\(expected_sha, f) -> do
          actual_sha <- File.readChunks f
	    & S.fold (F.lmap ArrayBA (hash_fold @SHA256))
          pure $
            if show actual_sha == expected_sha
            then Nothing
	    else Just (f, actual_sha)
        )
      & S.trace (\(invalid_f, expect_sha) ->
          liftIO $ T.putStrLn $ "invalid file: " <> T.pack invalid_f <> ", " <> T.pack (show expect_sha))
      & S.fold F.drain

  for_ ["dir", "file", "obj"] $ \p ->
    check $ hbk <> "/" <> p

tree_explorer :: (C.HasReader "hbk" (Hbk m) m, MonadIO m, MonadCatch m)
	      => T.Text -> m ()
tree_explorer sha = do
  flip fix [sha] $ \(~loop) stack -> when (not $ null stack) $ do
    let (s:ss) = stack
    ln <- liftIO $ T.getLine
    case T.words ln of
      ["ls"] -> do
        cat_dir s
          & S.fold (F.drainMapM $ liftIO . print)
        loop (s:ss)
      ["cd", p] -> do
        optDir <- cat_dir s
	  & S.mapMaybe (either Just (const Nothing))
	  & S.fold (F.find $ (p ==) . dir_name)
	case optDir of
          Nothing -> do
            liftIO $ T.putStrLn $ "no such path: " <> p
	    loop (s:ss)
          Just dir -> do
            loop ((dir_sha dir):s:ss)
      ["pwd"] -> do
        liftIO $ T.putStrLn $ T.intercalate "/" $ reverse (s:ss)
        loop (s:ss)
      ["b"] -> do
        loop ss
      ["q"] -> do
        loop []
      _ -> loop (s:ss)

mm :: (C.HasSource "x" Int m) => m Bool
mm = pure True

main :: IO ()
main = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  
  let hbk = MkHbk "bkp/" cwd (const $ pure False) (const $ pure False) (const $ pure False) (localRepo $ cwd </> [Path.reldir|bkp|])

  flip runHbkT hbk $ initRepositoryStructure 

  let
    handle_cmd cmd go =
      (do
        traceMarkerIO $ T.unpack cmd <> "-begin"
        r <- go
        traceMarkerIO $ T.unpack cmd <> "-end"
	pure r
      )
      `catch` \(e :: SomeException) -> do
        T.putStrLn $ "failed to run '" <> cmd <> "': " <> T.pack (show e)
        pure ()

  fix $ \(~loop) -> do
    T.putStrLn ":> "
    l <- T.getLine
    case T.words l of
      "backup":_ -> do
        traceMarkerIO "backup-begin"
        b <- getCurrentTime
        version <- flip runHbkT hbk $ do
          backup "dist-newstyle"
        print version
        e <- getCurrentTime
	print $ nominalDiffTimeToSeconds $ diffUTCTime e b
        traceMarkerIO "backup-end"
        loop
      "list-versions":_ -> do
        list_versions
          & S.morphInner (flip runHbkT hbk)
          & S.fold (F.drainMapM print)
        loop
      "cat-dir":sha:_ -> do
        handle_cmd "cat-dir" $ do
          cat_dir sha
            & S.morphInner (flip runHbkT hbk)
            & S.fold (F.drainMapM $ T.putStrLn . T.pack . show)
        loop
      "cat-file":sha:_ -> do
        handle_cmd "cat-file" $ do
          cat_file sha
            & S.morphInner (flip runHbkT hbk)
            & S.fold (F.drainMapM $ T.putStrLn . T.pack . show)
        loop
      "cat-object":sha:_ -> do
        handle_cmd "cat-object" $ do
          cat_object sha
            & S.morphInner (flip runHbkT hbk)
            & S.fold (Stdio.writeChunks)
        loop
      "cat-file-content":sha:to:_ -> do
        handle_cmd "cat-file-content" $ do
          cat_file sha
            & S.concatMap (cat_object . object_name)
            & S.morphInner (flip runHbkT hbk)
            & S.fold (File.writeChunks $ T.unpack to)
        loop
      "tree":sha:_ -> do
        handle_cmd "tree" $ do
          flip runHbkT hbk $ tree_explorer sha
        loop
      "check":_ -> do
        handle_cmd "check" $ do
          flip runHbkT hbk $ checksum
        loop
      "q":_ -> pure ()
      l:_ -> T.putStrLn ("no such command: " <> l) >> loop
      [] -> loop

withEmitUnfoldr :: ((Int -> IO ()) -> IO b)
     -> (S.Stream IO Int -> IO a) -> IO a
withEmitUnfoldr putter go = do
  tbq <- newTBQueueIO @Int 1

  withAsync (putter (atomically . writeTBQueue tbq)) $ \h -> do
    let 
      f = do
        e <- atomically $ (Just <$> readTBQueue tbq) <|> (Nothing <$ waitSTM h)
        case e of
          Just v -> pure (Just (v, ()))
	  Nothing -> pure Nothing

    go $ S.unfoldrM (\() -> f) ()
