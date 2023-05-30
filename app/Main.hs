{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
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

module Main ( main ) where

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
import System.IO.Error

import Text.Read

import qualified Path
import Path (Path(..), (</>))

import qualified System.Posix.IO as P
import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P
import qualified System.Posix.Temp as P

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Base16 as BS
import qualified Data.ByteString.Char8 as BC

import qualified Capability.Reader as C
import qualified Capability.Source as C

import Control.Monad
import Control.Monad.Reader
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Base
import Control.Monad.Trans.Control (MonadBaseControl)

import UnliftIO (MonadUnliftIO(..))
import UnliftIO.Async
import UnliftIO.STM
import UnliftIO.Directory
import qualified UnliftIO.IO.File as Un

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
import qualified Streamly.Console.Stdio as Stdio
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

import           Control.Applicative
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import Better.Hash
import Better.Repository
import Better.TempDir
import qualified Better.Repository as Repo
import qualified Better.Streamly.FileSystem.Dir as Dir

import Data.ByteArray (ByteArrayAccess(..))
import qualified Data.ByteArray.Encoding as BA

newtype ArrayBA a = ArrayBA (Array.Array a)

instance ByteArrayAccess (ArrayBA Word8) where
  length (ArrayBA arr) = Array.length arr
  withByteArray (ArrayBA arr) fp = Array.asPtrUnsafe (Array.castUnsafe arr) fp

data Hbk m = MkHbk
  { hbk_path :: Path Path.Abs Path.Dir
  , hbk_cwd :: Path Path.Abs Path.Dir
  , hbk_repo :: Repository
  , hbk_tmpdir :: Path Path.Abs Path.Dir
  , what :: m ()
  }
  deriving (Generic)

type MonadBackup m = (MonadRepository m, MonadMask m, MonadUnliftIO m, MonadTmp m)

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
    , MonadUnliftIO
    ) via (ReaderT (Hbk (HbkT m)) m)
  deriving 
    ( MonadRepository
    ) via TheMonadRepository
         (C.Rename "hbk_repo"
         (C.Field "hbk_repo" ()
         (C.MonadReader
	 (ReaderT (Hbk (HbkT m)) m))))
  deriving 
    ( MonadTmp
    ) via TheMonadTmp
         (C.Rename "hbk_tmpdir"
         (C.Field "hbk_tmpdir" ()
         (C.MonadReader
	 (ReaderT (Hbk (HbkT m)) m))))

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)

tree_content :: Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir) -> Digest SHA256 -> BS.ByteString
tree_content file_or_dir hash' =
  let
    name = either (T.pack . Path.toFilePath . Path.filename) file_name' file_or_dir
    t = either (const "file") (const "dir") file_or_dir
  in t <> " " <> TE.encodeUtf8 name <> " " <> d2b hash'

file_name' :: Path r Path.Dir -> T.Text
file_name' = T.replace "/" "" . T.pack . Path.toFilePath . Path.dirname

h_dir :: (MonadBackup m, MonadBaseControl IO m) => TBQueue (m ()) -> T.Text -> m (Digest SHA256)
h_dir tbq tree_name = withEmptyTmpFile $ \file_name' -> do

  rel_tree_name <- Path.parseRelDir $ T.unpack tree_name
  
  Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    Dir.readEither rel_tree_name
      & S.mapM (\fod ->do
          hash <- either (h_file tbq . T.pack . Path.fromRelFile . (rel_tree_name </>)) (h_dir tbq . T.pack . Path.fromRelDir . (rel_tree_name </>)) fod
          pure $ tree_content fod hash
        )
      & S.fold (F.drainMapM $ liftIO . BC.hPutStrLn fd)

  dir_hash <- File.readChunks (Path.fromAbsFile file_name')
    & S.parMapM (S.ordered True . S.maxBuffer 8) pure
    & S.fold hashArrayFold
  atomically $ writeTBQueue tbq $ do
    Repo.addDir' dir_hash (File.readChunks (Path.fromAbsFile file_name'))
    `finally`
      removeFile (Path.fromAbsFile file_name')
  pure dir_hash 

d2b :: Digest SHA256 -> BS.ByteString
d2b = BA.convertToBase BA.Base16

h_file :: (MonadBackup m, MonadBaseControl IO m) => TBQueue (m ()) -> T.Text -> m (Digest SHA256)
h_file tbq file_name = withEmptyTmpFile $ \file_name' -> do

  Un.withBinaryFile (Path.fromAbsFile file_name') WriteMode $ \fd -> do
    File.readChunksWith (1024 * 1024) (T.unpack file_name)
      & S.filter ((0 /=) . Array.length)
      & S.parMapM (S.ordered True . S.maxBuffer 4) (\chunk -> do
        chunk_hash <- S.fromPure chunk & S.fold hashArrayFold
        atomically $ writeTBQueue tbq $ do
          Repo.addBlob' chunk_hash (S.fromPure chunk)
        pure chunk_hash
      )
      & S.fold (F.drainMapM $ liftIO . BC.hPutStrLn fd . d2b)

  file_hash <- File.readChunks (Path.fromAbsFile file_name')
    & S.parMapM (S.ordered True . S.maxBuffer 8) pure
    & S.fold hashArrayFold
  atomically $ writeTBQueue tbq $ do
    Repo.addFile' file_hash (File.readChunks (Path.fromAbsFile file_name'))
    `finally`
      removeFile (Path.fromAbsFile file_name')
  
  pure file_hash

backup :: (MonadBackup m, MonadBaseControl IO m) => T.Text -> m Version
backup dir = do
  (root_hash, _) <- withEmitUnfoldr 50 (\tbq -> h_dir tbq dir)
    $ (\s -> s 
       & S.parSequence (S.maxBuffer 20 . S.maxThreads 10 . S.eager True)
       & S.fold F.drain
      )
  version_id <- Repo.nextBackupVersionId
  Repo.addVersion version_id root_hash
  return $ Version version_id root_hash

cat_dir sha = S.concatEffect $ do
  sha' <- t2d sha
  pure $ Repo.catTree sha'

cat_file sha = S.concatEffect $ do
  digest <- t2d sha
  pure $ Repo.catFile digest
{-# INLINE cat_file #-}

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

cat_object :: (MonadIO m, MonadThrow m, MonadRepository m)
	=> T.Text -> S.Stream m (Array.Array Word8)
cat_object sha = S.concatEffect $ do
  digest <- t2d sha
  pure $ catChunk digest
{-# INLINE cat_object #-}

tree_explorer :: (MonadIO m, MonadThrow m, MonadRepository m)
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
        optTree <- cat_dir s
	  & S.mapMaybe (either Just (const Nothing))
	  & S.fold (F.find $ (p ==) . tree_name)
	case optTree of
          Nothing -> do
            liftIO $ T.putStrLn $ "no such path: " <> p
	    loop (s:ss)
          Just dir -> do
            loop ((tree_sha dir):s:ss)
      ["pwd"] -> do
        liftIO $ T.putStrLn $ T.intercalate "/" $ reverse (s:ss)
        loop (s:ss)
      ["b"] -> do
        loop ss
      ["q"] -> do
        loop []
      _ -> loop (s:ss)

main :: IO ()
main = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  
  hbk <- pure $ MkHbk
      [Path.absdir|/tmp|]
      cwd
      (localRepo $ cwd </> [Path.reldir|bkp|])
      [Path.absdir|/tmp|]
      (pure ())

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
        traceMarkerIO $ T.unpack cmd <> "-catch-end"
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
          backup "test_dir"
        print version
        e <- getCurrentTime
	print $ nominalDiffTimeToSeconds $ diffUTCTime e b
        traceMarkerIO "backup-end"
        loop
      "list-versions":_ -> do
        Repo.listVersions
          & S.morphInner (flip runHbkT hbk)
          & S.fold (F.drainMapM print)
        loop
      "cat-dir":sha:_ -> do
        handle_cmd "cat-dir" $ do
          sha' <- t2d sha
          Repo.catTree sha'
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
      "check":n:_ -> do
        handle_cmd "check" $ do
          n' <- readIO $ T.unpack n	
          b <- getCurrentTime
          flip runHbkT hbk $ Repo.checksum n'
          e <- getCurrentTime
	  print $ nominalDiffTimeToSeconds $ diffUTCTime e b
        loop
      "q":_ -> pure ()
      l:_ -> T.putStrLn ("no such command: " <> l) >> loop
      [] -> loop

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
