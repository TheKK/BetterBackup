{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CApiFFI #-}
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

import GHC.Generics
import Debug.Trace

import Data.Time

import Data.Function
import Data.Word

import qualified Path
import Path (Path, (</>))

import qualified System.Posix.Directory as P

import qualified Data.ByteString.Base16 as BS

import qualified Capability.Reader as C

import Control.Monad
import Control.Monad.Reader
import Control.Monad.Catch

import UnliftIO (MonadUnliftIO(..))
import UnliftIO.Exception (throwString)

import Crypto.Hash

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe) 

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Console.Stdio as Stdio

import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as TE

import Better.TempDir
import Better.Repository
import qualified Better.Repository as Repo

import Data.ByteArray (ByteArrayAccess(..))

import qualified Config

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

cat_dir :: (MonadThrow m, MonadIO m, MonadRepository m) => T.Text -> S.Stream m (Either Tree FFile)
cat_dir sha = S.concatEffect $ do
  sha' <- t2d sha
  pure $ Repo.catTree sha'

cat_file :: (MonadThrow m, MonadIO m, MonadRepository m) => T.Text -> S.Stream m Object 
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
           => Digest SHA256 -> S.Stream m (Array.Array Word8)
cat_object digest = catChunk digest
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
            loop ((T.pack $ show $ tree_sha dir):s:ss)
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
  let config_path = cwd </> [Path.relfile|config.toml|]

  optConfig <- Config.parseConfig <$> (T.readFile $ Path.fromAbsFile config_path)
  repository <- case optConfig of
    Left err -> throwString $ T.unpack err
    Right config -> case Config.config_repoType config of
      Config.Local l -> pure $ localRepo $ either id (cwd </>) (Config.local_repo_path l)
  
  hbk <- pure $ MkHbk
      [Path.absdir|/tmp|]
      cwd
      repository
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
          Repo.backup "test_dir"
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
          digest <- t2d sha
          cat_object digest
            & S.morphInner (flip runHbkT hbk)
            & S.fold (Stdio.writeChunks)
        loop
      "cat-file-content":sha:to:_ -> do
        handle_cmd "cat-file-content" $ do
          cat_file sha
            & S.concatMap (cat_object . chunk_name)
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
      "gc":_ -> do
        handle_cmd "gc" $ do
          b <- getCurrentTime
          flip runHbkT hbk $ Repo.garbageCollection
          e <- getCurrentTime
	  print $ nominalDiffTimeToSeconds $ diffUTCTime e b
        loop
      "q":_ -> pure ()
      l:_ -> T.putStrLn ("no such command: " <> l) >> loop
      [] -> loop

