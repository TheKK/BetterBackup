{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
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

module Main
  ( main
  ) where

import Prelude hiding (read)

import System.LibFuse3
import System.IO (hPrint)
import Foreign.C.Error
import Data.Void

import Numeric.Natural

import Data.Word
import Data.Bifunctor
import Data.Function
import Data.Foldable
import Data.Maybe (fromMaybe, isJust, fromJust, isNothing)

import qualified Capability.Reader as C
import qualified Capability.Source as C

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
import Control.Monad.State
import Control.Monad.Base
import Control.Monad.Catch (MonadCatch, MonadThrow, MonadMask, throwM, handleIf)
import Control.Monad.Trans.Control (MonadBaseControl)

import qualified Streamly.Data.Array as Array
import qualified Streamly.Internal.Data.Array as Array (asPtrUnsafe, castUnsafe) 

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F
import qualified Streamly.Data.Unfold as U
import qualified Streamly.FileSystem.File as File
import qualified Streamly.Unicode.Stream as US
import qualified Streamly.Internal.Unicode.Stream as US

import System.IO.Error (isDoesNotExistError)

import qualified System.Posix.IO as P
import qualified System.Posix.Types as P
import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P

import Path (Path, (</>))
import qualified Path

import Crypto.Hash

import GHC.Generics

import qualified Streamly.Data.Array as Array

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import qualified Path
import Path (Path)

import Data.Word

import Control.Monad.IO.Class
import Data.ByteArray (ByteArrayAccess(..))

import qualified Better.Repository as Repo

exception_to_errno :: SomeException -> Errno
exception_to_errno e = eIO

count_file_size :: (Repo.MonadRepository m, MonadThrow m, MonadIO m)
                => Digest SHA256 -> m P.FileOffset
count_file_size sha = Repo.catFile sha
  & S.mapM (Repo.getChunkSize . Repo.chunk_name)
  & S.fold F.sum

traverse_repo' :: (Repo.MonadRepository m, MonadThrow m, MonadIO m)
               => FilePath -> m (Either (Digest SHA256) (Digest SHA256))
traverse_repo' p = do
  let pathes = filter (not . T.null) $ T.splitOn "/" $ T.pack p
  -- TODO using head
  v_id <- liftIO $ readIO $ T.unpack $ head pathes
  
  opt_ver <- Repo.tryCatingVersion $ v_id
  when (isNothing opt_ver) $ throwString $ "oh no " <> p

  a <- foldlM step (Just (Left (Repo.ver_root $ fromJust opt_ver))) $ tail pathes
  case a of
    Nothing -> throwString $ "noent " <> p
    Just sha -> pure sha
  where
    step (Just (Left sha)) p = Repo.catTree sha
      & S.fold (F.find $ (p ==) . either Repo.tree_name Repo.file_name)
      & fmap (fmap (bimap Repo.tree_sha Repo.file_sha))
    step s@(Just (Right sha)) p = pure $ Nothing
    step Nothing p = pure $ Nothing

fuseGetattr' :: (Repo.MonadRepository m, MonadThrow m, MonadIO m)
             => FilePath
             -> Maybe (Either (Digest SHA256) (Digest SHA256))
             -> m (Either Errno FileStat)
fuseGetattr' "/" _  = pure $ Right $ defaultFileStat
  { linkCount = 2
  , fileMode = foldl' P.unionFileModes P.directoryMode [0755]
  }
fuseGetattr' _ (Just tree_or_file_sha)  = do
  case tree_or_file_sha of
    Left tree_sha -> pure $ Right $ defaultFileStat
      { linkCount = 2
      , fileMode = foldl' P.unionFileModes P.directoryMode [0755]
      }
    Right file_sha -> do
      size <- count_file_size file_sha
      pure $ Right $ defaultFileStat
        { linkCount = 1
        , fileMode = foldl' P.unionFileModes P.regularFileMode [0755]
	, fileSize = size
        }
fuseGetattr' p Nothing  = do
  ret <- traverse_repo' p
  case ret of
    Left tree_sha -> pure $ Right $ defaultFileStat
      { linkCount = 2
      , fileMode = foldl' P.unionFileModes P.directoryMode [0755]
      }
    Right file_sha -> do
      size <- count_file_size file_sha
      pure $ Right $ defaultFileStat
        { linkCount = 1
        , fileMode = foldl' P.unionFileModes P.regularFileMode [0755]
	, fileSize = size
        }

fuseOpen' :: (Repo.MonadRepository m, MonadThrow m, MonadIO m)
          => FilePath -> P.OpenMode -> P.OpenFileFlags
	  -> m (Either Errno (Either (Digest SHA256) (Digest SHA256)))
fuseOpen' p P.ReadOnly _ = do
  ret <- traverse_repo' p
  pure $ Right $ ret
fuseOpen' _ _ _ = pure $ Left $ eNOTSUP

fuseOpendir' "/" = pure $ Right $ Nothing
fuseOpendir' p = do
  ret <- traverse_repo' p
  case ret of
    Left tree_sha -> pure $ Right $ Just tree_sha
    Right file_sha -> pure $ Left eNOENT

fuseReaddir' :: (Repo.MonadRepository m, MonadThrow m, MonadUnliftIO m, MonadIO m)
             => FilePath -> Maybe (Digest SHA256) -> m (Either Errno [(String, Maybe FileStat)])
fuseReaddir' _ (Just tree_sha) = do
  files <- Repo.catTree tree_sha
    & fmap ((\n -> (n, Nothing)) . T.unpack . either Repo.tree_name Repo.file_name)
    & S.toList
  pure $ Right $ [(".", Nothing), ("..", Nothing)] <> files
fuseReaddir' "/" Nothing = handle (pure . Left . exception_to_errno) $ fmap Right $ do
  versions <- Repo.listVersions
    & fmap Repo.ver_id
    & S.toList
  pure $ [(".", Nothing), ("..", Nothing)] <> fmap (\v -> (show v, Nothing)) versions
fuseReaddir' _ Nothing = throwString "fuseReaddir: unexpected case"

fuseReleasedir' _ _ = pure $ eOK

fuseRelease' _ _ = pure ()

xmpOper :: Hbk -> FuseOperations (Either (Digest SHA256) (Digest SHA256)) (Maybe (Digest SHA256))
xmpOper hbk = defaultFuseOperations
  { fuseGetattr  = Just $ \p dh -> flip runHbkT hbk $ fuseGetattr' p dh
  , fuseOpendir = Just $ \p -> flip runHbkT hbk $ fuseOpendir' p
  , fuseReaddir = Just $ \p dh -> flip runHbkT hbk $ fuseReaddir' p dh
  , fuseReleasedir = Just $ fuseReleasedir'
  , fuseOpen = Just $ \p a b -> flip runHbkT hbk $ fuseOpen' p a b
  , fuseRelease = Just $ fuseRelease'
  }

main :: IO ()
main = do
  cwd <- P.getWorkingDirectory >>= Path.parseAbsDir
  
  hbk <- pure $ MkHbk
      (Repo.localRepo $ cwd </> [Path.reldir|bkp|])

  fuseMain (xmpOper hbk) (\e -> do
    hPrint stderr (e :: SomeException)
    pure eIO)

runHbkT :: HbkT m a -> Hbk -> m a
runHbkT (HbkT readerT) hbk = runReaderT readerT hbk
{-# INLINE runHbkT #-}

newtype Hbk = MkHbk
  { hbk_repo :: Repo.Repository
  }
  deriving (Generic)

type MonadBackup m = (Repo.MonadRepository m, MonadMask m, MonadUnliftIO m)


newtype HbkT m a = HbkT { unHbkT :: ReaderT Hbk m a }
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
    ) via (ReaderT Hbk m)
  deriving 
    ( Repo.MonadRepository
    ) via Repo.TheMonadRepository
         (C.Rename "hbk_repo"
         (C.Field "hbk_repo" ()
         (C.MonadReader
	 (ReaderT Hbk m))))

deriving instance MonadUnliftIO m => MonadUnliftIO (C.Rename k m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.Field k k' m)
deriving instance MonadUnliftIO m => MonadUnliftIO (C.MonadReader m)
deriving instance MonadThrow m => MonadThrow (C.Rename k m)
deriving instance MonadThrow m => MonadThrow (C.Field k k' m)
deriving instance MonadThrow m => MonadThrow (C.MonadReader m)

--tt n = do
--  s <- S.fromList [1, 2, 3, 4, 5]
--    & S.mapM (\i -> do
--	modify' (\n' -> n' - i)
--        pure i
--      )
--    & S.runStateT (pure n)
--    & S.filter (\(bc, _) -> bc < 0)
--    & fmap (\(bc, len) -> (max 0 (len + bc), len))
--    & S.toList
--  print s
