{-# LANGUAGE Strict #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}

module Monad
  ( Hbk(..)
  , HbkT(..)
  ) where

import GHC.Generics (Generic(..))

import qualified Path
import Path (Path)

import qualified Capability.Reader as C

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch (MonadThrow(..), MonadCatch(..), MonadMask(..))
import Control.Monad.Reader (ReaderT(..))

import UnliftIO (MonadUnliftIO(..))

import Better.Repository (Repository, MonadRepository, TheMonadRepository(..))
import Better.TempDir (MonadTmp, TheMonadTmp(..))

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
