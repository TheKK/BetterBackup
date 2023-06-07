{-# LANGUAGE Strict #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
    

module Better.Repository.Class
  ( MonadRepository(..)
  --, TheMonadRepository(..)
  -- , Repository(..)
  ) where

import qualified Streamly.Data.Array as Array

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Fold as F

import qualified Path
import Path (Path)

import Data.Word

import Control.Monad.IO.Class
import System.Posix.Types (FileOffset(..))

class Monad m => MonadRepository m where
  mkPutFileFold :: forall n. (MonadIO n) => m (Path Path.Rel Path.File -> F.Fold n (Array.Array Word8) ())
  removeFiles :: [Path Path.Rel Path.File] -> m ()
  -- TODO File mode?
  createDirectory :: Path Path.Rel Path.Dir -> m () 
  fileExists :: Path Path.Rel Path.File -> m Bool
  fileSize :: Path Path.Rel Path.File -> m FileOffset
  mkRead :: forall n. (MonadIO n) => m (Path Path.Rel Path.File -> S.Stream n (Array.Array Word8))
  mkListFolderFiles :: forall n. (MonadIO n) => m (Path Path.Rel Path.Dir -> S.Stream n (Path Path.Rel Path.File))
