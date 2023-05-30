{-# LANGUAGE Strict #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}

module Better.TempDir
  ( MonadTmp(..)
  , TheMonadTmp(..)
  ) where


import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Catch (MonadThrow, MonadCatch, MonadMask)

import UnliftIO
import UnliftIO.Directory
import UnliftIO.Exception

import qualified Capability.Reader as C

import qualified System.Posix.Temp as P

import qualified Path
import Path (Path, (</>))

import Data.Coerce (coerce)

import Better.TempDir.Class (MonadTmp(..))

newtype TheMonadTmp m a = TheMonadTmp (m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask, MonadUnliftIO)

runTheMonadTmp :: TheMonadTmp m a -> m a
runTheMonadTmp (TheMonadTmp m) = m
{-# INLINE runTheMonadTmp #-}

instance (C.HasReader "tmp_dir" (Path Path.Abs Path.Dir) m, MonadUnliftIO m) => MonadTmp (TheMonadTmp m) where
  withEmptyTmpFile run = TheMonadTmp $ do
    tmp_dir <- C.ask @"tmp_dir" 
    let p = tmp_dir </> [Path.relfile|file|]
  
    bracketOnError
      (do
        (filename, fd) <- liftIO $ P.mkstemp $ Path.fromAbsFile p
        hClose fd
        pure filename
      )
      removeFile
      ((runTheMonadTmp . run) <=< (liftIO . Path.parseAbsFile))
