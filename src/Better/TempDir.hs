{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.TempDir (
  MonadTmp (..),
  TheMonadTmp (..),
) where

import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)

import qualified Capability.Reader as C

import qualified System.Posix.Temp as P

import Path (Path, (</>))
import qualified Path

import Better.TempDir.Class (MonadTmp (..))
import Control.Exception (bracketOnError)
import qualified Control.Monad.IO.Unlift as Un
import System.Directory (removeFile)
import System.IO (hClose)

newtype TheMonadTmp m a = TheMonadTmp (m a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask, Un.MonadUnliftIO)

runTheMonadTmp :: TheMonadTmp m a -> m a
runTheMonadTmp (TheMonadTmp m) = m
{-# INLINE runTheMonadTmp #-}

instance (C.HasReader "tmp_dir" (Path Path.Abs Path.Dir) m, Un.MonadUnliftIO m) => MonadTmp (TheMonadTmp m) where
  {-# INLINE withEmptyTmpFile #-}
  withEmptyTmpFile run = TheMonadTmp $ do
    tmp_dir <- C.ask @"tmp_dir"
    Un.withRunInIO $ \unlift -> do
      let p = tmp_dir </> [Path.relfile|file-|]

      bracketOnError
        ( do
            (filename, fd) <- P.mkstemp $ Path.fromAbsFile p
            hClose fd
            pure filename
        )
        removeFile
        ( \filename -> do
            abs_file <- Path.parseAbsFile filename
            unlift $ runTheMonadTmp $ run abs_file
        )
