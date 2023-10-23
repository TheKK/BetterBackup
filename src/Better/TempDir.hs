{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.TempDir (
  -- * Effectful
  runTmp,
  withEmptyTmpFile,
) where

import qualified System.Posix.Temp as P

import Path (Path, (</>))
import qualified Path

import Effectful ((:>))
import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES
import qualified Effectful.Dispatch.Static.Unsafe as EU

import Better.TempDir.Class (Tmp)
import Control.Exception (bracketOnError)
import System.Directory (removeFile)
import System.IO (hClose)

newtype instance ES.StaticRep Tmp = TmpRep (Path Path.Abs Path.Dir)

withEmptyTmpFile :: Tmp :> es => (Path Path.Abs Path.File -> E.Eff es a) -> E.Eff es a
withEmptyTmpFile run = do
  TmpRep tmp_dir <- ES.getStaticRep
  EU.reallyUnsafeUnliftIO $ \un -> do
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
          un $ run abs_file
      )

runTmp :: E.IOE :> es => Path Path.Abs Path.Dir -> E.Eff (Tmp : es) a -> E.Eff es a
runTmp tmp_dir = ES.evalStaticRep $ TmpRep tmp_dir
