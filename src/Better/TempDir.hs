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
  module Better.TempDir.Class,

  -- * Effectful handlers
  runTmp,

  -- * Effectful functions
  withEmptyTmpFile,
  withEmptyTmpFileFd,
  withTmpFileHandle,
  withTmpDir,
) where

import Better.TempDir.Class (Tmp)
import Control.Exception (bracketOnError)
import Effectful ((:>))
import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static qualified as ES
import Effectful.Dispatch.Static.Unsafe qualified as EU
import Path (Path, (</>))
import Path qualified
import System.Directory (removeFile)
import System.IO (Handle, hClose)
import System.IO.Temp (withTempDirectory, withTempFile)
import System.Posix.Temp qualified as P

newtype instance ES.StaticRep Tmp = TmpRep (Path Path.Abs Path.Dir)

-- | Provide temporary directory in @run@. Given 'Path' would be removed from filesystem recursively after
-- @run@ completes or failes.
withTmpDir :: Tmp :> es => (Path Path.Abs Path.Dir -> E.Eff es a) -> E.Eff es a
withTmpDir run = do
  TmpRep tmp_dir <- ES.getStaticRep
  E.unsafeSeqUnliftIO $ \seq_un -> do
    withTempDirectory (Path.toFilePath tmp_dir) "tmp-dir" $ \raw_tmp_dir -> do
      dir <- Path.parseAbsDir raw_tmp_dir
      seq_un $ run dir

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

-- | Provide temporary file in @run@. Given 'Handle' would be closed and removed from
-- filesystem after @run@ completes or failes.
withTmpFileHandle :: Tmp :> es => (Handle -> E.Eff es a) -> E.Eff es a
withTmpFileHandle run = do
  TmpRep tmp_dir <- ES.getStaticRep
  E.unsafeSeqUnliftIO $ \seq_un -> do
    withTempFile (Path.toFilePath tmp_dir) "empty-tmp" $ \p h -> seq_un $ run h

-- | Provide tmp file in @run
--
-- When exits successfully this function would close Handle but keep the file.
-- When exception throws this function would close Handle then remove corresponding file.
withEmptyTmpFileFd :: Tmp :> es => (Path Path.Abs Path.File -> Handle -> E.Eff es a) -> E.Eff es a
withEmptyTmpFileFd run = do
  TmpRep tmp_dir <- ES.getStaticRep
  EU.reallyUnsafeUnliftIO $ \un -> do
    let p = tmp_dir </> [Path.relfile|file-|]

    bracketOnError
      (P.mkstemp $ Path.fromAbsFile p)
      (\(filename, h) -> hClose h >> removeFile filename)
      ( \(filename, h) -> do
          abs_file <- Path.parseAbsFile filename
          ret <- un $ run abs_file h
          hClose h
          pure ret
      )

runTmp :: E.IOE :> es => Path Path.Abs Path.Dir -> E.Eff (Tmp : es) a -> E.Eff es a
runTmp tmp_dir = ES.evalStaticRep $ TmpRep tmp_dir
