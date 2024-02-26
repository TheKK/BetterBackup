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
  withEmptyTmpFileFd,
) where

import Better.TempDir.Class (Tmp)
import Control.Exception (bracketOnError)
import Effectful ((:>))
import Effectful qualified as E
import Effectful.Dispatch.Static qualified as ES
import Effectful.Dispatch.Static.Unsafe qualified as EU
import Path (Path, (</>))
import Path qualified
import System.Directory (removeFile)
import System.IO (Handle, hClose)
import System.Posix.Temp qualified as P

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
