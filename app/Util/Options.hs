{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Util.Options (
  versionDigestRead,
  treeDigestRead,
  fileDigestRead,
  chunkDigestRead,
  someBaseDirRead,
  absDirRead,
) where

import Options.Applicative (ReadM, eitherReader)

import Control.Exception (Exception (displayException))

import Data.ByteString.Base16 qualified as BSBase16

import Data.Bifunctor (Bifunctor (first))
import Data.String (fromString)

import Data.Text qualified as T

import Path qualified

import Better.Hash (
  ChunkDigest (..),
  Digest,
  FileDigest (..),
  TreeDigest (..),
  VersionDigest (..),
  digestFromByteString,
 )

-- TODO This is a copy-paste snippt.
digestRead :: ReadM Digest
digestRead = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest

versionDigestRead :: ReadM VersionDigest
versionDigestRead = UnsafeMkVersionDigest <$> digestRead

treeDigestRead :: ReadM TreeDigest
treeDigestRead = UnsafeMkTreeDigest <$> digestRead

fileDigestRead :: ReadM FileDigest
fileDigestRead = UnsafeMkFileDigest <$> digestRead

chunkDigestRead :: ReadM ChunkDigest
chunkDigestRead = UnsafeMkChunkDigest <$> digestRead

-- TODO This is a copy-paste snippt.
someBaseDirRead :: ReadM (Path.SomeBase Path.Dir)
someBaseDirRead = eitherReader $ first displayException . Path.parseSomeDir

absDirRead :: ReadM (Path.Path Path.Abs Path.Dir)
absDirRead = eitherReader $ first displayException . Path.parseAbsDir
