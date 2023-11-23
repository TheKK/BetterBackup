{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Util.Options (
  digestRead,
  someBaseDirRead,
  absDirRead,
) where

import Options.Applicative (ReadM, eitherReader)

import Control.Exception (Exception (displayException))

import qualified Data.ByteString.Base16 as BSBase16

import Data.Bifunctor (Bifunctor (first))
import Data.String (fromString)

import qualified Data.Text as T

import qualified Path

import Better.Hash (Digest, digestFromByteString)

-- TODO This is a copy-paste snippt.
digestRead :: ReadM Digest
digestRead = eitherReader $ \raw_sha -> do
  sha_decoded <- case BSBase16.decodeBase16Untyped $ fromString raw_sha of
    Left err -> Left $ "invalid sha256: " <> raw_sha <> ", " <> T.unpack err
    Right sha' -> pure sha'

  case digestFromByteString sha_decoded of
    Nothing -> Left $ "invalid sha256: " <> raw_sha <> ", incorrect length"
    Just digest -> pure digest

-- TODO This is a copy-paste snippt.
someBaseDirRead :: ReadM (Path.SomeBase Path.Dir)
someBaseDirRead = eitherReader $ first displayException . Path.parseSomeDir

absDirRead :: ReadM (Path.Path Path.Abs Path.Dir)
absDirRead = eitherReader $ first displayException . Path.parseAbsDir
