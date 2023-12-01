module Better.Repository.Types (
  Version (..),
) where

import Data.Binary qualified as Bin

import Data.Time (UTCTime ())
import Data.Time.Format.ISO8601 (iso8601ParseM, iso8601Show)

import Better.Hash (TreeDigest)

data Version = Version
  { ver_timestamp :: {-# UNPACK #-} !UTCTime
  , ver_root :: {-# UNPACK #-} !TreeDigest
  }
  deriving (Show, Eq, Ord)

instance Bin.Binary Version where
  put v = do
    Bin.put $ iso8601Show $ ver_timestamp v
    Bin.put $ ver_root v

  get = do
    timestamp <- iso8601ParseM =<< Bin.get
    digest <- Bin.get
    pure $ Version timestamp digest
