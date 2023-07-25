{-# LANGUAGE Strict #-}

module Better.Repository.Types
  ( Version(..)
  ) where

import Crypto.Hash (Digest, SHA256)

data Version
  = Version
  { ver_id :: {-# UNPACK #-} !Integer
  , ver_root :: {-# UNPACK #-} !(Digest SHA256)
  }
  deriving (Show)

