{-# LANGUAGE Strict #-}

module Better.Repository.Types
  ( Version(..)
  ) where

import Better.Hash (Digest)

data Version
  = Version
  { ver_id :: {-# UNPACK #-} !Integer
  , ver_root :: {-# UNPACK #-} !Digest
  }
  deriving (Show)

