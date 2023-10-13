module Better.TempDir.Class
  ( MonadTmp(..)
  ) where

import qualified Path
import Path (Path)

class MonadTmp m where
  withEmptyTmpFile :: (Path Path.Abs Path.File -> m a) -> m a
