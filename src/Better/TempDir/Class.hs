{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

module Better.TempDir.Class (
  MonadTmp (..),

  -- * Effectufl
  Tmp,
) where

import Path (Path)
import qualified Path

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES

class MonadTmp m where
  withEmptyTmpFile :: (Path Path.Abs Path.File -> m a) -> m a

data Tmp :: E.Effect
type instance E.DispatchOf Tmp = 'E.Static 'ES.WithSideEffects
