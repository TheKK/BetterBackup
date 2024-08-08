{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

module Better.TempDir.Class (
  -- * Effectful effects
  Tmp,
) where

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as ES

data Tmp :: E.Effect
type instance E.DispatchOf Tmp = 'E.Static 'ES.WithSideEffects
