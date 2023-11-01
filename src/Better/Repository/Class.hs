{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Repository.Class (
  Repository,
  RepositoryWrite,
) where

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES

data Repository :: E.Effect

type instance E.DispatchOf Repository = 'E.Static 'ES.WithSideEffects

data RepositoryWrite :: E.Effect

type instance E.DispatchOf RepositoryWrite = 'E.Static 'ES.WithSideEffects
