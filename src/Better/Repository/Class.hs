{-# LANGUAGE Strict #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}


module Better.Repository.Class
  ( Repository,
  ) where

import qualified Effectful as E
import qualified Effectful.Dispatch.Static as ES

data Repository :: E.Effect

type instance E.DispatchOf Repository = 'E.Static 'ES.WithSideEffects
