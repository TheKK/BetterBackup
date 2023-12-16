{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Better.Logging.Effect (
  -- * Effect
  Logging,

  -- * Effect operations
  logging,
  loggingWithoutLoc,
  loggingOnSyncException,

  -- * Run effect
  runLogging,
) where

import Control.Exception (Exception (), SomeAsyncException (SomeAsyncException), SomeException, fromException, toException)

import Effectful qualified as E
import Effectful.Dispatch.Static qualified as E
import Effectful.Dispatch.Static qualified as ES

import Katip qualified

import Control.Monad (when)
import Control.Monad.Catch (MonadThrow (throwM), catch)

import GHC.Stack (withFrozenCallStack)

data Logging :: E.Effect

type instance E.DispatchOf Logging = 'E.Static 'ES.WithSideEffects

data instance ES.StaticRep Logging
  = LoggingRep
      {-# UNPACK #-} !Katip.LogEnv
      {-# UNPACK #-} !Katip.Namespace
      {-# UNPACK #-} !Katip.LogContexts

logging :: (E.HasCallStack, Logging E.:> es) => Katip.Severity -> Katip.LogStr -> E.Eff es ()
logging s msg = withFrozenCallStack $ do
  LoggingRep env ns ctx <- ES.getStaticRep
  E.unsafeEff_ $ Katip.runKatipContextT env ctx ns $ Katip.logLocM s msg

loggingWithoutLoc :: (E.HasCallStack, Logging E.:> es) => Katip.Severity -> Katip.LogStr -> E.Eff es ()
loggingWithoutLoc s msg = withFrozenCallStack $ do
  LoggingRep env ns ctx <- ES.getStaticRep
  E.unsafeEff_ $ Katip.runKatipContextT env ctx ns $ Katip.logItemM Nothing s msg

loggingOnSyncException :: (E.HasCallStack, Logging E.:> es) => Katip.Severity -> Katip.LogStr -> E.Eff es a -> E.Eff es a
loggingOnSyncException s msg eff = withFrozenCallStack $ do
  LoggingRep env ns ctx <- ES.getStaticRep
  eff `catch` \(e :: SomeException) -> do
    let logStr = mconcat [msg, ": exception raised: ", Katip.ls (show e)]
    when (is_sync_exception e) $ do
      E.unsafeEff_ $ Katip.runKatipContextT env ctx ns $ Katip.logLocM s logStr
    throwM e

is_sync_exception :: Exception e => e -> Bool
is_sync_exception e = case fromException (toException e) of
  Just (SomeAsyncException _) -> True
  Nothing -> True

instance (E.IOE E.:> es, Logging E.:> es) => Katip.Katip (E.Eff es) where
  {-# INLINE getLogEnv #-}
  getLogEnv = do
    LoggingRep env _ns _ctx <- ES.getStaticRep
    pure env

  {-# INLINE localLogEnv #-}
  localLogEnv f = ES.localStaticRep (\(LoggingRep env ctx ns) -> LoggingRep (f env) ctx ns)

instance (E.IOE E.:> es, Logging E.:> es) => Katip.KatipContext (E.Eff es) where
  {-# INLINE getKatipContext #-}
  getKatipContext = do
    LoggingRep _env _ns ctx <- ES.getStaticRep
    pure ctx

  {-# INLINE localKatipContext #-}
  localKatipContext f = ES.localStaticRep (\(LoggingRep env ns ctx) -> LoggingRep env ns (f ctx))

  {-# INLINE getKatipNamespace #-}
  getKatipNamespace = do
    LoggingRep _env ns _ctx <- ES.getStaticRep
    pure ns

  {-# INLINE localKatipNamespace #-}
  localKatipNamespace f = ES.localStaticRep (\(LoggingRep env ns ctx) -> LoggingRep env (f ns) ctx)

runLogging :: (E.IOE E.:> es) => Katip.LogEnv -> E.Eff (Logging : es) a -> E.Eff es a
runLogging env = E.evalStaticRep (LoggingRep env mempty mempty)
