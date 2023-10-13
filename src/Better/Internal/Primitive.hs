{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UnliftedFFITypes #-}

module Better.Internal.Primitive (
  withMutableByteArrayContents,
)
where

import Control.Monad.Primitive (PrimBase (..), PrimMonad (PrimState, primitive), RealWorld)
import Data.Binary (Word8)
import Data.Kind (Type)
import Data.Primitive (Ptr (..))
import Data.Primitive.ByteArray (MutableByteArray (..), MutableByteArray#)
import GHC.Base (Addr#, State#, UnliftedType, keepAlive#, mutableByteArrayContents#)
import Unsafe.Coerce (unsafeCoerce#)

-- | A composition of 'mutableByteArrayContents' and 'keepAliveUnlifted'.
-- The callback function must not return the pointer. The argument byte
-- array must be /pinned/. See 'byteArrayContents' for an explanation
-- of which byte arrays are pinned.
withMutableByteArrayContents :: PrimBase m => MutableByteArray (PrimState m) -> (Ptr Word8 -> m a) -> m a
{-# INLINE withMutableByteArrayContents #-}
withMutableByteArrayContents (MutableByteArray arr#) f =
  keepAliveUnlifted arr# (f (Ptr (mutableByteArrayContentsShim arr#)))

-- \| Variant of 'keepAlive' in which the value kept alive is of an unlifted
-- boxed type.
keepAliveUnlifted :: forall (m :: Type -> Type) (a :: UnliftedType) (r :: Type). PrimBase m => a -> m r -> m r
{-# INLINE keepAliveUnlifted #-}
keepAliveUnlifted x k =
  primitive $ \s0 -> keepAliveUnliftedLifted# x s0 (internal k)

keepAliveUnliftedLifted#
  :: forall (s :: Type) (a :: UnliftedType) (b :: Type)
   . a
  -> State# s
  -> (State# s -> (# State# s, b #))
  -> (# State# s, b #)
{-# INLINE keepAliveUnliftedLifted# #-}
keepAliveUnliftedLifted# x s0 f =
  (unsafeCoerce# :: (# State# RealWorld, b #) -> (# State# s, b #))
    ( keepAlive#
        x
        ((unsafeCoerce# :: State# s -> State# RealWorld) s0)
        ( ( unsafeCoerce#
              :: (State# s -> (# State# s, b #))
              -> (State# RealWorld -> (# State# RealWorld, b #))
          )
            f
        )
    )

mutableByteArrayContentsShim :: MutableByteArray# s -> Addr#
{-# INLINE mutableByteArrayContentsShim #-}
mutableByteArrayContentsShim x =
  mutableByteArrayContents# x
