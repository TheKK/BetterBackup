{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Strict #-}

module Better.Database.Level.FFI
  (
  ) where

import Foreign
import Foreign.C
import Foreign.Ptr

import Control.Monad

import UnliftIO

{-#
LEVELDB_EXPORT leveldb_t* leveldb_open(const leveldb_options_t* options,
                                       const char* name, char** errptr);

LEVELDB_EXPORT void leveldb_close(leveldb_t* db);

LEVELDB_EXPORT void leveldb_put(leveldb_t* db,
                                const leveldb_writeoptions_t* options,
                                const char* key, size_t keylen, const char* val,
                                size_t vallen, char** errptr);

LEVELDB_EXPORT leveldb_options_t* leveldb_options_create(void);
LEVELDB_EXPORT void leveldb_options_destroy(leveldb_options_t*);
LEVELDB_EXPORT void leveldb_options_set_create_if_missing(leveldb_options_t*,
                                                          uint8_t);

#-}

newtype LeveldbT = LeveldbT (Ptr ())
  deriving (Eq)
newtype LeveldbWriteOptionsT = LeveldbWriteOptionsT (Ptr ())
newtype LeveldbOptionsT = LeveldbOptionsT (Ptr ())

foreign import capi "leveldb/c.h leveldb_options_create" c_leveldb_options_create
  :: IO LeveldbOptionsT

foreign import capi "leveldb/c.h leveldb_options_set_create_if_missing" c_leveldb_options_set_create_if_missing
  :: LeveldbOptionsT -> Word8 -> IO ()

foreign import capi "leveldb/c.h leveldb_options_destroy" c_leveldb_options_destroy
  :: LeveldbOptionsT -> IO ()

foreign import capi "leveldb/c.h leveldb_open" c_leveldb_open
  :: LeveldbOptionsT -> CString -> Ptr CString -> IO LeveldbT

foreign import capi "leveldb/c.h leveldb_close" c_leveldb_close
  :: LeveldbT -> IO ()

foreign import capi "leveldb/c.h leveldb_put" c_leveldb_put
  :: LeveldbT -> LeveldbWriteOptionsT -> CString -> CSize -> CString -> CSize -> Ptr CString -> IO ()

with_err_str :: (Ptr CString -> IO a) -> IO a
with_err_str go = alloca $ \err_ptr -> flip finally (release err_ptr) $ do
  poke err_ptr nullPtr 
  go err_ptr
  where
    release ptr = do
      err <- peek ptr
      when (err /= nullPtr) $ free err
  

ggg :: IO ()
ggg = do
  bracket c_leveldb_options_create c_leveldb_options_destroy $ \db_opt -> do
    c_leveldb_options_set_create_if_missing db_opt 1
    db <- withCString "ok.db" $ \cstr -> alloca $ \err_ptr -> do
      poke err_ptr nullPtr
      ret <- c_leveldb_open db_opt cstr err_ptr
      err <- peek err_ptr
      if err == nullPtr
      then
        pure ()
      else do
       ss <- peekCString err
       putStrLn ss 

      pure ret
    -- c_leveldb_put db undefined undefined undefined undefined undefined undefined
    when (db /= LeveldbT nullPtr) $
      c_leveldb_close db
    -- c_leveldb_options_destroy db_opt
  pure ()
