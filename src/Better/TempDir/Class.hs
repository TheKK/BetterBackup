{-# LANGUAGE Strict #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

module Better.TempDir.Class
  (
  ) where

-- touch_file :: (C.HasReader "hbk" (Hbk m) m, MonadIO m) => T.Text -> m T.Text
-- touch_file file_name = do
--   hbk <- C.asks @"hbk" hbk_path
--   liftIO $ do
--     (tmp_name, tmp_fd) <- P.mkstemp (T.unpack $ hbk <> "/tmp/")
--     hClose tmp_fd
--     T.writeFile tmp_name ""
--     pure $ T.pack tmp_name
-- 
-- try_touching_dir :: MonadIO m => Hbk m -> m T.Text
-- try_touching_dir hbk = liftIO $ do
--   (tmp_name, tmp_fd) <- P.mkstemp (T.unpack $ hbk_path hbk <> "/tmp/")
--   hClose tmp_fd
--   T.writeFile tmp_name ""
--   pure $ T.pack tmp_name
