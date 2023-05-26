{-# LANGUAGE Strict #-}
{-# LANGUAGE FlexibleContexts #-}

module Better.Streamly.FileSystem.Dir
  ( read
  , readFile
  , readEither
  , reader
  , fileReader
  , eitherReader
  ) where

import Prelude hiding (read, readFile)

import Data.Function
import Data.Either (isLeft, fromLeft)

import Control.Monad.Catch
import Control.Monad.Trans.Control (MonadBaseControl)

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Unfold as UF
import qualified Streamly.Internal.Data.Unfold as UF

import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P

import Path (Path(), (</>))
import qualified Path

import Control.Monad.IO.Class

read :: (MonadIO m, MonadBaseControl IO m, MonadCatch m)
     => Path rel_or_abs Path.Dir -> S.Stream m FilePath 
read = S.unfold reader
{-# INLINE read #-}

readFile :: (MonadIO m, MonadCatch m)
       => Path rel_or_abs Path.Dir -> S.Stream m (Path Path.Rel Path.File)
readFile = S.unfold fileReader

readEither :: (MonadIO m, MonadBaseControl IO m, MonadCatch m)
           => (Path rel_or_abs Path.Dir) -> S.Stream m (Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir)) 
readEither = S.unfold eitherReader
{-# INLINE readEither #-}

reader :: (MonadIO m, MonadCatch m)
       => UF.Unfold m (Path rel_or_abs Path.Dir) FilePath
reader
  = UF.filter (\x -> x /= "." && x /= "..")
  $ UF.bracketIO (P.openDirStream . Path.toFilePath) (P.closeDirStream)
  $ UF.unfoldrM $ \dirp -> do
    f <- liftIO $ P.readDirStream dirp
    pure $ case f of
      "" -> Nothing
      _ -> Just (f, dirp)
{-# INLINE reader #-}

fileReader :: (MonadIO m, MonadCatch m)
       => UF.Unfold m (Path rel_or_abs Path.Dir) (Path Path.Rel Path.File)
fileReader = UF.map (fromLeft $ error "impossible case") $ UF.filter isLeft $ eitherReader
{-# INLINE fileReader #-}

eitherReader :: (MonadIO m, MonadCatch m)
             => UF.Unfold m (Path rel_or_abs Path.Dir) (Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir))
eitherReader = UF.mapM2 f reader
  where
    f dir filepath = liftIO $ do
      filepath' <- Path.parseRelFile filepath
      isDir <- fmap P.isDirectory $ P.getFileStatus $ Path.toFilePath $ dir </> filepath'
      filepath
        & if isDir
          then fmap Right . Path.parseRelDir
          else fmap Left . Path.parseRelFile
{-# INLINE eitherReader #-}
