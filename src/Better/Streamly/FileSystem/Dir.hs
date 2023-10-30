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

import qualified Streamly.Data.Stream.Prelude as S
import qualified Streamly.Data.Unfold as UF
import qualified Streamly.Internal.Data.Unfold as UF

import qualified System.Posix.Files as P
import qualified System.Posix.Directory as P

import Path (Path(), (</>))
import qualified Path

import Control.Monad.IO.Class

read :: Path rel_or_abs Path.Dir -> S.Stream IO FilePath
read = S.unfold reader
{-# INLINE read #-}

readFile :: Path rel_or_abs Path.Dir -> S.Stream IO (Path Path.Rel Path.File)
readFile = S.unfold fileReader
{-# INLINE readFile #-}

readEither :: (Path rel_or_abs Path.Dir) -> S.Stream IO (Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir))
readEither = S.unfold eitherReader
{-# INLINE readEither #-}

reader :: UF.Unfold IO (Path rel_or_abs Path.Dir) FilePath
reader
  = UF.filter (\x -> x /= "." && x /= "..")
  $ UF.bracketIO (P.openDirStream . Path.toFilePath) (P.closeDirStream)
  $ UF.unfoldrM $ \dirp -> do
    f <- liftIO $ P.readDirStream dirp
    pure $ case f of
      "" -> Nothing
      _ -> Just (f, dirp)
{-# INLINE reader #-}

fileReader :: UF.Unfold IO (Path rel_or_abs Path.Dir) (Path Path.Rel Path.File)
fileReader = UF.map (fromLeft $ error "impossible case") $ UF.filter isLeft $ eitherReader
{-# INLINE fileReader #-}

eitherReader :: UF.Unfold IO (Path rel_or_abs Path.Dir) (Either (Path Path.Rel Path.File) (Path Path.Rel Path.Dir))
eitherReader = UF.mapM2 f reader
  where
    {-# INLINE f #-}
    f dir filepath = liftIO $ do
      filepath' <- Path.parseRelFile filepath
      isDir <- fmap P.isDirectory $ P.getSymbolicLinkStatus $ Path.toFilePath $ dir </> filepath'
      filepath
        & if isDir
          then fmap Right . Path.parseRelDir
          else fmap Left . Path.parseRelFile
{-# INLINE eitherReader #-}
