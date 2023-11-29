module Repository.Restore (
  restoreFile,
  restoreDirMeta,
) where

import Control.Parallel (par)

import Data.Function ((&))

import Streamly.Data.Stream.Prelude qualified as S
import Streamly.FileSystem.Handle qualified as Handle

import Effectful qualified as E
import Effectful.Dispatch.Static.Unsafe qualified as E

import System.IO (IOMode (WriteMode), withBinaryFile)

import Path (Path)
import Path qualified

import Better.Hash (Digest)
import Better.Repository qualified as Repo
import Better.Repository.Class (Repository)

-- | Restore meta of existed directory.
--
-- The parent path must exists before calling this function. And it's fine if the file has not existed yet.
-- If the file exists then its content and meta would be overwritten after this function.
{-# NOINLINE restoreFile #-}
restoreFile :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.File -> Digest -> E.Eff es ()
restoreFile abs_out_path digest = E.reallyUnsafeUnliftIO $ \un -> withBinaryFile (Path.fromAbsFile abs_out_path) WriteMode $ \h -> do
  Repo.catFile digest
    & S.morphInner un
    -- Use parConcatMap to open multiple chunk files concurrently.
    -- This allow us to read from catFile and open chunk file ahead of time before catual writing.
    & S.parConcatMap (S.eager True . S.ordered True . S.maxBuffer (6 * 5)) (S.mapM (\e -> par e $ pure e) . S.morphInner un . Repo.catChunk . Repo.chunk_name)
    -- Use parEval to read from chunks concurrently.
    -- Since read is often faster than write, using parEval with buffer should reduce running time.
    & S.parEval (S.maxBuffer 30)
    & S.fold (Handle.writeChunks h)

-- | Restore meta of existed directory.
--
-- Might throws exception If directory does not exist.
restoreDirMeta :: (Repository E.:> es, E.IOE E.:> es) => Path Path.Abs Path.Dir -> Digest -> E.Eff es ()
restoreDirMeta _ _ = do
  -- TODO Permission should be backed up as well.
  pure ()
