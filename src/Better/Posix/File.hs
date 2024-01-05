module Better.Posix.File (
  withFile,
) where

import Control.Monad.Catch (MonadMask, bracket)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Path (Path)
import Path qualified
import System.Posix.IO qualified as P
import System.Posix.Types qualified as P

withFile :: (MonadMask m, MonadIO m) => Path Path.Abs Path.File -> P.OpenMode -> P.OpenFileFlags -> (P.Fd -> m a) -> m a
withFile rel_file_name mode flags = bracket (liftIO $ P.openFd (Path.fromAbsFile rel_file_name) mode Nothing flags) (liftIO . P.closeFd)
