{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Cli.Backup (
  parser_info,
) where

import Options.Applicative (
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
 )

import Control.Concurrent (threadDelay)
import Control.Monad (forever, replicateM_, void)
import Control.Monad.Catch (mask_)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.Foldable (Foldable (fold))

import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.IO qualified as TL

import System.Console.ANSI qualified as Ansi
import System.Console.Terminal.Size qualified as Console
import System.Posix qualified as P

import Path qualified

import Prettyprinter qualified as PP
import Prettyprinter.Render.Text qualified as PP

import Effectful qualified as E
import Effectful.Ki qualified as EKi

import Better.Bin.Pretty (mkBackupStatisticsDoc)
import Better.Repository.Backup qualified as Repo
import Better.Statistics.Backup.Class (BackupStatistics)

import Monad (runRepositoryForBackupFromCwd)
import Util.Options (someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Construct new version"
        ]

    parser =
      go
        <$> argument
          someBaseDirRead
          ( fold
              [ metavar "BACKUP_ROOT"
              , help "directory you'd like to backup"
              ]
          )

    go dir_to_backup = void $ runRepositoryForBackupFromCwd $ do
      let
        process_reporter = forever $ do
          mask_ report_backup_stat
          liftIO $ threadDelay (1000 * 1000)

      abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory
      let
        abs_dir = case dir_to_backup of
          Path.Abs dir -> dir
          Path.Rel dir -> abs_pwd Path.</> dir

      (v_digest, v) <- EKi.scoped $ \scope -> do
        EKi.fork_ scope process_reporter
        Repo.runRepositoryBackup $ Repo.runBackupWorkersWithTBQueue 20 8 $ do
          Repo.backup_dir abs_dir

      liftIO (putStrLn "result:") >> report_backup_stat
      liftIO $ print v

      pure (v_digest, v)

report_backup_stat :: (BackupStatistics E.:> es, E.IOE E.:> es) => E.Eff es ()
report_backup_stat =
  liftIO Console.size >>= \case
    Nothing -> pure ()
    Just (Console.Window _h w) -> do
      doc_lazy_text <- render_lazy w <$> mkBackupStatisticsDoc (Just "Backup")
      let !doc_height = TL.foldl' (\(!acc) c -> if c == '\n' then acc + 1 else acc) 1 doc_lazy_text

      liftIO $ do
        -- TODO Maybe there's better way to do this.
        Ansi.clearLine >> TL.putStrLn ""
        TL.putStrLn doc_lazy_text
        replicateM_ (doc_height + 1) $ do
          Ansi.clearLine
          Ansi.cursorUpLine 1
  where
    render_lazy w = PP.renderLazy . PP.layoutPretty (PP.LayoutOptions $ PP.AvailablePerLine w 1)
