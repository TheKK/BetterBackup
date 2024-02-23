{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Cli.FamiliarBackup (
  parser_info,
) where

import Options.Applicative (
  Alternative (many),
  ParserInfo,
  argument,
  help,
  helper,
  info,
  metavar,
  progDesc,
  str,
 )

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Monad (forever, replicateM_, unless, void)
import Control.Monad.Catch (MonadThrow (throwM), mask_)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.ByteString.Char8 qualified as BC
import Data.Foldable (Foldable (fold, toList), for_)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.IO qualified as TL
import Data.Traversable (for)

import Prettyprinter (LayoutOptions (LayoutOptions), PageWidth (AvailablePerLine), layoutPretty, vsep)
import Prettyprinter.Render.Text (renderLazy)

import System.Console.ANSI qualified as Ansi
import System.Console.Terminal.Size qualified as Console
import System.Directory qualified as D
import System.Posix qualified as P

import Path qualified

import Katip (katipAddNamespace)
import Katip qualified as Log

import Effectful qualified as E
import Effectful.Concurrent.Async (pooledForConcurrentlyN, runConcurrent)
import Effectful.Dispatch.Static qualified as E
import Effectful.Ki qualified as EKi

import Better.Bin.Pretty (PrevStatistic, initPrevStatistic, mkBackupStatisticsDoc)
import Better.Logging.Effect (logging, loggingOnSyncException)
import Better.Repository.Backup (DirEntry (DirEntryDir))
import Better.Repository.Backup qualified as Repo
import Better.Statistics.Backup.Class (
  BackupStatistics,
  BackupStatisticsRep,
 )
import Better.Statistics.Backup.Class qualified as BackupSt

import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Monad (runRepositoryForBackupFromCwd)
import Util.Options (someBaseDirRead)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Construct new version in a familiar way"
        ]

    parser =
      go
        <$> argument
          someBaseDirRead
          ( fold
              [ metavar "OS_CONFIG_PATH"
              , help "path to OS config"
              ]
          )
        <*> many
          ( (,)
              <$> argument
                str
                ( fold
                    [ metavar "SHARE_NAME"
                    , help "name of share"
                    ]
                )
              <*> argument
                someBaseDirRead
                ( fold
                    [ metavar "SHARE_PATH"
                    , help "path to folder on local file system"
                    ]
                )
          )

    go :: Path.SomeBase Path.Dir -> [(String, Path.SomeBase Path.Dir)] -> IO ()
    go os_config_some_path list_of_share_and_filesystem_some_path = void $ do
      runRepositoryForBackupFromCwd $ katipAddNamespace "familiar_backup" $ do
        abs_pwd <- liftIO $ Path.parseAbsDir =<< P.getWorkingDirectory

        let
          to_abs p = case p of
            Path.Abs dir -> dir
            Path.Rel dir -> abs_pwd Path.</> dir

          process_reporter stat_map_tvar = forever $ do
            mask_ $ report_backup_stat (Just 0.3) stat_map_tvar
            liftIO $ threadDelay (300 * 1000)

        logging Log.InfoS "checking permission of pathes"
        loggingOnSyncException Log.ErrorS "checking permission of pathes failed" $
          for_ (os_config_some_path : map snd list_of_share_and_filesystem_some_path) $ \some_path -> do
            let abs_path = Path.toFilePath $ to_abs some_path

            exists <- liftIO $ D.doesDirectoryExist abs_path
            unless exists $ do
              throwM $ userError $ abs_path <> ": path does not exist"

            p <- liftIO $ D.getPermissions abs_path
            let required_permissions = [("readable" :: String, D.readable p), ("searchable", D.searchable p)]
            unless (all snd required_permissions) $ do
              throwM $ userError $ abs_path <> ": permissions are not enough: " <> show required_permissions

        stat_map_tvar <- liftIO $ newTVarIO $ Seq.empty
        let
          gen_statistic_rep_for name = do
            !rep <- BackupSt.mkBackupStatisticsRep
            prev_stat_ref <- liftIO $ newIORef initPrevStatistic
            liftIO $ atomically $ modifyTVar' stat_map_tvar (Seq.|> (name, rep, prev_stat_ref))
            pure rep

        stat_os_config <- gen_statistic_rep_for "OS CONFIG"
        list_of_share <- for list_of_share_and_filesystem_some_path $ \(name, some_path) -> do
          rep <- gen_statistic_rep_for name
          pure (name, some_path, rep)

        (v_digest, v) <- EKi.scoped $ \scope -> do
          EKi.fork_ scope $ process_reporter stat_map_tvar

          logging Log.InfoS "start familiar backup"
          Repo.runRepositoryBackup $ Repo.runBackupWorkersWithTBQueue 10 1 $ do
            logging Log.InfoS $ Log.ls $ "backup os config from " <> Path.toFilePath (to_abs os_config_some_path)
            os_dir_digest <- BackupSt.localBackupStatisticsWithRep stat_os_config $ Repo.runBackupWorkersWithTBQueue 40 8 $ do
              Repo.backup_dir $ to_abs os_config_some_path

            logging Log.InfoS $ Log.ls $ "backup shares" <> show list_of_share_and_filesystem_some_path
            share_dir_digest <- (Repo.backupDirFromList =<<) $ runConcurrent $ do
              pooledForConcurrentlyN 2 list_of_share $ \(share_name, share_some_path, stat_rep) -> do
                logging Log.InfoS $ Log.ls $ "backup share from " <> Path.toFilePath (to_abs share_some_path)
                BackupSt.localBackupStatisticsWithRep stat_rep $ Repo.runBackupWorkersWithTBQueue 40 4 $ do
                  !digest <- Repo.backup_dir $ to_abs share_some_path
                  pure $! Repo.DirEntryDir digest (BC.pack share_name)

            Repo.backupDirFromList
              [ DirEntryDir os_dir_digest "@OS_CONFIG"
              , DirEntryDir share_dir_digest "@SHARES"
              ]

        liftIO $ putStrLn "result:"
        report_backup_stat Nothing stat_map_tvar
        liftIO $ print v

        pure (v_digest, v)

report_backup_stat :: (BackupStatistics E.:> es, E.IOE E.:> es) => Maybe Double -> TVar (Seq (String, BackupStatisticsRep, IORef PrevStatistic)) -> E.Eff es ()
report_backup_stat opt_dt tvar_stat_map =
  E.unsafeEff_ Console.size >>= \case
    Nothing -> pure ()
    Just (Console.Window _h w) -> do
      doc_lazy_text <-
        renderLazy . layoutPretty (LayoutOptions $ AvailablePerLine w 1) <$> do
          stat_map <- liftIO $ readTVarIO tvar_stat_map
          docs <- for (toList stat_map) $ \(name, rep, prev_stat_ref) -> BackupSt.runBackupStatisticsWithRep rep $ do
            prev_stat <- E.unsafeEff_ $ readIORef prev_stat_ref
            (doc, cur_stat) <- mkBackupStatisticsDoc (Just name) $ fmap (prev_stat,) opt_dt
            E.unsafeEff_ $ writeIORef prev_stat_ref cur_stat
            pure doc
          pure $ vsep docs
      let !doc_height = TL.foldl' (\(!acc) c -> if c == '\n' then acc + 1 else acc) 1 doc_lazy_text
      E.unsafeEff_ $ do
        Ansi.clearLine >> TL.putStrLn ""
        TL.putStrLn doc_lazy_text
        replicateM_ (doc_height + 1) $ do
          Ansi.clearLine
          Ansi.cursorUpLine 1
