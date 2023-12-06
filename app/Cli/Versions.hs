{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli.Versions (
  parser_info,
) where

import Options.Applicative (ParserInfo, help, helper, info, long, progDesc, short, switch)

import Data.Foldable (Foldable (fold), for_)
import Data.Function ((&))
import Data.List (sortOn)

import Data.Aeson qualified as A

import Data.ByteString.Lazy.Char8 qualified as BL

import Data.Text qualified as T

import Data.Time (UTCTime, defaultTimeLocale, formatTime, utc, utcToZonedTime)
import Data.Time.LocalTime (getCurrentTimeZone)

import Streamly.Data.Stream.Prelude qualified as S

import Better.Hash (VersionDigest)
import Better.Repository qualified as Repo

import Monad (runReadonlyRepositoryFromCwd)

parser_info :: ParserInfo (IO ())
parser_info = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "List backuped versions"
        ]

    parser =
      go
        <$> switch
          ( fold
              [ long "utc"
              , help "display timestamp with timezone in UTC instead of CST"
              ]
          )
        <*> switch
          ( fold
              [ long "json"
              , short 'j'
              , help "display in JSON format"
              ]
          )

    {-# NOINLINE go #-}
    go use_utc is_json = do
      tz <- if use_utc then pure utc else getCurrentTimeZone
      let
        display_time = formatTime defaultTimeLocale "%c" . utcToZonedTime tz
        render =
          if is_json
            then render_json display_time
            else render_normally display_time

      digests_and_vs <- runReadonlyRepositoryFromCwd $ do
        Repo.listVersions & S.toList

      -- Sort on list should be acceptible in the context of "backup version".
      -- Currently I believe the number would be under 100,000 and causes no problem here.
      for_ (sortOn (Repo.ver_timestamp . snd) digests_and_vs) $ \(v_digest, v) -> do
        render v_digest v

    render_normally display_time v_digest v =
      putStrLn $
        display_time (Repo.ver_timestamp v)
          <> " - version digest ["
          <> show v_digest
          <> "] - tree root digest ["
          <> show (Repo.ver_root v)
          <> "]"

    render_json :: (UTCTime -> String) -> VersionDigest -> Repo.Version -> IO ()
    render_json display_time v_digest v =
      BL.putStrLn . A.encode $
        A.object
          [ ("digest", A.String (T.pack $ show v_digest))
          , ("timestamp", A.String (T.pack $ display_time (Repo.ver_timestamp v)))
          , ("tree_root", A.String (T.pack $ show (Repo.ver_root v)))
          ]
