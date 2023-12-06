module Cli (
  cmds,
) where

import Options.Applicative (
  CommandFields,
  Mod,
  Parser,
  ParserInfo,
  command,
  commandGroup,
  helper,
  hidden,
  info,
  progDesc,
  subparser,
 )

import Data.Foldable (Foldable (fold), asum)

import Cli.Backup (parser_info)
import Cli.CatChunk (parser_info)
import Cli.CatFileChunks (parser_info)
import Cli.CatTree (parser_info)
import Cli.FamiliarBackup (parser_info)
import Cli.FamiliarRestore (parser_info)
import Cli.GarbageCollection (parser_info)
import Cli.InitLocal qualified (parser_info)
import Cli.IntegrityCheck (parser_info)
import Cli.PatchBackup (parser_info)
import Cli.Ref qualified as Ref
import Cli.RestoreTree (parser_info)
import Cli.TreeList (parser_info)
import Cli.VersionFind (parser_info)
import Cli.Versions (parser_info)

-- TODO add ability to put trace markers
-- TODO add ability to collect running statistics
cmds :: ParserInfo (IO ())
cmds = info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc "Better operations"
        ]

    parser =
      group_commands
        [
          [ commandGroup "Repository commands"
          , sub_commands
              "init"
              "Initialize repository"
              [ command "local" Cli.InitLocal.parser_info
              ]
          , command "gc" Cli.GarbageCollection.parser_info
          , command "integrity-check" Cli.IntegrityCheck.parser_info
          ]
        ,
          [ commandGroup "Inspection commands"
          , sub_commands
              "version"
              "Version related operations"
              [ command "list" Cli.Versions.parser_info
              , command "find" Cli.VersionFind.parser_info
              ]
          , sub_commands
              "tree"
              "Tree related operations"
              [ command "ls" Cli.TreeList.parser_info
              , command "cat" Cli.CatTree.parser_info
              ]
          , sub_commands
              "file"
              "File related operations"
              [ command "cat" Cli.CatTree.parser_info
              , command "cat-chunk-list" Cli.CatFileChunks.parser_info
              ]
          , sub_commands
              "chunk"
              "Chunk related operations"
              [ command "cat" Cli.CatChunk.parser_info
              ]
          , command "ref" Ref.cmds
          ]
        ,
          [ commandGroup "Restoring commands"
          , sub_commands
              "restore"
              "Restoring related operations"
              [ command "tree" Cli.RestoreTree.parser_info
              ]
          ]
        ,
          [ command "backup" Cli.Backup.parser_info
          , command "patch-backup" Cli.PatchBackup.parser_info
          , command "familiar-backup" Cli.FamiliarBackup.parser_info
          , command "familiar-restore" Cli.FamiliarRestore.parser_info
          ]
        ]

group_commands :: [[Mod CommandFields a]] -> Parser a
group_commands cmd_groups = asum $ fmap (subparser . (hidden <>) . fold) cmd_groups

sub_commands :: String -> String -> [Mod CommandFields a] -> Mod CommandFields a
sub_commands name desc command_list = command name $ info (helper <*> parser) infoMod
  where
    infoMod =
      fold
        [ progDesc desc
        ]

    parser = subparser $ fold command_list
