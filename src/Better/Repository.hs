module Better.Repository (
  -- * Write

  -- initRepositoryStructure
  -- , addBlob'
  -- , addFile'
  -- , addDir'
  -- , addVersion
  -- , nextBackupVersionId

  -- * Read
  catVersion,
  tryCatingVersion,
  catTree,
  catFile,
  catChunk,
  getChunkSize,
  listFolderFiles,
  listVersions,
  checksum,

  -- * Deletion
  garbageCollection,

  -- * Repositories
  localRepo,

  -- * Types
  Repository,
  module Better.Repository.Types,

  -- * Version
  Version (..),
  Tree (..),
  FFile (..),
  Object (..),
) where

import Better.Internal.Repository
import Better.Internal.Repository.GarbageCollection
import Better.Internal.Repository.IntegrityCheck
import Better.Repository.Types
