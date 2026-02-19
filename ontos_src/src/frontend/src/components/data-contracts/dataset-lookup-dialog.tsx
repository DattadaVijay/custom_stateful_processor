/**
 * Dataset Lookup Dialog
 * 
 * @deprecated Use UCAssetLookupDialog from './uc-asset-lookup-dialog' instead.
 * This file re-exports the legacy DatasetLookupDialog for backward compatibility.
 */

// Re-export the legacy wrapper and types
export { 
  DatasetLookupDialog as default, 
  DatasetLookupDialog,
  type MetastoreTableInfo 
} from './uc-asset-lookup-dialog'

// Also export the new component and types for migration
export { 
  default as UCAssetLookupDialog,
  type UCAssetLookupDialogProps
} from './uc-asset-lookup-dialog'

export { 
  UCAssetType, 
  type UCAssetInfo,
  ALL_ASSET_TYPES 
} from '@/types/uc-asset'
