/**
 * Types for Industry Ontology Library feature.
 * 
 * Mirrors the backend Pydantic models for type-safe API interactions.
 */

/**
 * Maturity level of an ontology or module.
 */
export type MaturityLevel = 'release' | 'provisional' | 'informative';

/**
 * Summary of an industry vertical for listing.
 */
export interface VerticalSummary {
  id: string;
  name: string;
  icon: string;
  description?: string;
  ontology_count: number;
}

/**
 * Summary of an ontology for listing.
 */
export interface OntologySummary {
  id: string;
  name: string;
  full_name?: string;
  description?: string;
  handler: string;
  license?: string;
  requires_license_agreement: boolean;
  website?: string;
  version?: string;
  recommended_foundation: boolean;
  module_count: number;
  is_modular: boolean;
}

/**
 * A node in the module selection tree.
 * 
 * Used by the tree view component for rendering and selection.
 */
export interface ModuleTreeNode {
  id: string;
  name: string;
  description?: string;
  node_type: 'domain' | 'module' | 'ontology';
  maturity?: MaturityLevel;
  owl_url?: string;
  dependencies: string[];
  children: ModuleTreeNode[];
  selectable: boolean;
}

/**
 * Request to import selected ontology modules.
 */
export interface ImportRequest {
  ontology_id: string;
  vertical_id: string;
  module_ids: string[];
  include_dependencies: boolean;
  accept_license: boolean;
}

/**
 * Result of an import operation.
 */
export interface ImportResult {
  success: boolean;
  semantic_model_id?: string;
  semantic_model_name?: string;
  modules_imported: string[];
  dependencies_added: string[];
  triple_count: number;
  warnings: string[];
  error?: string;
}

/**
 * Cache status for an ontology.
 */
export interface CacheStatus {
  ontology_id: string;
  is_cached: boolean;
  cache_date?: string;
  cache_size_bytes?: number;
  is_stale: boolean;
}

/**
 * Result of a cache refresh operation.
 */
export interface RefreshResult {
  success: boolean;
  ontology_id: string;
  modules_refreshed: number;
  error?: string;
}

/**
 * UI state for module selection in the tree.
 */
export interface ModuleSelectionState {
  selectedModules: Set<string>;
  expandedNodes: Set<string>;
  partiallySelectedParents: Set<string>;
}

/**
 * Props for the ontology library dialog.
 */
export interface OntologyLibraryDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  onImportSuccess?: (result: ImportResult) => void;
}

/**
 * Maps maturity level to display color/variant.
 */
export const maturityColors: Record<MaturityLevel, { color: string; label: string; icon: string }> = {
  release: { color: 'bg-green-500', label: 'Release', icon: '🟢' },
  provisional: { color: 'bg-yellow-500', label: 'Provisional', icon: '🟡' },
  informative: { color: 'bg-orange-500', label: 'Informative', icon: '🟠' },
};
