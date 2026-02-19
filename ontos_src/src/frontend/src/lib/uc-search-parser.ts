/**
 * UC Search Parser
 * 
 * Parses search queries for the UC Asset Lookup Dialog.
 * Supports dot-syntax navigation and type prefix filtering.
 * 
 * Syntax: [type_prefix:]catalog_part.schema_part.object_part
 * 
 * Examples:
 * - "lars.te.taa" - Matches catalog starting with "lars", schema starting with "te", object starting with "taa"
 * - "t:main.def.ord" - Same but only tables
 * - "f:prod.utils" - Functions in schemas matching "utils" under catalogs matching "prod"
 * - "v:" - Only views (no path filter)
 */

import { UCAssetType, TYPE_PREFIX_MAP } from '@/types/uc-asset';

/**
 * Result of parsing a search query
 */
export interface ParsedSearchQuery {
  /** Optional type filter extracted from prefix (e.g., "t:" -> UCAssetType.TABLE) */
  typeFilter: UCAssetType | null;
  /** Path segments (catalog, schema, object parts) */
  segments: string[];
  /** The raw query string after removing type prefix */
  pathQuery: string;
  /** Whether the query ends with a dot (indicating expansion intent) */
  endsWithDot: boolean;
  /** The original raw query */
  rawQuery: string;
}

/**
 * Parse a search query into its components.
 * 
 * @param query - The raw search query string
 * @returns Parsed query components
 */
export function parseSearchQuery(query: string): ParsedSearchQuery {
  const rawQuery = query;
  let typeFilter: UCAssetType | null = null;
  let pathQuery = query.trim();
  
  // Check for type prefix (e.g., "t:", "table:", "func:")
  const colonIndex = pathQuery.indexOf(':');
  if (colonIndex > 0 && colonIndex < 20) { // Reasonable prefix length limit
    const potentialPrefix = pathQuery.substring(0, colonIndex).toLowerCase();
    
    // Look up the prefix in the type map
    if (potentialPrefix in TYPE_PREFIX_MAP) {
      typeFilter = TYPE_PREFIX_MAP[potentialPrefix];
      pathQuery = pathQuery.substring(colonIndex + 1).trim();
    }
  }
  
  // Check if query ends with a dot (indicating user wants to expand)
  const endsWithDot = pathQuery.endsWith('.');
  
  // Split by dots into segments, filtering out empty segments
  const segments = pathQuery
    .split('.')
    .map(s => s.trim())
    .filter(s => s.length > 0);
  
  return {
    typeFilter,
    segments,
    pathQuery,
    endsWithDot,
    rawQuery,
  };
}

/**
 * Check if a name matches a search segment using prefix matching.
 * 
 * @param name - The name to check (e.g., "lars_george")
 * @param segment - The search segment (e.g., "lars")
 * @returns True if the name starts with the segment (case-insensitive)
 */
export function matchesSegment(name: string, segment: string): boolean {
  if (!segment) return true;
  return name.toLowerCase().startsWith(segment.toLowerCase());
}

/**
 * Find the first item that matches a segment.
 * 
 * @param items - Array of items with 'name' property
 * @param segment - The search segment to match
 * @returns The first matching item or undefined
 */
export function findFirstMatch<T extends { name: string }>(
  items: T[],
  segment: string
): T | undefined {
  if (!segment) return items[0];
  return items.find(item => matchesSegment(item.name, segment));
}

/**
 * Filter items that match a segment.
 * 
 * @param items - Array of items with 'name' property
 * @param segment - The search segment to match
 * @returns Array of matching items
 */
export function filterBySegment<T extends { name: string }>(
  items: T[],
  segment: string
): T[] {
  if (!segment) return items;
  return items.filter(item => matchesSegment(item.name, segment));
}

/**
 * Get the display string for the type filter hint.
 * 
 * @param typeFilter - The type filter
 * @returns Display string for the filter
 */
export function getTypeFilterDisplayName(typeFilter: UCAssetType | null): string {
  if (!typeFilter) return 'All types';
  
  switch (typeFilter) {
    case UCAssetType.TABLE:
      return 'Tables';
    case UCAssetType.VIEW:
      return 'Views';
    case UCAssetType.MATERIALIZED_VIEW:
      return 'Materialized Views';
    case UCAssetType.STREAMING_TABLE:
      return 'Streaming Tables';
    case UCAssetType.FUNCTION:
      return 'Functions';
    case UCAssetType.MODEL:
      return 'Models';
    case UCAssetType.VOLUME:
      return 'Volumes';
    case UCAssetType.METRIC:
      return 'Metrics';
    default:
      return 'Unknown';
  }
}

/**
 * Build a path from segments up to a given level.
 * 
 * @param segments - Array of path segments
 * @param level - Level to build up to (0 = catalog, 1 = schema, 2 = object)
 * @returns Dot-separated path string
 */
export function buildPathToLevel(segments: string[], level: number): string {
  return segments.slice(0, level + 1).join('.');
}

/**
 * Get the current navigation level based on segments and expansion intent.
 * 
 * @param parsed - Parsed search query
 * @returns Navigation level: 0 = catalogs, 1 = schemas, 2 = objects
 */
export function getNavigationLevel(parsed: ParsedSearchQuery): number {
  const { segments, endsWithDot } = parsed;
  
  if (segments.length === 0) {
    return 0; // At catalog level
  }
  
  if (endsWithDot) {
    // User typed a dot, so they want to go one level deeper
    return Math.min(segments.length, 2);
  }
  
  // Still filtering at current level
  return Math.max(0, segments.length - 1);
}

/**
 * Extract the filter for the current level.
 * 
 * @param parsed - Parsed search query
 * @returns The filter string for the current level, or empty string
 */
export function getCurrentLevelFilter(parsed: ParsedSearchQuery): string {
  const { segments, endsWithDot } = parsed;
  
  if (segments.length === 0 || endsWithDot) {
    return ''; // No filter at current level
  }
  
  return segments[segments.length - 1];
}

/**
 * Check if a type is allowed based on the parsed query and allowed types.
 * 
 * @param type - The asset type to check
 * @param typeFilter - Type filter from the query (null means all)
 * @param allowedTypes - Array of allowed types (from dialog props)
 * @returns True if the type should be shown
 */
export function isTypeAllowed(
  type: UCAssetType,
  typeFilter: UCAssetType | null,
  allowedTypes: UCAssetType[]
): boolean {
  // First check if it's in the allowed types
  if (!allowedTypes.includes(type)) {
    return false;
  }
  
  // Then check if it matches the type filter
  if (typeFilter !== null && type !== typeFilter) {
    return false;
  }
  
  return true;
}
