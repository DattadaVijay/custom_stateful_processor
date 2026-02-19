/**
 * Master Data Management (MDM) Types
 */

// --- Enums --- //

export enum MdmConfigStatus {
  ACTIVE = 'active',
  PAUSED = 'paused',
  ARCHIVED = 'archived',
}

export enum MdmSourceLinkStatus {
  ACTIVE = 'active',
  PAUSED = 'paused',
}

export enum MdmMatchRunStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

export enum MdmMatchCandidateStatus {
  PENDING = 'pending',
  APPROVED = 'approved',
  REJECTED = 'rejected',
  MERGED = 'merged',
}

export enum MdmMatchType {
  EXACT = 'exact',
  FUZZY = 'fuzzy',
  PROBABILISTIC = 'probabilistic',
  NEW = 'new',
}

export enum MdmEntityType {
  CUSTOMER = 'customer',
  PRODUCT = 'product',
  SUPPLIER = 'supplier',
  LOCATION = 'location',
  OTHER = 'other',
}

export enum MatchRuleType {
  DETERMINISTIC = 'deterministic',
  PROBABILISTIC = 'probabilistic',
}

export enum SurvivorshipStrategy {
  MOST_RECENT = 'most_recent',
  MOST_COMPLETE = 'most_complete',
  MOST_TRUSTED = 'most_trusted',
  SOURCE_PRIORITY = 'source_priority',
  MASTER_WINS = 'master_wins',
  SOURCE_WINS = 'source_wins',
}

// --- Matching Rule Types --- //

export interface MatchingRule {
  name: string;
  type: MatchRuleType;
  fields: string[];
  weight: number;
  threshold: number;
  algorithm?: string;
}

export interface SurvivorshipRule {
  field: string;
  strategy: SurvivorshipStrategy;
  priority?: string[];
}

// --- MDM Configuration Types --- //

export interface MdmConfig {
  id: string;
  name: string;
  description?: string;
  entity_type: MdmEntityType;
  master_contract_id: string;
  master_contract_name?: string;
  status: MdmConfigStatus;
  project_id?: string;
  matching_rules: MatchingRule[];
  survivorship_rules: SurvivorshipRule[];
  source_count: number;
  last_run_at?: string;
  last_run_status?: string;
  created_at: string;
  updated_at: string;
  created_by?: string;
}

export interface MdmConfigCreate {
  name: string;
  description?: string;
  entity_type: MdmEntityType;
  master_contract_id: string;
  project_id?: string;
  matching_rules?: MatchingRule[];
  survivorship_rules?: SurvivorshipRule[];
}

export interface MdmConfigUpdate {
  name?: string;
  description?: string;
  status?: MdmConfigStatus;
  matching_rules?: MatchingRule[];
  survivorship_rules?: SurvivorshipRule[];
}

// --- Source Link Types --- //

export interface MdmSourceLink {
  id: string;
  config_id: string;
  source_contract_id: string;
  source_contract_name?: string;
  source_table_fqn?: string;
  key_column: string;
  column_mapping: Record<string, string>;
  priority: number;
  status: MdmSourceLinkStatus;
  last_sync_at?: string;
  created_at: string;
  updated_at: string;
}

export interface MdmSourceLinkCreate {
  source_contract_id: string;
  key_column: string;
  column_mapping?: Record<string, string>;
  priority?: number;
}

export interface MdmSourceLinkUpdate {
  key_column?: string;
  column_mapping?: Record<string, string>;
  priority?: number;
  status?: MdmSourceLinkStatus;
}

// --- Match Run Types --- //

export interface MdmMatchRun {
  id: string;
  config_id: string;
  source_link_id?: string;
  status: MdmMatchRunStatus;
  databricks_run_id?: string;
  total_source_records: number;
  total_master_records: number;
  matches_found: number;
  new_records: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
  triggered_by?: string;
  pending_review_count: number;
  approved_count: number; // Approved but not yet merged
}

// --- Match Candidate Types --- //

export interface MdmMatchCandidate {
  id: string;
  run_id: string;
  master_record_id?: string;
  source_record_id: string;
  source_contract_id: string;
  confidence_score: number;
  match_type: MdmMatchType;
  matched_fields?: string[];
  status: MdmMatchCandidateStatus;
  master_record_data?: Record<string, unknown>;
  source_record_data?: Record<string, unknown>;
  merged_record_data?: Record<string, unknown>;
  review_request_id?: string;
  reviewed_at?: string;
  reviewed_by?: string;
  merged_at?: string;
}

export interface MdmMatchCandidateUpdate {
  status: MdmMatchCandidateStatus;
  merged_record_data?: Record<string, unknown>;
}

// --- Review Types --- //

export interface MdmCreateReviewRequest {
  reviewer_email: string;
  notes?: string;
}

export interface MdmCreateReviewResponse {
  review_id: string;
  candidate_count: number;
  message: string;
}

// --- Merge Types --- //

export interface MdmMergeRequest {
  dry_run?: boolean;
}

export interface MdmMergeResponse {
  run_id: string;
  approved_count: number;
  merged_count: number;
  failed_count: number;
  message: string;
}

