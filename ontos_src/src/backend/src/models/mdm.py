"""
Master Data Management (MDM) API Models

Pydantic models for MDM API requests and responses.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# --- Enums --- #

class MdmConfigStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class MdmSourceLinkStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"


class MdmMatchRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class MdmMatchCandidateStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    MERGED = "merged"


class MdmMatchType(str, Enum):
    EXACT = "exact"
    FUZZY = "fuzzy"
    PROBABILISTIC = "probabilistic"
    NEW = "new"  # New record, no match in master


class MdmEntityType(str, Enum):
    CUSTOMER = "customer"
    PRODUCT = "product"
    SUPPLIER = "supplier"
    LOCATION = "location"
    OTHER = "other"


class MatchRuleType(str, Enum):
    DETERMINISTIC = "deterministic"
    PROBABILISTIC = "probabilistic"


class SurvivorshipStrategy(str, Enum):
    MOST_RECENT = "most_recent"
    MOST_COMPLETE = "most_complete"
    MOST_TRUSTED = "most_trusted"
    SOURCE_PRIORITY = "source_priority"
    MASTER_WINS = "master_wins"
    SOURCE_WINS = "source_wins"


# --- Matching Rule Models --- #

class MatchingRule(BaseModel):
    """A single matching rule for comparing records"""
    name: str = Field(..., description="Unique name for the rule")
    type: MatchRuleType = Field(..., description="Type of matching: deterministic or probabilistic")
    fields: List[str] = Field(..., description="Fields to match on")
    weight: float = Field(default=1.0, ge=0.0, le=1.0, description="Weight of this rule in overall score")
    threshold: float = Field(default=0.8, ge=0.0, le=1.0, description="Minimum score to consider a match")
    algorithm: Optional[str] = Field(None, description="Algorithm for probabilistic matching (e.g., jaro_winkler, levenshtein)")

    model_config = {"from_attributes": True}


class SurvivorshipRule(BaseModel):
    """A rule for determining which field value survives in merge"""
    field: str = Field(..., description="Field name")
    strategy: SurvivorshipStrategy = Field(..., description="Strategy for picking the surviving value")
    priority: Optional[List[str]] = Field(None, description="Source priority list for source_priority strategy")

    model_config = {"from_attributes": True}


# --- MDM Configuration Models --- #

class MdmConfigBase(BaseModel):
    """Base model for MDM configuration"""
    name: str = Field(..., description="Configuration name")
    description: Optional[str] = Field(None, description="Description of this MDM configuration")
    entity_type: MdmEntityType = Field(..., description="Type of entity being mastered")
    matching_rules: List[MatchingRule] = Field(default_factory=list, description="Rules for matching records")
    survivorship_rules: List[SurvivorshipRule] = Field(default_factory=list, description="Rules for merging records")


class MdmConfigCreate(MdmConfigBase):
    """Model for creating a new MDM configuration"""
    master_contract_id: str = Field(..., description="ID of the master data contract")
    project_id: Optional[str] = Field(None, description="Optional project ID")


class MdmConfigUpdate(BaseModel):
    """Model for updating an MDM configuration"""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[MdmConfigStatus] = None
    matching_rules: Optional[List[MatchingRule]] = None
    survivorship_rules: Optional[List[SurvivorshipRule]] = None


class MdmConfigApi(MdmConfigBase):
    """API response model for MDM configuration"""
    id: str
    master_contract_id: str
    master_contract_name: Optional[str] = None
    status: MdmConfigStatus
    project_id: Optional[str] = None
    source_count: int = 0
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None

    model_config = {"from_attributes": True}


# --- Source Link Models --- #

class MdmSourceLinkBase(BaseModel):
    """Base model for MDM source link"""
    key_column: str = Field(..., description="Primary key column in source data")
    column_mapping: Dict[str, str] = Field(default_factory=dict, description="Map source columns to master columns")
    priority: int = Field(default=0, description="Priority for survivorship (higher = preferred)")


class MdmSourceLinkCreate(MdmSourceLinkBase):
    """Model for creating a new source link"""
    source_contract_id: str = Field(..., description="ID of the source data contract")


class MdmSourceLinkUpdate(BaseModel):
    """Model for updating a source link"""
    key_column: Optional[str] = None
    column_mapping: Optional[Dict[str, str]] = None
    priority: Optional[int] = None
    status: Optional[MdmSourceLinkStatus] = None


class MdmSourceLinkApi(MdmSourceLinkBase):
    """API response model for source link"""
    id: str
    config_id: str
    source_contract_id: str
    source_contract_name: Optional[str] = None
    source_table_fqn: Optional[str] = None  # catalog.schema.table
    status: MdmSourceLinkStatus
    last_sync_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# --- Match Run Models --- #

class MdmMatchRunCreate(BaseModel):
    """Model for starting a new match run"""
    source_link_id: Optional[str] = Field(None, description="Specific source link to process, or all if None")


class MdmMatchRunApi(BaseModel):
    """API response model for match run"""
    id: str
    config_id: str
    source_link_id: Optional[str] = None
    status: MdmMatchRunStatus
    databricks_run_id: Optional[str] = None
    total_source_records: int = 0
    total_master_records: int = 0
    matches_found: int = 0
    new_records: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    triggered_by: Optional[str] = None
    pending_review_count: int = 0  # Derived field
    approved_count: int = 0  # Derived field: approved but not yet merged

    model_config = {"from_attributes": True}


# --- Match Candidate Models --- #

class MdmMatchCandidateApi(BaseModel):
    """API response model for match candidate"""
    id: str
    run_id: str
    master_record_id: Optional[str] = None
    source_record_id: str
    source_contract_id: str
    confidence_score: float
    match_type: MdmMatchType
    matched_fields: Optional[List[str]] = None
    status: MdmMatchCandidateStatus
    master_record_data: Optional[Dict[str, Any]] = None
    source_record_data: Optional[Dict[str, Any]] = None
    merged_record_data: Optional[Dict[str, Any]] = None
    review_request_id: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None
    merged_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class MdmMatchCandidateUpdate(BaseModel):
    """Model for updating a match candidate (during review)"""
    status: MdmMatchCandidateStatus
    merged_record_data: Optional[Dict[str, Any]] = None


# --- Review Integration Models --- #

class MdmCreateReviewRequest(BaseModel):
    """Request to create an asset review for match candidates"""
    reviewer_email: str = Field(..., description="Email of the reviewer/steward")
    notes: Optional[str] = Field(None, description="Optional notes for the reviewer")


class MdmCreateReviewResponse(BaseModel):
    """Response after creating an asset review"""
    review_id: str
    candidate_count: int
    message: str


# --- Merge Models --- #

class MdmMergeRequest(BaseModel):
    """Request to merge approved matches"""
    dry_run: bool = Field(default=False, description="If true, simulate merge without writing")


class MdmMergeResponse(BaseModel):
    """Response after merge operation"""
    run_id: str
    approved_count: int
    merged_count: int
    failed_count: int = 0
    message: str


# --- Statistics Models --- #

class MdmConfigStats(BaseModel):
    """Statistics for an MDM configuration"""
    config_id: str
    total_runs: int = 0
    total_matches_found: int = 0
    total_merged: int = 0
    pending_review: int = 0
    last_run_at: Optional[datetime] = None

