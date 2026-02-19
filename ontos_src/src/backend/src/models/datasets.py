"""
Datasets API Models

Pydantic models for Dataset CRUD operations.
Datasets are logical groupings of related data assets (tables, views).
Physical implementations are represented by DatasetInstance.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Any, Dict

from pydantic import BaseModel, Field

from src.models.tags import AssignedTag, AssignedTagCreate
from src.models.assets import UnifiedAssetType


# ============================================================================
# Enums
# ============================================================================

class DatasetStatus(str, Enum):
    """Dataset lifecycle status values."""
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DatasetInstanceStatus(str, Enum):
    """Instance lifecycle status values."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DatasetInstanceRole(str, Enum):
    """Purpose/role of a dataset instance within the dataset."""
    MAIN = "main"          # Primary fact table
    DIMENSION = "dimension"  # Dimension table for main
    LOOKUP = "lookup"        # Reference/lookup table
    REFERENCE = "reference"  # External reference data
    STAGING = "staging"      # Staging/intermediate table


class DatasetInstanceEnvironment(str, Enum):
    """SDLC environment stages for dataset instances."""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
    TEST = "test"
    QA = "qa"
    UAT = "uat"


# ============================================================================
# Custom Property Models
# ============================================================================

class DatasetCustomProperty(BaseModel):
    """Custom property key/value pair."""
    id: Optional[str] = None
    property: str = Field(..., description="Property name")
    value: Optional[str] = Field(None, description="Property value")

    model_config = {"from_attributes": True}


class DatasetCustomPropertyCreate(BaseModel):
    """Model for creating a custom property."""
    property: str = Field(..., description="Property name")
    value: Optional[str] = Field(None, description="Property value")


# ============================================================================
# Subscription Models
# ============================================================================

class DatasetSubscription(BaseModel):
    """Subscription to a dataset."""
    id: str = Field(..., description="Subscription ID")
    dataset_id: str = Field(..., description="Dataset ID")
    subscriber_email: str = Field(..., description="Subscriber's email address")
    subscribed_at: datetime = Field(..., description="When the subscription was created")
    subscription_reason: Optional[str] = Field(None, description="Reason for subscribing")

    model_config = {"from_attributes": True}


class DatasetSubscriptionCreate(BaseModel):
    """Model for creating a subscription."""
    reason: Optional[str] = Field(None, description="Optional reason for subscribing")


class DatasetSubscriptionResponse(BaseModel):
    """Response model for subscription operations."""
    subscribed: bool = Field(..., description="Whether the user is currently subscribed")
    subscription: Optional[DatasetSubscription] = Field(None, description="Subscription details if subscribed")


class DatasetSubscriberInfo(BaseModel):
    """Information about a subscriber."""
    email: str = Field(..., description="Subscriber's email address")
    subscribed_at: datetime = Field(..., description="When they subscribed")
    reason: Optional[str] = Field(None, description="Their subscription reason")

    model_config = {"from_attributes": True}


class DatasetSubscribersListResponse(BaseModel):
    """Response model for listing subscribers."""
    dataset_id: str = Field(..., description="Dataset ID")
    subscriber_count: int = Field(..., description="Total number of subscribers")
    subscribers: List[DatasetSubscriberInfo] = Field(default_factory=list, description="List of subscribers")


# ============================================================================
# Instance Models (Physical implementations)
# ============================================================================

class DatasetInstance(BaseModel):
    """Physical instance of a dataset in a specific system/environment."""
    id: str = Field(..., description="Instance ID")
    dataset_id: str = Field(..., description="Parent dataset ID")
    
    # Contract linkage
    contract_id: Optional[str] = Field(None, description="Contract version this instance implements")
    contract_name: Optional[str] = Field(None, description="Contract name (denormalized)")
    contract_version: Optional[str] = Field(None, description="Contract version (denormalized)")
    
    # Server linkage (from contract)
    contract_server_id: Optional[str] = Field(None, description="Server entry ID from the contract")
    server_type: Optional[str] = Field(None, description="Server type (databricks, snowflake, etc.) from contract server")
    server_environment: Optional[str] = Field(None, description="Environment (dev, prod, etc.) from contract server")
    server_name: Optional[str] = Field(None, description="Server identifier from contract server")
    
    # Physical location
    physical_path: str = Field(..., description="Physical path in the target system (flexible format)")
    
    # Asset type (unified across platforms)
    asset_type: Optional[str] = Field(None, description="Unified asset type (e.g., uc_table, snowflake_view, kafka_topic)")
    
    # Role and identity within the dataset
    role: str = Field("main", description="Purpose of this instance (main, dimension, lookup, reference, staging)")
    display_name: Optional[str] = Field(None, description="Human-readable display name within the dataset")
    environment: Optional[str] = Field(None, description="Deployment environment (dev, qa, test, staging, prod)")
    
    # Instance status
    status: str = Field("active", description="Instance status (active, deprecated, retired)")
    notes: Optional[str] = Field(None, description="Notes about this instance")
    
    # Tags (from unified tagging system)
    tags: List[AssignedTag] = Field(default_factory=list, description="Tags assigned to this instance")
    
    # Audit
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="Created by user")
    updated_by: Optional[str] = Field(None, description="Last updated by user")

    model_config = {"from_attributes": True}


class DatasetInstanceCreate(BaseModel):
    """Model for creating an instance."""
    contract_id: Optional[str] = Field(None, description="Contract version this instance implements")
    contract_server_id: Optional[str] = Field(None, description="Server entry ID from the contract")
    physical_path: str = Field(..., description="Physical path in the target system")
    asset_type: Optional[str] = Field(None, description="Unified asset type (e.g., uc_table, snowflake_view)")
    role: str = Field("main", description="Purpose of this instance (main, dimension, lookup, reference, staging)")
    display_name: Optional[str] = Field(None, description="Human-readable display name within the dataset")
    environment: Optional[str] = Field(None, description="Deployment environment (dev, qa, test, staging, prod)")
    status: str = Field("active", description="Instance status")
    notes: Optional[str] = Field(None, description="Notes about this instance")
    tags: Optional[List[AssignedTagCreate]] = Field(None, description="Tags to assign to this instance")


class DatasetInstanceUpdate(BaseModel):
    """Model for updating an instance."""
    contract_id: Optional[str] = Field(None, description="Contract version this instance implements")
    contract_server_id: Optional[str] = Field(None, description="Server entry ID from the contract")
    physical_path: Optional[str] = Field(None, description="Physical path in the target system")
    asset_type: Optional[str] = Field(None, description="Unified asset type (e.g., uc_table, snowflake_view)")
    role: Optional[str] = Field(None, description="Purpose of this instance (main, dimension, lookup, reference, staging)")
    display_name: Optional[str] = Field(None, description="Human-readable display name within the dataset")
    environment: Optional[str] = Field(None, description="Deployment environment (dev, qa, test, staging, prod)")
    status: Optional[str] = Field(None, description="Instance status")
    notes: Optional[str] = Field(None, description="Notes about this instance")
    tags: Optional[List[AssignedTagCreate]] = Field(None, description="Tags to assign to this instance")


class DatasetInstanceListResponse(BaseModel):
    """Response model for listing instances."""
    dataset_id: str = Field(..., description="Parent dataset ID")
    instance_count: int = Field(..., description="Number of instances")
    instances: List[DatasetInstance] = Field(default_factory=list, description="List of instances")


# ============================================================================
# Dataset List Item (Lightweight for list views)
# ============================================================================

class DatasetListItem(BaseModel):
    """Lightweight dataset representation for list views."""
    id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Dataset name")
    description: Optional[str] = Field(None, description="Dataset description")
    
    # Lifecycle
    status: str = Field(..., description="Lifecycle status")
    version: Optional[str] = Field(None, description="Version")
    published: bool = Field(False, description="Marketplace publication status")
    
    # Contract reference
    contract_id: Optional[str] = Field(None, description="Associated contract ID")
    contract_name: Optional[str] = Field(None, description="Associated contract name")
    
    # Ownership
    owner_team_id: Optional[str] = Field(None, description="Owner team ID")
    owner_team_name: Optional[str] = Field(None, description="Owner team name")
    project_id: Optional[str] = Field(None, description="Project ID")
    project_name: Optional[str] = Field(None, description="Project name")
    
    # Counts
    subscriber_count: Optional[int] = Field(None, description="Number of subscribers")
    instance_count: Optional[int] = Field(None, description="Number of physical instances")
    
    # Audit
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    model_config = {"from_attributes": True}


# ============================================================================
# Dataset Full Model
# ============================================================================

class Dataset(BaseModel):
    """
    Full dataset model with all details.
    
    A Dataset is a logical grouping of related data assets.
    Physical implementations are represented by DatasetInstance objects.
    """
    id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Dataset name")
    description: Optional[str] = Field(None, description="Dataset description")
    
    # Contract reference (optional default contract for the dataset)
    contract_id: Optional[str] = Field(None, description="Associated contract ID")
    contract_name: Optional[str] = Field(None, description="Associated contract name (denormalized)")
    
    # Ownership and project
    owner_team_id: Optional[str] = Field(None, description="Owner team ID")
    owner_team_name: Optional[str] = Field(None, description="Owner team name (denormalized)")
    project_id: Optional[str] = Field(None, description="Project ID")
    project_name: Optional[str] = Field(None, description="Project name (denormalized)")
    
    # Lifecycle
    status: str = Field("draft", description="Lifecycle status")
    version: Optional[str] = Field(None, description="Version")
    published: bool = Field(False, description="Marketplace publication status")
    
    # Metadata inheritance
    max_level_inheritance: int = Field(99, ge=0, le=999, description="Maximum metadata level to inherit from contracts")
    
    # Related data
    tags: List[AssignedTag] = Field(default_factory=list, description="Tags (from unified tagging system)")
    custom_properties: List[DatasetCustomProperty] = Field(default_factory=list, description="Custom properties")
    instances: List[DatasetInstance] = Field(default_factory=list, description="Physical instances")
    subscriber_count: Optional[int] = Field(None, description="Number of subscribers")
    instance_count: Optional[int] = Field(None, description="Number of physical instances")
    
    # Audit
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="Created by user")
    updated_by: Optional[str] = Field(None, description="Last updated by user")

    model_config = {"from_attributes": True}


# ============================================================================
# Create/Update Models
# ============================================================================

class DatasetCreate(BaseModel):
    """
    Model for creating a new dataset.
    
    A Dataset is a logical grouping - physical assets are added as instances later.
    """
    name: str = Field(..., description="Dataset name")
    description: Optional[str] = Field(None, description="Dataset description")
    
    # Contract reference (optional default contract)
    contract_id: Optional[str] = Field(None, description="Associated contract ID")
    
    # Ownership and project
    owner_team_id: Optional[str] = Field(None, description="Owner team ID")
    project_id: Optional[str] = Field(None, description="Project ID")
    
    # Lifecycle
    status: str = Field("draft", description="Lifecycle status")
    version: Optional[str] = Field(None, description="Version")
    published: bool = Field(False, description="Marketplace publication status")
    
    # Metadata inheritance
    max_level_inheritance: int = Field(99, ge=0, le=999, description="Maximum metadata level to inherit from contracts")
    
    # Optional related data (uses unified tagging system)
    tags: Optional[List[AssignedTagCreate]] = Field(None, description="Tags to assign (via unified tagging system)")
    custom_properties: Optional[List[DatasetCustomPropertyCreate]] = Field(None, description="Custom properties")

    model_config = {"from_attributes": True}


class DatasetUpdate(BaseModel):
    """Model for updating an existing dataset."""
    name: Optional[str] = Field(None, description="Dataset name")
    description: Optional[str] = Field(None, description="Dataset description")
    
    # Contract reference
    contract_id: Optional[str] = Field(None, description="Associated contract ID")
    
    # Ownership and project
    owner_team_id: Optional[str] = Field(None, description="Owner team ID")
    project_id: Optional[str] = Field(None, description="Project ID")
    
    # Lifecycle
    status: Optional[str] = Field(None, description="Lifecycle status")
    version: Optional[str] = Field(None, description="Version")
    published: Optional[bool] = Field(None, description="Marketplace publication status")
    
    # Metadata inheritance
    max_level_inheritance: Optional[int] = Field(None, ge=0, le=999, description="Maximum metadata level to inherit from contracts")
    
    # Optional related data (replaces existing if provided)
    tags: Optional[List[AssignedTagCreate]] = Field(None, description="Tags to assign (via unified tagging system)")
    custom_properties: Optional[List[DatasetCustomPropertyCreate]] = Field(None, description="Custom properties")

    model_config = {"from_attributes": True}


# ============================================================================
# Filter/Query Models
# ============================================================================

class DatasetFilter(BaseModel):
    """Filter options for querying datasets."""
    status: Optional[str] = Field(None, description="Filter by status")
    contract_id: Optional[str] = Field(None, description="Filter by contract")
    owner_team_id: Optional[str] = Field(None, description="Filter by owner team")
    project_id: Optional[str] = Field(None, description="Filter by project")
    published: Optional[bool] = Field(None, description="Filter by publication status")
    search: Optional[str] = Field(None, description="Search in name and description")

