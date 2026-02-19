"""
Datasets Database Models

Datasets are logical groupings of related data assets.
Physical implementations are represented by DatasetInstance objects,
which link to specific tables/views in Unity Catalog, Snowflake, etc.

Relationship: Data Product -> Data Contract <- Dataset (many-to-one from DS to DC)
              Dataset -> DatasetInstance (one-to-many)
"""

from uuid import uuid4
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Text,
    Boolean,
    Integer,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from src.common.database import Base


class DatasetDb(Base):
    """
    Dataset - A logical grouping of related data assets.
    
    A Dataset represents a logical entity that groups related tables/views.
    Physical implementations are stored in DatasetInstance records.
    The dataset can optionally be linked to a Data Contract for governance.
    
    Note: The physical asset fields (asset_type, catalog_name, schema_name,
    object_name, environment) are deprecated and kept nullable for backward
    compatibility. New datasets should not use these fields - physical assets
    are managed via DatasetInstance.
    """
    __tablename__ = "datasets"

    # Primary key
    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    
    # Basic information
    name = Column(String, nullable=False, index=True)
    description = Column(Text, nullable=True)
    
    # DEPRECATED: Physical asset reference - now managed via DatasetInstance
    # These columns are kept nullable for backward compatibility
    asset_type = Column(String, nullable=True)  # DEPRECATED
    catalog_name = Column(String, nullable=True)  # DEPRECATED
    schema_name = Column(String, nullable=True)  # DEPRECATED
    object_name = Column(String, nullable=True)  # DEPRECATED
    environment = Column(String, nullable=True)  # DEPRECATED
    
    # Contract reference (optional default contract for the dataset)
    contract_id = Column(String, ForeignKey("data_contracts.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Ownership and project association
    owner_team_id = Column(String, ForeignKey("teams.id", ondelete="SET NULL"), nullable=True, index=True)
    project_id = Column(String, ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Lifecycle status
    status = Column(String, nullable=False, default="draft", index=True)  # draft, active, deprecated, retired
    version = Column(String, nullable=True)
    
    # Marketplace publication status
    published = Column(Boolean, nullable=False, default=False, index=True)
    
    # Metadata Inheritance
    # Maximum level of metadata to inherit from associated contracts.
    # Only metadata with level <= this value AND inheritable=True will be inherited.
    # Default 99 means inherit almost everything that's marked inheritable.
    max_level_inheritance = Column(Integer, nullable=False, default=99)
    
    # Audit fields
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)
    
    # Relationships
    contract = relationship("DataContractDb", foreign_keys=[contract_id], lazy="selectin")
    owner_team = relationship("TeamDb", foreign_keys=[owner_team_id], lazy="selectin")
    project = relationship("ProjectDb", foreign_keys=[project_id], lazy="selectin")
    subscriptions = relationship("DatasetSubscriptionDb", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")
    # Tags now use unified tagging system (EntityTagAssociationDb) via TagsManager with entity_type="dataset"
    custom_properties = relationship("DatasetCustomPropertyDb", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")
    instances = relationship("DatasetInstanceDb", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")

    def __repr__(self):
        return f"<DatasetDb(id='{self.id}', name='{self.name}')>"


class DatasetSubscriptionDb(Base):
    """
    Dataset Subscription - Tracks consumer subscriptions to datasets.
    
    Subscriptions enable:
    - Consumer discovery of subscribed datasets
    - ITSM notifications for dataset changes (deprecation, new versions, compliance violations)
    - Audit trail of who is consuming which datasets
    """
    __tablename__ = "dataset_subscriptions"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    subscriber_email = Column(String, nullable=False, index=True)
    subscribed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    subscription_reason = Column(Text, nullable=True)  # Optional: why they subscribed
    
    # Relationship to dataset
    dataset = relationship("DatasetDb", back_populates="subscriptions")
    
    # Unique constraint: one subscription per user per dataset
    __table_args__ = (
        UniqueConstraint("dataset_id", "subscriber_email", name="uq_dataset_subscriber"),
    )

    def __repr__(self):
        return f"<DatasetSubscriptionDb(dataset_id='{self.dataset_id}', subscriber='{self.subscriber_email}')>"


class DatasetCustomPropertyDb(Base):
    """
    Dataset Custom Property - Key/value pairs for extensibility.
    """
    __tablename__ = "dataset_custom_properties"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    property = Column(String, nullable=False)  # Property name
    value = Column(Text, nullable=True)  # Property value (can be JSON string for complex values)
    
    # Relationship to dataset
    dataset = relationship("DatasetDb", back_populates="custom_properties")

    def __repr__(self):
        return f"<DatasetCustomPropertyDb(dataset_id='{self.dataset_id}', property='{self.property}')>"


class DatasetInstanceDb(Base):
    """
    Dataset Instance - Physical implementation of a dataset in a specific system/environment.
    
    Each instance links to:
    - A parent Dataset (logical entity)
    - A specific Data Contract version (for schema/quality requirements)
    - A server entry within that contract (defines system type and environment)
    - A flexible physical_path string (e.g., "catalog.schema.table" for UC, "db.schema.table" for Snowflake)
    
    This allows one Dataset to have multiple physical implementations across:
    - Different SDLC environments (dev, staging, prod)
    - Different contract versions (DEV using draft v2.0, PROD using active v1.5)
    - Different systems (Unity Catalog, Snowflake, BigQuery, etc.)
    """
    __tablename__ = "dataset_instances"

    # Primary key
    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    
    # Parent dataset
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Contract version this instance implements
    contract_id = Column(String, ForeignKey("data_contracts.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Server entry within the contract (defines system type + environment)
    contract_server_id = Column(String, ForeignKey("data_contract_servers.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Physical path/identifier in the target system (flexible format)
    # Examples: "catalog.schema.table" (UC), "database.schema.table" (Snowflake), "s3://bucket/path" (S3)
    physical_path = Column(String, nullable=False, index=True)
    
    # Unified asset type across platforms (e.g., uc_table, snowflake_view, kafka_topic)
    # This enables platform-agnostic asset handling while preserving type information
    # See src/models/assets.py UnifiedAssetType enum for valid values
    asset_type = Column(String, nullable=True, index=True)
    
    # Instance role (purpose of this table in the dataset)
    # main = primary fact table, dimension = dimension table, lookup = reference/lookup table,
    # reference = external reference data, staging = staging/intermediate table
    role = Column(String, nullable=False, default="main", index=True)
    
    # Human-readable display name for this table within the dataset
    display_name = Column(String, nullable=True)
    
    # Deployment environment (dev, qa, test, staging, prod)
    environment = Column(String, nullable=True, index=True)
    
    # Instance status
    status = Column(String, nullable=False, default="active", index=True)  # active, deprecated, retired
    
    # Optional notes about this instance
    notes = Column(Text, nullable=True)
    
    # Audit fields
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)
    
    # Relationships
    dataset = relationship("DatasetDb", back_populates="instances")
    contract = relationship("DataContractDb", foreign_keys=[contract_id], lazy="selectin")
    contract_server = relationship("DataContractServerDb", foreign_keys=[contract_server_id], lazy="selectin")
    
    # Unique constraint: one instance per dataset per server
    __table_args__ = (
        UniqueConstraint("dataset_id", "contract_server_id", name="uq_dataset_instance_server"),
        Index("ix_dataset_instance_path", "physical_path"),
    )

    def __repr__(self):
        return f"<DatasetInstanceDb(id='{self.id}', dataset_id='{self.dataset_id}', path='{self.physical_path}')>"

