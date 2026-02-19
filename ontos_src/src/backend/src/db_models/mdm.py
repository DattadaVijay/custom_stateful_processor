"""
Master Data Management (MDM) Database Models

This module implements the database schema for MDM functionality, enabling:
- Configuration of master entities tied to Data Contracts
- Linking source contracts to master configurations
- Tracking match runs and candidates
- Integration with the Asset Review pipeline
"""

from uuid import uuid4
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Text,
    ForeignKey,
    Float,
    Integer,
    Boolean,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from src.common.database import Base


class MdmConfigDb(Base):
    """MDM Configuration linking master contract to source contracts"""
    __tablename__ = 'mdm_configs'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    master_contract_id = Column(String, ForeignKey('data_contracts.id'), nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    entity_type = Column(String, nullable=False)  # customer, product, supplier, location
    status = Column(String, default='active', index=True)  # active, paused, archived

    # Matching configuration (JSON)
    matching_rules = Column(JSON, nullable=True)
    survivorship_rules = Column(JSON, nullable=True)

    # Project relationship (optional)
    project_id = Column(String, ForeignKey('projects.id'), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_by = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)

    # Relationships
    master_contract = relationship("DataContractDb", foreign_keys=[master_contract_id], lazy="selectin")
    source_links = relationship("MdmSourceLinkDb", back_populates="config", cascade="all, delete-orphan", lazy="selectin")
    match_runs = relationship("MdmMatchRunDb", back_populates="config", cascade="all, delete-orphan", lazy="selectin")

    def __repr__(self):
        return f"<MdmConfigDb(id='{self.id}', name='{self.name}', entity_type='{self.entity_type}')>"


class MdmSourceLinkDb(Base):
    """Links a source contract to an MDM configuration"""
    __tablename__ = 'mdm_source_links'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    config_id = Column(String, ForeignKey('mdm_configs.id'), nullable=False, index=True)
    source_contract_id = Column(String, ForeignKey('data_contracts.id'), nullable=False, index=True)

    # Source-specific configuration
    key_column = Column(String, nullable=True)  # Primary key column in source
    column_mapping = Column(JSON, nullable=True)  # Map source columns to master schema
    priority = Column(Integer, default=0)  # For survivorship: higher = preferred

    status = Column(String, default='active', index=True)  # active, paused
    last_sync_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    config = relationship("MdmConfigDb", back_populates="source_links")
    source_contract = relationship("DataContractDb", foreign_keys=[source_contract_id], lazy="selectin")

    def __repr__(self):
        return f"<MdmSourceLinkDb(id='{self.id}', config_id='{self.config_id}')>"


class MdmMatchRunDb(Base):
    """Tracks MDM matching job runs"""
    __tablename__ = 'mdm_match_runs'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    config_id = Column(String, ForeignKey('mdm_configs.id'), nullable=False, index=True)
    source_link_id = Column(String, ForeignKey('mdm_source_links.id'), nullable=True, index=True)

    status = Column(String, default='pending', index=True)  # pending, running, completed, failed
    databricks_run_id = Column(String, nullable=True)

    # Statistics
    total_source_records = Column(Integer, default=0)
    total_master_records = Column(Integer, default=0)
    matches_found = Column(Integer, default=0)
    new_records = Column(Integer, default=0)

    # Timestamps
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)

    triggered_by = Column(String, nullable=True)

    # Relationships
    config = relationship("MdmConfigDb", back_populates="match_runs")
    source_link = relationship("MdmSourceLinkDb", foreign_keys=[source_link_id], lazy="selectin")
    match_candidates = relationship("MdmMatchCandidateDb", back_populates="run", cascade="all, delete-orphan", lazy="selectin")

    def __repr__(self):
        return f"<MdmMatchRunDb(id='{self.id}', status='{self.status}', matches_found={self.matches_found})>"


class MdmMatchCandidateDb(Base):
    """Individual match candidates awaiting review"""
    __tablename__ = 'mdm_match_candidates'

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id = Column(String, ForeignKey('mdm_match_runs.id'), nullable=False, index=True)

    # Record identifiers
    master_record_id = Column(String, nullable=True, index=True)  # NULL if new record (no match in master)
    source_record_id = Column(String, nullable=False, index=True)
    source_contract_id = Column(String, nullable=False, index=True)

    # Match details
    confidence_score = Column(Float, nullable=False)
    match_type = Column(String, nullable=False)  # exact, fuzzy, probabilistic, new
    matched_fields = Column(JSON, nullable=True)  # Which fields matched and how

    # Review status (linked to Asset Review)
    review_request_id = Column(String, ForeignKey('data_asset_review_requests.id'), nullable=True, index=True)
    reviewed_asset_id = Column(String, nullable=True)  # ID within the review request

    status = Column(String, default='pending', index=True)  # pending, approved, rejected, merged

    # Record data snapshots (JSON)
    master_record_data = Column(JSON, nullable=True)
    source_record_data = Column(JSON, nullable=True)
    merged_record_data = Column(JSON, nullable=True)  # Populated after merge approval

    # Timestamps
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    reviewed_by = Column(String, nullable=True)
    merged_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    run = relationship("MdmMatchRunDb", back_populates="match_candidates")

    def __repr__(self):
        return f"<MdmMatchCandidateDb(id='{self.id}', confidence={self.confidence_score}, status='{self.status}')>"

