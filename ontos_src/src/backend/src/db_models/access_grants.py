"""
SQLAlchemy models for Access Grants.

Access grants track time-limited physical access permissions to assets.
This includes:
- AccessGrantRequestDb: Pending access requests with requested duration
- AccessGrantDb: Approved grants with expiration
- AccessGrantDurationConfigDb: Configurable duration options per entity type
"""

from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Optional
from uuid import uuid4

from sqlalchemy import Boolean, Column, DateTime, Enum, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.common.database import Base


class AccessGrantRequestStatus(str, PyEnum):
    """Status of an access grant request."""
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class AccessGrantStatus(str, PyEnum):
    """Status of an access grant."""
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class PermissionLevel(str, PyEnum):
    """Permission level for access grants."""
    READ = "READ"
    WRITE = "WRITE"
    MANAGE = "MANAGE"


class AccessGrantRequestDb(Base):
    """
    Access Grant Request model for tracking pending access requests.
    
    Users submit requests for time-limited access to assets. Admins or owners
    can approve or deny these requests.
    
    Attributes:
        id: Unique identifier for the request
        requester_email: Email of the user requesting access
        entity_type: Type of asset (data_product, dataset, table, etc.)
        entity_id: ID of the asset
        entity_name: Display name of the asset (for convenience)
        requested_duration_days: How many days of access is requested
        permission_level: Level of access requested (READ, WRITE, MANAGE)
        reason: User's justification for the request
        status: Current status of the request
        created_at: When the request was submitted
        handled_at: When the request was approved/denied
        handled_by: Email of admin who handled the request
        admin_message: Response message from admin
    """
    __tablename__ = "access_grant_requests"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    requester_email = Column(String(255), nullable=False, index=True)
    entity_type = Column(String(100), nullable=False, index=True)
    entity_id = Column(String(255), nullable=False, index=True)
    entity_name = Column(String(500), nullable=True)
    requested_duration_days = Column(Integer, nullable=False)
    permission_level = Column(String(50), nullable=False, default=PermissionLevel.READ.value)
    reason = Column(Text, nullable=True)
    status = Column(
        String(50),
        nullable=False,
        default=AccessGrantRequestStatus.PENDING.value,
        index=True
    )
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    handled_at = Column(DateTime(timezone=True), nullable=True)
    handled_by = Column(String(255), nullable=True)
    admin_message = Column(Text, nullable=True)
    
    # Relationship to the grant (if approved)
    grant = relationship("AccessGrantDb", back_populates="request", uselist=False)
    
    def __repr__(self) -> str:
        return f"<AccessGrantRequestDb(id={self.id}, requester='{self.requester_email}', entity={self.entity_type}/{self.entity_id}, status={self.status})>"
    
    @property
    def is_pending(self) -> bool:
        """Check if the request is still pending."""
        return self.status == AccessGrantRequestStatus.PENDING.value


class AccessGrantDb(Base):
    """
    Access Grant model for tracking approved time-limited access.
    
    When an access request is approved, a grant is created with an expiration date.
    Grants can be revoked manually or expire automatically.
    
    Attributes:
        id: Unique identifier for the grant
        request_id: FK to the original request (if any)
        grantee_email: Email of the user with access
        entity_type: Type of asset
        entity_id: ID of the asset
        entity_name: Display name of the asset (for convenience)
        permission_level: Level of access granted
        granted_at: When access was granted
        expires_at: When access expires
        granted_by: Email of admin who approved
        status: Current status of the grant
        revoked_at: When grant was revoked (if applicable)
        revoked_by: Email of user who revoked (if applicable)
        revocation_reason: Why the grant was revoked
    """
    __tablename__ = "access_grants"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    request_id = Column(UUID(as_uuid=True), ForeignKey('access_grant_requests.id', ondelete='SET NULL'), nullable=True, index=True)
    grantee_email = Column(String(255), nullable=False, index=True)
    entity_type = Column(String(100), nullable=False, index=True)
    entity_id = Column(String(255), nullable=False, index=True)
    entity_name = Column(String(500), nullable=True)
    permission_level = Column(String(50), nullable=False, default=PermissionLevel.READ.value)
    granted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    granted_by = Column(String(255), nullable=True)
    status = Column(
        String(50),
        nullable=False,
        default=AccessGrantStatus.ACTIVE.value,
        index=True
    )
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    revoked_by = Column(String(255), nullable=True)
    revocation_reason = Column(Text, nullable=True)
    
    # Relationship to the original request
    request = relationship("AccessGrantRequestDb", back_populates="grant")
    
    def __repr__(self) -> str:
        return f"<AccessGrantDb(id={self.id}, grantee='{self.grantee_email}', entity={self.entity_type}/{self.entity_id}, status={self.status})>"
    
    @property
    def is_expired(self) -> bool:
        """Check if the grant has expired based on current time."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at
    
    @property
    def is_active(self) -> bool:
        """Check if the grant is currently active (not expired or revoked)."""
        return self.status == AccessGrantStatus.ACTIVE.value and not self.is_expired
    
    @property
    def days_until_expiry(self) -> Optional[int]:
        """Get the number of days until expiry (negative if expired)."""
        if self.expires_at is None:
            return None
        delta = self.expires_at - datetime.now(timezone.utc)
        return delta.days


class AccessGrantDurationConfigDb(Base):
    """
    Configuration for allowed access grant durations per entity type.
    
    Admins can configure which duration options are available for each
    type of asset, and which process workflow to trigger on expiration.
    
    Attributes:
        id: Unique identifier
        entity_type: Type of asset this config applies to
        allowed_durations: JSON config - either list of days [30, 60, 90] or range {"min": 1, "max": 90}
        default_duration: Default duration in days
        expiry_workflow_id: ID of process workflow to trigger on expiration
        expiry_warning_days: Days before expiry to send warning notification
        allow_renewal: Whether users can request renewal
        max_renewals: Maximum number of times a grant can be renewed
        created_at: When config was created
        updated_at: When config was last updated
        created_by: Who created the config
        updated_by: Who last updated the config
    """
    __tablename__ = "access_grant_duration_configs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    entity_type = Column(String(100), nullable=False, unique=True, index=True)
    allowed_durations = Column(JSON, nullable=False, default=lambda: [30, 60, 90])
    default_duration = Column(Integer, nullable=False, default=30)
    expiry_workflow_id = Column(String(255), nullable=True)
    expiry_warning_days = Column(Integer, nullable=False, default=7)
    allow_renewal = Column(Boolean, nullable=False, default=True)
    max_renewals = Column(Integer, nullable=True, default=3)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    created_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    
    def __repr__(self) -> str:
        return f"<AccessGrantDurationConfigDb(entity_type='{self.entity_type}', durations={self.allowed_durations})>"
    
    def get_duration_options(self) -> list:
        """Get the list of allowed duration options."""
        if isinstance(self.allowed_durations, list):
            return sorted(self.allowed_durations)
        elif isinstance(self.allowed_durations, dict):
            # Range format: {"min": 1, "max": 90}
            min_val = self.allowed_durations.get("min", 1)
            max_val = self.allowed_durations.get("max", 90)
            # Return common intervals within range
            common_intervals = [7, 14, 30, 60, 90, 180, 365]
            return [d for d in common_intervals if min_val <= d <= max_val]
        return [30, 60, 90]  # Default fallback
    
    def is_duration_allowed(self, days: int) -> bool:
        """Check if a specific duration is allowed."""
        if isinstance(self.allowed_durations, list):
            return days in self.allowed_durations
        elif isinstance(self.allowed_durations, dict):
            min_val = self.allowed_durations.get("min", 1)
            max_val = self.allowed_durations.get("max", 90)
            return min_val <= days <= max_val
        return False

