from __future__ import annotations
from typing import Optional, List, Dict
from uuid import UUID
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, model_validator


class CommentStatus(str, Enum):
    ACTIVE = "active"
    DELETED = "deleted"


class CommentType(str, Enum):
    COMMENT = "comment"
    RATING = "rating"


class CommentBase(BaseModel):
    entity_id: str
    entity_type: str = Field(..., description="Type of entity being commented on (data_domain, data_product, data_contract, etc.)")
    title: Optional[str] = Field(None, max_length=255, description="Optional title for the comment")
    comment: str = Field(..., min_length=1, description="The comment content")
    audience: Optional[List[str]] = Field(None, description="List of audience tokens: plain groups, 'team:<team_id>', or 'role:<role_name>'. If null, visible to all users with access to the entity")
    project_id: Optional[str] = Field(None, description="Project ID to scope the comment. If null, visible globally (admin/owning team only)")
    comment_type: CommentType = Field(default=CommentType.COMMENT, description="Type: 'comment' or 'rating'")
    rating: Optional[int] = Field(None, ge=1, le=5, description="Star rating 1-5 (only for rating type)")
    
    @model_validator(mode='after')
    def validate_rating_fields(self):
        """Validate that rating is required for rating-type and forbidden for comment-type."""
        if self.comment_type == CommentType.RATING and self.rating is None:
            raise ValueError('Rating is required for rating-type comments')
        if self.comment_type == CommentType.COMMENT and self.rating is not None:
            raise ValueError('Rating should not be set for regular comments')
        return self


class CommentCreate(CommentBase):
    pass


class CommentUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=255)
    comment: Optional[str] = Field(None, min_length=1)
    audience: Optional[List[str]] = None


class Comment(CommentBase):
    id: UUID
    status: CommentStatus = CommentStatus.ACTIVE
    project_id: Optional[str] = None
    comment_type: CommentType = CommentType.COMMENT
    rating: Optional[int] = None
    created_by: str
    updated_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True
    }
    
    @model_validator(mode='after')
    def validate_rating_fields(self):
        """For response models, we allow any combination (DB might have legacy data)."""
        # No strict validation on response - trust the database
        return self


class CommentListResponse(BaseModel):
    """Response model for listing comments with metadata"""
    comments: List[Comment]
    total_count: int
    visible_count: int  # Number of comments visible to current user


class RatingCreate(BaseModel):
    """Simplified model for creating ratings"""
    entity_id: str
    entity_type: str
    rating: int = Field(..., ge=1, le=5, description="Star rating 1-5")
    comment: Optional[str] = Field(None, description="Optional review text")
    project_id: Optional[str] = None


class RatingAggregation(BaseModel):
    """Aggregated rating statistics for an entity"""
    entity_type: str
    entity_id: str
    average_rating: float
    total_ratings: int
    distribution: Dict[int, int]  # {1: count, 2: count, ..., 5: count}
    user_current_rating: Optional[int] = None  # Current user's latest rating