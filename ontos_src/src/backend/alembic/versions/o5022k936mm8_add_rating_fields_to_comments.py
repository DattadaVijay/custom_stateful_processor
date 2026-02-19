"""Add rating fields to comments table

Revision ID: o5022k936mm8
Revises: n4911j825ll7
Create Date: 2026-01-19

This migration adds support for star ratings on marketplace offerings
by extending the comments table with:
- comment_type: Enum to distinguish regular comments from ratings
- rating: Integer 1-5 for star ratings
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'o5022k936mm8'
down_revision: Union[str, None] = 'n4911j825ll7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the comment_type enum
    comment_type_enum = sa.Enum('comment', 'rating', name='commenttype')
    comment_type_enum.create(op.get_bind(), checkfirst=True)
    
    # Add comment_type column with default 'comment' for existing rows
    op.add_column('comments', sa.Column(
        'comment_type',
        sa.Enum('comment', 'rating', name='commenttype'),
        nullable=False,
        server_default='comment'
    ))
    
    # Add rating column (nullable, only used when comment_type is 'rating')
    op.add_column('comments', sa.Column('rating', sa.Integer(), nullable=True))
    
    # Add indexes for efficient rating queries
    op.create_index('ix_comments_comment_type', 'comments', ['comment_type'])
    op.create_index('ix_comments_entity_rating', 'comments', ['entity_type', 'entity_id', 'comment_type'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_comments_entity_rating', table_name='comments')
    op.drop_index('ix_comments_comment_type', table_name='comments')
    
    # Drop columns
    op.drop_column('comments', 'rating')
    op.drop_column('comments', 'comment_type')
    
    # Drop the enum type
    sa.Enum('comment', 'rating', name='commenttype').drop(op.get_bind(), checkfirst=True)

