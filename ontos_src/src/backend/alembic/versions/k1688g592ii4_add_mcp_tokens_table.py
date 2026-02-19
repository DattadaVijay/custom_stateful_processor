"""add mcp_tokens table

Revision ID: k1688g592ii4
Revises: j0577f481hh3
Create Date: 2026-01-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'k1688g592ii4'
down_revision: Union[str, None] = 'j0577f481hh3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create mcp_tokens table for MCP API key authentication
    op.create_table('mcp_tokens',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('token_hash', sa.String(length=255), nullable=False),
        sa.Column('scopes', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('created_by', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create index on token_hash for fast lookups
    op.create_index('ix_mcp_tokens_token_hash', 'mcp_tokens', ['token_hash'], unique=True)
    
    # Create index on is_active for filtering active tokens
    op.create_index('ix_mcp_tokens_is_active', 'mcp_tokens', ['is_active'])


def downgrade() -> None:
    op.drop_index('ix_mcp_tokens_is_active', table_name='mcp_tokens')
    op.drop_index('ix_mcp_tokens_token_hash', table_name='mcp_tokens')
    op.drop_table('mcp_tokens')

