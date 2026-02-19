"""add_llm_sessions_tables

Revision ID: b8e2f4a3c1d0
Revises: af278821247a
Create Date: 2025-12-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b8e2f4a3c1d0'
down_revision: Union[str, None] = 'af278821247a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create LLM sessions and messages tables."""
    # Create llm_sessions table
    op.create_table('llm_sessions',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_llm_sessions_user_id', 'llm_sessions', ['user_id'], unique=False)
    op.create_index('ix_llm_sessions_user_updated', 'llm_sessions', ['user_id', 'updated_at'], unique=False)

    # Create llm_messages table
    op.create_table('llm_messages',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('session_id', sa.String(), nullable=False),
        sa.Column('role', sa.String(), nullable=False),
        sa.Column('content', sa.Text(), nullable=True),
        sa.Column('tool_calls', sa.Text(), nullable=True),
        sa.Column('tool_call_id', sa.String(), nullable=True),
        sa.Column('sequence', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['session_id'], ['llm_sessions.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_llm_messages_session_id', 'llm_messages', ['session_id'], unique=False)
    op.create_index('ix_llm_messages_session_sequence', 'llm_messages', ['session_id', 'sequence'], unique=False)


def downgrade() -> None:
    """Drop LLM sessions and messages tables."""
    # Drop messages first (child table)
    op.drop_index('ix_llm_messages_session_sequence', table_name='llm_messages')
    op.drop_index('ix_llm_messages_session_id', table_name='llm_messages')
    op.drop_table('llm_messages')
    
    # Then drop sessions (parent table)
    op.drop_index('ix_llm_sessions_user_updated', table_name='llm_sessions')
    op.drop_index('ix_llm_sessions_user_id', table_name='llm_sessions')
    op.drop_table('llm_sessions')

