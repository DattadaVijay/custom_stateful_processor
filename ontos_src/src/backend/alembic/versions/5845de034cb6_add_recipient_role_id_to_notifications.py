"""add_recipient_role_id_to_notifications

Revision ID: 5845de034cb6
Revises: q7244m158oo0
Create Date: 2026-01-25 14:30:28.267524

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5845de034cb6'
down_revision: Union[str, None] = 'q7244m158oo0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add recipient_role_id column to notifications table for role UUID references
    op.add_column('notifications', sa.Column('recipient_role_id', sa.String(), nullable=True))
    op.create_index('ix_notifications_recipient_role_id', 'notifications', ['recipient_role_id'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_notifications_recipient_role_id', table_name='notifications')
    op.drop_column('notifications', 'recipient_role_id')
