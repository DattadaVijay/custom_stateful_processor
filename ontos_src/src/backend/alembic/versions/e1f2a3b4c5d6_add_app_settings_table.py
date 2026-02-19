"""add_app_settings_table

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2025-12-20 10:00:00.000000

This migration adds a key-value table for persisting application settings
that can be modified at runtime (e.g., WORKSPACE_DEPLOYMENT_PATH).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e1f2a3b4c5d6'
down_revision: Union[str, None] = 'd0e1f2a3b4c5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('app_settings',
        sa.Column('key', sa.String(255), primary_key=True, comment='Setting key/name'),
        sa.Column('value', sa.Text(), nullable=True, comment='Setting value (can be null to clear)'),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), 
                  nullable=True, comment='Last update timestamp'),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('app_settings')

