"""Add asset_type column to dataset_instances for multi-platform support

Revision ID: q7244m158oo0
Revises: p6133l047nn9
Create Date: 2026-01-24 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'q7244m158oo0'
down_revision: Union[str, None] = 'p6133l047nn9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add asset_type column (nullable for backward compatibility)
    # This column stores the UnifiedAssetType enum value (e.g., 'uc_table', 'snowflake_view')
    # enabling platform-agnostic asset handling across Unity Catalog, Snowflake, Kafka, PowerBI, etc.
    op.add_column('dataset_instances', sa.Column('asset_type', sa.String(), nullable=True))
    
    # Create index on asset_type column for efficient filtering
    op.create_index('ix_dataset_instances_asset_type', 'dataset_instances', ['asset_type'])


def downgrade() -> None:
    # Drop index
    op.drop_index('ix_dataset_instances_asset_type', 'dataset_instances')
    
    # Drop column
    op.drop_column('dataset_instances', 'asset_type')

