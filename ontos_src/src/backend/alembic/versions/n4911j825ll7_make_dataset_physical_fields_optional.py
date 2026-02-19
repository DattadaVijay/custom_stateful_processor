"""Make dataset physical fields optional

Revision ID: n4911j825ll7
Revises: m3800i714kk6
Create Date: 2026-01-11

This migration makes the physical asset fields on the datasets table nullable.
Physical asset information now belongs on dataset_instances, not on the dataset itself.
A Dataset is a logical grouping; DatasetInstance represents the physical reality.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'n4911j825ll7'
down_revision: Union[str, None] = 'm3800i714kk6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the unique constraint and composite index
    op.drop_constraint('uq_dataset_asset_env', 'datasets', type_='unique')
    op.drop_index('ix_dataset_full_path', table_name='datasets')
    
    # Make physical asset columns nullable
    op.alter_column('datasets', 'asset_type',
                    existing_type=sa.String(),
                    nullable=True)
    op.alter_column('datasets', 'catalog_name',
                    existing_type=sa.String(),
                    nullable=True)
    op.alter_column('datasets', 'schema_name',
                    existing_type=sa.String(),
                    nullable=True)
    op.alter_column('datasets', 'object_name',
                    existing_type=sa.String(),
                    nullable=True)
    op.alter_column('datasets', 'environment',
                    existing_type=sa.String(),
                    nullable=True)
    
    # Drop individual indexes on the now-deprecated columns
    op.drop_index('ix_datasets_asset_type', table_name='datasets')
    op.drop_index('ix_datasets_catalog_name', table_name='datasets')
    op.drop_index('ix_datasets_schema_name', table_name='datasets')
    op.drop_index('ix_datasets_object_name', table_name='datasets')
    op.drop_index('ix_datasets_environment', table_name='datasets')


def downgrade() -> None:
    # Recreate indexes
    op.create_index('ix_datasets_environment', 'datasets', ['environment'], unique=False)
    op.create_index('ix_datasets_object_name', 'datasets', ['object_name'], unique=False)
    op.create_index('ix_datasets_schema_name', 'datasets', ['schema_name'], unique=False)
    op.create_index('ix_datasets_catalog_name', 'datasets', ['catalog_name'], unique=False)
    op.create_index('ix_datasets_asset_type', 'datasets', ['asset_type'], unique=False)
    
    # Make columns non-nullable again (this will fail if there are NULL values)
    op.alter_column('datasets', 'environment',
                    existing_type=sa.String(),
                    nullable=False)
    op.alter_column('datasets', 'object_name',
                    existing_type=sa.String(),
                    nullable=False)
    op.alter_column('datasets', 'schema_name',
                    existing_type=sa.String(),
                    nullable=False)
    op.alter_column('datasets', 'catalog_name',
                    existing_type=sa.String(),
                    nullable=False)
    op.alter_column('datasets', 'asset_type',
                    existing_type=sa.String(),
                    nullable=False)
    
    # Recreate constraints
    op.create_index('ix_dataset_full_path', 'datasets', ['catalog_name', 'schema_name', 'object_name'], unique=False)
    op.create_unique_constraint('uq_dataset_asset_env', 'datasets', ['catalog_name', 'schema_name', 'object_name', 'environment'])

