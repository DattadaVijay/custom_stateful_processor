"""Drop dataset_tags table (moved to unified tagging system)

Revision ID: m3800i714kk6
Revises: l2799h603jj5
Create Date: 2026-01-11 12:01:00.000000

This migration removes the separate dataset_tags table as datasets now
use the unified tagging system via EntityTagAssociationDb with entity_type='dataset'.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'm3800i714kk6'
down_revision: Union[str, None] = 'l2799h603jj5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the unique constraint first
    op.drop_constraint('uq_dataset_tag', 'dataset_tags', type_='unique')
    
    # Drop the index
    op.drop_index('ix_dataset_tags_dataset_id', 'dataset_tags')
    
    # Drop the table
    op.drop_table('dataset_tags')


def downgrade() -> None:
    # Recreate the dataset_tags table
    op.create_table(
        'dataset_tags',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('dataset_id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
    )
    
    # Recreate index
    op.create_index('ix_dataset_tags_dataset_id', 'dataset_tags', ['dataset_id'])
    
    # Recreate unique constraint
    op.create_unique_constraint('uq_dataset_tag', 'dataset_tags', ['dataset_id', 'name'])

