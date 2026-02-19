"""Add versioning columns to data_products for personal drafts and version lineage

Revision ID: j0577f481hh3
Revises: i9466e370gg2
Create Date: 2026-01-04 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'j0577f481hh3'
down_revision: Union[str, None] = 'i9466e370gg2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add draft_owner_id column - when set, this is a personal draft
    op.add_column(
        'data_products',
        sa.Column('draft_owner_id', sa.String(), nullable=True)
    )
    op.create_index(
        'ix_data_products_draft_owner_id',
        'data_products',
        ['draft_owner_id']
    )
    
    # Add parent_product_id for version lineage
    op.add_column(
        'data_products',
        sa.Column('parent_product_id', sa.String(), nullable=True)
    )
    op.create_index(
        'ix_data_products_parent_product_id',
        'data_products',
        ['parent_product_id']
    )
    op.create_foreign_key(
        'fk_data_products_parent_product_id',
        'data_products',
        'data_products',
        ['parent_product_id'],
        ['id'],
        ondelete='SET NULL'
    )
    
    # Add base_name for grouping versions
    op.add_column(
        'data_products',
        sa.Column('base_name', sa.String(), nullable=True)
    )
    op.create_index(
        'ix_data_products_base_name',
        'data_products',
        ['base_name']
    )
    
    # Add change_summary for version notes
    op.add_column(
        'data_products',
        sa.Column('change_summary', sa.Text(), nullable=True)
    )
    
    # Add published flag for marketplace visibility
    op.add_column(
        'data_products',
        sa.Column('published', sa.Boolean(), nullable=False, server_default='false')
    )
    op.create_index(
        'ix_data_products_published',
        'data_products',
        ['published']
    )


def downgrade() -> None:
    # Drop indexes and columns in reverse order
    op.drop_index('ix_data_products_published', 'data_products')
    op.drop_column('data_products', 'published')
    
    op.drop_column('data_products', 'change_summary')
    
    op.drop_index('ix_data_products_base_name', 'data_products')
    op.drop_column('data_products', 'base_name')
    
    op.drop_constraint('fk_data_products_parent_product_id', 'data_products', type_='foreignkey')
    op.drop_index('ix_data_products_parent_product_id', 'data_products')
    op.drop_column('data_products', 'parent_product_id')
    
    op.drop_index('ix_data_products_draft_owner_id', 'data_products')
    op.drop_column('data_products', 'draft_owner_id')

