"""Add role, display_name, environment columns to dataset_instances

Revision ID: l2799h603jj5
Revises: k1688g592ii4
Create Date: 2026-01-11 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'l2799h603jj5'
down_revision: Union[str, None] = 'k1688g592ii4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add role column with default 'main'
    op.add_column('dataset_instances', sa.Column('role', sa.String(), nullable=False, server_default='main'))
    
    # Add display_name column (optional)
    op.add_column('dataset_instances', sa.Column('display_name', sa.String(), nullable=True))
    
    # Add environment column (optional)
    op.add_column('dataset_instances', sa.Column('environment', sa.String(), nullable=True))
    
    # Create index on role column
    op.create_index('ix_dataset_instances_role', 'dataset_instances', ['role'])
    
    # Create index on environment column
    op.create_index('ix_dataset_instances_environment', 'dataset_instances', ['environment'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_dataset_instances_environment', 'dataset_instances')
    op.drop_index('ix_dataset_instances_role', 'dataset_instances')
    
    # Drop columns
    op.drop_column('dataset_instances', 'environment')
    op.drop_column('dataset_instances', 'display_name')
    op.drop_column('dataset_instances', 'role')

