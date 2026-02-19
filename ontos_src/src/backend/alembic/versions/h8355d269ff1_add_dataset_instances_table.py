"""Add dataset_instances table

Revision ID: h8355d269ff1
Revises: g7244c158ee0
Create Date: 2026-01-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'h8355d269ff1'
down_revision: Union[str, None] = 'g7244c158ee0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the dataset_instances table
    op.create_table(
        'dataset_instances',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('dataset_id', sa.String(), nullable=False),
        sa.Column('contract_id', sa.String(), nullable=True),
        sa.Column('contract_server_id', sa.String(), nullable=True),
        sa.Column('physical_path', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False, server_default='active'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['contract_id'], ['data_contracts.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['contract_server_id'], ['data_contract_servers.id'], ondelete='SET NULL'),
    )
    
    # Create indexes
    op.create_index('ix_dataset_instances_dataset_id', 'dataset_instances', ['dataset_id'])
    op.create_index('ix_dataset_instances_contract_id', 'dataset_instances', ['contract_id'])
    op.create_index('ix_dataset_instances_contract_server_id', 'dataset_instances', ['contract_server_id'])
    op.create_index('ix_dataset_instances_status', 'dataset_instances', ['status'])
    op.create_index('ix_dataset_instance_path', 'dataset_instances', ['physical_path'])
    
    # Create unique constraint: one instance per dataset per server
    op.create_unique_constraint('uq_dataset_instance_server', 'dataset_instances', ['dataset_id', 'contract_server_id'])


def downgrade() -> None:
    # Drop unique constraint
    op.drop_constraint('uq_dataset_instance_server', 'dataset_instances', type_='unique')
    
    # Drop indexes
    op.drop_index('ix_dataset_instance_path', 'dataset_instances')
    op.drop_index('ix_dataset_instances_status', 'dataset_instances')
    op.drop_index('ix_dataset_instances_contract_server_id', 'dataset_instances')
    op.drop_index('ix_dataset_instances_contract_id', 'dataset_instances')
    op.drop_index('ix_dataset_instances_dataset_id', 'dataset_instances')
    
    # Drop table
    op.drop_table('dataset_instances')


