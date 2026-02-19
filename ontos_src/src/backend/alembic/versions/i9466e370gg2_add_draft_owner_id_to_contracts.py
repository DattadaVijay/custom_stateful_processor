"""Add draft_owner_id to data_contracts for personal draft visibility

Revision ID: i9466e370gg2
Revises: h8355d269ff1
Create Date: 2026-01-02 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'i9466e370gg2'
down_revision: Union[str, None] = 'h8355d269ff1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add draft_owner_id column to data_contracts table
    # When set, this is a personal draft visible only to the owner
    # When NULL, the contract follows normal team/project visibility rules
    op.add_column(
        'data_contracts',
        sa.Column('draft_owner_id', sa.String(), nullable=True)
    )
    
    # Create index for efficient filtering of personal drafts
    op.create_index(
        'ix_data_contracts_draft_owner_id',
        'data_contracts',
        ['draft_owner_id']
    )


def downgrade() -> None:
    # Drop index
    op.drop_index('ix_data_contracts_draft_owner_id', 'data_contracts')
    
    # Drop column
    op.drop_column('data_contracts', 'draft_owner_id')

