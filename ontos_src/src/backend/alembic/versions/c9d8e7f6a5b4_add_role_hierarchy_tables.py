"""add_role_hierarchy_tables

Revision ID: c9d8e7f6a5b4
Revises: b8e2f4a3c1d0
Create Date: 2025-12-19 10:00:00.000000

This migration adds tables for role request and approval hierarchies:
- role_request_permissions: which roles can request other roles
- role_approval_permissions: which roles can approve access to other roles

Note: For requestable_by_role_id, the special value '__NO_ROLE__' is used
to represent "anyone with no role can request this role".
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c9d8e7f6a5b4'
down_revision: Union[str, None] = 'b8e2f4a3c1d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create role_request_permissions table
    # Maps which roles can request access to other roles
    # requestable_by_role_id = '__NO_ROLE__' means anyone with no role can request
    op.create_table('role_request_permissions',
        sa.Column('role_id', sa.String(), nullable=False, comment='The role that can be requested'),
        sa.Column('requestable_by_role_id', sa.String(), nullable=False, 
                  comment='Role that can request (use __NO_ROLE__ for users without any role)'),
        sa.ForeignKeyConstraint(['role_id'], ['app_roles.id'], ondelete='CASCADE'),
        # Note: No FK for requestable_by_role_id as it can be '__NO_ROLE__'
        sa.PrimaryKeyConstraint('role_id', 'requestable_by_role_id', name='pk_role_request_permissions'),
    )
    
    # Create index for faster lookups
    op.create_index('ix_role_request_permissions_role_id', 'role_request_permissions', ['role_id'])
    op.create_index('ix_role_request_permissions_requestable_by', 'role_request_permissions', ['requestable_by_role_id'])
    
    # Create role_approval_permissions table
    # Maps which roles can approve access requests for other roles
    op.create_table('role_approval_permissions',
        sa.Column('role_id', sa.String(), nullable=False, comment='The role being requested'),
        sa.Column('approver_role_id', sa.String(), nullable=False, comment='Role that can approve the request'),
        sa.ForeignKeyConstraint(['role_id'], ['app_roles.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['approver_role_id'], ['app_roles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('role_id', 'approver_role_id', name='pk_role_approval_permissions'),
    )
    
    # Create index for faster lookups
    op.create_index('ix_role_approval_permissions_role_id', 'role_approval_permissions', ['role_id'])
    op.create_index('ix_role_approval_permissions_approver_role_id', 'role_approval_permissions', ['approver_role_id'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop indexes first
    op.drop_index('ix_role_approval_permissions_approver_role_id', table_name='role_approval_permissions')
    op.drop_index('ix_role_approval_permissions_role_id', table_name='role_approval_permissions')
    op.drop_table('role_approval_permissions')
    
    op.drop_index('ix_role_request_permissions_requestable_by', table_name='role_request_permissions')
    op.drop_index('ix_role_request_permissions_role_id', table_name='role_request_permissions')
    op.drop_table('role_request_permissions')
