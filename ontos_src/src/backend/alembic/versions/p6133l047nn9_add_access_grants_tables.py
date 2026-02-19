"""add access grants tables

Revision ID: p6133l047nn9
Revises: o5022k936mm8
Create Date: 2026-01-23 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'p6133l047nn9'
down_revision: Union[str, None] = 'o5022k936mm8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create access_grant_requests table
    op.create_table('access_grant_requests',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('requester_email', sa.String(length=255), nullable=False),
        sa.Column('entity_type', sa.String(length=100), nullable=False),
        sa.Column('entity_id', sa.String(length=255), nullable=False),
        sa.Column('entity_name', sa.String(length=500), nullable=True),
        sa.Column('requested_duration_days', sa.Integer(), nullable=False),
        sa.Column('permission_level', sa.String(length=50), nullable=False, server_default='READ'),
        sa.Column('reason', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='pending'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('handled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('handled_by', sa.String(length=255), nullable=True),
        sa.Column('admin_message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes on access_grant_requests
    op.create_index('ix_access_grant_requests_requester_email', 'access_grant_requests', ['requester_email'])
    op.create_index('ix_access_grant_requests_entity_type', 'access_grant_requests', ['entity_type'])
    op.create_index('ix_access_grant_requests_entity_id', 'access_grant_requests', ['entity_id'])
    op.create_index('ix_access_grant_requests_status', 'access_grant_requests', ['status'])
    
    # Create access_grants table
    op.create_table('access_grants',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('request_id', sa.UUID(), nullable=True),
        sa.Column('grantee_email', sa.String(length=255), nullable=False),
        sa.Column('entity_type', sa.String(length=100), nullable=False),
        sa.Column('entity_id', sa.String(length=255), nullable=False),
        sa.Column('entity_name', sa.String(length=500), nullable=True),
        sa.Column('permission_level', sa.String(length=50), nullable=False, server_default='READ'),
        sa.Column('granted_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('granted_by', sa.String(length=255), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='active'),
        sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('revoked_by', sa.String(length=255), nullable=True),
        sa.Column('revocation_reason', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['request_id'], ['access_grant_requests.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes on access_grants
    op.create_index('ix_access_grants_request_id', 'access_grants', ['request_id'])
    op.create_index('ix_access_grants_grantee_email', 'access_grants', ['grantee_email'])
    op.create_index('ix_access_grants_entity_type', 'access_grants', ['entity_type'])
    op.create_index('ix_access_grants_entity_id', 'access_grants', ['entity_id'])
    op.create_index('ix_access_grants_status', 'access_grants', ['status'])
    op.create_index('ix_access_grants_expires_at', 'access_grants', ['expires_at'])
    
    # Create access_grant_duration_configs table
    op.create_table('access_grant_duration_configs',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('entity_type', sa.String(length=100), nullable=False),
        sa.Column('allowed_durations', sa.JSON(), nullable=False, server_default='[30, 60, 90]'),
        sa.Column('default_duration', sa.Integer(), nullable=False, server_default='30'),
        sa.Column('expiry_workflow_id', sa.String(length=255), nullable=True),
        sa.Column('expiry_warning_days', sa.Integer(), nullable=False, server_default='7'),
        sa.Column('allow_renewal', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('max_renewals', sa.Integer(), nullable=True, server_default='3'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_by', sa.String(length=255), nullable=True),
        sa.Column('updated_by', sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('entity_type', name='uq_access_grant_duration_configs_entity_type')
    )
    
    # Create index on entity_type
    op.create_index('ix_access_grant_duration_configs_entity_type', 'access_grant_duration_configs', ['entity_type'])


def downgrade() -> None:
    # Drop access_grant_duration_configs
    op.drop_index('ix_access_grant_duration_configs_entity_type', table_name='access_grant_duration_configs')
    op.drop_table('access_grant_duration_configs')
    
    # Drop access_grants
    op.drop_index('ix_access_grants_expires_at', table_name='access_grants')
    op.drop_index('ix_access_grants_status', table_name='access_grants')
    op.drop_index('ix_access_grants_entity_id', table_name='access_grants')
    op.drop_index('ix_access_grants_entity_type', table_name='access_grants')
    op.drop_index('ix_access_grants_grantee_email', table_name='access_grants')
    op.drop_index('ix_access_grants_request_id', table_name='access_grants')
    op.drop_table('access_grants')
    
    # Drop access_grant_requests
    op.drop_index('ix_access_grant_requests_status', table_name='access_grant_requests')
    op.drop_index('ix_access_grant_requests_entity_id', table_name='access_grant_requests')
    op.drop_index('ix_access_grant_requests_entity_type', table_name='access_grant_requests')
    op.drop_index('ix_access_grant_requests_requester_email', table_name='access_grant_requests')
    op.drop_table('access_grant_requests')

