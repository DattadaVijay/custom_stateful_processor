"""add metadata sharing and inheritance

Revision ID: 5a8b9c0d1e2f
Revises: b8e2f4a3c1d0
Create Date: 2025-01-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5a8b9c0d1e2f'
down_revision: Union[str, None] = 'b8e2f4a3c1d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # === Add columns to rich_text_metadata ===
    op.add_column('rich_text_metadata', sa.Column('is_shared', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('rich_text_metadata', sa.Column('level', sa.Integer(), nullable=False, server_default='50'))
    op.add_column('rich_text_metadata', sa.Column('inheritable', sa.Boolean(), nullable=False, server_default='true'))
    op.create_index('ix_rich_text_shared', 'rich_text_metadata', ['is_shared'], unique=False)

    # === Add columns to link_metadata ===
    op.add_column('link_metadata', sa.Column('is_shared', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('link_metadata', sa.Column('level', sa.Integer(), nullable=False, server_default='50'))
    op.add_column('link_metadata', sa.Column('inheritable', sa.Boolean(), nullable=False, server_default='true'))
    op.create_index('ix_link_shared', 'link_metadata', ['is_shared'], unique=False)

    # === Add columns to document_metadata ===
    op.add_column('document_metadata', sa.Column('is_shared', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('document_metadata', sa.Column('level', sa.Integer(), nullable=False, server_default='50'))
    op.add_column('document_metadata', sa.Column('inheritable', sa.Boolean(), nullable=False, server_default='true'))
    op.create_index('ix_document_shared', 'document_metadata', ['is_shared'], unique=False)

    # === Add max_level_inheritance to data_products ===
    op.add_column('data_products', sa.Column('max_level_inheritance', sa.Integer(), nullable=False, server_default='99'))

    # === Add max_level_inheritance to datasets ===
    op.add_column('datasets', sa.Column('max_level_inheritance', sa.Integer(), nullable=False, server_default='99'))

    # === Create metadata_attachments table ===
    op.create_table(
        'metadata_attachments',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('entity_id', sa.String(), nullable=False),
        sa.Column('entity_type', sa.String(), nullable=False),
        sa.Column('asset_type', sa.String(), nullable=False),
        sa.Column('asset_id', sa.String(), nullable=False),
        sa.Column('level_override', sa.Integer(), nullable=True),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_attachment_entity', 'metadata_attachments', ['entity_type', 'entity_id'], unique=False)
    op.create_index('ix_attachment_asset', 'metadata_attachments', ['asset_type', 'asset_id'], unique=False)


def downgrade() -> None:
    # === Drop metadata_attachments table ===
    op.drop_index('ix_attachment_asset', table_name='metadata_attachments')
    op.drop_index('ix_attachment_entity', table_name='metadata_attachments')
    op.drop_table('metadata_attachments')

    # === Remove max_level_inheritance from datasets ===
    op.drop_column('datasets', 'max_level_inheritance')

    # === Remove max_level_inheritance from data_products ===
    op.drop_column('data_products', 'max_level_inheritance')

    # === Remove columns from document_metadata ===
    op.drop_index('ix_document_shared', table_name='document_metadata')
    op.drop_column('document_metadata', 'inheritable')
    op.drop_column('document_metadata', 'level')
    op.drop_column('document_metadata', 'is_shared')

    # === Remove columns from link_metadata ===
    op.drop_index('ix_link_shared', table_name='link_metadata')
    op.drop_column('link_metadata', 'inheritable')
    op.drop_column('link_metadata', 'level')
    op.drop_column('link_metadata', 'is_shared')

    # === Remove columns from rich_text_metadata ===
    op.drop_index('ix_rich_text_shared', table_name='rich_text_metadata')
    op.drop_column('rich_text_metadata', 'inheritable')
    op.drop_column('rich_text_metadata', 'level')
    op.drop_column('rich_text_metadata', 'is_shared')

