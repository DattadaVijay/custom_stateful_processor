"""add_datasets_tables

Revision ID: d0e1f2a3b4c5
Revises: c9d8e7f6a5b4
Create Date: 2025-12-19 12:00:00.000000

This migration adds tables for Datasets - physical implementations of Data Contracts:
- datasets: Main table for dataset records (tables/views in Unity Catalog)
- dataset_subscriptions: Consumer subscriptions to datasets
- dataset_tags: Simple string tags for categorization
- dataset_custom_properties: Key/value pairs for extensibility
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'd0e1f2a3b4c5'
down_revision: Union[str, None] = 'c9d8e7f6a5b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create datasets table
    op.create_table('datasets',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        
        # Physical asset reference
        sa.Column('asset_type', sa.String(), nullable=False),  # 'table' or 'view'
        sa.Column('catalog_name', sa.String(), nullable=False),
        sa.Column('schema_name', sa.String(), nullable=False),
        sa.Column('object_name', sa.String(), nullable=False),
        
        # SDLC environment
        sa.Column('environment', sa.String(), nullable=False),  # dev, staging, prod
        
        # Contract reference
        sa.Column('contract_id', sa.String(), nullable=True),
        
        # Ownership and project
        sa.Column('owner_team_id', sa.String(), nullable=True),
        sa.Column('project_id', sa.String(), nullable=True),
        
        # Lifecycle
        sa.Column('status', sa.String(), nullable=False, server_default='draft'),
        sa.Column('version', sa.String(), nullable=True),
        sa.Column('published', sa.Boolean(), nullable=False, server_default='false'),
        
        # Audit
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.Column('updated_by', sa.String(), nullable=True),
        
        # Primary key
        sa.PrimaryKeyConstraint('id'),
        
        # Foreign keys
        sa.ForeignKeyConstraint(['contract_id'], ['data_contracts.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['owner_team_id'], ['teams.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='SET NULL'),
        
        # Unique constraint: same asset can only be registered once per environment
        sa.UniqueConstraint('catalog_name', 'schema_name', 'object_name', 'environment', name='uq_dataset_asset_env'),
    )
    
    # Create indexes for datasets
    op.create_index('ix_datasets_name', 'datasets', ['name'])
    op.create_index('ix_datasets_asset_type', 'datasets', ['asset_type'])
    op.create_index('ix_datasets_catalog_name', 'datasets', ['catalog_name'])
    op.create_index('ix_datasets_schema_name', 'datasets', ['schema_name'])
    op.create_index('ix_datasets_object_name', 'datasets', ['object_name'])
    op.create_index('ix_datasets_environment', 'datasets', ['environment'])
    op.create_index('ix_datasets_contract_id', 'datasets', ['contract_id'])
    op.create_index('ix_datasets_owner_team_id', 'datasets', ['owner_team_id'])
    op.create_index('ix_datasets_project_id', 'datasets', ['project_id'])
    op.create_index('ix_datasets_status', 'datasets', ['status'])
    op.create_index('ix_datasets_published', 'datasets', ['published'])
    op.create_index('ix_dataset_full_path', 'datasets', ['catalog_name', 'schema_name', 'object_name'])
    
    # Create dataset_subscriptions table
    op.create_table('dataset_subscriptions',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('dataset_id', sa.String(), nullable=False),
        sa.Column('subscriber_email', sa.String(), nullable=False),
        sa.Column('subscribed_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('subscription_reason', sa.Text(), nullable=True),
        
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('dataset_id', 'subscriber_email', name='uq_dataset_subscriber'),
    )
    
    # Create indexes for dataset_subscriptions
    op.create_index('ix_dataset_subscriptions_dataset_id', 'dataset_subscriptions', ['dataset_id'])
    op.create_index('ix_dataset_subscriptions_subscriber_email', 'dataset_subscriptions', ['subscriber_email'])
    
    # Create dataset_tags table
    op.create_table('dataset_tags',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('dataset_id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('dataset_id', 'name', name='uq_dataset_tag'),
    )
    
    # Create index for dataset_tags
    op.create_index('ix_dataset_tags_dataset_id', 'dataset_tags', ['dataset_id'])
    
    # Create dataset_custom_properties table
    op.create_table('dataset_custom_properties',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('dataset_id', sa.String(), nullable=False),
        sa.Column('property', sa.String(), nullable=False),
        sa.Column('value', sa.Text(), nullable=True),
        
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dataset_id'], ['datasets.id'], ondelete='CASCADE'),
    )
    
    # Create index for dataset_custom_properties
    op.create_index('ix_dataset_custom_properties_dataset_id', 'dataset_custom_properties', ['dataset_id'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop dataset_custom_properties
    op.drop_index('ix_dataset_custom_properties_dataset_id', table_name='dataset_custom_properties')
    op.drop_table('dataset_custom_properties')
    
    # Drop dataset_tags
    op.drop_index('ix_dataset_tags_dataset_id', table_name='dataset_tags')
    op.drop_table('dataset_tags')
    
    # Drop dataset_subscriptions
    op.drop_index('ix_dataset_subscriptions_subscriber_email', table_name='dataset_subscriptions')
    op.drop_index('ix_dataset_subscriptions_dataset_id', table_name='dataset_subscriptions')
    op.drop_table('dataset_subscriptions')
    
    # Drop datasets indexes
    op.drop_index('ix_dataset_full_path', table_name='datasets')
    op.drop_index('ix_datasets_published', table_name='datasets')
    op.drop_index('ix_datasets_status', table_name='datasets')
    op.drop_index('ix_datasets_project_id', table_name='datasets')
    op.drop_index('ix_datasets_owner_team_id', table_name='datasets')
    op.drop_index('ix_datasets_contract_id', table_name='datasets')
    op.drop_index('ix_datasets_environment', table_name='datasets')
    op.drop_index('ix_datasets_object_name', table_name='datasets')
    op.drop_index('ix_datasets_schema_name', table_name='datasets')
    op.drop_index('ix_datasets_catalog_name', table_name='datasets')
    op.drop_index('ix_datasets_asset_type', table_name='datasets')
    op.drop_index('ix_datasets_name', table_name='datasets')
    
    # Drop datasets table
    op.drop_table('datasets')

