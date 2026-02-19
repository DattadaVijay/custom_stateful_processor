"""add_rdf_triples_table

Revision ID: g7244c158ee0
Revises: f6133b047dd9
Create Date: 2025-12-25 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'g7244c158ee0'
down_revision: Union[str, None] = 'cfd416adf7bd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create rdf_triples table for knowledge graph persistence."""
    op.create_table(
        'rdf_triples',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('subject_uri', sa.Text(), nullable=False),
        sa.Column('predicate_uri', sa.Text(), nullable=False),
        sa.Column('object_value', sa.Text(), nullable=False),
        sa.Column('object_is_uri', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('object_language', sa.String(10), nullable=True),
        sa.Column('object_datatype', sa.Text(), nullable=True),
        sa.Column('context_name', sa.Text(), nullable=False, server_default='default'),
        sa.Column('source_type', sa.String(20), nullable=True),
        sa.Column('source_identifier', sa.Text(), nullable=True),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for common query patterns
    op.create_index('ix_rdf_triples_subject_uri', 'rdf_triples', ['subject_uri'])
    op.create_index('ix_rdf_triples_predicate_uri', 'rdf_triples', ['predicate_uri'])
    op.create_index('ix_rdf_triples_context_name', 'rdf_triples', ['context_name'])
    op.create_index('ix_rdf_triples_source_type', 'rdf_triples', ['source_type'])
    
    # Composite index for triple lookups
    op.create_index(
        'ix_rdf_triples_spo',
        'rdf_triples',
        ['subject_uri', 'predicate_uri', 'object_value']
    )
    
    # Unique constraint to prevent duplicate triples within a context
    op.create_unique_constraint(
        'uq_rdf_triple',
        'rdf_triples',
        ['subject_uri', 'predicate_uri', 'object_value', 'object_language', 'object_datatype', 'context_name']
    )


def downgrade() -> None:
    """Drop rdf_triples table."""
    op.drop_constraint('uq_rdf_triple', 'rdf_triples', type_='unique')
    op.drop_index('ix_rdf_triples_spo', table_name='rdf_triples')
    op.drop_index('ix_rdf_triples_source_type', table_name='rdf_triples')
    op.drop_index('ix_rdf_triples_context_name', table_name='rdf_triples')
    op.drop_index('ix_rdf_triples_predicate_uri', table_name='rdf_triples')
    op.drop_index('ix_rdf_triples_subject_uri', table_name='rdf_triples')
    op.drop_table('rdf_triples')

