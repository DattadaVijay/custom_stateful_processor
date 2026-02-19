"""add uq_rdf_triple if missing

Revision ID: u1688q602tt5
Revises: t0577p491rr4
Create Date: 2026-02-12

Adds the unique constraint uq_rdf_triple to rdf_triples only if it does not exist.
This fixes databases that were created with create_all() (fresh DB path) and thus
never had the constraint, so ON CONFLICT in rdf_triples_repository works.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'u1688q602tt5'
down_revision: Union[str, None] = 't0577p491rr4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add uq_rdf_triple to rdf_triples only if it does not exist."""
    conn = op.get_bind()
    conn.execute(sa.text("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE c.conname = 'uq_rdf_triple' AND t.relname = 'rdf_triples'
          ) THEN
            ALTER TABLE rdf_triples
              ADD CONSTRAINT uq_rdf_triple UNIQUE (
                subject_uri, predicate_uri, object_value,
                object_language, object_datatype, context_name
              );
          END IF;
        END $$;
    """))


def downgrade() -> None:
    """Remove uq_rdf_triple only if it exists (no-op if added by this migration)."""
    conn = op.get_bind()
    conn.execute(sa.text("""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE c.conname = 'uq_rdf_triple' AND t.relname = 'rdf_triples'
          ) THEN
            ALTER TABLE rdf_triples DROP CONSTRAINT uq_rdf_triple;
          END IF;
        END $$;
    """))