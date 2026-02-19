"""Ensure commenttype enum has all required values

Revision ID: s9466o380qq3
Revises: r8355n269pp2
Create Date: 2026-01-27

This migration ensures the commenttype PostgreSQL enum has the 'rating' value.
The previous migration o5022k936mm8 used checkfirst=True which skips enum creation
if it already exists, but doesn't add missing values to existing enums.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 's9466o380qq3'
down_revision: Union[str, None] = 'r8355n269pp2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def ensure_enum_values(conn, enum_name: str, required_values: list[str]) -> None:
    """Ensure a PostgreSQL enum type exists with all required values."""
    # Check if the enum type exists
    result = conn.execute(sa.text(
        "SELECT 1 FROM pg_type WHERE typname = :enum_name"
    ), {"enum_name": enum_name})
    enum_exists = result.scalar() is not None
    
    if not enum_exists:
        # Create the enum type with all values
        values_str = ", ".join(f"'{v}'" for v in required_values)
        conn.execute(sa.text(f"CREATE TYPE {enum_name} AS ENUM ({values_str})"))
    else:
        # Enum exists, ensure all required values are present
        result = conn.execute(sa.text("""
            SELECT enumlabel FROM pg_enum 
            WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = :enum_name)
        """), {"enum_name": enum_name})
        existing_values = {row[0] for row in result}
        
        # Add missing values (PostgreSQL 9.1+ supports ADD VALUE IF NOT EXISTS)
        for value in required_values:
            if value not in existing_values:
                conn.execute(sa.text(
                    f"ALTER TYPE {enum_name} ADD VALUE IF NOT EXISTS '{value}'"
                ))


def upgrade() -> None:
    conn = op.get_bind()
    
    # Ensure commenttype enum has all required values (including 'rating')
    ensure_enum_values(conn, 'commenttype', ['comment', 'rating'])
    
    # Also re-check commentstatus in case previous migration was run with old code
    ensure_enum_values(conn, 'commentstatus', ['active', 'deleted'])


def downgrade() -> None:
    # Don't remove enum values as it would cause data loss
    pass

