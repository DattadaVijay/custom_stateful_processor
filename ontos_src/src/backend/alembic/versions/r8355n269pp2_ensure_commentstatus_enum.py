"""Ensure comment enum types exist with all required values

Revision ID: r8355n269pp2
Revises: 5845de034cb6
Create Date: 2026-01-27

This migration ensures the commentstatus and commenttype PostgreSQL enum types
exist with all required values. These enums were previously only created by 
SQLAlchemy's create_all() for fresh databases, or by migrations that used
checkfirst=True which skips creation if enum exists but doesn't add missing values.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'r8355n269pp2'
down_revision: Union[str, None] = '5845de034cb6'
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
    
    # Ensure commentstatus enum has all required values
    ensure_enum_values(conn, 'commentstatus', ['active', 'deleted'])
    
    # Ensure commenttype enum has all required values
    ensure_enum_values(conn, 'commenttype', ['comment', 'rating'])


def downgrade() -> None:
    # Don't drop the enum as the comments table might still reference it
    # and dropping would cause data loss
    pass

