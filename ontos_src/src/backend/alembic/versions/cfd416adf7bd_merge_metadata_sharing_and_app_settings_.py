"""merge metadata_sharing and app_settings branches

Revision ID: cfd416adf7bd
Revises: 5a8b9c0d1e2f, e1f2a3b4c5d6
Create Date: 2025-12-20 18:38:43.357508

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cfd416adf7bd'
down_revision: Union[str, None] = ('5a8b9c0d1e2f', 'e1f2a3b4c5d6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
