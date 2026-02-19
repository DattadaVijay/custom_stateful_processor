"""Database model for persisted application settings (key-value store)."""
from sqlalchemy import Column, String, Text, func, TIMESTAMP

from src.common.database import Base


class AppSettingDb(Base):
    """Key-value store for application settings that need to persist across restarts.
    
    Examples of settings stored here:
    - WORKSPACE_DEPLOYMENT_PATH: Path where workflows are deployed in the workspace
    """
    __tablename__ = 'app_settings'

    key = Column(String(255), primary_key=True, comment='Setting key/name')
    value = Column(Text, nullable=True, comment='Setting value (can be null to clear)')
    updated_at = Column(
        TIMESTAMP(timezone=True), 
        server_default=func.now(), 
        onupdate=func.now(), 
        nullable=True,
        comment='Last update timestamp'
    )

    def __repr__(self):
        return f"<AppSettingDb(key='{self.key}', value='{self.value[:50] if self.value else None}...')>"

