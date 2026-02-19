"""Repository for persisted application settings (key-value store)."""
from typing import Dict, Optional

from sqlalchemy.orm import Session

from src.db_models.app_settings import AppSettingDb
from src.common.logging import get_logger

logger = get_logger(__name__)


class AppSettingsRepository:
    """Repository for CRUD operations on app_settings table."""

    def get_by_key(self, db: Session, key: str) -> Optional[str]:
        """Get a setting value by key.
        
        Args:
            db: Database session
            key: Setting key to look up
            
        Returns:
            The setting value, or None if not found
        """
        setting = db.query(AppSettingDb).filter(AppSettingDb.key == key).first()
        return setting.value if setting else None

    def set(self, db: Session, key: str, value: Optional[str]) -> AppSettingDb:
        """Set a setting value (upsert).
        
        Args:
            db: Database session
            key: Setting key
            value: Setting value (can be None to clear)
            
        Returns:
            The created or updated setting
        """
        setting = db.query(AppSettingDb).filter(AppSettingDb.key == key).first()
        if setting:
            setting.value = value
            logger.debug(f"Updated app setting '{key}'")
        else:
            setting = AppSettingDb(key=key, value=value)
            db.add(setting)
            logger.debug(f"Created app setting '{key}'")
        db.commit()
        db.refresh(setting)
        return setting

    def get_all(self, db: Session) -> Dict[str, Optional[str]]:
        """Get all settings as a dictionary.
        
        Args:
            db: Database session
            
        Returns:
            Dictionary of key -> value for all settings
        """
        settings = db.query(AppSettingDb).all()
        return {s.key: s.value for s in settings}

    def delete(self, db: Session, key: str) -> bool:
        """Delete a setting by key.
        
        Args:
            db: Database session
            key: Setting key to delete
            
        Returns:
            True if deleted, False if not found
        """
        setting = db.query(AppSettingDb).filter(AppSettingDb.key == key).first()
        if setting:
            db.delete(setting)
            db.commit()
            logger.debug(f"Deleted app setting '{key}'")
            return True
        return False


# Singleton instance
app_settings_repo = AppSettingsRepository()

