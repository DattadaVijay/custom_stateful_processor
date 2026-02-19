"""
Connector Registry

This module provides a singleton registry for managing asset connectors.
Connectors are registered by their type (matching ODCS server types) and
can be retrieved for use in asset discovery and metadata retrieval.
"""

from typing import Any, Callable, Dict, List, Optional, Type
import threading

from src.connectors.base import (
    AssetConnector,
    ConnectorConfig,
    ConnectorNotFoundError,
)
from src.common.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Connector Registry
# ============================================================================

class ConnectorRegistry:
    """
    Singleton registry for asset connectors.
    
    This registry manages connector instances and their configurations.
    Connectors can be registered by type and retrieved when needed.
    
    Usage:
        registry = ConnectorRegistry()
        
        # Register a connector class
        registry.register_class("databricks", DatabricksConnector)
        
        # Get a connector instance
        connector = registry.get_connector("databricks")
        
        # List available connectors
        available = registry.list_available()
    """
    
    _instance: Optional["ConnectorRegistry"] = None
    _lock = threading.Lock()
    
    def __new__(cls) -> "ConnectorRegistry":
        """Ensure singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the registry (only once for singleton)."""
        if self._initialized:
            return
        
        # Connector class registry: type -> class
        self._connector_classes: Dict[str, Type[AssetConnector]] = {}
        
        # Connector instance cache: type -> instance
        self._connector_instances: Dict[str, AssetConnector] = {}
        
        # Connector configurations: type -> config
        self._connector_configs: Dict[str, ConnectorConfig] = {}
        
        # Factory functions for lazy initialization: type -> factory
        self._connector_factories: Dict[str, Callable[[], AssetConnector]] = {}
        
        # Default connector type (usually "databricks" in Databricks Apps)
        self._default_connector_type: Optional[str] = None
        
        self._initialized = True
        logger.info("ConnectorRegistry initialized")
    
    # -------------------------------------------------------------------------
    # Registration Methods
    # -------------------------------------------------------------------------
    
    def register_class(
        self,
        connector_type: str,
        connector_class: Type[AssetConnector],
        config: Optional[ConnectorConfig] = None,
        set_as_default: bool = False
    ) -> None:
        """
        Register a connector class.
        
        Args:
            connector_type: The connector type identifier (e.g., "databricks")
            connector_class: The connector class to register
            config: Optional configuration for the connector
            set_as_default: Whether to set this as the default connector
        """
        self._connector_classes[connector_type] = connector_class
        
        if config:
            self._connector_configs[connector_type] = config
        
        if set_as_default:
            self._default_connector_type = connector_type
        
        logger.info(f"Registered connector class: {connector_type} -> {connector_class.__name__}")
    
    def register_factory(
        self,
        connector_type: str,
        factory: Callable[[], AssetConnector],
        set_as_default: bool = False
    ) -> None:
        """
        Register a factory function for lazy connector initialization.
        
        This is useful when the connector requires dependencies that
        are not available at import time.
        
        Args:
            connector_type: The connector type identifier
            factory: A callable that returns a connector instance
            set_as_default: Whether to set this as the default connector
        """
        self._connector_factories[connector_type] = factory
        
        if set_as_default:
            self._default_connector_type = connector_type
        
        logger.info(f"Registered connector factory: {connector_type}")
    
    def register_instance(
        self,
        connector_type: str,
        instance: AssetConnector,
        set_as_default: bool = False
    ) -> None:
        """
        Register a pre-created connector instance.
        
        Args:
            connector_type: The connector type identifier
            instance: The connector instance to register
            set_as_default: Whether to set this as the default connector
        """
        self._connector_instances[connector_type] = instance
        
        if set_as_default:
            self._default_connector_type = connector_type
        
        logger.info(f"Registered connector instance: {connector_type}")
    
    def set_config(
        self,
        connector_type: str,
        config: ConnectorConfig
    ) -> None:
        """
        Set or update the configuration for a connector type.
        
        If an instance exists, it will be discarded and recreated
        with the new configuration on next access.
        
        Args:
            connector_type: The connector type identifier
            config: The new configuration
        """
        self._connector_configs[connector_type] = config
        
        # Clear cached instance so it's recreated with new config
        if connector_type in self._connector_instances:
            del self._connector_instances[connector_type]
            logger.info(f"Cleared cached instance for {connector_type} due to config change")
    
    def set_default(self, connector_type: str) -> None:
        """
        Set the default connector type.
        
        Args:
            connector_type: The connector type to set as default
        """
        if connector_type not in self._connector_classes and \
           connector_type not in self._connector_factories and \
           connector_type not in self._connector_instances:
            logger.warning(f"Setting default to unregistered connector: {connector_type}")
        
        self._default_connector_type = connector_type
        logger.info(f"Set default connector: {connector_type}")
    
    # -------------------------------------------------------------------------
    # Retrieval Methods
    # -------------------------------------------------------------------------
    
    def get_connector(
        self,
        connector_type: Optional[str] = None
    ) -> AssetConnector:
        """
        Get a connector instance by type.
        
        If no type is specified, returns the default connector.
        Instances are cached after first creation.
        
        Args:
            connector_type: The connector type (None for default)
            
        Returns:
            The connector instance
            
        Raises:
            ConnectorNotFoundError: If the connector type is not registered
        """
        if connector_type is None:
            connector_type = self._default_connector_type
            if connector_type is None:
                raise ConnectorNotFoundError(
                    "No default connector configured",
                    connector_type="none"
                )
        
        # Check for cached instance
        if connector_type in self._connector_instances:
            return self._connector_instances[connector_type]
        
        # Try factory function
        if connector_type in self._connector_factories:
            logger.info(f"Creating connector from factory: {connector_type}")
            instance = self._connector_factories[connector_type]()
            self._connector_instances[connector_type] = instance
            return instance
        
        # Try class registration
        if connector_type in self._connector_classes:
            logger.info(f"Creating connector from class: {connector_type}")
            connector_class = self._connector_classes[connector_type]
            config = self._connector_configs.get(connector_type)
            instance = connector_class(config)
            self._connector_instances[connector_type] = instance
            return instance
        
        raise ConnectorNotFoundError(
            f"Connector type '{connector_type}' is not registered",
            connector_type=connector_type
        )
    
    def get_default_connector(self) -> Optional[AssetConnector]:
        """
        Get the default connector if configured.
        
        Returns:
            The default connector instance, or None if not configured
        """
        if self._default_connector_type is None:
            return None
        
        try:
            return self.get_connector(self._default_connector_type)
        except ConnectorNotFoundError:
            return None
    
    def has_connector(self, connector_type: str) -> bool:
        """
        Check if a connector type is registered.
        
        Args:
            connector_type: The connector type to check
            
        Returns:
            True if registered, False otherwise
        """
        return (
            connector_type in self._connector_classes or
            connector_type in self._connector_factories or
            connector_type in self._connector_instances
        )
    
    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------
    
    def list_registered(self) -> List[str]:
        """
        List all registered connector types.
        
        Returns:
            List of connector type identifiers
        """
        types = set()
        types.update(self._connector_classes.keys())
        types.update(self._connector_factories.keys())
        types.update(self._connector_instances.keys())
        return sorted(types)
    
    def list_available(self) -> List[str]:
        """
        List connector types that are available (enabled and healthy).
        
        Returns:
            List of available connector type identifiers
        """
        available = []
        for connector_type in self.list_registered():
            try:
                connector = self.get_connector(connector_type)
                if connector.is_available:
                    available.append(connector_type)
            except Exception as e:
                logger.debug(f"Connector {connector_type} not available: {e}")
        return available
    
    def get_connector_info(self, connector_type: str) -> Dict[str, Any]:
        """
        Get information about a registered connector.
        
        Args:
            connector_type: The connector type
            
        Returns:
            Dict with connector information
        """
        info = {
            "connector_type": connector_type,
            "registered": self.has_connector(connector_type),
            "has_class": connector_type in self._connector_classes,
            "has_factory": connector_type in self._connector_factories,
            "has_instance": connector_type in self._connector_instances,
            "has_config": connector_type in self._connector_configs,
            "is_default": connector_type == self._default_connector_type,
        }
        
        if self.has_connector(connector_type):
            try:
                connector = self.get_connector(connector_type)
                info.update({
                    "display_name": connector.display_name,
                    "description": connector.description,
                    "available": connector.is_available,
                    "capabilities": {
                        "can_list_assets": connector.capabilities.can_list_assets,
                        "can_get_metadata": connector.capabilities.can_get_metadata,
                        "can_get_sample_data": connector.capabilities.can_get_sample_data,
                    }
                })
            except Exception as e:
                info["error"] = str(e)
        
        return info
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all registered connectors.
        
        Returns:
            Dict with health status for each connector
        """
        results = {
            "default_connector": self._default_connector_type,
            "registered_count": len(self.list_registered()),
            "available_count": len(self.list_available()),
            "connectors": {}
        }
        
        for connector_type in self.list_registered():
            try:
                connector = self.get_connector(connector_type)
                results["connectors"][connector_type] = connector.health_check()
            except Exception as e:
                results["connectors"][connector_type] = {
                    "available": False,
                    "error": str(e)
                }
        
        return results
    
    # -------------------------------------------------------------------------
    # Cleanup Methods
    # -------------------------------------------------------------------------
    
    def unregister(self, connector_type: str) -> bool:
        """
        Unregister a connector type.
        
        Args:
            connector_type: The connector type to unregister
            
        Returns:
            True if anything was unregistered, False otherwise
        """
        unregistered = False
        
        if connector_type in self._connector_classes:
            del self._connector_classes[connector_type]
            unregistered = True
        
        if connector_type in self._connector_factories:
            del self._connector_factories[connector_type]
            unregistered = True
        
        if connector_type in self._connector_instances:
            del self._connector_instances[connector_type]
            unregistered = True
        
        if connector_type in self._connector_configs:
            del self._connector_configs[connector_type]
        
        if self._default_connector_type == connector_type:
            self._default_connector_type = None
        
        if unregistered:
            logger.info(f"Unregistered connector: {connector_type}")
        
        return unregistered
    
    def clear(self) -> None:
        """Clear all registered connectors."""
        self._connector_classes.clear()
        self._connector_factories.clear()
        self._connector_instances.clear()
        self._connector_configs.clear()
        self._default_connector_type = None
        logger.info("Cleared all connectors from registry")
    
    def reset(self) -> None:
        """
        Reset the registry to its initial state.
        
        This is primarily useful for testing.
        """
        self.clear()


# ============================================================================
# Module-level convenience functions
# ============================================================================

_registry: Optional[ConnectorRegistry] = None


def get_registry() -> ConnectorRegistry:
    """
    Get the global connector registry.
    
    Returns:
        The singleton ConnectorRegistry instance
    """
    global _registry
    if _registry is None:
        _registry = ConnectorRegistry()
    return _registry


def get_connector(connector_type: Optional[str] = None) -> AssetConnector:
    """
    Get a connector from the global registry.
    
    Args:
        connector_type: The connector type (None for default)
        
    Returns:
        The connector instance
    """
    return get_registry().get_connector(connector_type)


def register_connector(
    connector_type: str,
    connector_class: Type[AssetConnector],
    config: Optional[ConnectorConfig] = None,
    set_as_default: bool = False
) -> None:
    """
    Register a connector class in the global registry.
    
    Args:
        connector_type: The connector type identifier
        connector_class: The connector class to register
        config: Optional configuration
        set_as_default: Whether to set as default
    """
    get_registry().register_class(
        connector_type,
        connector_class,
        config,
        set_as_default
    )


def has_connector(connector_type: str) -> bool:
    """
    Check if a connector is registered.
    
    Args:
        connector_type: The connector type to check
        
    Returns:
        True if registered
    """
    return get_registry().has_connector(connector_type)

