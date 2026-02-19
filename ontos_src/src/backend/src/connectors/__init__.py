"""
Connectors Package

This package provides a pluggable connector architecture for asset discovery
and metadata retrieval across multiple platforms (Unity Catalog, Snowflake,
Kafka, PowerBI, etc.).

Usage:
    from src.connectors import get_connector, ConnectorRegistry
    
    # Get a connector by type
    uc_connector = get_connector("databricks")
    
    # List assets
    assets = uc_connector.list_assets("catalog.schema")
    
    # Get asset metadata
    metadata = uc_connector.get_asset_metadata("catalog.schema.table")
"""

from src.connectors.base import (
    AssetConnector,
    ConnectorCapabilities,
    ConnectorConfig,
    ConnectorError,
    ConnectorNotFoundError,
    ConnectorConnectionError,
    ConnectorAuthenticationError,
    ConnectorPermissionError,
    ConnectorNotImplementedError,
    ListAssetsOptions,
)
from src.connectors.registry import (
    ConnectorRegistry,
    get_connector,
    register_connector,
    get_registry,
    has_connector,
)

__all__ = [
    # Base classes
    "AssetConnector",
    "ConnectorCapabilities",
    "ConnectorConfig",
    "ListAssetsOptions",
    # Errors
    "ConnectorError",
    "ConnectorNotFoundError",
    "ConnectorConnectionError",
    "ConnectorAuthenticationError",
    "ConnectorPermissionError",
    "ConnectorNotImplementedError",
    # Registry
    "ConnectorRegistry",
    "get_connector",
    "register_connector",
    "get_registry",
    "has_connector",
]

