from databricks.sdk import WorkspaceClient
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.common.workspace_client import get_obo_workspace_client, get_workspace_client_dependency
from src.common.dependencies import SettingsDep
from src.common.config import Settings
from src.controller.catalog_commander_manager import CatalogCommanderManager
# Import permission checker and feature level
from src.common.authorization import PermissionChecker
from src.common.features import FeatureAccessLevel

# Configure logging
from src.common.logging import get_logger
logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Catalog Commander"])

# Define the feature ID for permission checks
CATALOG_COMMANDER_FEATURE_ID = 'catalog-commander'

def get_catalog_manager(
    request: Request,
    sp_client: WorkspaceClient = Depends(get_workspace_client_dependency()),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> CatalogCommanderManager:
    """Get a configured catalog commander manager instance with both SP and OBO clients.

    The manager uses two workspace clients:
    - SP (Service Principal) client: For administrative operations, jobs, etc.
    - OBO (On-Behalf-Of) client: For browsing catalogs/schemas/tables with user permissions
    
    This ensures that catalog browsing respects the user's Unity Catalog permissions
    while administrative operations use the service principal's elevated privileges.

    Args:
        request: FastAPI request object (injected by FastAPI)
        sp_client: Service principal workspace client (injected by FastAPI)
        obo_client: OBO workspace client with user's token (injected by FastAPI)

    Returns:
        Configured catalog commander manager instance with both clients
    """
    # Get settings from app state for warehouse configuration
    settings = getattr(request.app.state, 'settings', None)
    return CatalogCommanderManager(sp_client=sp_client, obo_client=obo_client, settings=settings)

# --- Read-Only Routes (Require READ_ONLY or higher) ---

@router.get('/catalogs', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_catalogs(
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager),
    force_refresh: bool = False
):
    """List all catalogs in the Databricks workspace."""
    try:
        logger.info(f"Starting to fetch catalogs (force_refresh={force_refresh})")
        if force_refresh:
            # Clear the catalogs cache
            catalog_manager.client.clear_cache('catalogs.list')
        catalogs = catalog_manager.list_catalogs()
        logger.info(f"Successfully fetched {len(catalogs)} catalogs")
        return catalogs
    except Exception as e:
        error_msg = f"Failed to fetch catalogs: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get('/catalogs/{catalog_name}/schemas', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_schemas(
    catalog_name: str,
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List all schemas in a catalog."""
    try:
        logger.info(f"Fetching schemas for catalog: {catalog_name}")
        schemas = catalog_manager.list_schemas(catalog_name)
        logger.info(f"Successfully fetched {len(schemas)} schemas for catalog {catalog_name}")
        return schemas
    except Exception as e:
        error_msg = f"Failed to fetch schemas for catalog {catalog_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get('/catalogs/{catalog_name}/schemas/{schema_name}/tables', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_tables(
    catalog_name: str,
    schema_name: str,
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List all tables in a schema."""
    try:
        logger.info(f"Fetching tables for schema: {catalog_name}.{schema_name}")
        tables = catalog_manager.list_tables(catalog_name, schema_name)
        logger.info(f"Successfully fetched {len(tables)} tables for schema {catalog_name}.{schema_name}")
        return tables
    except Exception as e:
        error_msg = f"Failed to fetch tables for schema {catalog_name}.{schema_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get('/catalogs/{catalog_name}/schemas/{schema_name}/views', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_views(
    catalog_name: str,
    schema_name: str,
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List all views in a schema."""
    try:
        logger.info(f"Fetching views for schema: {catalog_name}.{schema_name}")
        views = catalog_manager.list_views(catalog_name, schema_name)
        logger.info(f"Successfully fetched {len(views)} views for schema {catalog_name}.{schema_name}")
        return views
    except Exception as e:
        error_msg = f"Failed to fetch views for schema {catalog_name}.{schema_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get('/catalogs/{catalog_name}/schemas/{schema_name}/functions', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_functions(
    catalog_name: str,
    schema_name: str,
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List all functions in a schema."""
    try:
        logger.info(f"Fetching functions for schema: {catalog_name}.{schema_name}")
        functions = catalog_manager.list_functions(catalog_name, schema_name)
        logger.info(f"Successfully fetched {len(functions)} functions for schema {catalog_name}.{schema_name}")
        return functions
    except Exception as e:
        error_msg = f"Failed to fetch functions for schema {catalog_name}.{schema_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get('/catalogs/{catalog_name}/schemas/{schema_name}/objects', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def list_objects(
    catalog_name: str,
    schema_name: str,
    asset_types: Optional[List[str]] = Query(
        default=None,
        description="Filter by asset types (table, view, function, model, volume, metric). If not provided, returns all types."
    ),
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List all objects (tables, views, functions, models, volumes, metrics) in a schema.
    
    This unified endpoint returns all securable objects in a schema,
    optionally filtered by asset type(s).
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        asset_types: Optional list of asset types to filter by
        
    Returns:
        List of objects with their type information
    """
    try:
        # Handle comma-separated asset types (e.g., "table,view" as a single string)
        parsed_types = None
        if asset_types:
            parsed_types = []
            for t in asset_types:
                # Split each item by comma in case it's "table,view" format
                parsed_types.extend([x.strip() for x in t.split(',') if x.strip()])
        
        logger.info(f"Fetching objects for schema: {catalog_name}.{schema_name} (types={parsed_types})")
        objects = catalog_manager.list_objects(catalog_name, schema_name, asset_types=parsed_types)
        logger.info(f"Successfully fetched {len(objects)} objects for schema {catalog_name}.{schema_name}")
        return objects
    except Exception as e:
        error_msg = f"Failed to fetch objects for schema {catalog_name}.{schema_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get(
    '/catalogs/{catalog_name}/schemas/{schema_name}/objects/{object_name}/columns',
    dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def list_object_columns(
    catalog_name: str,
    schema_name: str,
    object_name: str,
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """List columns for a table or view. Used by UC asset lookup when includeColumns is true."""
    try:
        logger.info(f"Fetching columns for {catalog_name}.{schema_name}.{object_name}")
        columns = catalog_manager.list_columns(catalog_name, schema_name, object_name)
        return columns
    except Exception as e:
        error_msg = f"Failed to fetch columns for {catalog_name}.{schema_name}.{object_name}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get('/catalogs/dataset/{dataset_path:path}', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def get_dataset(
    dataset_path: str,
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of rows to return"),
    offset: int = Query(default=0, ge=0, description="Number of rows to skip for pagination"),
    catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)
):
    """Get dataset content and schema with paginated data rows."""
    try:
        logger.info(f"Fetching dataset: {dataset_path} (limit={limit}, offset={offset})")
        dataset = catalog_manager.get_dataset(dataset_path, limit=limit, offset=offset)
        logger.info(f"Successfully fetched dataset {dataset_path}")
        return dataset
    except ValueError as e:
        # Validation errors (invalid path, SQL injection attempt)
        error_msg = f"Invalid dataset path: {e!s}"
        logger.warning(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        error_msg = f"Failed to fetch dataset {dataset_path}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

# --- Health Check (Usually doesn't require auth, but let's add READ_ONLY for consistency) ---
@router.get('/catalogs/health', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.READ_ONLY))])
async def health_check(catalog_manager: CatalogCommanderManager = Depends(get_catalog_manager)):
    """Check if the catalog API is healthy."""
    try:
        logger.info("Performing health check")
        status = catalog_manager.health_check()
        logger.info("Health check successful")
        return status
    except Exception as e:
        error_msg = f"Health check failed: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

# --- TODO: Add Write Routes (Require FULL or higher) ---
# Placeholder for future routes like move, create, delete, rename
# Example:
# @router.post('/catalogs/move', dependencies=[Depends(PermissionChecker(CATALOG_COMMANDER_FEATURE_ID, FeatureAccessLevel.FULL))])
# async def move_asset(...):
#    ...

def register_routes(app):
    """Register routes with the app"""
    app.include_router(router)
    logger.info("Catalog commander routes registered")
