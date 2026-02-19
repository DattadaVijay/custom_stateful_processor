"""
Data Catalog / Data Dictionary Routes

Provides REST API endpoints for:
- Column dictionary browsing (all columns across tables)
- Column search
- Table details
- Lineage visualization
- Impact analysis
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Request, Query

from databricks.sdk import WorkspaceClient
from sqlalchemy.orm import Session

from src.common.logging import get_logger
from src.common.authorization import PermissionChecker
from src.common.features import FeatureAccessLevel
from src.common.workspace_client import get_obo_workspace_client
from src.common.config import get_settings, Settings
from src.common.dependencies import DBSessionDep
from src.controller.data_catalog_manager import DataCatalogManager
from src.controller.datasets_manager import DatasetsManager
from src.controller.data_contracts_manager import DataContractsManager
from src.models.data_catalog import (
    DataDictionaryResponse,
    ColumnSearchResponse,
    TableListResponse,
    TableInfo,
    LineageGraph,
    ImpactAnalysis,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/data-catalog", tags=["Data Catalog"])

DATA_CATALOG_FEATURE_ID = "data-catalog"


def get_data_catalog_manager(
    request: Request,
    db: Session = Depends(lambda: None),  # Will be replaced by route-level dep
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> DataCatalogManager:
    """
    Get or create DataCatalogManager instance.
    
    Uses OBO client and queries ONLY registered assets from the database.
    Does NOT scan the entire Unity Catalog.
    """
    settings = getattr(request.app.state, 'settings', None)
    
    # Get managers from app state
    datasets_manager = getattr(request.app.state, 'datasets_manager', None)
    contracts_manager = getattr(request.app.state, 'data_contracts_manager', None)
    
    return DataCatalogManager(
        obo_client=obo_client,
        db_session=db,
        datasets_manager=datasets_manager,
        contracts_manager=contracts_manager,
        settings=settings
    )


# =============================================================================
# Column Dictionary Endpoints
# =============================================================================

def _get_manager(request: Request, db: DBSessionDep, obo_client: WorkspaceClient = Depends(get_obo_workspace_client)) -> DataCatalogManager:
    """Helper to create manager with all dependencies."""
    settings = getattr(request.app.state, 'settings', None)
    datasets_manager = getattr(request.app.state, 'datasets_manager', None)
    contracts_manager = getattr(request.app.state, 'data_contracts_manager', None)
    
    return DataCatalogManager(
        obo_client=obo_client,
        db_session=db,
        datasets_manager=datasets_manager,
        contracts_manager=contracts_manager,
        settings=settings
    )


@router.get(
    "/columns",
    response_model=DataDictionaryResponse,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_all_columns(
    request: Request,
    db: DBSessionDep,
    catalog: Optional[str] = Query(None, description="Filter to specific catalog"),
    schema: Optional[str] = Query(None, description="Filter to specific schema"),
    table: Optional[str] = Query(None, description="Filter to specific table (FQN)"),
    limit: int = Query(2000, ge=1, le=5000, description="Maximum columns to return"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> DataDictionaryResponse:
    """
    Get all columns from REGISTERED assets for the Data Dictionary view.
    
    Only returns columns from tables that are registered as Datasets.
    Does NOT scan the entire Unity Catalog.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting columns from registered assets (catalog={catalog}, schema={schema}, table={table})")
        return manager.get_all_columns(
            catalog_filter=catalog,
            schema_filter=schema,
            table_filter=table,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Error getting columns: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch columns: {str(e)}")


@router.get(
    "/columns/search",
    response_model=ColumnSearchResponse,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def search_columns(
    request: Request,
    db: DBSessionDep,
    q: str = Query(..., min_length=1, description="Search query for column name"),
    catalog: Optional[str] = Query(None, description="Filter to specific catalog"),
    schema: Optional[str] = Query(None, description="Filter to specific schema"),
    table: Optional[str] = Query(None, description="Filter to specific table (FQN)"),
    limit: int = Query(500, ge=1, le=2000, description="Maximum results"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> ColumnSearchResponse:
    """
    Search columns by name across REGISTERED assets only.
    
    Use this to find all registered tables containing a column with a specific name.
    Does NOT scan the entire Unity Catalog.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Searching columns in registered assets: q='{q}'")
        return manager.search_columns(
            query=q,
            catalog_filter=catalog,
            schema_filter=schema,
            table_filter=table,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Error searching columns: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to search columns: {str(e)}")


# =============================================================================
# Table Endpoints
# =============================================================================

@router.get(
    "/tables",
    response_model=TableListResponse,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_table_list(
    request: Request,
    db: DBSessionDep,
    catalog: Optional[str] = Query(None, description="Filter to specific catalog"),
    schema: Optional[str] = Query(None, description="Filter to specific schema"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> TableListResponse:
    """
    Get list of REGISTERED tables for the filter dropdown.
    
    Only returns tables registered as Datasets.
    Does NOT scan the entire Unity Catalog.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting registered table list (catalog={catalog}, schema={schema})")
        return manager.get_table_list(
            catalog_filter=catalog,
            schema_filter=schema
        )
    except Exception as e:
        logger.error(f"Error getting table list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table list: {str(e)}")


@router.get(
    "/tables/{table_fqn:path}",
    response_model=TableInfo,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_table_details(
    request: Request,
    db: DBSessionDep,
    table_fqn: str,
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> TableInfo:
    """
    Get full table details including all columns.
    
    Args:
        table_fqn: Fully qualified table name (catalog.schema.table)
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting table details: {table_fqn}")
        result = manager.get_table_details(table_fqn)
        if not result:
            raise HTTPException(status_code=404, detail=f"Table not found: {table_fqn}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting table details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table details: {str(e)}")


# =============================================================================
# Lineage Endpoints
# =============================================================================

@router.get(
    "/tables/{table_fqn:path}/lineage",
    response_model=LineageGraph,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_table_lineage(
    request: Request,
    db: DBSessionDep,
    table_fqn: str,
    direction: str = Query("both", regex="^(upstream|downstream|both)$", description="Lineage direction"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> LineageGraph:
    """
    Get lineage graph for a table.
    
    Returns nodes and edges for visualization of upstream and/or
    downstream dependencies.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting lineage for {table_fqn}, direction={direction}")
        return manager.get_table_lineage(table_fqn, direction)
    except Exception as e:
        logger.error(f"Error getting lineage: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get lineage: {str(e)}")


@router.get(
    "/tables/{table_fqn:path}/columns/{column_name}/lineage",
    response_model=LineageGraph,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_column_lineage(
    request: Request,
    db: DBSessionDep,
    table_fqn: str,
    column_name: str,
    direction: str = Query("both", regex="^(upstream|downstream|both)$", description="Lineage direction"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> LineageGraph:
    """
    Get column-level lineage.
    
    Traces lineage for a specific column, showing how data flows
    through transformations.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting column lineage for {table_fqn}.{column_name}")
        return manager.get_column_lineage(table_fqn, column_name, direction)
    except Exception as e:
        logger.error(f"Error getting column lineage: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get column lineage: {str(e)}")


# =============================================================================
# Impact Analysis Endpoints
# =============================================================================

@router.get(
    "/tables/{table_fqn:path}/impact",
    response_model=ImpactAnalysis,
    dependencies=[Depends(PermissionChecker(DATA_CATALOG_FEATURE_ID, FeatureAccessLevel.READ_ONLY))]
)
async def get_table_impact(
    request: Request,
    db: DBSessionDep,
    table_fqn: str,
    column: Optional[str] = Query(None, description="Optional column for column-level impact"),
    obo_client: WorkspaceClient = Depends(get_obo_workspace_client)
) -> ImpactAnalysis:
    """
    Get impact analysis for changing a table or column.
    
    Analyzes all downstream dependencies that would be affected
    by a change to this table or column.
    """
    try:
        manager = _get_manager(request, db, obo_client)
        logger.info(f"Getting impact analysis for {table_fqn}" + (f".{column}" if column else ""))
        return manager.get_impact_analysis(table_fqn, column)
    except Exception as e:
        logger.error(f"Error getting impact analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get impact analysis: {str(e)}")


# =============================================================================
# Route Registration
# =============================================================================

def register_routes(app):
    """Register data catalog routes with the FastAPI app."""
    app.include_router(router)
    logger.info("Data Catalog routes registered")

