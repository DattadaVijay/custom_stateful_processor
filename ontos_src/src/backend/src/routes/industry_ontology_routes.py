"""
FastAPI routes for Industry Ontology Library.

Provides endpoints for:
- Listing industry verticals and ontologies
- Getting module selection trees
- Importing selected modules
- Managing ontology cache
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Request

from src.controller.industry_ontology_manager import IndustryOntologyManager
from src.controller.semantic_models_manager import SemanticModelsManager
from src.models.industry_ontology import (
    VerticalSummary,
    OntologySummary,
    ModuleTreeNode,
    ImportRequest,
    ImportResult,
    CacheStatus,
    RefreshResult,
)
from src.common.dependencies import CurrentUserDep, AuditManagerDep, DBSessionDep, AuditCurrentUserDep
from src.common.authorization import PermissionChecker
from src.common.features import FeatureAccessLevel
from src.common.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/industry-ontologies", tags=["Industry Ontologies"])


def get_industry_ontology_manager(request: Request) -> IndustryOntologyManager:
    """Retrieves the IndustryOntologyManager singleton from app.state."""
    manager = getattr(request.app.state, 'industry_ontology_manager', None)
    if manager is None:
        # Initialize on first access if not yet created
        from pathlib import Path
        from src.common.database import get_db
        
        db = next(get_db())
        semantic_models_manager = getattr(request.app.state, 'semantic_models_manager', None)
        
        manager = IndustryOntologyManager(
            db=db,
            semantic_models_manager=semantic_models_manager
        )
        request.app.state.industry_ontology_manager = manager
        
    return manager


# =============================================================================
# Vertical and Ontology Listing Endpoints
# =============================================================================

@router.get('/verticals', response_model=List[VerticalSummary])
async def list_verticals(
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> List[VerticalSummary]:
    """
    List all available industry verticals.
    
    Returns a list of verticals with their names, icons, and ontology counts.
    """
    try:
        return manager.list_verticals()
    except Exception as e:
        logger.error(f"Failed to list verticals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/verticals/{vertical_id}/ontologies', response_model=List[OntologySummary])
async def list_ontologies_in_vertical(
    vertical_id: str,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> List[OntologySummary]:
    """
    List ontologies in a specific vertical.
    
    Args:
        vertical_id: The vertical ID (e.g., 'healthcare', 'finance')
    """
    try:
        ontologies = manager.list_ontologies(vertical_id)
        if not ontologies:
            # Check if vertical exists
            vertical = manager.get_vertical(vertical_id)
            if not vertical:
                raise HTTPException(status_code=404, detail=f"Vertical not found: {vertical_id}")
        return ontologies
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list ontologies for {vertical_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/ontologies', response_model=List[OntologySummary])
async def list_all_ontologies(
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> List[OntologySummary]:
    """List all ontologies across all verticals."""
    try:
        return manager.list_all_ontologies()
    except Exception as e:
        logger.error(f"Failed to list all ontologies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/recommendations', response_model=List[OntologySummary])
async def get_recommended_foundations(
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> List[OntologySummary]:
    """Get list of recommended foundational ontologies (BFO, PROV-O, SKOS, etc.)."""
    try:
        return manager.get_recommended_foundations()
    except Exception as e:
        logger.error(f"Failed to get recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Module Tree Endpoints
# =============================================================================

@router.get('/verticals/{vertical_id}/ontologies/{ontology_id}/modules', response_model=List[ModuleTreeNode])
async def get_ontology_modules(
    vertical_id: str,
    ontology_id: str,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> List[ModuleTreeNode]:
    """
    Get the module selection tree for an ontology.
    
    Returns a hierarchical tree of modules that can be selected for import.
    For simple ontologies, this is a flat list.
    For modular ontologies like FIBO, this includes domains and module groups.
    
    Args:
        vertical_id: The vertical ID
        ontology_id: The ontology ID
    """
    try:
        # Verify ontology exists
        ontology = manager.get_ontology(vertical_id, ontology_id)
        if not ontology:
            raise HTTPException(
                status_code=404, 
                detail=f"Ontology not found: {vertical_id}/{ontology_id}"
            )
        
        return manager.get_module_tree(vertical_id, ontology_id)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get modules for {vertical_id}/{ontology_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Import Endpoints
# =============================================================================

@router.post('/import', response_model=ImportResult)
async def import_ontology_modules(
    request: Request,
    import_request: ImportRequest,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_WRITE))
) -> ImportResult:
    """
    Import selected ontology modules into Ontos.
    
    This will:
    1. Resolve dependencies for selected modules
    2. Fetch module content from cache or remote sources
    3. Combine modules into a single RDF graph
    4. Create a new semantic model in Ontos
    
    Args:
        import_request: The import request with module selections
    """
    try:
        result = manager.import_modules(import_request, username=current_user.username)
        
        # Audit log
        audit_manager.log_action(
            db=db,
            username=current_user.username,
            ip_address=request.client.host if request.client else None,
            feature="industry-ontologies",
            action="IMPORT",
            success=result.success,
            details={
                "ontology_id": import_request.ontology_id,
                "vertical_id": import_request.vertical_id,
                "modules_requested": len(import_request.module_ids),
                "modules_imported": len(result.modules_imported),
                "dependencies_added": len(result.dependencies_added),
                "triple_count": result.triple_count,
                "semantic_model_id": result.semantic_model_id,
                "error": result.error,
            }
        )
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to import ontology modules: {e}")
        
        # Audit log failure
        audit_manager.log_action(
            db=db,
            username=current_user.username,
            ip_address=request.client.host if request.client else None,
            feature="industry-ontologies",
            action="IMPORT",
            success=False,
            details={
                "ontology_id": import_request.ontology_id,
                "error": str(e),
            }
        )
        
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Cache Management Endpoints
# =============================================================================

@router.get('/verticals/{vertical_id}/ontologies/{ontology_id}/cache', response_model=CacheStatus)
async def get_cache_status(
    vertical_id: str,
    ontology_id: str,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_ONLY))
) -> CacheStatus:
    """Get the cache status for an ontology."""
    try:
        return manager.get_cache_status(ontology_id)
    except Exception as e:
        logger.error(f"Failed to get cache status for {ontology_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/verticals/{vertical_id}/ontologies/{ontology_id}/refresh', response_model=RefreshResult)
async def refresh_ontology_cache(
    vertical_id: str,
    ontology_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_WRITE))
) -> RefreshResult:
    """
    Refresh the cache for an ontology.
    
    This will clear the existing cache and re-fetch all modules from remote sources.
    """
    try:
        result = manager.refresh_cache(vertical_id, ontology_id)
        
        # Audit log
        audit_manager.log_action(
            db=db,
            username=current_user.username,
            ip_address=request.client.host if request.client else None,
            feature="industry-ontologies",
            action="REFRESH_CACHE",
            success=result.success,
            details={
                "ontology_id": ontology_id,
                "modules_refreshed": result.modules_refreshed,
                "error": result.error,
            }
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to refresh cache for {ontology_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete('/cache')
async def clear_all_cache(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    ontology_id: Optional[str] = None,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_WRITE))
) -> dict:
    """
    Clear cached ontology files.
    
    Args:
        ontology_id: Optional ontology ID to clear. If not provided, clears all cache.
    """
    try:
        count = manager.clear_cache(ontology_id)
        
        # Audit log
        audit_manager.log_action(
            db=db,
            username=current_user.username,
            ip_address=request.client.host if request.client else None,
            feature="industry-ontologies",
            action="CLEAR_CACHE",
            success=True,
            details={
                "ontology_id": ontology_id or "all",
                "files_cleared": count,
            }
        )
        
        return {
            "success": True,
            "files_cleared": count,
            "message": f"Cleared {count} cached files"
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Registry Management Endpoints
# =============================================================================

@router.post('/reload')
async def reload_registry(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: IndustryOntologyManager = Depends(get_industry_ontology_manager),
    _: bool = Depends(PermissionChecker('semantic-models', FeatureAccessLevel.READ_WRITE))
) -> dict:
    """
    Reload the industry ontologies registry from disk.
    
    Use this after modifying the industry_ontologies.yaml file.
    """
    try:
        manager.reload_registry()
        verticals = manager.list_verticals()
        
        # Audit log
        audit_manager.log_action(
            db=db,
            username=current_user.username,
            ip_address=request.client.host if request.client else None,
            feature="industry-ontologies",
            action="RELOAD_REGISTRY",
            success=True,
            details={
                "verticals_loaded": len(verticals),
            }
        )
        
        return {
            "success": True,
            "verticals_loaded": len(verticals),
            "message": f"Reloaded registry with {len(verticals)} verticals"
        }
        
    except Exception as e:
        logger.error(f"Failed to reload registry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Route Registration
# =============================================================================

def register_routes(app):
    """Register industry ontology routes with the FastAPI app."""
    app.include_router(router)
