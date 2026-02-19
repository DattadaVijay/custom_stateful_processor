import os
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request

from src.common.authorization import PermissionChecker
from src.common.dependencies import DBSessionDep, AuditManagerDep, AuditCurrentUserDep
from src.common.features import FeatureAccessLevel
from src.common.workspace_client import get_workspace_client_dependency
from src.controller.entitlements_sync_manager import EntitlementsSyncManager
from src.models.entitlements_sync import EntitlementSyncConfig

# Configure logging
from src.common.logging import get_logger
logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Entitlements Sync"])

# Create a function to get the manager with dependency injection
def get_entitlements_sync_manager(workspace_client = Depends(get_workspace_client_dependency(timeout=30))):
    manager = EntitlementsSyncManager()
    manager.workspace_client = workspace_client

    # Check for YAML file in data directory
    yaml_path = Path(__file__).parent.parent / 'data' / 'entitlements_sync.yaml'
    if os.path.exists(yaml_path):
        try:
            manager.load_from_yaml(str(yaml_path))
            logger.info(f"Successfully loaded entitlements sync configurations from {yaml_path}")
        except Exception as e:
            logger.exception(f"Error loading entitlements sync configurations from YAML: {e!s}")

    return manager

@router.get("/entitlements-sync/configs", response_model=List[EntitlementSyncConfig])
async def get_configs(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Get all entitlements sync configurations"""
    success = False
    details = {"params": {}}

    try:
        configs = manager.get_configs()
        success = True
        details["config_count"] = len(configs)
        return configs
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error getting entitlements sync configs", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to get entitlements sync configs")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="LIST",
            success=success,
            details=details
        )

@router.get("/entitlements-sync/configs/{config_id}", response_model=EntitlementSyncConfig)
async def get_config(
    config_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Get a specific sync configuration by ID"""
    success = False
    details = {
        "params": {
            "config_id": config_id
        }
    }

    try:
        config = manager.get_config(config_id)
        if not config:
            raise HTTPException(status_code=404, detail="Configuration not found")
        success = True
        return config
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error getting entitlements sync config %s", config_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to get entitlements sync config")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="GET",
            success=success,
            details=details
        )

@router.post("/entitlements-sync/configs", response_model=EntitlementSyncConfig)
async def create_config(
    config: EntitlementSyncConfig,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Create a new sync configuration"""
    success = False
    details = {
        "params": {
            "config_id": config.id if config.id else None,
            "config_name": config.name if hasattr(config, 'name') else None
        }
    }
    created_config_id = None

    try:
        created_config = manager.create_config(config)
        success = True
        created_config_id = created_config.id if created_config.id else None
        return created_config
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error creating entitlements sync config", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to create entitlements sync config")
    finally:
        if created_config_id:
            details["created_resource_id"] = created_config_id
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="CREATE",
            success=success,
            details=details
        )

@router.put("/entitlements-sync/configs/{config_id}", response_model=EntitlementSyncConfig)
async def update_config(
    config_id: str,
    config: EntitlementSyncConfig,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Update an existing sync configuration"""
    success = False
    details = {
        "params": {
            "config_id": config_id
        }
    }

    try:
        updated_config = manager.update_config(config_id, config)
        if not updated_config:
            raise HTTPException(status_code=404, detail="Configuration not found")
        success = True
        return updated_config
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error updating entitlements sync config %s", config_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to update entitlements sync config")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="UPDATE",
            success=success,
            details=details
        )

@router.delete("/entitlements-sync/configs/{config_id}")
async def delete_config(
    config_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Delete a sync configuration"""
    success = False
    details = {
        "params": {
            "config_id": config_id
        }
    }

    try:
        if not manager.delete_config(config_id):
            raise HTTPException(status_code=404, detail="Configuration not found")
        success = True
        return {"message": "Configuration deleted successfully"}
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error deleting entitlements sync config %s", config_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to delete entitlements sync config")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="DELETE",
            success=success,
            details=details
        )

@router.get("/entitlements-sync/connections")
async def get_connections(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Get available Unity Catalog connections"""
    success = False
    details = {"params": {}}

    try:
        connections = manager.get_connections()
        success = True
        details["connection_count"] = len(connections) if connections else 0
        return connections
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error getting Unity Catalog connections", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to get Unity Catalog connections")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="GET_CONNECTIONS",
            success=success,
            details=details
        )

@router.get("/entitlements-sync/catalogs")
async def get_catalogs(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    manager: EntitlementsSyncManager = Depends(get_entitlements_sync_manager),
    _: bool = Depends(PermissionChecker('entitlements-sync', FeatureAccessLevel.ADMIN))
):
    """Get available Unity Catalog catalogs"""
    success = False
    details = {"params": {}}

    try:
        catalogs = manager.get_catalogs()
        success = True
        details["catalog_count"] = len(catalogs) if catalogs else 0
        return catalogs
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error getting Unity Catalog catalogs", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to get Unity Catalog catalogs")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="GET_CATALOGS",
            success=success,
            details=details
        )

def register_routes(app):
    """Register routes with the app"""
    app.include_router(router)
    logger.info("Entitlements Sync routes registered")
