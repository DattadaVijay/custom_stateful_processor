import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from src.common.authorization import PermissionChecker
from src.common.dependencies import DBSessionDep, AuditManagerDep, AuditCurrentUserDep
from src.common.features import FeatureAccessLevel
from src.controller.security_features_manager import SecurityFeaturesManager
from src.models.security_features import SecurityFeature, SecurityFeatureType

# Configure logging
from src.common.logging import get_logger
logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Security Features"])

# Create a single instance of the manager
manager = SecurityFeaturesManager()

# Check for YAML file in data directory
yaml_path = Path(__file__).parent.parent / 'data' / 'security_features.yaml'
if os.path.exists(yaml_path):
    try:
        # Load data from YAML file
        success = manager.load_from_yaml(yaml_path)
        if success:
            logger.info(f"Successfully loaded security features from {yaml_path}")
        else:
            logger.warning(f"Failed to load security features from {yaml_path}")
    except Exception as e:
        logger.error(f"Error loading security features: {e!s}")

# Pydantic models for request/response
class SecurityFeatureCreate(BaseModel):
    name: str
    description: str
    type: SecurityFeatureType
    target: str
    conditions: List[str] = []
    status: str = "active"

class SecurityFeatureUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[SecurityFeatureType] = None
    target: Optional[str] = None
    conditions: Optional[List[str]] = None
    status: Optional[str] = None

class SecurityFeatureResponse(BaseModel):
    id: str
    name: str
    description: str
    type: SecurityFeatureType
    status: str
    target: str
    conditions: List[str]
    last_updated: datetime

    class Config:
        from_attributes = True

@router.post("/security-features", response_model=SecurityFeatureResponse)
async def create_security_feature(
    feature: SecurityFeatureCreate,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('security-features', FeatureAccessLevel.ADMIN))
) -> SecurityFeatureResponse:
    """Create a new security feature"""
    success = False
    details = {
        "params": {
            "feature_name": feature.name,
            "feature_type": feature.type.value if feature.type else None,
            "target": feature.target,
            "status": feature.status
        }
    }
    created_feature_id = None

    try:
        logger.info(f"Creating security feature: {feature.name}")
        new_feature = SecurityFeature(
            id=str(len(manager.features) + 1),
            name=feature.name,
            description=feature.description,
            type=feature.type,
            status=feature.status,
            target=feature.target,
            conditions=feature.conditions
        )
        created_feature = manager.create_feature(new_feature)
        success = True
        created_feature_id = created_feature.id
        return SecurityFeatureResponse.from_orm(created_feature)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error creating security feature", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to create security feature")
    finally:
        if created_feature_id:
            details["created_resource_id"] = created_feature_id
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="security-features",
            action="CREATE",
            success=success,
            details=details
        )

@router.get("/security-features", response_model=List[SecurityFeatureResponse])
async def list_security_features(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('security-features', FeatureAccessLevel.ADMIN))
):
    """List all security features"""
    success = False
    details = {"params": {}}

    try:
        features = manager.list_features()
        logger.debug(f"Found {len(features)} security features")
        success = True
        details["feature_count"] = len(features)
        return [SecurityFeatureResponse.from_orm(feature) for feature in features]
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error listing security features", exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to list security features")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="security-features",
            action="LIST",
            success=success,
            details=details
        )

@router.get("/security-features/{feature_id}", response_model=SecurityFeatureResponse)
async def get_security_feature(
    feature_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('security-features', FeatureAccessLevel.ADMIN))
) -> SecurityFeatureResponse:
    """Get a security feature by ID"""
    success = False
    details = {
        "params": {
            "feature_id": feature_id
        }
    }

    try:
        logger.debug(f"Getting security feature: {feature_id}")
        feature = manager.get_feature(feature_id)
        if not feature:
            raise HTTPException(status_code=404, detail="Security feature not found")
        success = True
        return SecurityFeatureResponse.from_orm(feature)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error getting security feature %s", feature_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to get security feature")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="security-features",
            action="GET",
            success=success,
            details=details
        )

@router.put("/security-features/{feature_id}", response_model=SecurityFeatureResponse)
async def update_security_feature(
    feature_id: str,
    feature_update: SecurityFeatureUpdate,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('security-features', FeatureAccessLevel.ADMIN))
) -> SecurityFeatureResponse:
    """Update a security feature"""
    success = False
    details = {
        "params": {
            "feature_id": feature_id,
            "updates": feature_update.dict(exclude_unset=True)
        }
    }

    try:
        logger.debug(f"Updating security feature: {feature_id}")
        existing_feature = manager.get_feature(feature_id)
        if not existing_feature:
            raise HTTPException(status_code=404, detail="Security feature not found")

        update_data = feature_update.dict(exclude_unset=True)
        updated_feature = SecurityFeature(
            id=existing_feature.id,
            name=update_data.get('name', existing_feature.name),
            description=update_data.get('description', existing_feature.description),
            type=update_data.get('type', existing_feature.type),
            status=update_data.get('status', existing_feature.status),
            target=update_data.get('target', existing_feature.target),
            conditions=update_data.get('conditions', existing_feature.conditions),
            last_updated=datetime.utcnow()
        )

        result = manager.update_feature(feature_id, updated_feature)
        if not result:
            raise HTTPException(status_code=500, detail="Failed to update security feature")
        success = True
        return SecurityFeatureResponse.from_orm(result)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error updating security feature %s", feature_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to update security feature")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="security-features",
            action="UPDATE",
            success=success,
            details=details
        )

@router.delete("/security-features/{feature_id}")
async def delete_security_feature(
    feature_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('security-features', FeatureAccessLevel.ADMIN))
):
    """Delete a security feature"""
    success = False
    details = {
        "params": {
            "feature_id": feature_id
        }
    }

    try:
        logger.debug(f"Deleting security feature: {feature_id}")
        if not manager.delete_feature(feature_id):
            raise HTTPException(status_code=404, detail="Security feature not found")
        success = True
        return {"message": "Security feature deleted successfully"}
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        logger.error("Error deleting security feature %s", feature_id, exc_info=True)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail="Failed to delete security feature")
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="security-features",
            action="DELETE",
            success=success,
            details=details
        )

def register_routes(app):
    """Register security features routes with the app"""
    app.include_router(router)
    logger.info("Security features routes registered")
