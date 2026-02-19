from fastapi import APIRouter, Depends, HTTPException, Request

from src.common.authorization import PermissionChecker
from src.common.dependencies import DBSessionDep, AuditManagerDep, AuditCurrentUserDep
from src.common.features import FeatureAccessLevel
from src.controller.entitlements_manager import EntitlementsManager

# Configure logging
from src.common.logging import get_logger
logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Entitlements"])

# Create a single instance of the manager (YAML loaded automatically in __init__)
entitlements_manager = EntitlementsManager()

@router.get('/entitlements/personas')
async def get_personas(
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Get all personas"""
    success = False
    details = {"params": {}}

    try:
        formatted_personas = entitlements_manager.get_personas_formatted()
        logger.info(f"Retrieved {len(formatted_personas)} personas")
        success = True
        details["persona_count"] = len(formatted_personas)
        return formatted_personas
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error retrieving personas: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
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

@router.get('/entitlements/personas/{persona_id}')
async def get_persona(
    persona_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Get a specific persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id
        }
    }

    try:
        formatted_persona = entitlements_manager.get_persona_formatted(persona_id)
        if not formatted_persona:
            logger.warning(f"Persona not found with ID: {persona_id}")
            raise HTTPException(status_code=404, detail="Persona not found")

        logger.info(f"Retrieved persona with ID: {persona_id}")
        success = True
        return formatted_persona
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error retrieving persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
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

@router.post('/entitlements/personas')
async def create_persona(
    persona_data: dict,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Create a new persona"""
    success = False
    details = {
        "params": {
            "persona_name": persona_data.get('name', ''),
            "privileges_count": len(persona_data.get('privileges', []))
        }
    }
    created_persona_id = None

    try:
        logger.info(f"Creating new persona: {persona_data.get('name', '')}")

        # Create persona (auto-persists to YAML)
        persona = entitlements_manager.create_persona(
            name=persona_data.get('name', ''),
            description=persona_data.get('description', ''),
            privileges=persona_data.get('privileges', [])
        )

        # Format and return
        response = entitlements_manager._format_persona(persona)
        logger.info(f"Successfully created persona with ID: {persona.id}")
        success = True
        created_persona_id = persona.id
        return response
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error creating persona: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if created_persona_id:
            details["created_resource_id"] = created_persona_id
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="CREATE",
            success=success,
            details=details
        )

@router.put('/entitlements/personas/{persona_id}')
async def update_persona(
    persona_id: str,
    persona_data: dict,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Update a persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id,
            "update_fields": list(persona_data.keys())
        }
    }

    try:
        # Update persona (auto-persists to YAML)
        updated_persona = entitlements_manager.update_persona(
            persona_id=persona_id,
            name=persona_data.get('name'),
            description=persona_data.get('description'),
            privileges=persona_data.get('privileges')
        )

        if not updated_persona:
            logger.warning(f"Persona not found with ID: {persona_id}")
            raise HTTPException(status_code=404, detail="Persona not found")

        logger.info(f"Successfully updated persona with ID: {persona_id}")
        success = True
        return entitlements_manager._format_persona(updated_persona)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error updating persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
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

@router.delete('/entitlements/personas/{persona_id}')
async def delete_persona(
    persona_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Delete a persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id
        }
    }

    try:
        if not entitlements_manager.get_persona(persona_id):
            logger.warning(f"Persona not found for deletion with ID: {persona_id}")
            raise HTTPException(status_code=404, detail="Persona not found")

        logger.info(f"Deleting persona with ID: {persona_id}")
        entitlements_manager.delete_persona(persona_id)  # Auto-persists to YAML

        logger.info(f"Successfully deleted persona with ID: {persona_id}")
        success = True
        return None
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error deleting persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
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

@router.post('/entitlements/personas/{persona_id}/privileges')
async def add_privilege(
    persona_id: str,
    privilege_data: dict,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Add a privilege to a persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id,
            "securable_id": privilege_data.get('securable_id', ''),
            "securable_type": privilege_data.get('securable_type', ''),
            "permission": privilege_data.get('permission', 'READ')
        }
    }

    try:
        # Add privilege (auto-persists to YAML)
        updated_persona = entitlements_manager.add_privilege(
            persona_id=persona_id,
            securable_id=privilege_data.get('securable_id', ''),
            securable_type=privilege_data.get('securable_type', ''),
            permission=privilege_data.get('permission', 'READ')
        )

        if not updated_persona:
            logger.warning(f"Persona not found with ID: {persona_id}")
            raise HTTPException(status_code=404, detail="Persona not found")

        logger.info(f"Successfully added privilege to persona with ID: {persona_id}")
        success = True
        return entitlements_manager._format_persona(updated_persona)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error adding privilege to persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="ADD_PRIVILEGE",
            success=success,
            details=details
        )

@router.delete('/entitlements/personas/{persona_id}/privileges/{securable_id:path}')
async def remove_privilege(
    persona_id: str,
    securable_id: str,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Remove a privilege from a persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id,
            "securable_id": securable_id
        }
    }

    try:
        logger.info(f"Removing privilege {securable_id} from persona with ID: {persona_id}")

        # Remove privilege (auto-persists to YAML)
        updated_persona = entitlements_manager.remove_privilege(
            persona_id=persona_id,
            securable_id=securable_id
        )

        if not updated_persona:
            logger.warning(f"Persona not found with ID: {persona_id}")
            raise HTTPException(status_code=404, detail="Persona not found")

        logger.info(f"Successfully removed privilege from persona with ID: {persona_id}")
        success = True
        return entitlements_manager._format_persona(updated_persona)
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error removing privilege from persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="REMOVE_PRIVILEGE",
            success=success,
            details=details
        )

@router.put('/entitlements/personas/{persona_id}/groups')
async def update_persona_groups(
    persona_id: str,
    groups_data: dict,
    request: Request,
    db: DBSessionDep,
    audit_manager: AuditManagerDep,
    current_user: AuditCurrentUserDep,
    _: bool = Depends(PermissionChecker('entitlements', FeatureAccessLevel.ADMIN))
):
    """Update groups for a persona"""
    success = False
    details = {
        "params": {
            "persona_id": persona_id,
            "groups_count": len(groups_data.get('groups', [])) if isinstance(groups_data.get('groups'), list) else 0
        }
    }

    try:
        if not isinstance(groups_data.get('groups'), list):
            raise HTTPException(status_code=400, detail="Invalid groups data")

        # Update groups (auto-persists to YAML)
        updated_persona = entitlements_manager.update_persona_groups(
            persona_id=persona_id,
            groups=groups_data['groups']
        )

        logger.info(f"Successfully updated groups for persona with ID: {persona_id}")
        success = True
        return entitlements_manager._format_persona(updated_persona)
    except ValueError as e:
        # Persona not found
        logger.warning(f"Persona not found: {e}")
        details["exception"] = {
            "type": "ValueError",
            "message": str(e)
        }
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException as e:
        details["exception"] = {
            "type": "HTTPException",
            "status_code": e.status_code,
            "detail": e.detail
        }
        raise
    except Exception as e:
        error_msg = f"Error updating groups for persona {persona_id}: {e!s}"
        logger.error(error_msg)
        details["exception"] = {
            "type": type(e).__name__,
            "message": str(e)
        }
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else "anonymous",
            ip_address=request.client.host if request.client else None,
            feature="entitlements",
            action="UPDATE_GROUPS",
            success=success,
            details=details
        )

def register_routes(app):
    """Register routes with the app"""
    app.include_router(router)
    logger.info("Entitlements routes registered")
