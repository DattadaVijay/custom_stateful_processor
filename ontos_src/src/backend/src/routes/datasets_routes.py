"""
Datasets API Routes

FastAPI endpoints for Dataset CRUD operations, subscriptions, and queries.
"""

from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, status

from src.common.dependencies import (
    DBSessionDep,
    CurrentUserDep,
    AuditManagerDep,
    AuditCurrentUserDep,
)
from src.common.authorization import PermissionChecker
from src.common.features import FeatureAccessLevel
from src.common.logging import get_logger
from src.controller.datasets_manager import DatasetsManager
from src.models.datasets import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    DatasetListItem,
    DatasetSubscriptionCreate,
    DatasetSubscriptionResponse,
    DatasetSubscribersListResponse,
    DatasetInstance,
    DatasetInstanceCreate,
    DatasetInstanceUpdate,
    DatasetInstanceListResponse,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["Datasets"])

# Feature ID for permission checking
FEATURE_ID = "datasets"


# =============================================================================
# Helper to get manager instance
# =============================================================================

def get_datasets_manager(request: Request) -> DatasetsManager:
    """Get the DatasetsManager from app.state (initialized at startup with TagsManager)."""
    manager = getattr(request.app.state, 'datasets_manager', None)
    if not manager:
        logger.critical("DatasetsManager not found in application state during request!")
        raise HTTPException(status_code=503, detail="Datasets service not configured.")
    return manager


# =============================================================================
# List & Query Endpoints
# =============================================================================

# NOTE: Static routes must be defined BEFORE dynamic {dataset_id} routes

@router.get("/datasets/published", response_model=List[DatasetListItem])
async def get_published_datasets(
    request: Request,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    search: Optional[str] = Query(None, description="Search in name and description"),
):
    """
    Get all published datasets.
    
    Returns datasets that have been marked as published and are available
    for consumption. Useful for marketplace/discovery views.
    """
    logger.debug(f"Get published datasets request from user {current_user.username}")
    
    manager = get_datasets_manager(request)
    datasets = manager.list_datasets(
        skip=skip,
        limit=limit,
        published=True,
        status="active",
        search=search,
    )
    
    return datasets


@router.get("/datasets/my-subscriptions", response_model=List[DatasetListItem])
async def get_my_dataset_subscriptions(
    request: Request,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Get all datasets the current user is subscribed to.
    
    Returns datasets where the current user has an active subscription.
    """
    if not current_user or not current_user.username:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    manager = get_datasets_manager(request)
    return manager.get_user_subscriptions(
        subscriber_email=current_user.username,
        skip=skip,
        limit=limit,
    )


@router.get("/datasets", response_model=List[DatasetListItem])
async def list_datasets(
    request: Request,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    status: Optional[str] = Query(None, description="Filter by status"),
    contract_id: Optional[str] = Query(None, description="Filter by contract ID"),
    owner_team_id: Optional[str] = Query(None, description="Filter by owner team ID"),
    project_id: Optional[str] = Query(None, description="Filter by project ID"),
    published: Optional[bool] = Query(None, description="Filter by publication status"),
    search: Optional[str] = Query(None, description="Search in name and description"),
):
    """
    List all datasets with optional filtering.
    
    Datasets are logical groupings of related data assets.
    Physical implementations are accessed via dataset instances.
    """
    logger.debug(f"List datasets request from user {current_user.username}")
    
    manager = get_datasets_manager(request)
    datasets = manager.list_datasets(
        skip=skip,
        limit=limit,
        status=status,
        contract_id=contract_id,
        owner_team_id=owner_team_id,
        project_id=project_id,
        published=published,
        search=search,
    )
    
    return datasets


@router.get("/datasets/by-contract/{contract_id}", response_model=List[DatasetListItem])
async def get_datasets_by_contract(
    request: Request,
    contract_id: str,
    db: DBSessionDep,

    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Get all datasets implementing a specific contract.
    
    This endpoint returns all datasets that are linked to the given contract ID,
    useful for understanding which physical implementations exist for a contract.
    """
    logger.debug(f"Get datasets for contract {contract_id}")
    
    manager = get_datasets_manager(request)
    datasets = manager.get_datasets_by_contract(
        contract_id=contract_id,
        skip=skip,
        limit=limit,
    )
    
    return datasets


# =============================================================================
# CRUD Endpoints
# =============================================================================

@router.get("/datasets/{dataset_id}", response_model=Dataset)
async def get_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,

    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
):
    """
    Get a single dataset by ID.
    
    Returns the full dataset details including related contract, owner team,
    project, tags, custom properties, and subscriber count.
    """
    logger.debug(f"Get dataset {dataset_id}")
    
    manager = get_datasets_manager(request)
    dataset = manager.get_dataset(dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found",
        )
    
    return dataset


@router.post("/datasets", response_model=Dataset, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    request: Request,
    db: DBSessionDep,
    background_tasks: BackgroundTasks,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    dataset_data: DatasetCreate = Body(...),
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Create a new dataset.
    
    A dataset represents a physical implementation of a data contract,
    such as a Unity Catalog table or view in a specific environment.
    """
    logger.info(f"Create dataset request from user {current_user.username}: {dataset_data.name}")
    
    manager = get_datasets_manager(request)
    
    try:
        # Delivery handled via DeliveryMixin in manager
        dataset = manager.create_dataset(
            data=dataset_data,
            created_by=current_user.username,
            background_tasks=background_tasks,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='CREATE',
            success=True,
            details={'dataset_id': dataset.id, 'dataset_name': dataset.name}
        )
        
        return dataset
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error creating dataset: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create dataset",
        )


@router.put("/datasets/{dataset_id}", response_model=Dataset)
async def update_dataset(
    dataset_id: str,
    request: Request,
    db: DBSessionDep,
    background_tasks: BackgroundTasks,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    dataset_data: DatasetUpdate = Body(...),
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Update an existing dataset.
    
    All fields are optional - only provided fields will be updated.
    """
    logger.info(f"Update dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        # Delivery handled via DeliveryMixin in manager
        dataset = manager.update_dataset(
            dataset_id=dataset_id,
            data=dataset_data,
            updated_by=current_user.username,
            background_tasks=background_tasks,
        )
        
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found",
            )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='UPDATE',
            success=True,
            details={'dataset_id': dataset_id, 'dataset_name': dataset.name}
        )
        
        return dataset
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update dataset",
        )


@router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.ADMIN)),
):
    """
    Delete a dataset.
    
    This will also delete all related subscriptions, tags, and custom properties.
    Requires ADMIN permission level.
    """
    logger.info(f"Delete dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    # Get dataset info before deleting for audit
    dataset = manager.get_dataset(dataset_id)
    dataset_name = dataset.name if dataset else 'unknown'
    
    success = manager.delete_dataset(dataset_id, deleted_by=current_user.username if current_user else None)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found",
        )
    
    audit_manager.log_action(
        db=db,
        username=current_user.username if current_user else 'unknown',
        ip_address=request.client.host if request.client else None,
        feature=FEATURE_ID,
        action='DELETE',
        success=True,
        details={'dataset_id': dataset_id, 'dataset_name': dataset_name}
    )


# =============================================================================
# Publish/Unpublish Endpoints
# =============================================================================

@router.post("/datasets/{dataset_id}/publish")
async def publish_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Publish a dataset to make it available in the marketplace.
    
    Validates that the dataset is in a publishable status (active/approved/certified)
    and any linked contract is at least approved.
    """
    logger.info(f"Publish dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        dataset = manager.publish_dataset(
            dataset_id=dataset_id,
            current_user=current_user.username,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='PUBLISH',
            success=True,
            details={'dataset_id': dataset_id, 'dataset_name': dataset.name}
        )
        
        return {"status": dataset.status, "published": dataset.published}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error publishing dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish dataset",
        )


@router.post("/datasets/{dataset_id}/unpublish")
async def unpublish_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Remove a dataset from the marketplace.
    
    The dataset will no longer appear in marketplace views but remains
    accessible to existing subscribers.
    """
    logger.info(f"Unpublish dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        dataset = manager.unpublish_dataset(
            dataset_id=dataset_id,
            current_user=current_user.username,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='UNPUBLISH',
            success=True,
            details={'dataset_id': dataset_id, 'dataset_name': dataset.name}
        )
        
        return {"status": dataset.status, "published": dataset.published}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error unpublishing dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to unpublish dataset",
        )


# =============================================================================
# Status Change & Review Endpoints
# =============================================================================

@router.post("/datasets/{dataset_id}/change-status")
async def change_dataset_status(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
    new_status: str = Body(..., embed=True),
):
    """
    Directly change the status of a dataset (for users with permission).
    
    This endpoint performs immediate status changes without requiring approval.
    Allowed transitions:
    - draft -> active, deprecated
    - active -> deprecated
    - deprecated -> retired, active (reactivate)
    - retired -> (terminal, no further transitions)
    """
    logger.info(f"Change status of dataset {dataset_id} to '{new_status}' by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    # Get old status for audit
    old_dataset = manager.get_dataset(dataset_id)
    old_status = old_dataset.status if old_dataset else 'unknown'
    
    try:
        dataset = manager.change_status(
            dataset_id=dataset_id,
            new_status=new_status,
            changed_by=current_user.username,
        )
        
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset {dataset_id} not found",
            )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='CHANGE_STATUS',
            success=True,
            details={'dataset_id': dataset_id, 'old_status': old_status, 'new_status': new_status}
        )
        
        return {"status": dataset.status, "message": f"Status changed to {new_status}"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error changing status of dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to change dataset status",
        )


@router.post("/datasets/{dataset_id}/request-status-change")
async def request_status_change(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    target_status: str = Body(...),
    justification: str = Body(...),
    current_status: Optional[str] = Body(None),
):
    """
    Request approval for a status change (for users without direct change permission).
    
    Creates an approval request that will be reviewed by administrators.
    The user will be notified when the request is approved or rejected.
    """
    logger.info(f"Request status change for dataset {dataset_id} to '{target_status}' by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        result = manager.request_status_change(
            dataset_id=dataset_id,
            target_status=target_status,
            justification=justification,
            requested_by=current_user.email or current_user.username,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='REQUEST_STATUS_CHANGE',
            success=True,
            details={'dataset_id': dataset_id, 'target_status': target_status}
        )
        
        return {
            "status": "pending",
            "message": "Status change request submitted for approval",
            "request_id": result.get("request_id") if result else None,
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error requesting status change for dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to submit status change request",
        )


@router.post("/datasets/{dataset_id}/request-review")
async def request_steward_review(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    message: Optional[str] = Body(None, embed=True),
):
    """
    Request a data steward review for a dataset.
    
    This is typically used for draft datasets that are ready for review.
    A data steward will be notified and can approve or request changes.
    """
    logger.info(f"Request steward review for dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        result = manager.request_review(
            dataset_id=dataset_id,
            requested_by=current_user.email or current_user.username,
            message=message,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='REQUEST_REVIEW',
            success=True,
            details={'dataset_id': dataset_id}
        )
        
        return {
            "status": "review_requested",
            "message": "Review request submitted to data steward",
            "request_id": result.get("request_id") if result else None,
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error requesting review for dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to submit review request",
        )


# =============================================================================
# Contract Assignment Endpoints
# =============================================================================

@router.post("/datasets/{dataset_id}/contract/{contract_id}", response_model=Dataset)
async def assign_contract(
    request: Request,
    dataset_id: str,
    contract_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Assign a contract to a dataset.
    
    Links the dataset to a data contract, indicating that this dataset
    implements the schema and quality requirements defined in the contract.
    """
    logger.info(f"Assign contract {contract_id} to dataset {dataset_id}")
    
    manager = get_datasets_manager(request)
    
    dataset = manager.assign_contract(
        dataset_id=dataset_id,
        contract_id=contract_id,
        updated_by=current_user.username,
    )
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found",
        )
    
    audit_manager.log_action(
        db=db,
        username=current_user.username if current_user else 'unknown',
        ip_address=request.client.host if request.client else None,
        feature=FEATURE_ID,
        action='ASSIGN_CONTRACT',
        success=True,
        details={'dataset_id': dataset_id, 'contract_id': contract_id}
    )
    
    return dataset


@router.delete("/datasets/{dataset_id}/contract", response_model=Dataset)
async def unassign_contract(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Remove contract assignment from a dataset.
    
    Unlinks the dataset from its currently assigned contract.
    """
    logger.info(f"Unassign contract from dataset {dataset_id}")
    
    manager = get_datasets_manager(request)
    
    # Get old contract for audit
    old_dataset = manager.get_dataset(dataset_id)
    old_contract_id = old_dataset.contract_id if old_dataset else None
    
    dataset = manager.unassign_contract(
        dataset_id=dataset_id,
        updated_by=current_user.username,
    )
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found",
        )
    
    audit_manager.log_action(
        db=db,
        username=current_user.username if current_user else 'unknown',
        ip_address=request.client.host if request.client else None,
        feature=FEATURE_ID,
        action='UNASSIGN_CONTRACT',
        success=True,
        details={'dataset_id': dataset_id, 'old_contract_id': old_contract_id}
    )
    
    return dataset


# =============================================================================
# Subscription Endpoints
# =============================================================================

@router.get("/datasets/{dataset_id}/subscription", response_model=DatasetSubscriptionResponse)
async def get_subscription_status(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,

    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
):
    """
    Check if the current user is subscribed to a dataset.
    """
    manager = get_datasets_manager(request)
    
    return manager.get_subscription_status(
        dataset_id=dataset_id,
        email=current_user.username,
    )


@router.post("/datasets/{dataset_id}/subscribe", response_model=DatasetSubscriptionResponse)
async def subscribe_to_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    data: Optional[DatasetSubscriptionCreate] = None,
):
    """
    Subscribe the current user to a dataset.
    
    Subscribers receive notifications about dataset changes,
    deprecation, new versions, and compliance violations.
    """
    logger.info(f"User {current_user.username} subscribing to dataset {dataset_id}")
    
    manager = get_datasets_manager(request)
    
    try:
        result = manager.subscribe(
            dataset_id=dataset_id,
            email=current_user.username,
            reason=data.reason if data else None,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='SUBSCRIBE',
            success=True,
            details={'dataset_id': dataset_id}
        )
        
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.delete("/datasets/{dataset_id}/subscribe", response_model=DatasetSubscriptionResponse)
async def unsubscribe_from_dataset(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
):
    """
    Unsubscribe the current user from a dataset.
    """
    logger.info(f"User {current_user.username} unsubscribing from dataset {dataset_id}")
    
    manager = get_datasets_manager(request)
    
    result = manager.unsubscribe(
        dataset_id=dataset_id,
        email=current_user.username,
    )
    
    audit_manager.log_action(
        db=db,
        username=current_user.username if current_user else 'unknown',
        ip_address=request.client.host if request.client else None,
        feature=FEATURE_ID,
        action='UNSUBSCRIBE',
        success=True,
        details={'dataset_id': dataset_id}
    )
    
    return result


@router.get("/datasets/{dataset_id}/subscribers", response_model=DatasetSubscribersListResponse)
async def get_dataset_subscribers(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,

    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Get all subscribers for a dataset.
    
    Requires READ_WRITE permission to view subscriber list.
    """
    manager = get_datasets_manager(request)
    
    return manager.get_subscribers(
        dataset_id=dataset_id,
        skip=skip,
        limit=limit,
    )


# =============================================================================
# Instance Endpoints (Physical Implementations)
# =============================================================================

@router.get("/datasets/{dataset_id}/instances", response_model=DatasetInstanceListResponse)
async def list_dataset_instances(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    List all physical instances for a dataset.
    
    Returns information about where the dataset is physically implemented,
    including the system type, environment, and physical path.
    """
    manager = get_datasets_manager(request)
    
    try:
        return manager.list_instances(
            dataset_id=dataset_id,
            skip=skip,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.get("/datasets/{dataset_id}/instances/{instance_id}", response_model=DatasetInstance)
async def get_dataset_instance(
    request: Request,
    dataset_id: str,
    instance_id: str,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
):
    """
    Get a single instance by ID.
    """
    manager = get_datasets_manager(request)
    
    instance = manager.get_instance(instance_id)
    
    if not instance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instance {instance_id} not found",
        )
    
    # Verify instance belongs to the dataset
    if instance.dataset_id != dataset_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instance {instance_id} not found in dataset {dataset_id}",
        )
    
    return instance


@router.post("/datasets/{dataset_id}/instances", response_model=DatasetInstance, status_code=status.HTTP_201_CREATED)
async def add_dataset_instance(
    request: Request,
    dataset_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    instance_data: DatasetInstanceCreate = Body(...),
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Add a physical instance to a dataset.
    
    Links the dataset to a specific physical implementation in a system/environment.
    Each instance references a contract version and server entry from that contract.
    """
    logger.info(f"Add instance to dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        instance = manager.add_instance(
            dataset_id=dataset_id,
            data=instance_data,
            created_by=current_user.username,
        )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='ADD_INSTANCE',
            success=True,
            details={'dataset_id': dataset_id, 'instance_id': instance.id, 'environment': instance.environment}
        )
        
        return instance
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"Error adding instance to dataset {dataset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add instance",
        )


@router.put("/datasets/{dataset_id}/instances/{instance_id}", response_model=DatasetInstance)
async def update_dataset_instance(
    request: Request,
    dataset_id: str,
    instance_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    instance_data: DatasetInstanceUpdate = Body(...),
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Update an existing instance.
    """
    logger.info(f"Update instance {instance_id} in dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    try:
        instance = manager.update_instance(
            instance_id=instance_id,
            data=instance_data,
            updated_by=current_user.username,
        )
        
        if not instance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instance {instance_id} not found",
            )
        
        # Verify instance belongs to the dataset
        if instance.dataset_id != dataset_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instance {instance_id} not found in dataset {dataset_id}",
            )
        
        audit_manager.log_action(
            db=db,
            username=current_user.username if current_user else 'unknown',
            ip_address=request.client.host if request.client else None,
            feature=FEATURE_ID,
            action='UPDATE_INSTANCE',
            success=True,
            details={'dataset_id': dataset_id, 'instance_id': instance_id}
        )
        
        return instance
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating instance {instance_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update instance",
        )


@router.delete("/datasets/{dataset_id}/instances/{instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_dataset_instance(
    request: Request,
    dataset_id: str,
    instance_id: str,
    db: DBSessionDep,
    current_user: AuditCurrentUserDep,
    audit_manager: AuditManagerDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_WRITE)),
):
    """
    Remove an instance from a dataset.
    """
    logger.info(f"Remove instance {instance_id} from dataset {dataset_id} by user {current_user.username}")
    
    manager = get_datasets_manager(request)
    
    # First verify the instance exists and belongs to the dataset
    instance = manager.get_instance(instance_id)
    if not instance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instance {instance_id} not found",
        )
    
    if instance.dataset_id != dataset_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instance {instance_id} not found in dataset {dataset_id}",
        )
    
    instance_environment = instance.environment
    
    success = manager.remove_instance(instance_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instance {instance_id} not found",
        )
    
    audit_manager.log_action(
        db=db,
        username=current_user.username if current_user else 'unknown',
        ip_address=request.client.host if request.client else None,
        feature=FEATURE_ID,
        action='REMOVE_INSTANCE',
        success=True,
        details={'dataset_id': dataset_id, 'instance_id': instance_id, 'environment': instance_environment}
    )


# =============================================================================
# Validation Endpoints
# =============================================================================

@router.get("/datasets/validate-asset/{catalog_name}/{schema_name}/{object_name}")
async def validate_asset(
    request: Request,
    catalog_name: str,
    schema_name: str,
    object_name: str,
    db: DBSessionDep,
    current_user: CurrentUserDep,
    _: bool = Depends(PermissionChecker(FEATURE_ID, FeatureAccessLevel.READ_ONLY)),
):
    """
    Validate that a Unity Catalog asset exists.
    
    Returns information about the asset if found, or an error message if not.
    Useful for validating asset paths before creating datasets.
    """
    manager = get_datasets_manager(request)
    
    return manager.validate_asset_exists(
        catalog_name=catalog_name,
        schema_name=schema_name,
        object_name=object_name,
    )


# =============================================================================
# Route Registration
# =============================================================================

def register_routes(app):
    """Register dataset routes with the FastAPI app."""
    app.include_router(router)
    logger.info("Dataset routes registered with prefix /api/datasets")

