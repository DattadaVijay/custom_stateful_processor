"""
Datasets Manager

Business logic controller for Datasets.
Implements SearchableAsset interface for global search functionality.
"""

from typing import List, Optional, Dict, Any
from uuid import uuid4

from sqlalchemy.orm import Session
from databricks.sdk import WorkspaceClient

from src.common.logging import get_logger
from src.common.search_interfaces import SearchableAsset, SearchIndexItem
from src.common.search_registry import searchable_asset
from src.common.delivery_mixin import DeliveryMixin
from src.connectors import get_registry, ConnectorRegistry
from src.connectors.base import ConnectorNotFoundError
from src.db_models.datasets import (
    DatasetDb,
    DatasetCustomPropertyDb,
    DatasetInstanceDb,
)
from src.repositories.datasets_repository import (
    dataset_repo,
    dataset_subscription_repo,
    dataset_custom_property_repo,
    dataset_instance_repo,
)
from src.repositories.tags_repository import entity_tag_repo
from src.models.tags import AssignedTag, AssignedTagCreate
from src.models.datasets import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    DatasetListItem,
    DatasetSubscription,
    DatasetSubscriptionCreate,
    DatasetSubscriptionResponse,
    DatasetSubscriberInfo,
    DatasetSubscribersListResponse,
    DatasetInstance,
    DatasetInstanceCreate,
    DatasetInstanceUpdate,
    DatasetInstanceListResponse,
)

logger = get_logger(__name__)


@searchable_asset
class DatasetsManager(DeliveryMixin, SearchableAsset):
    """
    Manager for Dataset business logic.
    
    Handles CRUD operations, contract assignment, subscriptions,
    and provides search indexing.
    
    Inherits DeliveryMixin to support automatic delivery of changes
    to configured delivery modes (Direct, Indirect, Manual).
    """
    
    # DeliveryMixin configuration
    DELIVERY_ENTITY_TYPE = "Dataset"

    def __init__(
        self,
        db: Session,
        ws_client: Optional[WorkspaceClient] = None,
        tags_manager: Optional["TagsManager"] = None,
    ):
        self._db = db
        self._ws_client = ws_client
        self._tags_manager = tags_manager
        logger.info("DatasetsManager initialized")

    # =========================================================================
    # SearchableAsset Interface Implementation
    # =========================================================================

    def get_search_index_items(self) -> List[SearchIndexItem]:
        """
        Fetches datasets and maps them to SearchIndexItem format for global search.
        Uses the unified tag system (EntityTagAssociationDb) for tag lookups.
        """
        items: List[SearchIndexItem] = []
        try:
            datasets = dataset_repo.get_multi(db=self._db, limit=1000)
            for ds in datasets:
                # Get tags from unified tag system (EntityTagAssociationDb)
                tags = []
                try:
                    assigned_tags = entity_tag_repo.get_assigned_tags_for_entity(
                        db=self._db,
                        entity_id=str(ds.id),
                        entity_type="dataset"
                    )
                    for tag in assigned_tags:
                        if hasattr(tag, 'fully_qualified_name') and tag.fully_qualified_name:
                            tags.append(tag.fully_qualified_name)
                except Exception as tag_err:
                    logger.debug(f"Could not load tags for dataset {ds.id}: {tag_err}")
                
                description = ds.description or f"Dataset: {ds.name}"
                
                # Build extra_data for configurable search fields
                extra_data = {
                    "status": getattr(ds, 'status', '') or "",
                    "published": str(getattr(ds, 'published', False)),
                    "instance_count": str(len(ds.instances) if ds.instances else 0),
                }

                items.append(SearchIndexItem(
                    id=f"dataset::{ds.id}",
                    type="dataset",
                    title=ds.name,
                    description=description,
                    link=f"/datasets/{ds.id}",
                    tags=tags,
                    feature_id="datasets",
                    extra_data=extra_data,
                ))
        except Exception as e:
            logger.error(f"Error fetching datasets for search index: {e}", exc_info=True)
        
        return items

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    def list_datasets(
        self,
        skip: int = 0,
        limit: int = 100,
        status: Optional[str] = None,
        contract_id: Optional[str] = None,
        owner_team_id: Optional[str] = None,
        project_id: Optional[str] = None,
        published: Optional[bool] = None,
        search: Optional[str] = None,
    ) -> List[DatasetListItem]:
        """List datasets with optional filtering."""
        try:
            datasets = dataset_repo.get_multi(
                db=self._db,
                skip=skip,
                limit=limit,
                status=status,
                contract_id=contract_id,
                owner_team_id=owner_team_id,
                project_id=project_id,
                published=published,
                search=search,
            )
            
            return [self._to_list_item(ds) for ds in datasets]
        except Exception as e:
            logger.error(f"Error listing datasets: {e}", exc_info=True)
            raise

    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """Get a single dataset by ID with all related data."""
        try:
            ds = dataset_repo.get_with_all(db=self._db, id=dataset_id)
            if not ds:
                return None
            return self._to_api_model(ds)
        except Exception as e:
            logger.error(f"Error getting dataset {dataset_id}: {e}", exc_info=True)
            raise

    def create_dataset(
        self,
        data: DatasetCreate,
        created_by: Optional[str] = None,
        background_tasks: Optional[Any] = None,
    ) -> Dataset:
        """Create a new dataset (logical grouping - physical assets added as instances).
        
        Args:
            data: Dataset creation data
            created_by: Username of creator
            background_tasks: Optional FastAPI BackgroundTasks for async delivery
        """
        from src.controller.delivery_service import DeliveryChangeType
        
        try:
            # Generate ID
            dataset_id = str(uuid4())
            
            # Create the dataset record (logical entity only)
            db_dataset = DatasetDb(
                id=dataset_id,
                name=data.name,
                description=data.description,
                contract_id=data.contract_id,
                owner_team_id=data.owner_team_id,
                project_id=data.project_id,
                status=data.status or "draft",
                version=data.version,
                published=data.published or False,
                max_level_inheritance=data.max_level_inheritance,
                created_by=created_by,
                updated_by=created_by,
            )
            
            self._db.add(db_dataset)
            self._db.flush()
            
            # Assign tags via unified tagging system
            if data.tags and self._tags_manager:
                self._tags_manager.set_tags_for_entity(
                    db=self._db,
                    entity_id=dataset_id,
                    entity_type="dataset",
                    tags=data.tags,
                    user_email=created_by,
                )
            
            # Create custom properties if provided
            if data.custom_properties:
                for prop_data in data.custom_properties:
                    prop = DatasetCustomPropertyDb(
                        dataset_id=dataset_id,
                        property=prop_data.property,
                        value=prop_data.value,
                    )
                    self._db.add(prop)
            
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Created dataset {dataset_id}: {data.name}")
            result = self._to_api_model(db_dataset)
            
            # Log to change log for timeline
            try:
                from src.controller.change_log_manager import change_log_manager
                change_log_manager.log_change_with_details(
                    self._db,
                    entity_type="dataset",
                    entity_id=str(dataset_id),
                    action="CREATE",
                    username=created_by,
                    details={
                        "name": data.name,
                        "status": data.status or "draft",
                        "summary": f"Dataset '{data.name}' created" + (f" by {created_by}" if created_by else ""),
                    },
                )
            except Exception as log_err:
                logger.warning(f"Failed to log change for dataset creation: {log_err}")
            
            # Queue delivery for active modes
            self._queue_delivery(
                entity=db_dataset,
                change_type=DeliveryChangeType.DATASET_CREATE,
                user=created_by,
                background_tasks=background_tasks,
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating dataset: {e}", exc_info=True)
            self._db.rollback()
            raise

    def update_dataset(
        self,
        dataset_id: str,
        data: DatasetUpdate,
        updated_by: Optional[str] = None,
        background_tasks: Optional[Any] = None,
    ) -> Optional[Dataset]:
        """Update an existing dataset (logical fields only).
        
        Args:
            dataset_id: Dataset ID to update
            data: Update data
            updated_by: Username of updater
            background_tasks: Optional FastAPI BackgroundTasks for async delivery
        """
        from src.controller.delivery_service import DeliveryChangeType
        
        try:
            db_dataset = dataset_repo.get_with_all(db=self._db, id=dataset_id)
            if not db_dataset:
                return None
            
            # Update logical fields if provided
            if data.name is not None:
                db_dataset.name = data.name
            if data.description is not None:
                db_dataset.description = data.description
            if data.contract_id is not None:
                db_dataset.contract_id = data.contract_id
            if data.owner_team_id is not None:
                db_dataset.owner_team_id = data.owner_team_id
            if data.project_id is not None:
                db_dataset.project_id = data.project_id
            if data.status is not None:
                db_dataset.status = data.status
            if data.version is not None:
                db_dataset.version = data.version
            if data.published is not None:
                db_dataset.published = data.published
            if data.max_level_inheritance is not None:
                db_dataset.max_level_inheritance = data.max_level_inheritance
            
            db_dataset.updated_by = updated_by
            
            # Replace tags via unified tagging system
            if data.tags is not None and self._tags_manager:
                self._tags_manager.set_tags_for_entity(
                    db=self._db,
                    entity_id=dataset_id,
                    entity_type="dataset",
                    tags=data.tags,
                    user_email=updated_by,
                )
            
            # Replace custom properties if provided
            if data.custom_properties is not None:
                dataset_custom_property_repo.delete_by_dataset(db=self._db, dataset_id=dataset_id)
                for prop_data in data.custom_properties:
                    dataset_custom_property_repo.create_property(
                        db=self._db,
                        dataset_id=dataset_id,
                        property=prop_data.property,
                        value=prop_data.value,
                    )
            
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Updated dataset {dataset_id}")
            result = self._to_api_model(db_dataset)
            
            # Log to change log for timeline
            try:
                from src.controller.change_log_manager import change_log_manager
                change_log_manager.log_change_with_details(
                    self._db,
                    entity_type="dataset",
                    entity_id=str(dataset_id),
                    action="UPDATE",
                    username=updated_by,
                    details={
                        "name": db_dataset.name,
                        "status": db_dataset.status,
                        "summary": f"Dataset '{db_dataset.name}' updated" + (f" by {updated_by}" if updated_by else ""),
                    },
                )
            except Exception as log_err:
                logger.warning(f"Failed to log change for dataset update: {log_err}")
            
            # Queue delivery for active modes
            self._queue_delivery(
                entity=db_dataset,
                change_type=DeliveryChangeType.DATASET_UPDATE,
                user=updated_by,
                background_tasks=background_tasks,
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def delete_dataset(self, dataset_id: str, deleted_by: Optional[str] = None) -> bool:
        """Delete a dataset and all related data."""
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                return False
            
            # Capture name before deletion for change log
            dataset_name = db_dataset.name
            
            self._db.delete(db_dataset)
            
            # Log to change log for timeline (before flush/commit)
            try:
                from src.controller.change_log_manager import change_log_manager
                change_log_manager.log_change_with_details(
                    self._db,
                    entity_type="dataset",
                    entity_id=str(dataset_id),
                    action="DELETE",
                    username=deleted_by,
                    details={
                        "name": dataset_name,
                        "summary": f"Dataset '{dataset_name}' deleted" + (f" by {deleted_by}" if deleted_by else ""),
                    },
                )
            except Exception as log_err:
                logger.warning(f"Failed to log change for dataset deletion: {log_err}")
            
            self._db.flush()
            
            logger.info(f"Deleted dataset {dataset_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    # =========================================================================
    # Publish Operations
    # =========================================================================

    def publish_dataset(self, dataset_id: str, current_user: Optional[str] = None) -> Dataset:
        """
        Publish a dataset to make it available in the marketplace.
        
        Validates:
        - Dataset status is 'active', 'approved', or 'certified'
        - If linked to a contract, contract status must be 'approved' or higher
        
        Args:
            dataset_id: ID of the dataset to publish
            current_user: Username for audit trail
            
        Returns:
            Updated dataset with published=True
            
        Raises:
            ValueError: If validation fails
        """
        from src.common.workflow_triggers import get_trigger_registry
        from src.models.process_workflows import EntityType
        
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            # Already published?
            if db_dataset.published:
                logger.info(f"Dataset {dataset_id} is already published")
                return self._to_api_model(db_dataset)
            
            # Validate status
            valid_statuses = ['active', 'approved', 'certified']
            if db_dataset.status not in valid_statuses:
                raise ValueError(
                    f"Cannot publish dataset in status '{db_dataset.status}'. "
                    f"Must be one of: {', '.join(valid_statuses)}"
                )
            
            # Validate linked contract status (if any)
            if db_dataset.contract_id and db_dataset.contract:
                contract = db_dataset.contract
                contract_valid_statuses = ['approved', 'active', 'certified']
                contract_status = (contract.status or '').lower()
                if contract_status not in contract_valid_statuses:
                    raise ValueError(
                        f"Cannot publish: linked contract '{contract.name}' is in status '{contract.status}'. "
                        f"Contract must be one of: {', '.join(contract_valid_statuses)}"
                    )
            
            # Set published flag
            db_dataset.published = True
            db_dataset.updated_by = current_user
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Published dataset {dataset_id} to marketplace")
            
            # Trigger ON_REQUEST_PUBLISH workflow
            try:
                trigger_registry = get_trigger_registry(self._db)
                entity_data = {
                    "dataset_id": dataset_id,
                    "dataset_name": db_dataset.name,
                    "status": db_dataset.status,
                    "published": True,
                    "action": "publish",
                    "owner_team_id": db_dataset.owner_team_id,
                    "owner_email": db_dataset.owner_email,
                    "contract_id": db_dataset.contract_id,
                }
                
                executions = trigger_registry.on_request_publish(
                    entity_type=EntityType.DATASET,
                    entity_id=dataset_id,
                    entity_name=db_dataset.name,
                    entity_data=entity_data,
                    user_email=current_user,
                    blocking=False,  # Non-blocking - don't wait for workflow completion
                )
                
                if executions:
                    logger.info(f"Triggered ON_REQUEST_PUBLISH workflow for dataset {dataset_id}. Execution IDs: {[e.id for e in executions]}")
                else:
                    logger.debug(f"No ON_REQUEST_PUBLISH workflow found for dataset {dataset_id}")
            except Exception as wf_err:
                logger.warning(f"Failed to trigger publish workflow for dataset {dataset_id}: {wf_err}")
                # Don't fail the publish operation if workflow trigger fails
            
            return self._to_api_model(db_dataset)
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error publishing dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def unpublish_dataset(self, dataset_id: str, current_user: Optional[str] = None) -> Dataset:
        """
        Remove a dataset from the marketplace.
        
        Args:
            dataset_id: ID of the dataset to unpublish
            current_user: Username for audit trail
            
        Returns:
            Updated dataset with published=False
            
        Raises:
            ValueError: If dataset not found
        """
        from src.common.workflow_triggers import get_trigger_registry
        from src.models.process_workflows import EntityType
        
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            # Already unpublished?
            if not db_dataset.published:
                logger.info(f"Dataset {dataset_id} is already unpublished")
                return self._to_api_model(db_dataset)
            
            # Remove from marketplace
            db_dataset.published = False
            db_dataset.updated_by = current_user
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Unpublished dataset {dataset_id} from marketplace")
            
            # Trigger ON_STATUS_CHANGE workflow (publication status change)
            try:
                trigger_registry = get_trigger_registry(self._db)
                entity_data = {
                    "dataset_id": dataset_id,
                    "dataset_name": db_dataset.name,
                    "status": db_dataset.status,
                    "published": False,
                    "action": "unpublish",
                    "owner_team_id": db_dataset.owner_team_id,
                    "owner_email": db_dataset.owner_email,
                    "contract_id": db_dataset.contract_id,
                }
                
                executions = trigger_registry.on_status_change(
                    entity_type=EntityType.DATASET,
                    entity_id=dataset_id,
                    from_status="published",
                    to_status="unpublished",
                    entity_name=db_dataset.name,
                    entity_data=entity_data,
                    user_email=current_user,
                    blocking=False,  # Non-blocking
                )
                
                if executions:
                    logger.info(f"Triggered ON_STATUS_CHANGE workflow for unpublish of dataset {dataset_id}. Execution IDs: {[e.id for e in executions]}")
                else:
                    logger.debug(f"No ON_STATUS_CHANGE workflow found for unpublish of dataset {dataset_id}")
            except Exception as wf_err:
                logger.warning(f"Failed to trigger unpublish workflow for dataset {dataset_id}: {wf_err}")
                # Don't fail the unpublish operation if workflow trigger fails
            
            return self._to_api_model(db_dataset)
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error unpublishing dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    # =========================================================================
    # Status Change & Review Operations
    # =========================================================================

    # Allowed status transitions for datasets (simpler than ODPS/ODCS)
    ALLOWED_TRANSITIONS = {
        'draft': ['active', 'deprecated', 'in_review'],
        'in_review': ['draft', 'active'],  # draft = rejected/cancelled, active = approved
        'active': ['deprecated'],
        'deprecated': ['retired', 'active'],  # active = reactivate
        'retired': [],  # Terminal state
    }

    def change_status(
        self,
        dataset_id: str,
        new_status: str,
        changed_by: Optional[str] = None,
    ) -> Optional[Dataset]:
        """
        Directly change the status of a dataset.
        
        This is for users with READ_WRITE permission who can change status
        without requiring approval.
        
        Args:
            dataset_id: ID of the dataset
            new_status: Target status
            changed_by: Username for audit trail
            
        Returns:
            Updated dataset
            
        Raises:
            ValueError: If transition is not allowed
        """
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            current_status = (db_dataset.status or 'draft').lower()
            target_status = new_status.lower()
            
            # Validate transition
            allowed = self.ALLOWED_TRANSITIONS.get(current_status, [])
            if target_status not in allowed:
                raise ValueError(
                    f"Cannot change status from '{current_status}' to '{target_status}'. "
                    f"Allowed transitions: {allowed or 'none (terminal state)'}"
                )
            
            # Apply the status change
            db_dataset.status = target_status
            db_dataset.updated_by = changed_by
            
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Changed dataset {dataset_id} status from '{current_status}' to '{target_status}' by {changed_by}")
            return self._to_api_model(db_dataset)
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error changing status of dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def request_status_change(
        self,
        dataset_id: str,
        target_status: str,
        justification: str,
        requested_by: str,
    ) -> Dict[str, Any]:
        """
        Request approval for a status change via workflow.
        
        Fires the ON_REQUEST_STATUS_CHANGE trigger which executes any configured
        workflows. Workflows can include approval steps that pause execution until
        an approver responds.
        
        Args:
            dataset_id: ID of the dataset
            target_status: Desired status
            justification: Reason for the change
            requested_by: Username of requester
            
        Returns:
            Request metadata including workflow execution info
            
        Raises:
            ValueError: If dataset not found or transition not valid
        """
        from src.common.workflow_triggers import get_trigger_registry
        from src.models.process_workflows import EntityType
        
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            current_status = (db_dataset.status or 'draft').lower()
            target = target_status.lower()
            
            # Validate the transition is valid (even if requesting approval)
            allowed = self.ALLOWED_TRANSITIONS.get(current_status, [])
            if target not in allowed:
                raise ValueError(
                    f"Cannot request status change from '{current_status}' to '{target}'. "
                    f"Allowed transitions: {allowed or 'none (terminal state)'}"
                )
            
            request_id = str(uuid4())
            
            logger.info(
                f"Status change request for dataset {dataset_id}: "
                f"'{current_status}' -> '{target}' by {requested_by}. "
                f"Request ID: {request_id}"
            )
            
            # Fire the ON_REQUEST_STATUS_CHANGE trigger
            trigger_registry = get_trigger_registry(self._db)
            executions = trigger_registry.on_request_status_change(
                entity_type=EntityType.DATASET,
                entity_id=dataset_id,
                from_status=current_status,
                to_status=target,
                entity_name=db_dataset.name,
                entity_data={
                    "dataset_id": dataset_id,
                    "dataset_name": db_dataset.name,
                    "fqn": f"dataset:{db_dataset.name}",
                    "current_status": current_status,
                    "target_status": target,
                    "justification": justification,
                    "request_id": request_id,
                },
                user_email=requested_by,
            )
            
            # Determine response based on workflow execution
            if executions:
                execution = executions[0]
                return {
                    "request_id": request_id,
                    "execution_id": execution.id,
                    "dataset_id": dataset_id,
                    "current_status": current_status,
                    "target_status": target,
                    "requested_by": requested_by,
                    "justification": justification,
                    "status": execution.status.value,
                    "message": "Status change request submitted via workflow",
                }
            else:
                # No workflow configured - fall back to simple pending status
                return {
                    "request_id": request_id,
                    "dataset_id": dataset_id,
                    "current_status": current_status,
                    "target_status": target,
                    "requested_by": requested_by,
                    "justification": justification,
                    "status": "pending",
                    "message": "Status change request submitted (no workflow configured)",
                }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error requesting status change for dataset {dataset_id}: {e}", exc_info=True)
            raise

    def request_review(
        self,
        dataset_id: str,
        requested_by: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Request a data steward review for a dataset via workflow.
        
        Fires the ON_REQUEST_REVIEW trigger which executes any configured
        workflows. Workflows typically include notification and approval steps.
        
        Args:
            dataset_id: ID of the dataset
            requested_by: Username of requester
            message: Optional message for the reviewer
            
        Returns:
            Request metadata including workflow execution info
            
        Raises:
            ValueError: If dataset not found or not in draft status
        """
        from src.common.workflow_triggers import get_trigger_registry
        from src.models.process_workflows import EntityType
        
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            current_status = (db_dataset.status or 'draft').lower()
            
            # Only draft datasets can request review
            if current_status != 'draft':
                raise ValueError(
                    f"Cannot request review for dataset in status '{current_status}'. "
                    f"Only datasets in 'draft' status can request steward review."
                )
            
            request_id = str(uuid4())
            
            # Update dataset status to "in_review"
            db_dataset.status = "in_review"
            self._db.commit()
            self._db.refresh(db_dataset)
            
            logger.info(
                f"Steward review requested for dataset {dataset_id} by {requested_by}. "
                f"Status changed to 'in_review'. Request ID: {request_id}"
            )
            
            # Fire the ON_REQUEST_REVIEW trigger
            trigger_registry = get_trigger_registry(self._db)
            executions = trigger_registry.on_request_review(
                entity_type=EntityType.DATASET,
                entity_id=dataset_id,
                entity_name=db_dataset.name,
                entity_data={
                    "dataset_id": dataset_id,
                    "dataset_name": db_dataset.name,
                    "fqn": f"dataset:{db_dataset.name}",
                    "current_status": current_status,
                    "message": message,
                    "request_id": request_id,
                },
                user_email=requested_by,
            )
            
            # Determine response based on workflow execution
            if executions:
                execution = executions[0]
                return {
                    "request_id": request_id,
                    "execution_id": execution.id,
                    "dataset_id": dataset_id,
                    "current_status": current_status,
                    "requested_by": requested_by,
                    "message": message,
                    "status": execution.status.value,
                    "workflow_message": "Review request submitted via workflow",
                }
            else:
                # No workflow configured - fall back to simple status
                return {
                    "request_id": request_id,
                    "dataset_id": dataset_id,
                    "current_status": current_status,
                    "requested_by": requested_by,
                    "message": message,
                    "status": "review_requested",
                    "workflow_message": "Review request submitted (no workflow configured)",
                }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error requesting review for dataset {dataset_id}: {e}", exc_info=True)
            raise

    # =========================================================================
    # Contract Operations
    # =========================================================================

    def get_datasets_by_contract(
        self,
        contract_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> List[DatasetListItem]:
        """Get all datasets implementing a specific contract."""
        try:
            datasets = dataset_repo.get_by_contract(
                db=self._db,
                contract_id=contract_id,
                skip=skip,
                limit=limit,
            )
            return [self._to_list_item(ds) for ds in datasets]
        except Exception as e:
            logger.error(f"Error getting datasets for contract {contract_id}: {e}", exc_info=True)
            raise

    def assign_contract(
        self,
        dataset_id: str,
        contract_id: str,
        updated_by: Optional[str] = None,
    ) -> Optional[Dataset]:
        """Assign a contract to a dataset."""
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                return None
            
            db_dataset.contract_id = contract_id
            db_dataset.updated_by = updated_by
            
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Assigned contract {contract_id} to dataset {dataset_id}")
            return self._to_api_model(db_dataset)
            
        except Exception as e:
            logger.error(f"Error assigning contract to dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def unassign_contract(
        self,
        dataset_id: str,
        updated_by: Optional[str] = None,
    ) -> Optional[Dataset]:
        """Remove contract assignment from a dataset."""
        try:
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                return None
            
            db_dataset.contract_id = None
            db_dataset.updated_by = updated_by
            
            self._db.flush()
            self._db.refresh(db_dataset)
            
            logger.info(f"Unassigned contract from dataset {dataset_id}")
            return self._to_api_model(db_dataset)
            
        except Exception as e:
            logger.error(f"Error unassigning contract from dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    # =========================================================================
    # Subscription Operations
    # =========================================================================

    def subscribe(
        self,
        dataset_id: str,
        email: str,
        reason: Optional[str] = None,
    ) -> DatasetSubscriptionResponse:
        """Subscribe a user to a dataset."""
        try:
            # Verify dataset exists
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            subscription = dataset_subscription_repo.subscribe(
                db=self._db,
                dataset_id=dataset_id,
                email=email,
                reason=reason,
            )
            
            logger.info(f"User {email} subscribed to dataset {dataset_id}")
            
            # Log to change log for timeline
            try:
                from src.controller.change_log_manager import change_log_manager
                change_log_manager.log_change_with_details(
                    self._db,
                    entity_type="dataset",
                    entity_id=dataset_id,
                    action="SUBSCRIBE",
                    username=email,
                    details={
                        "name": db_dataset.name,
                        "subscriber_email": email,
                        "reason": reason,
                        "summary": f"User '{email}' subscribed to dataset '{db_dataset.name}'",
                    },
                )
            except Exception as log_err:
                logger.warning(f"Failed to log subscription change: {log_err}")
            
            # Trigger subscription workflow
            try:
                from src.common.workflow_triggers import get_trigger_registry
                from src.models.process_workflows import EntityType
                
                trigger_registry = get_trigger_registry(self._db)
                entity_data = {
                    "subscription_id": subscription.id,
                    "dataset_id": dataset_id,
                    "dataset_name": db_dataset.name,
                    "subscriber_email": email,
                    "reason": reason,
                }
                
                executions = trigger_registry.on_subscribe(
                    entity_type=EntityType.DATASET,
                    entity_id=dataset_id,
                    entity_name=db_dataset.name,
                    entity_data=entity_data,
                    user_email=email,
                    blocking=False,
                )
                
                if executions:
                    logger.info(f"Triggered {len(executions)} workflow(s) for subscription to dataset {dataset_id}")
            except Exception as workflow_err:
                logger.error(f"Failed to trigger subscription workflow: {workflow_err}", exc_info=True)
            
            return DatasetSubscriptionResponse(
                subscribed=True,
                subscription=DatasetSubscription(
                    id=subscription.id,
                    dataset_id=subscription.dataset_id,
                    subscriber_email=subscription.subscriber_email,
                    subscribed_at=subscription.subscribed_at,
                    subscription_reason=subscription.subscription_reason,
                ),
            )
        except Exception as e:
            logger.error(f"Error subscribing to dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def unsubscribe(
        self,
        dataset_id: str,
        email: str,
    ) -> DatasetSubscriptionResponse:
        """Unsubscribe a user from a dataset."""
        try:
            # Get dataset name for workflow
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            dataset_name = db_dataset.name if db_dataset else dataset_id
            
            success = dataset_subscription_repo.unsubscribe(
                db=self._db,
                dataset_id=dataset_id,
                email=email,
            )
            
            if success:
                logger.info(f"User {email} unsubscribed from dataset {dataset_id}")
                
                # Log to change log for timeline
                try:
                    from src.controller.change_log_manager import change_log_manager
                    change_log_manager.log_change_with_details(
                        self._db,
                        entity_type="dataset",
                        entity_id=dataset_id,
                        action="UNSUBSCRIBE",
                        username=email,
                        details={
                            "name": dataset_name,
                            "subscriber_email": email,
                            "summary": f"User '{email}' unsubscribed from dataset '{dataset_name}'",
                        },
                    )
                except Exception as log_err:
                    logger.warning(f"Failed to log unsubscription change: {log_err}")
                
                # Trigger unsubscribe workflow
                try:
                    from src.common.workflow_triggers import get_trigger_registry
                    from src.models.process_workflows import EntityType
                    
                    trigger_registry = get_trigger_registry(self._db)
                    entity_data = {
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "subscriber_email": email,
                    }
                    
                    executions = trigger_registry.on_unsubscribe(
                        entity_type=EntityType.DATASET,
                        entity_id=dataset_id,
                        entity_name=dataset_name,
                        entity_data=entity_data,
                        user_email=email,
                        blocking=False,
                    )
                    
                    if executions:
                        logger.info(f"Triggered {len(executions)} workflow(s) for unsubscription from dataset {dataset_id}")
                except Exception as workflow_err:
                    logger.error(f"Failed to trigger unsubscribe workflow: {workflow_err}", exc_info=True)
            
            return DatasetSubscriptionResponse(
                subscribed=False,
                subscription=None,
            )
        except Exception as e:
            logger.error(f"Error unsubscribing from dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def get_subscription_status(
        self,
        dataset_id: str,
        email: str,
    ) -> DatasetSubscriptionResponse:
        """Check if a user is subscribed to a dataset."""
        try:
            subscription = dataset_subscription_repo.get_by_dataset_and_email(
                db=self._db,
                dataset_id=dataset_id,
                email=email,
            )
            
            if subscription:
                return DatasetSubscriptionResponse(
                    subscribed=True,
                    subscription=DatasetSubscription(
                        id=subscription.id,
                        dataset_id=subscription.dataset_id,
                        subscriber_email=subscription.subscriber_email,
                        subscribed_at=subscription.subscribed_at,
                        subscription_reason=subscription.subscription_reason,
                    ),
                )
            
            return DatasetSubscriptionResponse(
                subscribed=False,
                subscription=None,
            )
        except Exception as e:
            logger.error(f"Error getting subscription status for dataset {dataset_id}: {e}", exc_info=True)
            raise

    def get_user_subscriptions(
        self,
        subscriber_email: str,
        skip: int = 0,
        limit: int = 100,
    ) -> List[DatasetListItem]:
        """Get all datasets a user is subscribed to."""
        try:
            subscriptions = dataset_subscription_repo.get_by_subscriber(
                db=self._db,
                email=subscriber_email,
                skip=skip,
                limit=limit,
            )
            
            # Get the dataset for each subscription
            datasets = []
            for sub in subscriptions:
                ds = dataset_repo.get_with_all(db=self._db, id=sub.dataset_id)
                if ds:
                    datasets.append(self._to_list_item(ds))
            
            return datasets
        except Exception as e:
            logger.error(f"Error getting subscriptions for user {subscriber_email}: {e}", exc_info=True)
            raise

    def get_subscribers(
        self,
        dataset_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> DatasetSubscribersListResponse:
        """Get all subscribers for a dataset."""
        try:
            subscriptions = dataset_subscription_repo.get_by_dataset(
                db=self._db,
                dataset_id=dataset_id,
                skip=skip,
                limit=limit,
            )
            
            count = dataset_subscription_repo.count_by_dataset(db=self._db, dataset_id=dataset_id)
            
            subscribers = [
                DatasetSubscriberInfo(
                    email=sub.subscriber_email,
                    subscribed_at=sub.subscribed_at,
                    reason=sub.subscription_reason,
                )
                for sub in subscriptions
            ]
            
            return DatasetSubscribersListResponse(
                dataset_id=dataset_id,
                subscriber_count=count,
                subscribers=subscribers,
            )
        except Exception as e:
            logger.error(f"Error getting subscribers for dataset {dataset_id}: {e}", exc_info=True)
            raise

    # =========================================================================
    # Instance Operations (Physical Implementations)
    # =========================================================================

    def list_instances(
        self,
        dataset_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> DatasetInstanceListResponse:
        """List all instances for a dataset."""
        try:
            # Verify dataset exists
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            instances = dataset_instance_repo.get_by_dataset(
                db=self._db,
                dataset_id=dataset_id,
                skip=skip,
                limit=limit,
            )
            
            count = dataset_instance_repo.count_by_dataset(db=self._db, dataset_id=dataset_id)
            
            return DatasetInstanceListResponse(
                dataset_id=dataset_id,
                instance_count=count,
                instances=[self._instance_to_api_model(inst) for inst in instances],
            )
        except Exception as e:
            logger.error(f"Error listing instances for dataset {dataset_id}: {e}", exc_info=True)
            raise

    def get_instance(self, instance_id: str) -> Optional[DatasetInstance]:
        """Get a single instance by ID."""
        try:
            db_instance = dataset_instance_repo.get_with_relations(db=self._db, id=instance_id)
            if not db_instance:
                return None
            return self._instance_to_api_model(db_instance)
        except Exception as e:
            logger.error(f"Error getting instance {instance_id}: {e}", exc_info=True)
            raise

    def add_instance(
        self,
        dataset_id: str,
        data: DatasetInstanceCreate,
        created_by: Optional[str] = None,
    ) -> DatasetInstance:
        """Add a new instance to a dataset."""
        try:
            # Verify dataset exists
            db_dataset = dataset_repo.get(db=self._db, id=dataset_id)
            if not db_dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            
            # Validate contract_server_id belongs to contract_id (if both provided)
            if data.contract_id and data.contract_server_id:
                self._validate_server_belongs_to_contract(data.contract_id, data.contract_server_id)
            
            # Check for duplicate (dataset + server)
            if data.contract_server_id:
                existing = dataset_instance_repo.get_by_dataset_and_server(
                    db=self._db,
                    dataset_id=dataset_id,
                    contract_server_id=data.contract_server_id,
                )
                if existing:
                    raise ValueError(f"Instance already exists for this dataset and server")
            
            db_instance = dataset_instance_repo.create_instance(
                db=self._db,
                dataset_id=dataset_id,
                contract_id=data.contract_id,
                contract_server_id=data.contract_server_id,
                physical_path=data.physical_path,
                asset_type=data.asset_type,
                role=data.role or "main",
                display_name=data.display_name,
                environment=data.environment,
                status=data.status or "active",
                notes=data.notes,
                created_by=created_by,
            )
            
            # Refresh to load relationships
            self._db.refresh(db_instance)
            
            # Assign tags to instance via unified tagging system
            if data.tags and self._tags_manager:
                self._tags_manager.set_tags_for_entity(
                    db=self._db,
                    entity_id=db_instance.id,
                    entity_type="dataset_instance",
                    tags=data.tags,
                    user_email=created_by,
                )
            
            logger.info(f"Added instance {db_instance.id} to dataset {dataset_id}")
            return self._instance_to_api_model(db_instance)
            
        except Exception as e:
            logger.error(f"Error adding instance to dataset {dataset_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def update_instance(
        self,
        instance_id: str,
        data: DatasetInstanceUpdate,
        updated_by: Optional[str] = None,
    ) -> Optional[DatasetInstance]:
        """Update an existing instance."""
        try:
            db_instance = dataset_instance_repo.get_with_relations(db=self._db, id=instance_id)
            if not db_instance:
                return None
            
            # Validate contract_server_id belongs to contract_id (if both provided)
            contract_id = data.contract_id if data.contract_id is not None else db_instance.contract_id
            server_id = data.contract_server_id if data.contract_server_id is not None else db_instance.contract_server_id
            if contract_id and server_id:
                self._validate_server_belongs_to_contract(contract_id, server_id)
            
            db_instance = dataset_instance_repo.update_instance(
                db=self._db,
                instance=db_instance,
                contract_id=data.contract_id,
                contract_server_id=data.contract_server_id,
                physical_path=data.physical_path,
                asset_type=data.asset_type,
                role=data.role,
                display_name=data.display_name,
                environment=data.environment,
                status=data.status,
                notes=data.notes,
                updated_by=updated_by,
            )
            
            # Update tags via unified tagging system (if provided)
            if data.tags is not None and self._tags_manager:
                self._tags_manager.set_tags_for_entity(
                    db=self._db,
                    entity_id=instance_id,
                    entity_type="dataset_instance",
                    tags=data.tags,
                    user_email=updated_by,
                )
            
            logger.info(f"Updated instance {instance_id}")
            return self._instance_to_api_model(db_instance)
            
        except Exception as e:
            logger.error(f"Error updating instance {instance_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def remove_instance(self, instance_id: str) -> bool:
        """Remove an instance."""
        try:
            db_instance = dataset_instance_repo.get(db=self._db, id=instance_id)
            if not db_instance:
                return False
            
            self._db.delete(db_instance)
            self._db.flush()
            
            logger.info(f"Removed instance {instance_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing instance {instance_id}: {e}", exc_info=True)
            self._db.rollback()
            raise

    def get_instances_by_contract(
        self,
        contract_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> List[DatasetInstance]:
        """Get all instances implementing a specific contract version."""
        try:
            instances = dataset_instance_repo.get_by_contract(
                db=self._db,
                contract_id=contract_id,
                skip=skip,
                limit=limit,
            )
            return [self._instance_to_api_model(inst) for inst in instances]
        except Exception as e:
            logger.error(f"Error getting instances for contract {contract_id}: {e}", exc_info=True)
            raise

    # =========================================================================
    # Instance Tagging Operations
    # =========================================================================

    def list_instance_tags(self, instance_id: str) -> List[AssignedTag]:
        """Get tags for a specific dataset instance."""
        if not self._tags_manager:
            logger.warning("TagsManager not available, returning empty tags")
            return []
        
        return self._tags_manager.list_assigned_tags(
            db=self._db,
            entity_id=instance_id,
            entity_type="dataset_instance"
        )

    def set_instance_tags(
        self,
        instance_id: str,
        tags: List[AssignedTagCreate],
        assigned_by: Optional[str] = None,
    ) -> List[AssignedTag]:
        """Set tags for a specific dataset instance (replaces existing)."""
        if not self._tags_manager:
            logger.warning("TagsManager not available, cannot set tags")
            return []
        
        self._tags_manager.set_tags_for_entity(
            db=self._db,
            entity_id=instance_id,
            entity_type="dataset_instance",
            tags=tags,
            user_email=assigned_by,
        )
        
        return self.list_instance_tags(instance_id)

    # =========================================================================
    # Dataset Tagging Operations
    # =========================================================================

    def list_dataset_tags(self, dataset_id: str) -> List[AssignedTag]:
        """Get tags for a specific dataset."""
        if not self._tags_manager:
            logger.warning("TagsManager not available, returning empty tags")
            return []
        
        return self._tags_manager.list_assigned_tags(
            db=self._db,
            entity_id=dataset_id,
            entity_type="dataset"
        )

    def set_dataset_tags(
        self,
        dataset_id: str,
        tags: List[AssignedTagCreate],
        assigned_by: Optional[str] = None,
    ) -> List[AssignedTag]:
        """Set tags for a specific dataset (replaces existing)."""
        if not self._tags_manager:
            logger.warning("TagsManager not available, cannot set tags")
            return []
        
        self._tags_manager.set_tags_for_entity(
            db=self._db,
            entity_id=dataset_id,
            entity_type="dataset",
            tags=tags,
            user_email=assigned_by,
        )
        
        return self.list_dataset_tags(dataset_id)

    def _validate_server_belongs_to_contract(self, contract_id: str, server_id: str) -> None:
        """Validate that a server entry belongs to the specified contract."""
        from src.db_models.data_contracts import DataContractServerDb
        
        server = self._db.query(DataContractServerDb).filter(
            DataContractServerDb.id == server_id
        ).first()
        
        if not server:
            raise ValueError(f"Server {server_id} not found")
        
        if server.contract_id != contract_id:
            raise ValueError(f"Server {server_id} does not belong to contract {contract_id}")

    def _instance_to_api_model(self, db_instance: DatasetInstanceDb) -> DatasetInstance:
        """Convert DB instance model to API model."""
        contract_name = None
        contract_version = None
        server_type = None
        server_environment = None
        server_name = None
        
        if db_instance.contract:
            contract_name = db_instance.contract.name
            contract_version = db_instance.contract.version
        
        if db_instance.contract_server:
            server_type = db_instance.contract_server.type
            server_environment = db_instance.contract_server.environment
            server_name = db_instance.contract_server.server
        
        # Fetch tags from unified tagging system for this instance
        instance_tags: List[AssignedTag] = []
        if self._tags_manager:
            instance_tags = self._tags_manager.list_assigned_tags(
                db=self._db,
                entity_id=str(db_instance.id),
                entity_type="dataset_instance"
            )
        
        return DatasetInstance(
            id=db_instance.id,
            dataset_id=db_instance.dataset_id,
            contract_id=db_instance.contract_id,
            contract_name=contract_name,
            contract_version=contract_version,
            contract_server_id=db_instance.contract_server_id,
            server_type=server_type,
            server_environment=server_environment,
            server_name=server_name,
            physical_path=db_instance.physical_path,
            asset_type=getattr(db_instance, 'asset_type', None),
            role=getattr(db_instance, 'role', 'main') or 'main',
            display_name=getattr(db_instance, 'display_name', None),
            environment=getattr(db_instance, 'environment', None),
            status=db_instance.status,
            notes=db_instance.notes,
            tags=instance_tags,
            created_at=db_instance.created_at,
            updated_at=db_instance.updated_at,
            created_by=db_instance.created_by,
            updated_by=db_instance.updated_by,
        )

    # =========================================================================
    # Asset Validation
    # =========================================================================

    def validate_asset_exists(
        self,
        catalog_name: str,
        schema_name: str,
        object_name: str,
        connector_type: Optional[str] = "databricks",
    ) -> Dict[str, Any]:
        """
        Validate that an asset exists in the specified platform.
        
        Uses the connector registry to get the appropriate connector for the
        platform. Falls back to direct WorkspaceClient for backward compatibility
        with Unity Catalog.
        
        Args:
            catalog_name: Catalog/database name
            schema_name: Schema name
            object_name: Object/table name
            connector_type: Platform connector type (default: "databricks")
        
        Returns:
            Dict with exists status and asset details
        """
        full_name = f"{catalog_name}.{schema_name}.{object_name}"
        
        # Try to use the connector registry first
        try:
            registry = get_registry()
            if registry.has_connector(connector_type):
                connector = registry.get_connector(connector_type)
                if connector.is_available:
                    result = connector.validate_asset_exists(full_name)
                    return {
                        "exists": result.exists,
                        "validated": result.validated,
                        "asset_type": result.asset_type.value if result.asset_type else None,
                        "message": result.message,
                        "name": object_name,
                        "catalog": catalog_name,
                        "schema": schema_name,
                    }
        except ConnectorNotFoundError:
            logger.debug(f"Connector {connector_type} not found, falling back to direct validation")
        except Exception as e:
            logger.debug(f"Connector validation failed, falling back: {e}")
        
        # Fallback to direct WorkspaceClient for backward compatibility
        if not self._ws_client:
            logger.warning("WorkspaceClient not available, skipping asset validation")
            return {"exists": True, "validated": False, "message": "Validation skipped - no workspace client"}
        
        try:
            # Try to get the table info
            table_info = self._ws_client.tables.get(full_name)
            
            return {
                "exists": True,
                "validated": True,
                "asset_type": table_info.table_type.value.lower() if table_info.table_type else "table",
                "name": table_info.name,
                "catalog": table_info.catalog_name,
                "schema": table_info.schema_name,
            }
        except Exception as e:
            logger.debug(f"Asset {full_name} not found or error: {e}")
            return {
                "exists": False,
                "validated": True,
                "message": str(e),
            }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _to_list_item(self, db_dataset: DatasetDb) -> DatasetListItem:
        """Convert DB model to list item API model."""
        subscriber_count = len(db_dataset.subscriptions) if db_dataset.subscriptions else 0
        instance_count = len(db_dataset.instances) if db_dataset.instances else 0
        
        return DatasetListItem(
            id=db_dataset.id,
            name=db_dataset.name,
            description=db_dataset.description,
            status=db_dataset.status,
            version=db_dataset.version,
            published=db_dataset.published,
            contract_id=db_dataset.contract_id,
            contract_name=db_dataset.contract.name if db_dataset.contract else None,
            owner_team_id=db_dataset.owner_team_id,
            owner_team_name=db_dataset.owner_team.name if db_dataset.owner_team else None,
            project_id=db_dataset.project_id,
            project_name=db_dataset.project.name if db_dataset.project else None,
            subscriber_count=subscriber_count,
            instance_count=instance_count,
            created_at=db_dataset.created_at,
            updated_at=db_dataset.updated_at,
        )

    def _to_api_model(self, db_dataset: DatasetDb) -> Dataset:
        """Convert DB model to full API model."""
        from src.models.datasets import DatasetCustomProperty
        
        subscriber_count = len(db_dataset.subscriptions) if db_dataset.subscriptions else 0
        instance_count = len(db_dataset.instances) if db_dataset.instances else 0
        
        # Fetch tags from unified tagging system
        tags: List[AssignedTag] = []
        if self._tags_manager:
            tags = self._tags_manager.list_assigned_tags(
                db=self._db,
                entity_id=str(db_dataset.id),
                entity_type="dataset"
            )
        
        custom_properties = [
            DatasetCustomProperty(id=prop.id, property=prop.property, value=prop.value)
            for prop in (db_dataset.custom_properties or [])
        ]
        
        instances = [
            self._instance_to_api_model(inst)
            for inst in (db_dataset.instances or [])
        ]
        
        return Dataset(
            id=db_dataset.id,
            name=db_dataset.name,
            description=db_dataset.description,
            contract_id=db_dataset.contract_id,
            contract_name=db_dataset.contract.name if db_dataset.contract else None,
            owner_team_id=db_dataset.owner_team_id,
            owner_team_name=db_dataset.owner_team.name if db_dataset.owner_team else None,
            project_id=db_dataset.project_id,
            project_name=db_dataset.project.name if db_dataset.project else None,
            status=db_dataset.status,
            version=db_dataset.version,
            published=db_dataset.published,
            max_level_inheritance=db_dataset.max_level_inheritance,
            tags=tags,
            custom_properties=custom_properties,
            instances=instances,
            subscriber_count=subscriber_count,
            instance_count=instance_count,
            created_at=db_dataset.created_at,
            updated_at=db_dataset.updated_at,
            created_by=db_dataset.created_by,
            updated_by=db_dataset.updated_by,
        )

