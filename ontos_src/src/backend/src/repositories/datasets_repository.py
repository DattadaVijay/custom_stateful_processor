"""
Datasets Repository

Database access layer for Datasets using the Repository pattern.
"""

from typing import Any, Dict, Optional, List, Union

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import or_

from src.common.repository import CRUDBase
from src.db_models.datasets import (
    DatasetDb,
    DatasetSubscriptionDb,
    DatasetCustomPropertyDb,
    DatasetInstanceDb,
)
from src.common.logging import get_logger

logger = get_logger(__name__)


class DatasetRepository(CRUDBase[DatasetDb, Dict[str, Any], Union[Dict[str, Any], DatasetDb]]):
    """Repository for Dataset CRUD operations."""
    
    def __init__(self):
        super().__init__(DatasetDb)

    def get_with_all(self, db: Session, *, id: str) -> Optional[DatasetDb]:
        """Get a dataset with all related data loaded."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.contract),
                    selectinload(self.model.owner_team),
                    selectinload(self.model.project),
                    selectinload(self.model.subscriptions),
                    # Tags now loaded via TagsManager (entity_type="dataset")
                    selectinload(self.model.custom_properties),
                    selectinload(self.model.instances).selectinload(DatasetInstanceDb.contract),
                    selectinload(self.model.instances).selectinload(DatasetInstanceDb.contract_server),
                )
                .filter(self.model.id == id)
                .first()
            )
        except Exception as e:
            logger.error(f"Error fetching Dataset with all relations for id {id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_name(self, db: Session, *, name: str) -> Optional[DatasetDb]:
        """Get dataset by name."""
        try:
            return db.query(self.model).filter(self.model.name == name).first()
        except Exception as e:
            logger.error(f"Error fetching Dataset by name {name}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_contract(
        self,
        db: Session,
        *,
        contract_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetDb]:
        """Get all datasets implementing a specific contract."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.owner_team),
                    selectinload(self.model.subscriptions),
                )
                .filter(self.model.contract_id == contract_id)
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching Datasets by contract {contract_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_multi(
        self,
        db: Session,
        *,
        skip: int = 0,
        limit: int = 100,
        status: Optional[str] = None,
        contract_id: Optional[str] = None,
        owner_team_id: Optional[str] = None,
        project_id: Optional[str] = None,
        published: Optional[bool] = None,
        search: Optional[str] = None,
        is_admin: bool = False
    ) -> List[DatasetDb]:
        """Get multiple datasets with optional filtering."""
        logger.debug(f"Fetching Datasets with filters: status={status}, search={search}")
        try:
            query = db.query(self.model).options(
                selectinload(self.model.contract),
                selectinload(self.model.owner_team),
                selectinload(self.model.project),
                selectinload(self.model.subscriptions),
                # Tags now loaded via TagsManager (entity_type="dataset")
                selectinload(self.model.instances),
            )

            # Apply filters
            if status:
                query = query.filter(self.model.status == status)
            if contract_id:
                query = query.filter(self.model.contract_id == contract_id)
            if owner_team_id:
                query = query.filter(self.model.owner_team_id == owner_team_id)
            if project_id:
                # Include datasets with matching project OR no project assigned (orphans)
                query = query.filter(
                    or_(
                        self.model.project_id == project_id,
                        self.model.project_id.is_(None)
                    )
                )
            if published is not None:
                query = query.filter(self.model.published == published)
            if search:
                search_pattern = f"%{search}%"
                query = query.filter(
                    or_(
                        self.model.name.ilike(search_pattern),
                        self.model.description.ilike(search_pattern),
                    )
                )

            return query.order_by(self.model.name).offset(skip).limit(limit).all()
        except Exception as e:
            logger.error(f"Database error fetching Datasets: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_project(
        self,
        db: Session,
        project_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetDb]:
        """Get datasets filtered by project_id."""
        try:
            return (
                db.query(self.model)
                .filter(self.model.project_id == project_id)
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching Datasets by project {project_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def count_by_contract(self, db: Session, contract_id: str) -> int:
        """Count datasets implementing a specific contract."""
        try:
            return db.query(self.model).filter(self.model.contract_id == contract_id).count()
        except Exception as e:
            logger.error(f"Error counting Datasets by contract {contract_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def create(self, db: Session, *, obj_in: Union[Dict[str, Any], DatasetDb]) -> DatasetDb:
        """Create a new dataset."""
        try:
            if isinstance(obj_in, DatasetDb):
                db.add(obj_in)
                db.flush()
                db.refresh(obj_in)
                return obj_in
            payload: Dict[str, Any] = dict(obj_in)
            db_obj = self.model(**payload)
            db.add(db_obj)
            db.flush()
            db.refresh(db_obj)
            return db_obj
        except Exception as e:
            logger.error(f"Error creating Dataset: {e}", exc_info=True)
            db.rollback()
            raise


# Singleton instance
dataset_repo = DatasetRepository()


class DatasetSubscriptionRepository(CRUDBase[DatasetSubscriptionDb, Dict[str, Any], DatasetSubscriptionDb]):
    """Repository for Dataset Subscription operations."""

    def __init__(self):
        super().__init__(DatasetSubscriptionDb)

    def get_by_dataset_and_email(
        self,
        db: Session,
        *,
        dataset_id: str,
        email: str
    ) -> Optional[DatasetSubscriptionDb]:
        """Get subscription by dataset and subscriber email."""
        try:
            return (
                db.query(self.model)
                .filter(
                    self.model.dataset_id == dataset_id,
                    self.model.subscriber_email == email
                )
                .first()
            )
        except Exception as e:
            logger.error(f"Error fetching subscription for dataset {dataset_id} and email {email}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_dataset(
        self,
        db: Session,
        *,
        dataset_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetSubscriptionDb]:
        """Get all subscriptions for a dataset."""
        try:
            return (
                db.query(self.model)
                .filter(self.model.dataset_id == dataset_id)
                .order_by(self.model.subscribed_at.desc())
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching subscriptions for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_subscriber(
        self,
        db: Session,
        *,
        email: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetSubscriptionDb]:
        """Get all subscriptions for a subscriber."""
        try:
            return (
                db.query(self.model)
                .filter(self.model.subscriber_email == email)
                .order_by(self.model.subscribed_at.desc())
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching subscriptions for subscriber {email}: {e}", exc_info=True)
            db.rollback()
            raise

    def count_by_dataset(self, db: Session, dataset_id: str) -> int:
        """Count subscribers for a dataset."""
        try:
            return db.query(self.model).filter(self.model.dataset_id == dataset_id).count()
        except Exception as e:
            logger.error(f"Error counting subscribers for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def subscribe(
        self,
        db: Session,
        *,
        dataset_id: str,
        email: str,
        reason: Optional[str] = None
    ) -> DatasetSubscriptionDb:
        """Create a subscription to a dataset."""
        try:
            # Check if already subscribed
            existing = self.get_by_dataset_and_email(db=db, dataset_id=dataset_id, email=email)
            if existing:
                return existing
            
            subscription = DatasetSubscriptionDb(
                dataset_id=dataset_id,
                subscriber_email=email,
                subscription_reason=reason
            )
            db.add(subscription)
            db.flush()
            db.refresh(subscription)
            return subscription
        except Exception as e:
            logger.error(f"Error creating subscription for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def unsubscribe(
        self,
        db: Session,
        *,
        dataset_id: str,
        email: str
    ) -> bool:
        """Remove a subscription from a dataset."""
        try:
            subscription = self.get_by_dataset_and_email(db=db, dataset_id=dataset_id, email=email)
            if not subscription:
                return False
            db.delete(subscription)
            db.flush()
            return True
        except Exception as e:
            logger.error(f"Error removing subscription for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise


# Singleton instance
dataset_subscription_repo = DatasetSubscriptionRepository()


class DatasetCustomPropertyRepository(CRUDBase[DatasetCustomPropertyDb, Dict[str, Any], DatasetCustomPropertyDb]):
    """Repository for Dataset Custom Property operations."""

    def __init__(self):
        super().__init__(DatasetCustomPropertyDb)

    def get_by_dataset(self, db: Session, *, dataset_id: str) -> List[DatasetCustomPropertyDb]:
        """Get all custom properties for a dataset."""
        try:
            return db.query(self.model).filter(self.model.dataset_id == dataset_id).all()
        except Exception as e:
            logger.error(f"Error fetching custom properties for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def create_property(
        self,
        db: Session,
        *,
        dataset_id: str,
        property: str,
        value: Optional[str] = None
    ) -> DatasetCustomPropertyDb:
        """Create a custom property for a dataset."""
        try:
            prop = DatasetCustomPropertyDb(
                dataset_id=dataset_id,
                property=property,
                value=value
            )
            db.add(prop)
            db.flush()
            db.refresh(prop)
            return prop
        except Exception as e:
            logger.error(f"Error creating custom property for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def delete_by_dataset(self, db: Session, *, dataset_id: str) -> int:
        """Delete all custom properties for a dataset."""
        try:
            count = db.query(self.model).filter(self.model.dataset_id == dataset_id).delete()
            db.flush()
            return count
        except Exception as e:
            logger.error(f"Error deleting custom properties for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise


# Singleton instance
dataset_custom_property_repo = DatasetCustomPropertyRepository()


class DatasetInstanceRepository(CRUDBase[DatasetInstanceDb, Dict[str, Any], DatasetInstanceDb]):
    """Repository for Dataset Instance operations."""

    def __init__(self):
        super().__init__(DatasetInstanceDb)

    def get_with_relations(self, db: Session, *, id: str) -> Optional[DatasetInstanceDb]:
        """Get an instance with contract and server relations loaded."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.contract),
                    selectinload(self.model.contract_server),
                )
                .filter(self.model.id == id)
                .first()
            )
        except Exception as e:
            logger.error(f"Error fetching instance with relations for id {id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_dataset(
        self,
        db: Session,
        *,
        dataset_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetInstanceDb]:
        """Get all instances for a dataset."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.contract),
                    selectinload(self.model.contract_server),
                )
                .filter(self.model.dataset_id == dataset_id)
                .order_by(self.model.created_at)
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching instances for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_contract(
        self,
        db: Session,
        *,
        contract_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[DatasetInstanceDb]:
        """Get all instances implementing a specific contract version."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.dataset),
                    selectinload(self.model.contract_server),
                )
                .filter(self.model.contract_id == contract_id)
                .offset(skip)
                .limit(limit)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching instances by contract {contract_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_server(
        self,
        db: Session,
        *,
        contract_server_id: str
    ) -> List[DatasetInstanceDb]:
        """Get all instances linked to a specific contract server."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.dataset),
                    selectinload(self.model.contract),
                )
                .filter(self.model.contract_server_id == contract_server_id)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching instances by server {contract_server_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_physical_path(
        self,
        db: Session,
        *,
        physical_path: str
    ) -> List[DatasetInstanceDb]:
        """Get instances by physical path (across all datasets)."""
        try:
            return (
                db.query(self.model)
                .options(
                    selectinload(self.model.dataset),
                    selectinload(self.model.contract),
                    selectinload(self.model.contract_server),
                )
                .filter(self.model.physical_path == physical_path)
                .all()
            )
        except Exception as e:
            logger.error(f"Error fetching instances by path {physical_path}: {e}", exc_info=True)
            db.rollback()
            raise

    def get_by_dataset_and_server(
        self,
        db: Session,
        *,
        dataset_id: str,
        contract_server_id: str
    ) -> Optional[DatasetInstanceDb]:
        """Get instance by dataset and server (unique pair)."""
        try:
            return (
                db.query(self.model)
                .filter(
                    self.model.dataset_id == dataset_id,
                    self.model.contract_server_id == contract_server_id
                )
                .first()
            )
        except Exception as e:
            logger.error(f"Error fetching instance for dataset {dataset_id} and server {contract_server_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def count_by_dataset(self, db: Session, dataset_id: str) -> int:
        """Count instances for a dataset."""
        try:
            return db.query(self.model).filter(self.model.dataset_id == dataset_id).count()
        except Exception as e:
            logger.error(f"Error counting instances for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def create_instance(
        self,
        db: Session,
        *,
        dataset_id: str,
        contract_id: Optional[str] = None,
        contract_server_id: Optional[str] = None,
        physical_path: str,
        asset_type: Optional[str] = None,
        role: str = "main",
        display_name: Optional[str] = None,
        environment: Optional[str] = None,
        status: str = "active",
        notes: Optional[str] = None,
        created_by: Optional[str] = None
    ) -> DatasetInstanceDb:
        """Create an instance for a dataset."""
        try:
            instance = DatasetInstanceDb(
                dataset_id=dataset_id,
                contract_id=contract_id,
                contract_server_id=contract_server_id,
                physical_path=physical_path,
                asset_type=asset_type,
                role=role,
                display_name=display_name,
                environment=environment,
                status=status,
                notes=notes,
                created_by=created_by,
            )
            db.add(instance)
            db.flush()
            db.refresh(instance)
            return instance
        except Exception as e:
            logger.error(f"Error creating instance for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise

    def update_instance(
        self,
        db: Session,
        *,
        instance: DatasetInstanceDb,
        contract_id: Optional[str] = None,
        contract_server_id: Optional[str] = None,
        physical_path: Optional[str] = None,
        asset_type: Optional[str] = None,
        role: Optional[str] = None,
        display_name: Optional[str] = None,
        environment: Optional[str] = None,
        status: Optional[str] = None,
        notes: Optional[str] = None,
        updated_by: Optional[str] = None
    ) -> DatasetInstanceDb:
        """Update an instance."""
        try:
            if contract_id is not None:
                instance.contract_id = contract_id
            if contract_server_id is not None:
                instance.contract_server_id = contract_server_id
            if physical_path is not None:
                instance.physical_path = physical_path
            if asset_type is not None:
                instance.asset_type = asset_type
            if role is not None:
                instance.role = role
            if display_name is not None:
                instance.display_name = display_name
            if environment is not None:
                instance.environment = environment
            if status is not None:
                instance.status = status
            if notes is not None:
                instance.notes = notes
            if updated_by is not None:
                instance.updated_by = updated_by
            
            db.flush()
            db.refresh(instance)
            return instance
        except Exception as e:
            logger.error(f"Error updating instance {instance.id}: {e}", exc_info=True)
            db.rollback()
            raise

    def delete_by_dataset(self, db: Session, *, dataset_id: str) -> int:
        """Delete all instances for a dataset."""
        try:
            count = db.query(self.model).filter(self.model.dataset_id == dataset_id).delete()
            db.flush()
            return count
        except Exception as e:
            logger.error(f"Error deleting instances for dataset {dataset_id}: {e}", exc_info=True)
            db.rollback()
            raise


# Singleton instance
dataset_instance_repo = DatasetInstanceRepository()

