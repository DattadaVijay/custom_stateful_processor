"""File model for Datasets YAML serialization."""

from typing import Any, Dict, List

from .base import FileModel, register_file_model
from src.db_models.datasets import DatasetDb


@register_file_model
class DatasetFileModel(FileModel[DatasetDb]):
    """File model for Datasets.
    
    Each dataset is stored in its own YAML file.
    Filename format: {full_name}.yaml
    """
    
    ENTITY_TYPE = "Dataset"
    SUBDIRECTORY = "datasets"
    ONE_FILE_PER_RECORD = True
    
    @classmethod
    def get_filename(cls, entity: DatasetDb) -> str:
        """Generate filename for a dataset."""
        full_name = entity.full_name or f"{entity.catalog}.{entity.schema}.{entity.table_name}"
        safe_name = full_name.lower().replace('.', '-').replace(' ', '-')
        return f"{safe_name}.yaml"
    
    @classmethod
    def to_yaml_dict(cls, entity: DatasetDb) -> Dict[str, Any]:
        """Convert Dataset to YAML-serializable dictionary."""
        result: Dict[str, Any] = {}
        
        # Core identity - Unity Catalog location
        result['catalog'] = entity.catalog
        result['schema'] = entity.schema
        result['tableName'] = entity.table_name
        result['fullName'] = entity.full_name
        result['assetType'] = entity.asset_type
        
        # Environment and contract linkage
        if entity.environment:
            result['environment'] = entity.environment
        if entity.contract_id:
            result['contractId'] = entity.contract_id
        
        # Ownership
        if entity.owner_team_id:
            result['ownerTeamId'] = entity.owner_team_id
        
        # Status
        result['status'] = entity.status or 'active'
        
        # Description
        if entity.description:
            result['description'] = entity.description
        
        # Access grants
        if hasattr(entity, 'instances') and entity.instances:
            grants = []
            for instance in entity.instances:
                if hasattr(instance, 'access_grants') and instance.access_grants:
                    for grant in instance.access_grants:
                        grants.append({
                            'principal': grant.principal,
                            'principalType': grant.principal_type,
                            'privileges': grant.privileges.split(',') if grant.privileges else [],
                        })
            if grants:
                result['accessGrants'] = grants
        
        # Subscriptions
        if hasattr(entity, 'subscriptions') and entity.subscriptions:
            result['subscriptions'] = [
                {
                    'subscriberId': sub.subscriber_id,
                    'purpose': sub.purpose,
                    'status': sub.status,
                }
                for sub in entity.subscriptions
            ]
        
        return result

