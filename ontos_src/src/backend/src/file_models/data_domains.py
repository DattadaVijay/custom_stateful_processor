"""File model for Data Domains YAML serialization."""

from typing import Any, Dict

from .base import FileModel, register_file_model
from src.db_models.data_domains import DataDomain


@register_file_model
class DataDomainFileModel(FileModel[DataDomain]):
    """File model for Data Domains.
    
    All domains are stored in a single YAML file.
    """
    
    ENTITY_TYPE = "DataDomain"
    SUBDIRECTORY = "config"
    ONE_FILE_PER_RECORD = False  # All domains in one file
    
    @classmethod
    def get_filename(cls, entity: DataDomain = None) -> str:
        """Get the filename for domains."""
        return "domains.yaml"
    
    @classmethod
    def to_yaml_dict(cls, entity: DataDomain) -> Dict[str, Any]:
        """Convert DataDomain to YAML-serializable dictionary."""
        result: Dict[str, Any] = {
            'name': entity.name,
        }
        
        if entity.description:
            result['description'] = entity.description
        if entity.parent_id:
            result['parentId'] = entity.parent_id
        if entity.owner:
            result['owner'] = entity.owner
        if entity.status:
            result['status'] = entity.status
        
        return result

