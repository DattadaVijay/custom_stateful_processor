"""File model for Tags YAML serialization."""

from typing import Any, Dict, List

from .base import FileModel, register_file_model
from src.db_models.tags import TagNamespaceDb, TagDb


@register_file_model
class TagFileModel(FileModel[TagNamespaceDb]):
    """File model for Tag Namespaces and their Tags.
    
    All tag namespaces are stored in a single YAML file with their tags.
    """
    
    ENTITY_TYPE = "TagNamespace"
    SUBDIRECTORY = "config"
    ONE_FILE_PER_RECORD = False  # All namespaces in one file
    
    @classmethod
    def get_filename(cls, entity: TagNamespaceDb = None) -> str:
        """Get the filename for tags."""
        return "tags.yaml"
    
    @classmethod
    def to_yaml_dict(cls, entity: TagNamespaceDb) -> Dict[str, Any]:
        """Convert TagNamespace to YAML-serializable dictionary."""
        result: Dict[str, Any] = {
            'name': entity.name,
        }
        
        if entity.display_name:
            result['displayName'] = entity.display_name
        if entity.description:
            result['description'] = entity.description
        if entity.owner:
            result['owner'] = entity.owner
        
        result['allowFreeform'] = entity.allow_freeform
        
        # Serialize tags in this namespace
        if hasattr(entity, 'tags') and entity.tags:
            result['tags'] = cls._serialize_tags(entity.tags)
        
        return result
    
    @classmethod
    def _serialize_tags(cls, tags: List[TagDb]) -> List[Dict[str, Any]]:
        """Serialize tags in a namespace."""
        result = []
        for tag in tags:
            t: Dict[str, Any] = {
                'name': tag.name,
            }
            if tag.display_name:
                t['displayName'] = tag.display_name
            if tag.description:
                t['description'] = tag.description
            if tag.allowed_values:
                t['allowedValues'] = tag.allowed_values
            result.append(t)
        return result

