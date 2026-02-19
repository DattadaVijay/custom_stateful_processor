"""Base classes for file models providing YAML serialization."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic
import yaml

from src.common.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


class FileModel(ABC, Generic[T]):
    """Base class for file models that serialize entities to YAML.
    
    Subclasses should implement:
    - ENTITY_TYPE: String identifier for the entity type
    - SUBDIRECTORY: Subdirectory in the Git repo for this entity type
    - ONE_FILE_PER_RECORD: Whether each record gets its own file
    - to_yaml_dict(): Convert entity to YAML-serializable dict
    - get_filename(): Get the filename for an entity
    """
    
    # Override in subclasses
    ENTITY_TYPE: str = ""
    SUBDIRECTORY: str = ""
    ONE_FILE_PER_RECORD: bool = True  # True = one file per record, False = all records in one file
    API_VERSION: str = "ontos/v1"
    
    @classmethod
    @abstractmethod
    def to_yaml_dict(cls, entity: T) -> Dict[str, Any]:
        """Convert an entity to a YAML-serializable dictionary.
        
        Args:
            entity: The entity to convert
            
        Returns:
            Dictionary suitable for YAML serialization
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_filename(cls, entity: T) -> str:
        """Get the filename for storing this entity.
        
        Args:
            entity: The entity to get filename for
            
        Returns:
            Filename (without path) for this entity
        """
        pass
    
    @classmethod
    def wrap_as_resource(cls, entity: T, data: Dict[str, Any]) -> Dict[str, Any]:
        """Wrap entity data in a Kubernetes-style resource format.
        
        Args:
            entity: The source entity
            data: The serialized entity data
            
        Returns:
            Resource-wrapped dictionary
        """
        return {
            "apiVersion": cls.API_VERSION,
            "kind": cls.ENTITY_TYPE,
            "metadata": cls._get_metadata(entity),
            "spec": data
        }
    
    @classmethod
    def _get_metadata(cls, entity: T) -> Dict[str, Any]:
        """Extract metadata from entity.
        
        Override in subclasses for entity-specific metadata extraction.
        
        Args:
            entity: The entity to extract metadata from
            
        Returns:
            Metadata dictionary
        """
        metadata: Dict[str, Any] = {}
        
        # Common metadata fields
        if hasattr(entity, 'id'):
            metadata['id'] = str(entity.id)
        if hasattr(entity, 'name'):
            metadata['name'] = entity.name
        if hasattr(entity, 'version'):
            metadata['version'] = entity.version
        if hasattr(entity, 'created_at') and entity.created_at:
            metadata['createdAt'] = entity.created_at.isoformat() if isinstance(entity.created_at, datetime) else str(entity.created_at)
        if hasattr(entity, 'updated_at') and entity.updated_at:
            metadata['updatedAt'] = entity.updated_at.isoformat() if isinstance(entity.updated_at, datetime) else str(entity.updated_at)
        
        return metadata
    
    @classmethod
    def serialize(cls, entity: T, wrap: bool = True) -> str:
        """Serialize an entity to YAML string.
        
        Args:
            entity: The entity to serialize
            wrap: Whether to wrap in resource format
            
        Returns:
            YAML string
        """
        data = cls.to_yaml_dict(entity)
        if wrap:
            data = cls.wrap_as_resource(entity, data)
        return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    @classmethod
    def serialize_all(cls, entities: List[T], wrap: bool = True) -> str:
        """Serialize multiple entities to a YAML string.
        
        For ONE_FILE_PER_RECORD=False, this is used to create a single file.
        
        Args:
            entities: List of entities to serialize
            wrap: Whether to wrap in resource format
            
        Returns:
            YAML string containing all entities
        """
        if cls.ONE_FILE_PER_RECORD:
            # For multi-file entities, return YAML document stream
            docs = []
            for entity in entities:
                data = cls.to_yaml_dict(entity)
                if wrap:
                    data = cls.wrap_as_resource(entity, data)
                docs.append(data)
            return yaml.dump_all(docs, default_flow_style=False, sort_keys=False, allow_unicode=True)
        else:
            # For single-file entities, return a list
            items = []
            for entity in entities:
                data = cls.to_yaml_dict(entity)
                if wrap:
                    data = cls.wrap_as_resource(entity, data)
                items.append(data)
            return yaml.dump(
                {"apiVersion": cls.API_VERSION, "kind": f"{cls.ENTITY_TYPE}List", "items": items},
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True
            )


class FileModelRegistry:
    """Registry for file model classes.
    
    Provides a central place to look up file models by entity type.
    """
    
    _models: Dict[str, Type[FileModel]] = {}
    
    @classmethod
    def register(cls, model_class: Type[FileModel]) -> Type[FileModel]:
        """Register a file model class.
        
        Can be used as a decorator.
        
        Args:
            model_class: The file model class to register
            
        Returns:
            The registered class (for decorator usage)
        """
        entity_type = model_class.ENTITY_TYPE
        if entity_type:
            cls._models[entity_type] = model_class
            logger.debug(f"Registered file model for entity type: {entity_type}")
        return model_class
    
    @classmethod
    def get(cls, entity_type: str) -> Optional[Type[FileModel]]:
        """Get a file model class by entity type.
        
        Args:
            entity_type: The entity type to look up
            
        Returns:
            The file model class, or None if not found
        """
        return cls._models.get(entity_type)
    
    @classmethod
    def list_types(cls) -> List[str]:
        """List all registered entity types.
        
        Returns:
            List of entity type strings
        """
        return list(cls._models.keys())
    
    @classmethod
    def all(cls) -> Dict[str, Type[FileModel]]:
        """Get all registered file models.
        
        Returns:
            Dictionary of entity type to file model class
        """
        return dict(cls._models)


def register_file_model(cls: Type[FileModel]) -> Type[FileModel]:
    """Decorator to register a file model class."""
    return FileModelRegistry.register(cls)

