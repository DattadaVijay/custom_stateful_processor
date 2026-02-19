"""
Industry Ontology Manager.

This manager handles the Industry Ontology Library feature, providing:
- Loading and parsing the industry_ontologies.yaml registry
- Listing verticals and ontologies
- Building module trees for UI selection
- Importing selected modules into Ontos as semantic models
"""

from pathlib import Path
from typing import Optional, List, Dict, Any
import yaml

from rdflib import Graph
from sqlalchemy.orm import Session

from src.models.industry_ontology import (
    IndustryOntologyRegistry,
    IndustryVertical,
    IndustryOntology,
    VerticalSummary,
    OntologySummary,
    ModuleTreeNode,
    ImportRequest,
    ImportResult,
    CacheStatus,
    RefreshResult,
)
from src.controller.ontology_handlers import get_handler, OntologyHandler
from src.controller.semantic_models_manager import SemanticModelsManager
from src.models.semantic_models import SemanticModelCreate
from src.common.logging import get_logger

logger = get_logger(__name__)


class IndustryOntologyManager:
    """
    Manager for the Industry Ontology Library.
    
    Provides methods to:
    - List available industry verticals and ontologies
    - Get module trees for ontology selection UI
    - Import selected modules into Ontos
    - Manage ontology cache
    """
    
    def __init__(
        self, 
        db: Session, 
        data_dir: Optional[Path] = None,
        semantic_models_manager: Optional[SemanticModelsManager] = None
    ):
        """
        Initialize the manager.
        
        Args:
            db: Database session
            data_dir: Directory containing the industry_ontologies.yaml file
            semantic_models_manager: Manager for importing ontologies
        """
        self._db = db
        self._data_dir = data_dir or Path(__file__).parent.parent / "data"
        self._cache_dir = self._data_dir / "cache" / "ontologies"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._semantic_models_manager = semantic_models_manager
        
        # Load the registry
        self._registry: Optional[IndustryOntologyRegistry] = None
        self._load_registry()
        
        logger.info(f"IndustryOntologyManager initialized with {len(self._registry.verticals if self._registry else [])} verticals")
    
    def _load_registry(self) -> None:
        """Load the industry ontologies registry from YAML."""
        registry_path = self._data_dir / "industry_ontologies.yaml"
        
        if not registry_path.exists():
            logger.warning(f"Industry ontologies registry not found: {registry_path}")
            self._registry = IndustryOntologyRegistry(verticals=[])
            return
        
        try:
            with open(registry_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            self._registry = IndustryOntologyRegistry(**data)
            logger.info(f"Loaded {len(self._registry.verticals)} industry verticals")
            
        except Exception as e:
            logger.error(f"Failed to load industry ontologies registry: {e}")
            self._registry = IndustryOntologyRegistry(verticals=[])
    
    def reload_registry(self) -> None:
        """Reload the registry from disk."""
        self._load_registry()
    
    # =========================================================================
    # Listing methods
    # =========================================================================
    
    def list_verticals(self) -> List[VerticalSummary]:
        """List all available industry verticals."""
        if not self._registry:
            return []
        
        return [
            VerticalSummary(
                id=v.id,
                name=v.name,
                icon=v.icon,
                description=v.description,
                ontology_count=len(v.ontologies)
            )
            for v in self._registry.verticals
        ]
    
    def get_vertical(self, vertical_id: str) -> Optional[IndustryVertical]:
        """Get a specific vertical by ID."""
        if not self._registry:
            return None
        
        for v in self._registry.verticals:
            if v.id == vertical_id:
                return v
        return None
    
    def list_ontologies(self, vertical_id: str) -> List[OntologySummary]:
        """List ontologies in a specific vertical."""
        vertical = self.get_vertical(vertical_id)
        if not vertical:
            return []
        
        return [
            self._ontology_to_summary(o)
            for o in vertical.ontologies
        ]
    
    def list_all_ontologies(self) -> List[OntologySummary]:
        """List all ontologies across all verticals."""
        if not self._registry:
            return []
        
        result = []
        for v in self._registry.verticals:
            for o in v.ontologies:
                summary = self._ontology_to_summary(o)
                result.append(summary)
        return result
    
    def _ontology_to_summary(self, ontology: IndustryOntology) -> OntologySummary:
        """Convert an ontology to a summary."""
        # Count modules
        module_count = len(ontology.modules)
        is_modular = False
        
        if ontology.domains:
            is_modular = True
            module_count = sum(
                len(domain.modules)
                for domain in ontology.domains
            )
        
        return OntologySummary(
            id=ontology.id,
            name=ontology.name,
            full_name=ontology.full_name,
            description=ontology.description,
            handler=ontology.handler,
            license=ontology.license,
            requires_license_agreement=ontology.requires_license_agreement,
            website=ontology.website,
            version=ontology.version,
            recommended_foundation=ontology.recommended_foundation,
            module_count=module_count,
            is_modular=is_modular,
        )
    
    def get_ontology(self, vertical_id: str, ontology_id: str) -> Optional[IndustryOntology]:
        """Get a specific ontology."""
        vertical = self.get_vertical(vertical_id)
        if not vertical:
            return None
        
        for o in vertical.ontologies:
            if o.id == ontology_id:
                return o
        return None
    
    def find_ontology(self, ontology_id: str) -> Optional[tuple[str, IndustryOntology]]:
        """Find an ontology across all verticals. Returns (vertical_id, ontology)."""
        if not self._registry:
            return None
        
        for v in self._registry.verticals:
            for o in v.ontologies:
                if o.id == ontology_id:
                    return (v.id, o)
        return None
    
    # =========================================================================
    # Module tree methods
    # =========================================================================
    
    def get_module_tree(self, vertical_id: str, ontology_id: str) -> List[ModuleTreeNode]:
        """
        Get the module selection tree for an ontology.
        
        Args:
            vertical_id: The vertical ID
            ontology_id: The ontology ID
            
        Returns:
            List of root tree nodes for the UI
        """
        ontology = self.get_ontology(vertical_id, ontology_id)
        if not ontology:
            logger.warning(f"Ontology not found: {vertical_id}/{ontology_id}")
            return []
        
        # Get the appropriate handler
        handler_class = get_handler(ontology.handler)
        handler = handler_class(self._cache_dir)
        
        return handler.get_module_tree(ontology)
    
    # =========================================================================
    # Import methods
    # =========================================================================
    
    def import_modules(self, request: ImportRequest, username: Optional[str] = None) -> ImportResult:
        """
        Import selected ontology modules into Ontos.
        
        Args:
            request: The import request with selected modules
            username: Username for audit trail
            
        Returns:
            Import result with status and details
        """
        # Find the ontology
        result = self.find_ontology(request.ontology_id)
        if not result:
            return ImportResult(
                success=False,
                error=f"Ontology not found: {request.ontology_id}"
            )
        
        vertical_id, ontology = result
        
        # Check license agreement if required
        if ontology.requires_license_agreement and not request.accept_license:
            return ImportResult(
                success=False,
                error=f"License agreement required for {ontology.name}. Please accept the license terms."
            )
        
        # Get handler
        handler_class = get_handler(ontology.handler)
        handler = handler_class(self._cache_dir)
        
        # Resolve dependencies
        all_modules, auto_added = handler.resolve_dependencies(ontology, request.module_ids)
        
        if not all_modules:
            return ImportResult(
                success=False,
                error="No modules to import"
            )
        
        warnings = []
        
        # Fetch module contents
        try:
            module_contents = handler.fetch_modules(ontology, all_modules)
            
            if not module_contents:
                return ImportResult(
                    success=False,
                    error="Failed to fetch any modules"
                )
            
            # Report missing modules
            missing = set(all_modules) - set(module_contents.keys())
            if missing:
                warnings.append(f"Could not fetch {len(missing)} modules: {', '.join(list(missing)[:5])}")
            
        except Exception as e:
            logger.error(f"Failed to fetch modules: {e}")
            return ImportResult(
                success=False,
                error=f"Failed to fetch modules: {e}"
            )
        
        # Combine modules into a single graph
        try:
            combined_graph = handler.combine_modules(ontology, module_contents)
            triple_count = len(combined_graph)
            
            if triple_count == 0:
                return ImportResult(
                    success=False,
                    error="Combined graph is empty - no triples were parsed"
                )
            
        except Exception as e:
            logger.error(f"Failed to combine modules: {e}")
            return ImportResult(
                success=False,
                error=f"Failed to combine modules: {e}"
            )
        
        # Serialize to Turtle format
        try:
            # serialize() returns bytes when no destination is given, decode to string
            turtle_bytes = combined_graph.serialize(format='turtle')
            if isinstance(turtle_bytes, bytes):
                turtle_content = turtle_bytes.decode('utf-8')
            else:
                turtle_content = turtle_bytes
            
        except Exception as e:
            logger.error(f"Failed to serialize graph: {e}")
            return ImportResult(
                success=False,
                error=f"Failed to serialize graph: {e}"
            )
        
        # Create semantic model name
        model_name = f"{ontology.name}"
        if len(request.module_ids) == 1:
            # Use specific module name
            module_name = request.module_ids[0].split("/")[-1]
            model_name = f"{ontology.name} - {module_name}"
        elif len(request.module_ids) < 5:
            # List a few module names
            module_names = [m.split("/")[-1] for m in request.module_ids[:3]]
            model_name = f"{ontology.name} ({', '.join(module_names)}...)"
        
        # Import into Ontos via SemanticModelsManager
        try:
            if not self._semantic_models_manager:
                return ImportResult(
                    success=False,
                    error="SemanticModelsManager not available"
                )
            
            # Create the semantic model
            # Note: SemanticFormat only allows 'rdfs' or 'skos', use 'rdfs' for OWL/RDF imports
            create_data = SemanticModelCreate(
                name=model_name,
                format="rdfs",
                content_text=turtle_content,
                original_filename=f"{ontology.id}_import.ttl",
                size_bytes=str(len(turtle_content.encode('utf-8'))),
            )
            
            created = self._semantic_models_manager.create(
                data=create_data,
                created_by=username or "system@industry-library"
            )
            
            logger.info(f"Imported {ontology.name} with {triple_count} triples as {created.id}")
            
            # Rebuild the in-memory graph so concepts appear immediately
            try:
                self._semantic_models_manager.rebuild_graph_from_enabled()
                logger.info("Rebuilt semantic graph after import")
            except Exception as e:
                logger.warning(f"Failed to rebuild graph after import: {e}")
            
            return ImportResult(
                success=True,
                semantic_model_id=created.id,
                semantic_model_name=created.name,
                modules_imported=list(module_contents.keys()),
                dependencies_added=auto_added,
                triple_count=triple_count,
                warnings=warnings,
            )
            
        except Exception as e:
            logger.error(f"Failed to create semantic model: {e}")
            return ImportResult(
                success=False,
                error=f"Failed to create semantic model: {e}"
            )
    
    # =========================================================================
    # Cache management methods
    # =========================================================================
    
    def get_cache_status(self, ontology_id: str) -> CacheStatus:
        """Get cache status for an ontology."""
        result = self.find_ontology(ontology_id)
        if not result:
            return CacheStatus(ontology_id=ontology_id, is_cached=False)
        
        vertical_id, ontology = result
        handler_class = get_handler(ontology.handler)
        handler = handler_class(self._cache_dir)
        
        # Check if any modules are cached
        is_cached = False
        cache_date = None
        total_size = 0
        is_stale = True
        
        # Check modules
        for module in ontology.modules:
            if handler.is_cached(ontology_id, module.id):
                is_cached = True
                if not handler.is_cache_stale(ontology_id, module.id):
                    is_stale = False
                
                cache_path = handler.get_cache_path(ontology_id, module.id)
                if cache_path.exists():
                    total_size += cache_path.stat().st_size
        
        return CacheStatus(
            ontology_id=ontology_id,
            is_cached=is_cached,
            cache_size_bytes=total_size if is_cached else None,
            is_stale=is_stale,
        )
    
    def refresh_cache(self, vertical_id: str, ontology_id: str) -> RefreshResult:
        """Refresh the cache for an ontology."""
        ontology = self.get_ontology(vertical_id, ontology_id)
        if not ontology:
            return RefreshResult(
                success=False,
                ontology_id=ontology_id,
                error=f"Ontology not found: {ontology_id}"
            )
        
        handler_class = get_handler(ontology.handler)
        handler = handler_class(self._cache_dir)
        
        # Clear existing cache
        handler.clear_cache(ontology_id)
        
        # Fetch all modules with force_refresh
        all_module_ids = [f"{ontology_id}/{m.id}" for m in ontology.modules]
        
        try:
            module_contents = handler.fetch_modules(ontology, all_module_ids, force_refresh=True)
            
            return RefreshResult(
                success=True,
                ontology_id=ontology_id,
                modules_refreshed=len(module_contents),
            )
            
        except Exception as e:
            return RefreshResult(
                success=False,
                ontology_id=ontology_id,
                error=str(e),
            )
    
    def clear_cache(self, ontology_id: Optional[str] = None) -> int:
        """
        Clear cached ontology files.
        
        Args:
            ontology_id: If provided, clear only this ontology's cache.
                        Otherwise clear all cached files.
                        
        Returns:
            Number of files cleared
        """
        count = 0
        
        if ontology_id:
            # Clear specific ontology
            for f in self._cache_dir.iterdir():
                if f.name.startswith(f"{ontology_id}_"):
                    try:
                        f.unlink()
                        count += 1
                    except Exception:
                        pass
        else:
            # Clear all
            for f in self._cache_dir.iterdir():
                try:
                    f.unlink()
                    count += 1
                except Exception:
                    pass
        
        logger.info(f"Cleared {count} cached ontology files")
        return count
    
    # =========================================================================
    # Utility methods
    # =========================================================================
    
    def get_recommended_foundations(self) -> List[OntologySummary]:
        """Get list of recommended foundational ontologies."""
        if not self._registry:
            return []
        
        result = []
        for v in self._registry.verticals:
            for o in v.ontologies:
                if o.recommended_foundation:
                    result.append(self._ontology_to_summary(o))
        return result
