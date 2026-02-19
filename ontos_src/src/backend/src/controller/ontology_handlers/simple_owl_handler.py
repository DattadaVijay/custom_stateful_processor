"""
Handler for simple single-file OWL/RDF ontologies.

This handler is used for ontologies that:
- Have a flat list of modules (not hierarchical domains)
- Can be downloaded as single files from URLs
- Don't require complex dependency resolution

Examples: BFO, PROV-O, SKOS, Dublin Core, FOAF, etc.
"""

from pathlib import Path
from typing import Optional, List, Dict, Tuple
import httpx

from rdflib import Graph

from src.controller.ontology_handlers.base_handler import OntologyHandler
from src.models.industry_ontology import (
    IndustryOntology,
    OntologyModule,
    ModuleTreeNode,
    MaturityLevel,
)
from src.common.logging import get_logger

logger = get_logger(__name__)


class SimpleOWLHandler(OntologyHandler):
    """
    Handler for simple single-file ontologies.
    
    Expects ontologies with a flat `modules` list, where each module
    has an `owl_url` pointing to a downloadable OWL/RDF file.
    """
    
    def get_module_tree(self, ontology: IndustryOntology) -> List[ModuleTreeNode]:
        """Build a flat list of module nodes for the tree view."""
        nodes = []
        
        for module in ontology.modules:
            node = self.create_module_node(
                id=f"{ontology.id}/{module.id}",
                name=module.name,
                node_type="ontology",
                description=module.description,
                maturity=module.maturity if isinstance(module.maturity, MaturityLevel) 
                         else MaturityLevel(module.maturity) if module.maturity else MaturityLevel.RELEASE,
                owl_url=module.owl_url,
                dependencies=[f"{ontology.id}/{dep}" for dep in module.dependencies],
                selectable=bool(module.owl_url),
            )
            nodes.append(node)
        
        return nodes
    
    def resolve_dependencies(
        self, 
        ontology: IndustryOntology, 
        selected_ids: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Resolve dependencies for selected modules.
        
        For simple ontologies, dependencies are typically minimal.
        """
        # Build dependency map
        dep_map: Dict[str, List[str]] = {}
        for module in ontology.modules:
            full_id = f"{ontology.id}/{module.id}"
            dep_map[full_id] = [f"{ontology.id}/{dep}" for dep in module.dependencies]
        
        # Resolve all dependencies recursively
        all_required = set(selected_ids)
        auto_added = set()
        
        def add_deps(module_id: str):
            for dep in dep_map.get(module_id, []):
                if dep not in all_required:
                    all_required.add(dep)
                    auto_added.add(dep)
                    add_deps(dep)
        
        for selected in list(selected_ids):
            add_deps(selected)
        
        return list(all_required), list(auto_added)
    
    def fetch_modules(
        self, 
        ontology: IndustryOntology, 
        module_ids: List[str],
        force_refresh: bool = False
    ) -> Dict[str, str]:
        """Fetch module content from URLs or cache."""
        results = {}
        
        # Build module lookup
        module_lookup: Dict[str, OntologyModule] = {}
        for module in ontology.modules:
            full_id = f"{ontology.id}/{module.id}"
            module_lookup[full_id] = module
        
        for module_id in module_ids:
            module = module_lookup.get(module_id)
            if not module:
                logger.warning(f"Module not found: {module_id}")
                continue
            
            if not module.owl_url:
                logger.warning(f"No URL for module: {module_id}")
                continue
            
            # Check cache first
            if not force_refresh and self.is_cached(ontology.id, module.id):
                if not self.is_cache_stale(ontology.id, module.id):
                    cached = self.read_from_cache(ontology.id, module.id)
                    if cached:
                        results[module_id] = cached
                        logger.debug(f"Using cached content for {module_id}")
                        continue
            
            # Fetch from URL
            try:
                logger.info(f"Fetching {module_id} from {module.owl_url}")
                with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                    response = client.get(module.owl_url)
                    response.raise_for_status()
                    content = response.text
                
                # Cache the content
                self.write_to_cache(ontology.id, module.id, content, module.owl_url)
                results[module_id] = content
                
            except Exception as e:
                logger.error(f"Failed to fetch {module_id}: {e}")
                # Try cache as fallback
                cached = self.read_from_cache(ontology.id, module.id)
                if cached:
                    logger.info(f"Using stale cache for {module_id}")
                    results[module_id] = cached
        
        return results
    
    def combine_modules(
        self, 
        ontology: IndustryOntology,
        module_contents: Dict[str, str]
    ) -> Graph:
        """Combine all module contents into a single graph."""
        combined = Graph()
        
        # Build module lookup for format hints
        module_lookup: Dict[str, OntologyModule] = {}
        for module in ontology.modules:
            full_id = f"{ontology.id}/{module.id}"
            module_lookup[full_id] = module
        
        for module_id, content in module_contents.items():
            module = module_lookup.get(module_id)
            format_hint = module.format if module else None
            owl_url = module.owl_url if module else None
            
            try:
                g = self.parse_graph(content, format_hint=format_hint, url=owl_url)
                combined += g
                logger.debug(f"Added {len(g)} triples from {module_id}")
            except Exception as e:
                logger.error(f"Failed to parse {module_id}: {e}")
        
        logger.info(f"Combined graph has {len(combined)} triples")
        return combined
