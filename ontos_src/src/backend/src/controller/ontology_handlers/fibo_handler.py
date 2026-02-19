"""
Handler for FIBO (Financial Industry Business Ontology).

FIBO is organized hierarchically:
- Domains (FND, BE, FBC, SEC, DER, LOAN, IND)
- Module Groups within domains (e.g., FND/Accounting, FND/AgentsAndPeople)
- Individual ontology files within modules

This handler knows how to:
1. Build the hierarchical tree for the UI
2. Resolve domain-level dependencies (e.g., SEC depends on FND, BE, FBC)
3. Fetch and combine selected modules
"""

from pathlib import Path
from typing import Optional, List, Dict, Tuple, Set
import httpx

from rdflib import Graph

from src.controller.ontology_handlers.base_handler import OntologyHandler
from src.models.industry_ontology import (
    IndustryOntology,
    OntologyDomain,
    OntologyModuleGroup,
    OntologyModule,
    ModuleTreeNode,
    MaturityLevel,
)
from src.common.logging import get_logger

logger = get_logger(__name__)


class FIBOHandler(OntologyHandler):
    """
    Handler for FIBO and similar hierarchical domain-based ontologies.
    
    Expects ontologies with a `domains` list containing hierarchical structure:
    domains -> modules -> ontologies
    """
    
    def get_module_tree(self, ontology: IndustryOntology) -> List[ModuleTreeNode]:
        """Build the hierarchical tree for the UI."""
        nodes = []
        
        for domain in ontology.domains:
            domain_node = self._build_domain_node(ontology.id, domain)
            nodes.append(domain_node)
        
        return nodes
    
    def _build_domain_node(self, ontology_id: str, domain: OntologyDomain) -> ModuleTreeNode:
        """Build a tree node for a domain."""
        domain_path = f"{ontology_id}/{domain.id}"
        
        # Build child module nodes
        children = []
        for module_group in domain.modules:
            module_node = self._build_module_node(domain_path, module_group)
            children.append(module_node)
        
        return self.create_module_node(
            id=domain_path,
            name=domain.name,
            node_type="domain",
            description=domain.description,
            maturity=domain.maturity if isinstance(domain.maturity, MaturityLevel) 
                     else MaturityLevel(domain.maturity) if domain.maturity else MaturityLevel.RELEASE,
            dependencies=[f"{ontology_id}/{dep}" for dep in domain.dependencies],
            children=children,
            selectable=True,  # Selecting a domain selects all its modules
        )
    
    def _build_module_node(self, parent_path: str, module_group: OntologyModuleGroup) -> ModuleTreeNode:
        """Build a tree node for a module group."""
        module_path = f"{parent_path}/{module_group.id}"
        
        # Build child ontology nodes if present
        children = []
        for onto in module_group.ontologies:
            onto_node = self.create_module_node(
                id=f"{module_path}/{onto.id}",
                name=onto.name,
                node_type="ontology",
                description=onto.description,
                maturity=onto.maturity if isinstance(onto.maturity, MaturityLevel) 
                         else MaturityLevel(onto.maturity) if onto.maturity else MaturityLevel.RELEASE,
                owl_url=onto.owl_url,
                dependencies=[f"{module_path}/{dep}" for dep in onto.dependencies] if onto.dependencies else [],
                selectable=True,
            )
            children.append(onto_node)
        
        return self.create_module_node(
            id=module_path,
            name=module_group.name,
            node_type="module",
            description=module_group.description,
            maturity=module_group.maturity if isinstance(module_group.maturity, MaturityLevel) 
                     else MaturityLevel(module_group.maturity) if module_group.maturity else MaturityLevel.RELEASE,
            dependencies=[f"{parent_path}/{dep}" for dep in module_group.dependencies] if module_group.dependencies else [],
            children=children,
            selectable=True,
        )
    
    def resolve_dependencies(
        self, 
        ontology: IndustryOntology, 
        selected_ids: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Resolve all dependencies for selected modules.
        
        FIBO has domain-level dependencies (e.g., SEC depends on FND, BE, FBC).
        When a domain is selected, all its modules are included.
        """
        # Build maps for lookups
        domain_deps: Dict[str, List[str]] = {}
        module_deps: Dict[str, List[str]] = {}
        domain_modules: Dict[str, List[str]] = {}
        
        for domain in ontology.domains:
            domain_path = f"{ontology.id}/{domain.id}"
            domain_deps[domain_path] = [f"{ontology.id}/{dep}" for dep in domain.dependencies]
            
            modules_in_domain = []
            for module_group in domain.modules:
                module_path = f"{domain_path}/{module_group.id}"
                modules_in_domain.append(module_path)
                module_deps[module_path] = [f"{domain_path}/{dep}" for dep in (module_group.dependencies or [])]
                
                # Add individual ontology paths
                for onto in module_group.ontologies:
                    onto_path = f"{module_path}/{onto.id}"
                    modules_in_domain.append(onto_path)
                    if onto.dependencies:
                        module_deps[onto_path] = [f"{module_path}/{dep}" for dep in onto.dependencies]
            
            domain_modules[domain_path] = modules_in_domain
        
        all_required: Set[str] = set()
        auto_added: Set[str] = set()
        
        def expand_selection(path: str):
            """Expand a selection to include all children if it's a domain/module."""
            result = {path}
            
            # If it's a domain, add all its modules
            if path in domain_modules:
                for mod in domain_modules[path]:
                    result.add(mod)
            
            return result
        
        def add_deps(path: str, is_auto: bool = False):
            """Recursively add dependencies."""
            # Check domain-level deps
            domain_path = "/".join(path.split("/")[:2])
            for dep in domain_deps.get(domain_path, []):
                if dep not in all_required:
                    all_required.add(dep)
                    auto_added.add(dep)
                    # Expand domain to all its modules
                    for mod in domain_modules.get(dep, []):
                        if mod not in all_required:
                            all_required.add(mod)
                            auto_added.add(mod)
                    add_deps(dep, is_auto=True)
            
            # Check module-level deps
            for dep in module_deps.get(path, []):
                if dep not in all_required:
                    all_required.add(dep)
                    auto_added.add(dep)
                    add_deps(dep, is_auto=True)
        
        # Process all selected items
        for selected in selected_ids:
            expanded = expand_selection(selected)
            for item in expanded:
                if item not in all_required:
                    all_required.add(item)
                    if item != selected:
                        # Children of selected domain/module are auto-added
                        pass  # Don't mark as auto_added since user selected parent
        
        # Now resolve dependencies for all items
        for item in list(all_required):
            add_deps(item)
        
        return list(all_required), list(auto_added)
    
    def fetch_modules(
        self, 
        ontology: IndustryOntology, 
        module_ids: List[str],
        force_refresh: bool = False
    ) -> Dict[str, str]:
        """Fetch module content from FIBO's URLs or cache."""
        results = {}
        
        # Build lookup map for all modules
        module_lookup: Dict[str, OntologyModule] = {}
        module_url_lookup: Dict[str, str] = {}
        
        base_url = ontology.base_url or "https://spec.edmcouncil.org/fibo/ontology"
        
        for domain in ontology.domains:
            domain_path = f"{ontology.id}/{domain.id}"
            
            for module_group in domain.modules:
                module_path = f"{domain_path}/{module_group.id}"
                
                # Module group might have an owl_path we can construct URL from
                if module_group.owl_path:
                    # This is a path like "FND/Accounting/"
                    # The AllFND.rdf style aggregates are at domain level
                    pass
                
                for onto in module_group.ontologies:
                    onto_path = f"{module_path}/{onto.id}"
                    module_lookup[onto_path] = onto
                    
                    if onto.owl_url:
                        module_url_lookup[onto_path] = onto.owl_url
                    elif module_group.owl_path:
                        # Construct URL from base + path
                        constructed_url = f"{base_url}/{module_group.owl_path}{onto.name}/"
                        module_url_lookup[onto_path] = constructed_url
        
        for module_id in module_ids:
            # Skip non-leaf nodes (domains and module groups)
            if module_id not in module_lookup:
                logger.debug(f"Skipping non-leaf node: {module_id}")
                continue
            
            module = module_lookup[module_id]
            url = module_url_lookup.get(module_id)
            
            if not url:
                logger.warning(f"No URL for module: {module_id}")
                continue
            
            # Check cache first
            cache_key = module_id.replace(f"{ontology.id}/", "")
            if not force_refresh and self.is_cached(ontology.id, cache_key):
                if not self.is_cache_stale(ontology.id, cache_key):
                    cached = self.read_from_cache(ontology.id, cache_key)
                    if cached:
                        results[module_id] = cached
                        logger.debug(f"Using cached content for {module_id}")
                        continue
            
            # Fetch from URL
            try:
                logger.info(f"Fetching {module_id} from {url}")
                with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                    # FIBO URLs often return RDF/XML
                    headers = {
                        "Accept": "application/rdf+xml, text/turtle, application/ld+json"
                    }
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    content = response.text
                
                # Cache the content
                self.write_to_cache(ontology.id, cache_key, content, url)
                results[module_id] = content
                
            except Exception as e:
                logger.error(f"Failed to fetch {module_id}: {e}")
                # Try cache as fallback
                cached = self.read_from_cache(ontology.id, cache_key)
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
        
        # FIBO typically uses RDF/XML format
        for module_id, content in module_contents.items():
            try:
                g = self.parse_graph(content, format_hint=None, url=module_id)
                combined += g
                logger.debug(f"Added {len(g)} triples from {module_id}")
            except Exception as e:
                logger.error(f"Failed to parse {module_id}: {e}")
        
        logger.info(f"Combined FIBO graph has {len(combined)} triples")
        return combined
