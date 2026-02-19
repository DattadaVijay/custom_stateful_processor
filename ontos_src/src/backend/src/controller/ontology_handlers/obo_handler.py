"""
Handler for OBO Foundry ontologies.

OBO Foundry hosts many biomedical ontologies in a standardized format,
including Gene Ontology (GO), Disease Ontology (DOID), etc.

These ontologies are typically available from the OBO PURL service
(purl.obolibrary.org) in OWL format.
"""

from pathlib import Path
from typing import Optional, List, Dict, Tuple
import httpx

from rdflib import Graph

from src.controller.ontology_handlers.simple_owl_handler import SimpleOWLHandler
from src.models.industry_ontology import (
    IndustryOntology,
    ModuleTreeNode,
    MaturityLevel,
)
from src.common.logging import get_logger

logger = get_logger(__name__)


class OBOHandler(SimpleOWLHandler):
    """
    Handler for OBO Foundry ontologies.
    
    OBO ontologies are typically large and provided as single OWL files.
    This handler adds OBO-specific URL construction and caching behavior.
    """
    
    # OBO Foundry PURL base
    OBO_PURL_BASE = "http://purl.obolibrary.org/obo/"
    
    def fetch_modules(
        self, 
        ontology: IndustryOntology, 
        module_ids: List[str],
        force_refresh: bool = False
    ) -> Dict[str, str]:
        """
        Fetch OBO ontology files.
        
        OBO files can be large, so we use longer timeouts and 
        aggressively cache results.
        """
        results = {}
        
        for module_id in module_ids:
            # Find the module definition
            url = None
            for module in ontology.modules:
                if f"{ontology.id}/{module.id}" == module_id:
                    url = module.owl_url
                    break
            
            if not url:
                logger.warning(f"No URL for OBO module: {module_id}")
                continue
            
            # Check cache first (OBO files are large, prefer cache)
            cache_key = module_id.replace(f"{ontology.id}/", "")
            if not force_refresh and self.is_cached(ontology.id, cache_key):
                # For OBO, accept stale cache unless force_refresh
                cached = self.read_from_cache(ontology.id, cache_key)
                if cached:
                    results[module_id] = cached
                    logger.debug(f"Using cached OBO content for {module_id}")
                    continue
            
            # Fetch from URL (with extended timeout for large files)
            try:
                logger.info(f"Fetching OBO ontology from {url} (this may take a while)")
                with httpx.Client(timeout=300.0, follow_redirects=True) as client:
                    headers = {
                        "Accept": "application/rdf+xml, application/owl+xml"
                    }
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    content = response.text
                
                # Cache the content
                self.write_to_cache(ontology.id, cache_key, content, url)
                results[module_id] = content
                logger.info(f"Successfully fetched OBO {module_id} ({len(content)} bytes)")
                
            except Exception as e:
                logger.error(f"Failed to fetch OBO {module_id}: {e}")
                # Try cache as fallback
                cached = self.read_from_cache(ontology.id, cache_key)
                if cached:
                    logger.info(f"Using stale cache for OBO {module_id}")
                    results[module_id] = cached
        
        return results
    
    def combine_modules(
        self, 
        ontology: IndustryOntology,
        module_contents: Dict[str, str]
    ) -> Graph:
        """Combine OBO ontology modules."""
        combined = Graph()
        
        for module_id, content in module_contents.items():
            try:
                # OBO files are typically OWL/RDF-XML
                g = self.parse_graph(content, format_hint="xml", url=module_id)
                combined += g
                logger.debug(f"Added {len(g)} triples from OBO {module_id}")
            except Exception as e:
                logger.error(f"Failed to parse OBO {module_id}: {e}")
        
        logger.info(f"Combined OBO graph has {len(combined)} triples")
        return combined
