"""
Handler for GS1 Web Vocabulary.

GS1 provides vocabularies for products (GTIN), locations (GLN), 
supply chain events (EPCIS), and related concepts.
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


class GS1Handler(SimpleOWLHandler):
    """
    Handler for GS1 Web Vocabulary.
    
    GS1 vocabularies are typically provided as Turtle files.
    """
    
    # GS1 vocabulary URLs
    GS1_CORE_URL = "https://www.gs1.org/voc/gs1.ttl"
    GS1_EPCIS_URL = "https://www.gs1.org/voc/epcis.ttl"
    
    def fetch_modules(
        self, 
        ontology: IndustryOntology, 
        module_ids: List[str],
        force_refresh: bool = False
    ) -> Dict[str, str]:
        """
        Fetch GS1 vocabulary files.
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
                # Default URLs based on module name
                if "epcis" in module_id.lower():
                    url = self.GS1_EPCIS_URL
                else:
                    url = self.GS1_CORE_URL
            
            # Check cache first
            cache_key = module_id.replace(f"{ontology.id}/", "")
            if not force_refresh and self.is_cached(ontology.id, cache_key):
                if not self.is_cache_stale(ontology.id, cache_key):
                    cached = self.read_from_cache(ontology.id, cache_key)
                    if cached:
                        results[module_id] = cached
                        logger.debug(f"Using cached GS1 content for {module_id}")
                        continue
            
            # Fetch from URL
            try:
                logger.info(f"Fetching GS1 from {url}")
                with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                    headers = {
                        "Accept": "text/turtle, application/rdf+xml"
                    }
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    content = response.text
                
                # Cache the content
                self.write_to_cache(ontology.id, cache_key, content, url)
                results[module_id] = content
                
            except Exception as e:
                logger.error(f"Failed to fetch GS1 {module_id}: {e}")
                # Try cache as fallback
                cached = self.read_from_cache(ontology.id, cache_key)
                if cached:
                    logger.info(f"Using stale cache for GS1 {module_id}")
                    results[module_id] = cached
        
        return results
    
    def combine_modules(
        self, 
        ontology: IndustryOntology,
        module_contents: Dict[str, str]
    ) -> Graph:
        """Combine GS1 vocabulary modules."""
        combined = Graph()
        
        for module_id, content in module_contents.items():
            try:
                # GS1 provides Turtle format
                g = self.parse_graph(content, format_hint="turtle", url=module_id)
                combined += g
                logger.debug(f"Added {len(g)} triples from GS1 {module_id}")
            except Exception as e:
                logger.error(f"Failed to parse GS1 {module_id}: {e}")
        
        logger.info(f"Combined GS1 graph has {len(combined)} triples")
        return combined
