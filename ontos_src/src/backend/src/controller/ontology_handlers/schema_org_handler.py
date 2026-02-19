"""
Handler for Schema.org vocabulary.

Schema.org provides a single comprehensive vocabulary file that can be
downloaded in various formats. The handler supports selecting the core
vocabulary or the full vocabulary with pending extensions.
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


class SchemaOrgHandler(SimpleOWLHandler):
    """
    Handler for Schema.org vocabulary.
    
    Extends SimpleOWLHandler with Schema.org-specific logic for handling
    the vocabulary's structure and extensions.
    """
    
    # Schema.org download URLs
    SCHEMA_CORE_URL = "https://schema.org/version/latest/schemaorg-current-https.ttl"
    SCHEMA_ALL_URL = "https://schema.org/version/latest/schemaorg-all-https.ttl"
    
    def fetch_modules(
        self, 
        ontology: IndustryOntology, 
        module_ids: List[str],
        force_refresh: bool = False
    ) -> Dict[str, str]:
        """
        Fetch Schema.org vocabulary.
        
        Uses specific URLs for Schema.org and handles format appropriately.
        """
        results = {}
        
        for module_id in module_ids:
            # Determine the correct URL
            if "pending" in module_id.lower() or "all" in module_id.lower():
                url = self.SCHEMA_ALL_URL
            else:
                url = self.SCHEMA_CORE_URL
            
            # Find the module to get its actual URL if specified
            for module in ontology.modules:
                if f"{ontology.id}/{module.id}" == module_id:
                    if module.owl_url:
                        url = module.owl_url
                    break
            
            # Check cache first
            cache_key = module_id.replace(f"{ontology.id}/", "")
            if not force_refresh and self.is_cached(ontology.id, cache_key):
                if not self.is_cache_stale(ontology.id, cache_key):
                    cached = self.read_from_cache(ontology.id, cache_key)
                    if cached:
                        results[module_id] = cached
                        logger.debug(f"Using cached Schema.org content for {module_id}")
                        continue
            
            # Fetch from URL
            try:
                logger.info(f"Fetching Schema.org from {url}")
                with httpx.Client(timeout=120.0, follow_redirects=True) as client:
                    headers = {
                        "Accept": "text/turtle, application/n-triples, application/rdf+xml"
                    }
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    content = response.text
                
                # Cache the content
                self.write_to_cache(ontology.id, cache_key, content, url)
                results[module_id] = content
                
            except Exception as e:
                logger.error(f"Failed to fetch Schema.org {module_id}: {e}")
                # Try cache as fallback
                cached = self.read_from_cache(ontology.id, cache_key)
                if cached:
                    logger.info(f"Using stale cache for Schema.org {module_id}")
                    results[module_id] = cached
        
        return results
    
    def combine_modules(
        self, 
        ontology: IndustryOntology,
        module_contents: Dict[str, str]
    ) -> Graph:
        """Combine Schema.org modules."""
        combined = Graph()
        
        for module_id, content in module_contents.items():
            try:
                # Schema.org typically provides Turtle format
                g = self.parse_graph(content, format_hint="turtle", url=module_id)
                combined += g
                logger.debug(f"Added {len(g)} triples from Schema.org {module_id}")
            except Exception as e:
                logger.error(f"Failed to parse Schema.org {module_id}: {e}")
        
        logger.info(f"Combined Schema.org graph has {len(combined)} triples")
        return combined
