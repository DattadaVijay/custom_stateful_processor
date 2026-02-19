"""
Ontology Handlers Package.

This package contains handlers for different types of industry ontologies.
Each handler knows how to fetch, parse, and combine modules for its ontology type.
"""

from src.controller.ontology_handlers.base_handler import OntologyHandler
from src.controller.ontology_handlers.simple_owl_handler import SimpleOWLHandler
from src.controller.ontology_handlers.fibo_handler import FIBOHandler
from src.controller.ontology_handlers.schema_org_handler import SchemaOrgHandler
from src.controller.ontology_handlers.gs1_handler import GS1Handler
from src.controller.ontology_handlers.obo_handler import OBOHandler

# Handler registry - maps handler type names to handler classes
HANDLER_REGISTRY: dict[str, type[OntologyHandler]] = {
    "simple_owl": SimpleOWLHandler,
    "fibo": FIBOHandler,
    "schema_org": SchemaOrgHandler,
    "gs1": GS1Handler,
    "obo": OBOHandler,
    # Additional handlers can use simple_owl as fallback
    "snomed": SimpleOWLHandler,
    "loinc": SimpleOWLHandler,
    "rxnorm": SimpleOWLHandler,
    "iso20022": SimpleOWLHandler,
}


def get_handler(handler_type: str) -> type[OntologyHandler]:
    """
    Get the handler class for a given handler type.
    
    Args:
        handler_type: The handler type name from the ontology definition
        
    Returns:
        The handler class
        
    Raises:
        ValueError: If handler type is not registered
    """
    if handler_type not in HANDLER_REGISTRY:
        # Fall back to simple_owl for unknown handlers
        return SimpleOWLHandler
    return HANDLER_REGISTRY[handler_type]


__all__ = [
    "OntologyHandler",
    "SimpleOWLHandler", 
    "FIBOHandler",
    "SchemaOrgHandler",
    "GS1Handler",
    "OBOHandler",
    "HANDLER_REGISTRY",
    "get_handler",
]
