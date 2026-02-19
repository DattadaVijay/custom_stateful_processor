"""
Pydantic models for Industry Ontology Library.

These models represent the structure of industry ontologies organized by verticals,
with support for modular ontology definitions like FIBO.
"""

from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


class MaturityLevel(str, Enum):
    """Maturity level of an ontology or module."""
    RELEASE = "release"
    PROVISIONAL = "provisional"
    INFORMATIVE = "informative"


class OntologyFormat(str, Enum):
    """Supported ontology file formats."""
    TURTLE = "turtle"
    RDFXML = "rdfxml"
    JSONLD = "jsonld"
    OWL = "owl"
    N3 = "n3"
    NTRIPLES = "ntriples"


# =============================================================================
# Ontology Module Models (leaf nodes in the tree)
# =============================================================================

class OntologyModule(BaseModel):
    """
    Represents a single ontology module (leaf node).
    
    This is the actual OWL/RDF file that can be imported.
    """
    id: str = Field(..., description="Unique identifier for the module")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Brief description")
    owl_url: Optional[str] = Field(None, description="URL to download the ontology file")
    owl_path: Optional[str] = Field(None, description="Relative path within the ontology (for FIBO-style)")
    format: Optional[OntologyFormat] = Field(None, description="File format hint")
    maturity: MaturityLevel = Field(MaturityLevel.RELEASE, description="Maturity level")
    dependencies: List[str] = Field(default_factory=list, description="IDs of dependent modules")
    
    class Config:
        use_enum_values = True


class OntologyModuleGroup(BaseModel):
    """
    A group of related modules (e.g., FIBO domain modules like Accounting, AgentsAndPeople).
    
    Can contain either direct ontologies or nested module groups.
    """
    id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Brief description")
    owl_path: Optional[str] = Field(None, description="Relative path for the module group")
    maturity: MaturityLevel = Field(MaturityLevel.RELEASE, description="Overall maturity")
    dependencies: List[str] = Field(default_factory=list, description="Dependencies on other modules")
    ontologies: List[OntologyModule] = Field(default_factory=list, description="Individual ontology files")
    
    class Config:
        use_enum_values = True


class OntologyDomain(BaseModel):
    """
    A domain within a modular ontology (e.g., FIBO's FND, BE, SEC).
    
    Contains module groups which in turn contain individual ontologies.
    """
    id: str = Field(..., description="Domain identifier (e.g., 'fnd', 'be')")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Brief description")
    maturity: MaturityLevel = Field(MaturityLevel.RELEASE, description="Overall domain maturity")
    dependencies: List[str] = Field(default_factory=list, description="Dependencies on other domains")
    modules: List[OntologyModuleGroup] = Field(default_factory=list, description="Module groups in this domain")
    
    class Config:
        use_enum_values = True


# =============================================================================
# Ontology Definition Models
# =============================================================================

class IndustryOntology(BaseModel):
    """
    Represents a complete industry ontology (e.g., FIBO, Schema.org, SNOMED CT).
    
    An ontology can be:
    - Simple: single file with modules list
    - Modular: hierarchical with domains > modules > ontologies (like FIBO)
    """
    id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Short display name")
    full_name: Optional[str] = Field(None, description="Full official name")
    description: Optional[str] = Field(None, description="Brief description")
    handler: str = Field(..., description="Handler type for fetching/parsing")
    license: Optional[str] = Field(None, description="License information")
    license_url: Optional[str] = Field(None, description="URL to license details")
    requires_license_agreement: bool = Field(False, description="Whether user must agree to license")
    website: Optional[str] = Field(None, description="Official website URL")
    base_url: Optional[str] = Field(None, description="Base URL for ontology files")
    version: Optional[str] = Field(None, description="Current version")
    recommended_foundation: bool = Field(False, description="Whether this is a recommended foundational ontology")
    
    # For simple ontologies with flat module list
    modules: List[OntologyModule] = Field(default_factory=list, description="Flat list of modules")
    
    # For modular ontologies like FIBO with hierarchical structure
    domains: List[OntologyDomain] = Field(default_factory=list, description="Hierarchical domains")
    
    class Config:
        use_enum_values = True


# =============================================================================
# Industry Vertical Models
# =============================================================================

class IndustryVertical(BaseModel):
    """
    Represents an industry vertical containing related ontologies.
    
    Examples: Healthcare, Finance, Manufacturing, etc.
    """
    id: str = Field(..., description="Unique identifier (e.g., 'healthcare', 'finance')")
    name: str = Field(..., description="Display name")
    icon: str = Field(..., description="Emoji icon for the vertical")
    description: Optional[str] = Field(None, description="Brief description")
    ontologies: List[IndustryOntology] = Field(default_factory=list, description="Ontologies in this vertical")


class IndustryOntologyRegistry(BaseModel):
    """
    The complete registry of all industry ontologies organized by vertical.
    
    This is the root model that represents the entire industry_ontologies.yaml file.
    """
    verticals: List[IndustryVertical] = Field(default_factory=list, description="All industry verticals")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Registry metadata")


# =============================================================================
# API Request/Response Models
# =============================================================================

class VerticalSummary(BaseModel):
    """Summary of a vertical for listing."""
    id: str
    name: str
    icon: str
    description: Optional[str] = None
    ontology_count: int = 0


class OntologySummary(BaseModel):
    """Summary of an ontology for listing."""
    id: str
    name: str
    full_name: Optional[str] = None
    description: Optional[str] = None
    handler: str
    license: Optional[str] = None
    requires_license_agreement: bool = False
    website: Optional[str] = None
    version: Optional[str] = None
    recommended_foundation: bool = False
    module_count: int = 0
    is_modular: bool = False  # True if has hierarchical domains


class ModuleTreeNode(BaseModel):
    """
    Represents a node in the module selection tree.
    
    Used by the frontend to render the tree with checkboxes.
    Can represent a domain, module group, or individual ontology.
    """
    id: str = Field(..., description="Unique path identifier (e.g., 'fibo/fnd/accounting')")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Brief description")
    node_type: str = Field(..., description="Type: 'domain', 'module', 'ontology'")
    maturity: Optional[MaturityLevel] = Field(None, description="Maturity level")
    owl_url: Optional[str] = Field(None, description="Download URL if leaf node")
    dependencies: List[str] = Field(default_factory=list, description="Required dependencies")
    children: List["ModuleTreeNode"] = Field(default_factory=list, description="Child nodes")
    selectable: bool = Field(True, description="Whether this node can be selected")
    
    class Config:
        use_enum_values = True


# Allow self-referencing in ModuleTreeNode
ModuleTreeNode.model_rebuild()


class ModuleSelection(BaseModel):
    """Selection of modules to import."""
    module_ids: List[str] = Field(..., description="List of selected module path IDs")


class ImportRequest(BaseModel):
    """Request to import selected ontology modules."""
    ontology_id: str = Field(..., description="ID of the ontology (e.g., 'fibo', 'schema-org')")
    vertical_id: str = Field(..., description="ID of the vertical (e.g., 'finance', 'ecommerce')")
    module_ids: List[str] = Field(..., description="List of module path IDs to import")
    include_dependencies: bool = Field(True, description="Auto-include required dependencies")
    accept_license: bool = Field(False, description="User accepts license terms if required")


class ImportResult(BaseModel):
    """Result of an ontology import operation."""
    success: bool = Field(..., description="Whether import succeeded")
    semantic_model_id: Optional[str] = Field(None, description="ID of created semantic model")
    semantic_model_name: Optional[str] = Field(None, description="Name of created semantic model")
    modules_imported: List[str] = Field(default_factory=list, description="List of imported module IDs")
    dependencies_added: List[str] = Field(default_factory=list, description="Auto-added dependencies")
    triple_count: int = Field(0, description="Number of RDF triples imported")
    warnings: List[str] = Field(default_factory=list, description="Any warnings during import")
    error: Optional[str] = Field(None, description="Error message if failed")


class CacheStatus(BaseModel):
    """Status of cached ontology data."""
    ontology_id: str
    is_cached: bool = False
    cache_date: Optional[str] = None
    cache_size_bytes: Optional[int] = None
    is_stale: bool = False  # True if cache is older than configured TTL


class RefreshResult(BaseModel):
    """Result of refreshing cached ontology data."""
    success: bool
    ontology_id: str
    modules_refreshed: int = 0
    error: Optional[str] = None
