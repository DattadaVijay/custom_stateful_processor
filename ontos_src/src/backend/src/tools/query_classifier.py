"""
Query Classifier for Tool Filtering.

Classifies user queries to determine which tool categories are relevant,
reducing the number of tools sent to the LLM to stay within context limits.
"""

from typing import List, Set

from src.common.logging import get_logger

logger = get_logger(__name__)

# Category definitions with their trigger keywords
CATEGORY_KEYWORDS = {
    "unity_catalog": [
        "catalog", "catalogs", "schema", "schemas", "table", "tables", 
        "view", "views", "database", "databases", "sql", "query", "column",
        "columns", "unity", "uc", "explore", "browse", "list catalog",
        "my catalogs", "own catalog", "owner"
    ],
    "data_products": [
        "data product", "product", "products", "output port", "output table",
        "create product", "draft product", "publish"
    ],
    "data_contracts": [
        "contract", "contracts", "data contract", "agreement", "sla",
        "service level", "quality check", "validation"
    ],
    "organization": [
        "domain", "domains", "team", "teams", "project", "projects",
        "organization", "org", "department", "group"
    ],
    "semantic": [
        "glossary", "term", "terms", "business term", "definition",
        "concept", "semantic", "sparql", "ontology", "meaning",
        "hierarchy", "relationship", "link"
    ],
    "tags": [
        "tag", "tags", "label", "labels", "assign tag", "tagging",
        "categorize", "classify"
    ],
    "costs": [
        "cost", "costs", "price", "pricing", "spend", "spending",
        "budget", "expense", "billing", "usage cost"
    ],
    "analytics": [
        "analyze", "analysis", "aggregate", "sum", "count", "average",
        "statistics", "metrics", "measure", "calculate"
    ],
}

# Categories that are always included for general discovery
ALWAYS_INCLUDED_CATEGORIES = ["discovery"]

# Default categories when no specific match is found
DEFAULT_CATEGORIES = ["discovery", "data_products", "unity_catalog"]


def classify_query(query: str) -> List[str]:
    """
    Classify a user query to determine relevant tool categories.
    
    Args:
        query: The user's message/query
        
    Returns:
        List of relevant category names
    """
    if not query:
        return DEFAULT_CATEGORIES.copy()
    
    query_lower = query.lower()
    matched_categories: Set[str] = set(ALWAYS_INCLUDED_CATEGORIES)
    
    # Check each category's keywords
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in query_lower:
                matched_categories.add(category)
                break  # Found a match for this category, move to next
    
    # If only discovery matched (no specific category), add defaults
    if matched_categories == set(ALWAYS_INCLUDED_CATEGORIES):
        matched_categories.update(DEFAULT_CATEGORIES)
    
    result = list(matched_categories)
    logger.info(f"Query classification: '{query[:50]}...' -> categories: {result}")
    return result


def get_all_categories() -> List[str]:
    """
    Get all available tool categories.
    
    Returns:
        List of all category names
    """
    return ALWAYS_INCLUDED_CATEGORIES + list(CATEGORY_KEYWORDS.keys())

