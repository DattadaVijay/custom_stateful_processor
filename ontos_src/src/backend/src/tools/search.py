"""
Search tools for LLM.

Tools for global search across all indexed features.
"""

from typing import Any, Dict, List, Optional

from src.common.logging import get_logger
from src.tools.base import BaseTool, ToolContext, ToolResult

logger = get_logger(__name__)


class GlobalSearchTool(BaseTool):
    """Search across all indexed features (data products, contracts, glossary, etc.)."""
    
    name = "global_search"
    category = "discovery"
    description = "Search across all indexed features including data products, data contracts, glossary terms, and more. Returns results ranked by relevance."
    parameters = {
        "query": {
            "type": "string",
            "description": "Search query (e.g., 'customer analytics', 'PII data', 'sales metrics')"
        },
        "limit": {
            "type": "integer",
            "description": "Maximum number of results to return (default: 50, max: 100)"
        }
    }
    required_params = ["query"]
    required_scope = "search:read"
    
    async def execute(
        self,
        ctx: ToolContext,
        query: str,
        limit: int = 50
    ) -> ToolResult:
        """Execute global search."""
        logger.info(f"[global_search] Starting - query='{query}', limit={limit}")
        
        if not ctx.search_manager:
            logger.warning("[global_search] FAILED: search_manager is None")
            return ToolResult(
                success=False,
                error="Search not available",
                data={"results": []}
            )
        
        # Enforce limit
        limit = min(limit, 100)
        
        try:
            # Access the search index directly for MCP (no user permission filtering)
            # MCP has its own scope-based access control via tokens
            query_lower = query.lower().strip()
            if not query_lower:
                return ToolResult(
                    success=True,
                    data={"results": [], "total": 0, "query": query}
                )
            
            # Direct index search (simplified for MCP context)
            matches = []
            for item in ctx.search_manager.index:
                score = 0.0
                
                # Check title match
                if item.title and query_lower in item.title.lower():
                    score += 10.0
                    if item.title.lower() == query_lower:
                        score += 20.0  # Exact match bonus
                
                # Check description match
                if item.description and query_lower in item.description.lower():
                    score += 5.0
                
                # Check tags match
                if item.tags:
                    for tag in item.tags:
                        if query_lower in tag.lower():
                            score += 3.0
                            break
                
                if score > 0:
                    matches.append({
                        "id": item.id,
                        "type": item.type,
                        "title": item.title,
                        "description": item.description[:200] if item.description else None,
                        "link": item.link,
                        "tags": item.tags[:5] if item.tags else [],
                        "feature_id": item.feature_id,
                        "relevance_score": score
                    })
            
            # Sort by relevance score
            matches.sort(key=lambda x: x["relevance_score"], reverse=True)
            results = matches[:limit]
            
            logger.info(f"[global_search] SUCCESS: Found {len(results)} results (total matches: {len(matches)})")
            return ToolResult(
                success=True,
                data={
                    "results": results,
                    "total": len(matches),
                    "returned": len(results),
                    "query": query
                }
            )
            
        except Exception as e:
            logger.error(f"[global_search] FAILED: {type(e).__name__}: {e}", exc_info=True)
            return ToolResult(
                success=False,
                error=f"{type(e).__name__}: {str(e)}",
                data={"results": []}
            )

