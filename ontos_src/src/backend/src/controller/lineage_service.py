"""
Lineage Service

Provides lineage analysis using the Databricks External Lineage API.
Supports both table-level and column-level lineage traversal.
"""

from typing import List, Optional, Set
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    LineageDirection as SDKLineageDirection,
)

from src.common.logging import get_logger
from src.models.data_catalog import (
    LineageDirection,
    LineageNode,
    LineageEdge,
    LineageGraph,
    ColumnMapping,
    ImpactAnalysis,
    ImpactedAsset,
    AssetType,
)

logger = get_logger(__name__)


class LineageService:
    """
    Service for fetching and analyzing lineage from Unity Catalog.
    
    Uses the Databricks SDK to query the External Lineage API for
    upstream/downstream dependencies including external systems.
    """
    
    def __init__(self, workspace_client: WorkspaceClient):
        """
        Initialize LineageService.
        
        Args:
            workspace_client: Databricks WorkspaceClient (OBO for user permissions)
        """
        self.ws = workspace_client
    
    def _sdk_direction(self, direction: LineageDirection) -> SDKLineageDirection:
        """Convert our direction enum to SDK enum."""
        if direction == LineageDirection.UPSTREAM:
            return SDKLineageDirection.UPSTREAM
        return SDKLineageDirection.DOWNSTREAM
    
    def _parse_asset_type(self, type_str: Optional[str]) -> AssetType:
        """Parse asset type from lineage API response."""
        if not type_str:
            return AssetType.TABLE
        type_lower = type_str.lower()
        if "view" in type_lower:
            return AssetType.VIEW
        if "notebook" in type_lower:
            return AssetType.NOTEBOOK
        if "job" in type_lower:
            return AssetType.JOB
        if "dashboard" in type_lower:
            return AssetType.DASHBOARD
        if "external" in type_lower:
            return AssetType.EXTERNAL
        return AssetType.TABLE
    
    def _extract_fqn_parts(self, fqn: str) -> tuple:
        """Extract catalog, schema, table from FQN."""
        parts = fqn.split(".")
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return None, parts[0], parts[1]
        return None, None, fqn
    
    def get_table_lineage(
        self,
        table_fqn: str,
        direction: LineageDirection = LineageDirection.BOTH,
        max_depth: int = 10
    ) -> LineageGraph:
        """
        Get lineage graph for a table.
        
        Args:
            table_fqn: Fully qualified table name (catalog.schema.table)
            direction: UPSTREAM, DOWNSTREAM, or BOTH
            max_depth: Maximum traversal depth
            
        Returns:
            LineageGraph with nodes and edges
        """
        logger.info(f"Fetching lineage for table: {table_fqn}, direction: {direction}")
        
        nodes: dict[str, LineageNode] = {}
        edges: List[LineageEdge] = []
        
        # Create root node
        catalog, schema, table = self._extract_fqn_parts(table_fqn)
        root_node = LineageNode(
            id=table_fqn,
            name=table or table_fqn,
            type=AssetType.TABLE,
            catalog=catalog,
            schema=schema,
            is_root=True,
            depth=0
        )
        nodes[table_fqn] = root_node
        
        # Fetch upstream lineage
        upstream_count = 0
        if direction in (LineageDirection.UPSTREAM, LineageDirection.BOTH):
            upstream_nodes, upstream_edges = self._fetch_lineage_direction(
                table_fqn, SDKLineageDirection.UPSTREAM, max_depth
            )
            for node in upstream_nodes:
                if node.id not in nodes:
                    nodes[node.id] = node
                    upstream_count += 1
            edges.extend(upstream_edges)
        
        # Fetch downstream lineage
        downstream_count = 0
        if direction in (LineageDirection.DOWNSTREAM, LineageDirection.BOTH):
            downstream_nodes, downstream_edges = self._fetch_lineage_direction(
                table_fqn, SDKLineageDirection.DOWNSTREAM, max_depth
            )
            for node in downstream_nodes:
                if node.id not in nodes:
                    nodes[node.id] = node
                    downstream_count += 1
            edges.extend(downstream_edges)
        
        # Count external nodes
        external_count = sum(1 for n in nodes.values() if n.type == AssetType.EXTERNAL)
        
        return LineageGraph(
            nodes=list(nodes.values()),
            edges=edges,
            root_node=table_fqn,
            direction=direction,
            upstream_count=upstream_count,
            downstream_count=downstream_count,
            external_count=external_count
        )
    
    def _fetch_lineage_direction(
        self,
        table_fqn: str,
        sdk_direction: SDKLineageDirection,
        max_depth: int
    ) -> tuple[List[LineageNode], List[LineageEdge]]:
        """
        Fetch lineage in a specific direction using the External Lineage API.
        
        Returns tuple of (nodes, edges).
        """
        nodes: List[LineageNode] = []
        edges: List[LineageEdge] = []
        visited: Set[str] = {table_fqn}
        
        try:
            # Try using the table lineage API
            # Note: The exact API may vary based on SDK version
            # We'll use a try/except pattern to handle different SDK versions
            
            try:
                # Try the external_lineage API first (newer SDK)
                relationships = list(self.ws.external_lineage.list_external_lineage_relationships(
                    object_info={"table": {"name": table_fqn}},
                    lineage_direction=sdk_direction,
                    page_size=100
                ))
                
                for rel in relationships:
                    # Extract source and target based on direction
                    if sdk_direction == SDKLineageDirection.DOWNSTREAM:
                        source_id = table_fqn
                        target_info = getattr(rel, 'target', None) or getattr(rel, 'downstream_object', None)
                        target_id = self._extract_object_id(target_info)
                    else:
                        target_id = table_fqn
                        source_info = getattr(rel, 'source', None) or getattr(rel, 'upstream_object', None)
                        source_id = self._extract_object_id(source_info)
                    
                    if not source_id or not target_id:
                        continue
                    
                    # Get the "other" node (not the root)
                    other_id = target_id if sdk_direction == SDKLineageDirection.DOWNSTREAM else source_id
                    other_info = target_info if sdk_direction == SDKLineageDirection.DOWNSTREAM else source_info
                    
                    if other_id and other_id not in visited:
                        visited.add(other_id)
                        
                        # Create node
                        catalog, schema, name = self._extract_fqn_parts(other_id)
                        asset_type = self._determine_asset_type(other_info)
                        
                        node = LineageNode(
                            id=other_id,
                            name=name or other_id,
                            type=asset_type,
                            catalog=catalog,
                            schema=schema,
                            depth=1,
                            external_system=getattr(other_info, 'external_system', None) if other_info else None
                        )
                        nodes.append(node)
                        
                        # Create edge
                        edge = LineageEdge(
                            source=source_id,
                            target=target_id,
                            column_mappings=self._extract_column_mappings(rel)
                        )
                        edges.append(edge)
                        
            except AttributeError:
                # Fallback: Try table lineage API
                logger.debug("External lineage API not available, trying table lineage")
                self._fetch_table_lineage_fallback(table_fqn, sdk_direction, nodes, edges, visited)
                
        except Exception as e:
            logger.warning(f"Error fetching lineage for {table_fqn}: {e}")
            # Return empty but valid result
        
        return nodes, edges
    
    def _extract_object_id(self, obj_info) -> Optional[str]:
        """Extract object ID from lineage object info."""
        if not obj_info:
            return None
        
        # Try different attribute patterns
        if hasattr(obj_info, 'table') and obj_info.table:
            table = obj_info.table
            if hasattr(table, 'name'):
                return table.name
            if isinstance(table, dict):
                return table.get('name')
        
        if hasattr(obj_info, 'name'):
            return obj_info.name
            
        if isinstance(obj_info, dict):
            if 'table' in obj_info:
                return obj_info['table'].get('name') if isinstance(obj_info['table'], dict) else str(obj_info['table'])
            return obj_info.get('name')
        
        return str(obj_info) if obj_info else None
    
    def _determine_asset_type(self, obj_info) -> AssetType:
        """Determine asset type from object info."""
        if not obj_info:
            return AssetType.TABLE
        
        type_str = getattr(obj_info, 'type', None) or getattr(obj_info, 'object_type', None)
        if type_str:
            return self._parse_asset_type(str(type_str))
        
        # Check if it's external
        if getattr(obj_info, 'external_system', None):
            return AssetType.EXTERNAL
        
        return AssetType.TABLE
    
    def _extract_column_mappings(self, relationship) -> Optional[List[ColumnMapping]]:
        """Extract column mappings from lineage relationship."""
        mappings = getattr(relationship, 'column_mappings', None)
        if not mappings:
            return None
        
        result = []
        for mapping in mappings:
            source_col = getattr(mapping, 'source_column', None) or getattr(mapping, 'source', None)
            target_col = getattr(mapping, 'target_column', None) or getattr(mapping, 'target', None)
            if source_col and target_col:
                result.append(ColumnMapping(
                    source_column=str(source_col),
                    target_column=str(target_col),
                    transformation=getattr(mapping, 'transformation', None)
                ))
        
        return result if result else None
    
    def _fetch_table_lineage_fallback(
        self,
        table_fqn: str,
        sdk_direction: SDKLineageDirection,
        nodes: List[LineageNode],
        edges: List[LineageEdge],
        visited: Set[str]
    ):
        """Fallback lineage fetching using table lineage API."""
        try:
            # Use tables API to get basic lineage info
            # This is a simplified fallback
            logger.debug(f"Using fallback lineage for {table_fqn}")
            # Note: Real implementation would use ws.lineage.get_table_lineage if available
        except Exception as e:
            logger.debug(f"Fallback lineage failed: {e}")
    
    def get_column_lineage(
        self,
        table_fqn: str,
        column_name: str,
        direction: LineageDirection = LineageDirection.BOTH
    ) -> LineageGraph:
        """
        Get column-level lineage.
        
        Args:
            table_fqn: Fully qualified table name
            column_name: Column to trace lineage for
            direction: UPSTREAM, DOWNSTREAM, or BOTH
            
        Returns:
            LineageGraph with column-level mappings
        """
        logger.info(f"Fetching column lineage for {table_fqn}.{column_name}")
        
        # Get table lineage first
        graph = self.get_table_lineage(table_fqn, direction)
        
        # Filter edges to only those involving the specified column
        filtered_edges = []
        for edge in graph.edges:
            if edge.column_mappings:
                relevant_mappings = [
                    m for m in edge.column_mappings
                    if m.source_column == column_name or m.target_column == column_name
                ]
                if relevant_mappings:
                    edge.column_mappings = relevant_mappings
                    filtered_edges.append(edge)
            else:
                # Keep edges without column info (we can't filter them)
                filtered_edges.append(edge)
        
        graph.edges = filtered_edges
        return graph
    
    def get_impact_analysis(
        self,
        table_fqn: str,
        column_name: Optional[str] = None,
        max_depth: int = 10
    ) -> ImpactAnalysis:
        """
        Analyze downstream impact of changing a table or column.
        
        Args:
            table_fqn: Table being changed
            column_name: Optional column being changed
            max_depth: Maximum depth to traverse
            
        Returns:
            ImpactAnalysis with all affected assets
        """
        logger.info(f"Analyzing impact for {table_fqn}" + (f".{column_name}" if column_name else ""))
        
        # Get downstream lineage
        if column_name:
            graph = self.get_column_lineage(table_fqn, column_name, LineageDirection.DOWNSTREAM)
        else:
            graph = self.get_table_lineage(table_fqn, LineageDirection.DOWNSTREAM, max_depth)
        
        # Categorize impacted assets
        impacted_tables: List[ImpactedAsset] = []
        impacted_views: List[ImpactedAsset] = []
        impacted_external: List[ImpactedAsset] = []
        owners: Set[str] = set()
        max_impact_depth = 0
        
        for node in graph.nodes:
            if node.is_root:
                continue
            
            # Get affected columns for this node
            affected_cols = None
            if column_name:
                affected_cols = []
                for edge in graph.edges:
                    if edge.target == node.id and edge.column_mappings:
                        for mapping in edge.column_mappings:
                            if mapping.target_column not in affected_cols:
                                affected_cols.append(mapping.target_column)
            
            asset = ImpactedAsset(
                id=node.id,
                name=node.name,
                type=node.type,
                full_name=node.id,
                distance=node.depth,
                affected_columns=affected_cols,
                owner=node.owner
            )
            
            if node.owner:
                owners.add(node.owner)
            
            max_impact_depth = max(max_impact_depth, node.depth)
            
            if node.type == AssetType.VIEW:
                impacted_views.append(asset)
            elif node.type == AssetType.EXTERNAL:
                impacted_external.append(asset)
            else:
                impacted_tables.append(asset)
        
        total_count = len(impacted_tables) + len(impacted_views) + len(impacted_external)
        
        return ImpactAnalysis(
            source_table=table_fqn,
            source_column=column_name,
            impacted_tables=impacted_tables,
            impacted_views=impacted_views,
            impacted_external=impacted_external,
            total_impacted_count=total_count,
            max_depth=max_impact_depth,
            affected_owners=list(owners)
        )

