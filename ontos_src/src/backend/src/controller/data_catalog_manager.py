"""
Data Catalog Manager

Provides functionality for the Data Dictionary feature:
- Browse columns from REGISTERED assets (Data Contracts + Datasets only)
- Search columns by name across registered assets
- Get table details with lineage

NOTE: This does NOT scan the entire Unity Catalog - it only shows
assets that are already registered in the app via Data Contracts.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from databricks.sdk import WorkspaceClient

from src.common.logging import get_logger
from src.common.config import Settings
from src.models.data_catalog import (
    ColumnDictionaryEntry,
    ColumnInfo,
    TableInfo,
    TableListItem,
    ColumnSearchRequest,
    ColumnSearchResponse,
    DataDictionaryResponse,
    TableListResponse,
    LineageGraph,
    LineageDirection,
    ImpactAnalysis,
)
from src.controller.lineage_service import LineageService
from src.controller.datasets_manager import DatasetsManager
from src.controller.data_contracts_manager import DataContractsManager

logger = get_logger(__name__)


class DataCatalogManager:
    """
    Manager for Data Dictionary / Data Catalog operations.
    
    Only indexes assets that are REGISTERED in the application:
    - Tables/views referenced in Data Contracts
    - Tables/views registered as Datasets
    
    Does NOT scan the entire Unity Catalog.
    """
    
    def __init__(
        self,
        obo_client: WorkspaceClient,
        db_session: Session,
        datasets_manager: Optional[DatasetsManager] = None,
        contracts_manager: Optional[DataContractsManager] = None,
        settings: Optional[Settings] = None
    ):
        """
        Initialize DataCatalogManager.
        
        Args:
            obo_client: OBO workspace client for user-specific UC access
            db_session: Database session for querying registered assets
            datasets_manager: Manager for Datasets (registered UC tables)
            contracts_manager: Manager for Data Contracts
            settings: Application settings
        """
        self.client = obo_client
        self.db = db_session
        self.datasets_manager = datasets_manager
        self.contracts_manager = contracts_manager
        self.settings = settings
        self.lineage_service = LineageService(obo_client)
        
        logger.debug("DataCatalogManager initialized (registered assets only)")
    
    # =========================================================================
    # Get Registered Assets from Database
    # =========================================================================
    
    def _get_columns_from_contracts(self) -> List[ColumnDictionaryEntry]:
        """
        Get all columns from Data Contracts' schema definitions.
        
        This extracts column information directly from the contract schemas,
        which define the expected structure of data regardless of whether
        the physical tables exist in Unity Catalog.
        
        Columns are deduplicated by (table_full_name, column_name) to avoid
        showing the same column multiple times.
        
        Returns:
            List of ColumnDictionaryEntry from all contracts (deduplicated)
        """
        # Use dict for deduplication: key = (table_full_name, column_name)
        columns_map: Dict[tuple, ColumnDictionaryEntry] = {}
        
        try:
            # Query contracts directly from database with eager loading
            from sqlalchemy.orm import selectinload
            from src.db_models.data_contracts import DataContractDb, SchemaObjectDb, SchemaPropertyDb
            
            if not self.db:
                logger.warning("Database session not available")
                return []
            
            # Get all contracts with schema objects, properties, and authoritative definitions eager loaded
            db_contracts = (
                self.db.query(DataContractDb)
                .options(
                    selectinload(DataContractDb.schema_objects)
                    .selectinload(SchemaObjectDb.properties)
                    .selectinload(SchemaPropertyDb.authoritative_definitions)
                )
                .all()
            )
            logger.info(f"Processing {len(db_contracts)} contracts for column extraction")
            
            for db_contract in db_contracts:
                contract_id = db_contract.id
                contract_name = db_contract.name
                contract_version = db_contract.version or "1.0"
                contract_status = db_contract.status
                
                # Get schema objects from database contract
                schema_objects = getattr(db_contract, 'schema_objects', []) or []
                
                for schema_obj in schema_objects:
                    schema_name = getattr(schema_obj, 'name', 'unknown')
                    physical_name = getattr(schema_obj, 'physical_name', None) or schema_name
                    schema_description = getattr(schema_obj, 'description', None)
                    
                    # Get columns (properties) from schema object
                    properties = getattr(schema_obj, 'properties', []) or []
                    
                    for idx, prop in enumerate(properties):
                        col_name = getattr(prop, 'name', 'unknown')
                        logical_type = getattr(prop, 'logical_type', 'unknown')
                        physical_type = getattr(prop, 'physical_type', None)
                        description = getattr(prop, 'transform_description', None) or getattr(prop, 'description', None)
                        required = getattr(prop, 'required', False)
                        pk_position = getattr(prop, 'primary_key_position', -1)
                        is_pk = pk_position is not None and pk_position >= 0
                        classification = getattr(prop, 'classification', None)
                        business_name = getattr(prop, 'business_name', None)
                        
                        # Extract business terms from authoritative definitions
                        business_terms: List[Dict[str, str]] = []
                        auth_defs = getattr(prop, 'authoritative_definitions', []) or []
                        for auth_def in auth_defs:
                            url = getattr(auth_def, 'url', None)
                            def_type = getattr(auth_def, 'type', None)
                            if url:
                                # Extract label from URL (last segment or after #)
                                label = url.split('#')[-1].split('/')[-1] if url else None
                                business_terms.append({
                                    "iri": url,
                                    "label": label or url,
                                    "type": def_type or "businessTerm"
                                })
                        
                        table_full_name = f"{contract_name}.{schema_name}"
                        dedup_key = (table_full_name, col_name)
                        
                        if dedup_key in columns_map:
                            # Merge business terms from duplicate entry
                            existing = columns_map[dedup_key]
                            existing_iris = {t.get("iri") for t in existing.business_terms}
                            for term in business_terms:
                                if term.get("iri") not in existing_iris:
                                    existing.business_terms.append(term)
                        else:
                            columns_map[dedup_key] = ColumnDictionaryEntry(
                                column_name=col_name,
                                column_label=business_name,
                                column_type=physical_type or logical_type,
                                description=description,
                                nullable=not required,
                                position=idx,
                                table_name=schema_name,
                                table_full_name=table_full_name,
                                schema_name=contract_name,  # Use contract name as "schema"
                                catalog_name=contract_version,  # Use version as "catalog"
                                table_type="CONTRACT",
                                is_primary_key=is_pk,
                                classification=classification,
                                contract_id=contract_id,
                                contract_name=contract_name,
                                contract_version=contract_version,
                                contract_status=contract_status,
                                business_terms=business_terms,
                            )
            
            columns = list(columns_map.values())
            logger.info(f"Extracted {len(columns)} unique columns from {len(db_contracts)} contracts")
            
        except Exception as e:
            logger.error(f"Error extracting columns from contracts: {e}", exc_info=True)
            return []
        
        return columns
    
    # =========================================================================
    # Column Dictionary Methods
    # =========================================================================
    
    def get_all_columns(
        self,
        catalog_filter: Optional[str] = None,
        schema_filter: Optional[str] = None,
        table_filter: Optional[str] = None,
        limit: int = 2000
    ) -> DataDictionaryResponse:
        """
        Get all columns from Data Contracts for the Data Dictionary view.
        
        Extracts column definitions from contract schemas, which define
        the expected data structure regardless of physical table existence.
        
        Args:
            catalog_filter: Optional version/catalog to filter to
            schema_filter: Optional contract name to filter to
            table_filter: Optional schema object name to filter to
            limit: Maximum columns to return
            
        Returns:
            DataDictionaryResponse with columns from Data Contracts
        """
        logger.info(f"Fetching columns from contracts (catalog={catalog_filter}, schema={schema_filter}, table={table_filter})")
        
        try:
            # Get all columns from contracts
            all_columns = self._get_columns_from_contracts()
            
            # Apply filters
            filtered_columns = []
            for col in all_columns:
                # Filter by version (catalog)
                if catalog_filter and col.catalog_name != catalog_filter:
                    continue
                # Filter by contract name (schema)
                if schema_filter and col.schema_name != schema_filter:
                    continue
                # Filter by schema object name (table)
                if table_filter and col.table_name != table_filter:
                    continue
                
                filtered_columns.append(col)
            
            # Count unique tables/schema objects
            unique_tables = set(col.table_full_name for col in filtered_columns)
            
            return DataDictionaryResponse(
                table_count=len(unique_tables),
                column_count=len(filtered_columns),
                columns=filtered_columns[:limit],
                table_filter=table_filter
            )
            
        except Exception as e:
            logger.error(f"Error fetching columns: {e}", exc_info=True)
            return DataDictionaryResponse(
                table_count=0,
                column_count=0,
                columns=[],
                table_filter=table_filter
            )
    
    def _table_to_columns(self, table_info: TableInfo) -> List[ColumnDictionaryEntry]:
        """Convert TableInfo to list of ColumnDictionaryEntry."""
        columns = []
        for idx, col in enumerate(table_info.columns):
            columns.append(ColumnDictionaryEntry(
                column_name=col.name,
                column_label=None,
                column_type=col.type_text,
                description=col.comment,
                nullable=col.nullable,
                position=idx,
                table_name=table_info.name,
                table_full_name=table_info.full_name,
                schema_name=table_info.schema_name,
                catalog_name=table_info.catalog_name,
                table_type=table_info.table_type
            ))
        return columns
    
    def search_columns(
        self,
        query: str,
        catalog_filter: Optional[str] = None,
        schema_filter: Optional[str] = None,
        table_filter: Optional[str] = None,
        limit: int = 500
    ) -> ColumnSearchResponse:
        """
        Search columns by name across Data Contracts.
        
        Args:
            query: Search query (searches column name, description, label)
            catalog_filter: Optional version filter
            schema_filter: Optional contract name filter
            table_filter: Optional schema object filter
            limit: Maximum results
            
        Returns:
            ColumnSearchResponse with matching columns from contracts
        """
        logger.info(f"Searching columns in contracts: query='{query}'")
        
        query_lower = query.lower().strip()
        
        if not query_lower:
            return ColumnSearchResponse(
                query=query,
                total_count=0,
                columns=[],
                has_more=False,
                filters_applied={}
            )
        
        # Get all columns from contracts
        all_response = self.get_all_columns(
            catalog_filter=catalog_filter,
            schema_filter=schema_filter,
            table_filter=table_filter,
            limit=5000
        )
        
        # Helper to check if query matches any business term
        def matches_business_terms(col: ColumnDictionaryEntry) -> bool:
            for term in col.business_terms:
                iri = term.get("iri", "").lower()
                label = term.get("label", "").lower()
                if query_lower in iri or query_lower in label:
                    return True
            return False
        
        # Filter by query - search in name, description, label, contract name, and business terms
        matching = [
            col for col in all_response.columns
            if query_lower in col.column_name.lower()
            or (col.description and query_lower in col.description.lower())
            or (col.column_label and query_lower in col.column_label.lower())
            or (col.contract_name and query_lower in col.contract_name.lower())
            or (col.table_name and query_lower in col.table_name.lower())
            or matches_business_terms(col)
        ]
        
        has_more = len(matching) > limit
        
        return ColumnSearchResponse(
            query=query,
            total_count=len(matching),
            columns=matching[:limit],
            has_more=has_more,
            filters_applied={
                "catalog": catalog_filter,
                "schema": schema_filter,
                "table": table_filter
            }
        )
    
    # =========================================================================
    # Table Methods
    # =========================================================================
    
    def get_table_list(
        self,
        catalog_filter: Optional[str] = None,
        schema_filter: Optional[str] = None
    ) -> TableListResponse:
        """
        Get list of schema objects from Data Contracts for the filter dropdown.
        
        Returns schema objects (tables) defined in contract schemas.
        
        Args:
            catalog_filter: Optional version filter
            schema_filter: Optional contract name filter
            
        Returns:
            TableListResponse with contract schema objects and column counts
        """
        logger.info(f"Getting schema objects from contracts (catalog={catalog_filter}, schema={schema_filter})")
        
        tables: List[TableListItem] = []
        table_columns: Dict[str, int] = {}  # full_name -> column count
        
        try:
            # Get all columns from contracts
            all_columns = self._get_columns_from_contracts()
            
            # Group columns by table
            for col in all_columns:
                # Apply filters
                if catalog_filter and col.catalog_name != catalog_filter:
                    continue
                if schema_filter and col.schema_name != schema_filter:
                    continue
                
                full_name = col.table_full_name
                table_columns[full_name] = table_columns.get(full_name, 0) + 1
            
            # Create table list items from unique tables
            seen_tables = set()
            for col in all_columns:
                full_name = col.table_full_name
                if full_name in seen_tables:
                    continue
                    
                # Apply filters
                if catalog_filter and col.catalog_name != catalog_filter:
                    continue
                if schema_filter and col.schema_name != schema_filter:
                    continue
                
                seen_tables.add(full_name)
                
                tables.append(TableListItem(
                    full_name=full_name,
                    name=col.table_name,
                    schema_name=col.schema_name,
                    catalog_name=col.catalog_name,
                    table_type="CONTRACT",
                    column_count=table_columns.get(full_name, 0),
                    comment=None,
                    contract_id=col.contract_id,
                    contract_name=col.contract_name,
                    contract_version=col.contract_version,
                    contract_status=col.contract_status,
                ))
                
        except Exception as e:
            logger.error(f"Error getting table list: {e}", exc_info=True)
        
        total_columns = sum(table_columns.values())
        
        return TableListResponse(
            tables=tables,
            total_count=len(tables),
            total_column_count=total_columns
        )
    
    def get_table_details(self, full_name: str) -> Optional[TableInfo]:
        """
        Get full table details including all columns from Unity Catalog.
        
        Args:
            full_name: Fully qualified table name (catalog.schema.table)
            
        Returns:
            TableInfo or None if not found
        """
        logger.info(f"Getting table details: {full_name}")
        
        try:
            table = self.client.tables.get(full_name=full_name)
            
            # Parse FQN
            parts = full_name.split(".")
            catalog_name = parts[0] if len(parts) >= 1 else ""
            schema_name = parts[1] if len(parts) >= 2 else ""
            table_name = parts[2] if len(parts) >= 3 else full_name
            
            # Convert columns
            columns = []
            if table.columns:
                for idx, col in enumerate(table.columns):
                    columns.append(ColumnInfo(
                        name=col.name,
                        type_text=col.type_text or str(col.type_name) if col.type_name else "UNKNOWN",
                        type_name=str(col.type_name) if col.type_name else None,
                        position=idx,
                        nullable=col.nullable if col.nullable is not None else True,
                        comment=col.comment,
                        partition_index=col.partition_index
                    ))
            
            # Get tags if available
            tags = None
            if hasattr(table, 'tags') and table.tags:
                tags = {t.key: t.value for t in table.tags}
            
            return TableInfo(
                full_name=full_name,
                name=table_name,
                schema_name=schema_name,
                catalog_name=catalog_name,
                table_type=str(table.table_type).replace("TableType.", "") if table.table_type else "TABLE",
                owner=table.owner,
                comment=table.comment,
                created_at=table.created_at if hasattr(table, 'created_at') else None,
                updated_at=table.updated_at if hasattr(table, 'updated_at') else None,
                created_by=table.created_by if hasattr(table, 'created_by') else None,
                updated_by=table.updated_by if hasattr(table, 'updated_by') else None,
                storage_location=table.storage_location if hasattr(table, 'storage_location') else None,
                data_source_format=str(table.data_source_format) if hasattr(table, 'data_source_format') and table.data_source_format else None,
                columns=columns,
                tags=tags
            )
            
        except Exception as e:
            logger.error(f"Error getting table details for {full_name}: {e}", exc_info=True)
            return None
    
    # =========================================================================
    # Lineage Methods (delegated to LineageService)
    # =========================================================================
    
    def get_table_lineage(
        self,
        table_fqn: str,
        direction: str = "both"
    ) -> LineageGraph:
        """Get lineage graph for a table."""
        dir_enum = LineageDirection(direction.lower())
        return self.lineage_service.get_table_lineage(table_fqn, dir_enum)
    
    def get_column_lineage(
        self,
        table_fqn: str,
        column_name: str,
        direction: str = "both"
    ) -> LineageGraph:
        """Get column-level lineage."""
        dir_enum = LineageDirection(direction.lower())
        return self.lineage_service.get_column_lineage(table_fqn, column_name, dir_enum)
    
    def get_impact_analysis(
        self,
        table_fqn: str,
        column_name: Optional[str] = None
    ) -> ImpactAnalysis:
        """Get impact analysis for table or column change."""
        return self.lineage_service.get_impact_analysis(table_fqn, column_name)
