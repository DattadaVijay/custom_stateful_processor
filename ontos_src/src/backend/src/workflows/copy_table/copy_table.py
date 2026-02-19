from databricks.sdk import WorkspaceClient


def copy_table(
    source_table: str,
    target_table: str,
    product_name: str = "ontos",
    product_version: str = "0.0.0"
) -> None:
    """Copy a table from source to target.
    
    Args:
        source_table: Fully qualified name of source table
        target_table: Fully qualified name of target table
        product_name: Product name for telemetry (default: "ontos")
        product_version: Product version for telemetry (default: "0.0.0")
    """
    client = WorkspaceClient(product=product_name, product_version=product_version)

    # Get source table info
    source_table_info = client.tables.get(source_table)

    # Create target table with same schema
    client.tables.create(
        name=target_table,
        catalog_name=source_table_info.catalog_name,
        schema_name=source_table_info.schema_name,
        table_type=source_table_info.table_type,
        data_schema=source_table_info.data_schema,
        storage_location=source_table_info.storage_location
    )

    # Copy data
    client.sql.execute(f"INSERT INTO {target_table} SELECT * FROM {source_table}")
