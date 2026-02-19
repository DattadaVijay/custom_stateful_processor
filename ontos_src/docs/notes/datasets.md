# Datasets

## Overview

Datasets are **physical implementations of Data Contracts**. They represent a logical grouping of physical data assets (tables, views) that exist across different systems and SDLC environments (development, staging, production, etc.).

The relationship model is:

```
Data Product → Data Contract ← Dataset
     (DP)           (DC)         (DS)
                      ↑
              DatasetInstance
              (physical impl)
```

- A **Data Product** references **Data Contracts** through its output ports
- A **Dataset** is a logical entity that can have multiple **physical instances**
- Each **DatasetInstance** links to a specific contract version and server (system + environment)
- Multiple instances can exist for the same dataset (e.g., dev vs prod, or different systems like Unity Catalog and Snowflake)

## Key Concepts

### Physical Asset Reference

Each Dataset points to a specific Unity Catalog asset:

- **Catalog**: The Unity Catalog catalog name (e.g., `lob_gtm_prod`)
- **Schema**: The database schema name (e.g., `sales_data`)
- **Object Name**: The table or view name (e.g., `transactions`)
- **Asset Type**: Either `table` or `view`

The full path is: `catalog.schema.object_name`

### SDLC Environments

Datasets support tracking across different environments:

| Environment | Description |
|-------------|-------------|
| `dev` | Development environment |
| `staging` | Staging/pre-production environment |
| `prod` | Production environment |
| `test` | Test environment |
| `qa` | Quality Assurance environment |
| `uat` | User Acceptance Testing environment |

A common pattern is to have the same logical table represented as separate Dataset records for each environment:

- `lob_gtm_dev.sales.transactions` (dev)
- `lob_gtm_staging.sales.transactions` (staging)
- `lob_gtm_prod.sales.transactions` (prod)

All three would reference the same Data Contract.

### Lifecycle Status

Datasets follow a lifecycle similar to other entities:

| Status | Description |
|--------|-------------|
| `draft` | Initial state, not yet ready for use |
| `active` | Dataset is in use and maintained |
| `deprecated` | Dataset is scheduled for retirement |
| `retired` | Dataset is no longer maintained |

### Subscriptions

Users can subscribe to Datasets to receive notifications about:

- Status changes (deprecation, retirement)
- New versions
- Compliance violations
- Contract changes

## Data Model

### Core Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | UUID | Unique identifier |
| `name` | String | Human-readable name |
| `description` | Text | Optional description |
| `asset_type` | Enum | `table` or `view` |
| `catalog_name` | String | Unity Catalog catalog |
| `schema_name` | String | Schema/database name |
| `object_name` | String | Table/view name |
| `environment` | Enum | SDLC environment |
| `status` | Enum | Lifecycle status |
| `version` | String | Optional version string |
| `published` | Boolean | Marketplace publication status |

### Relationships

| Relationship | Type | Description |
|--------------|------|-------------|
| `contract` | Many-to-One | Data Contract this dataset implements (legacy) |
| `owner_team` | Many-to-One | Team that owns this dataset |
| `project` | Many-to-One | Project this dataset belongs to |
| `subscriptions` | One-to-Many | User subscriptions |
| `tags` | One-to-Many | Simple string tags |
| `custom_properties` | One-to-Many | Key-value custom properties |
| `instances` | One-to-Many | Physical implementations (see below) |

### Dataset Instances (Physical Implementations)

Each Dataset can have multiple physical instances. An instance represents a concrete implementation in a specific system and environment.

| Field | Type | Description |
|-------|------|-------------|
| `id` | UUID | Unique identifier |
| `dataset_id` | UUID | Parent dataset |
| `contract_id` | UUID | Contract version this instance implements |
| `contract_server_id` | UUID | Server entry from the contract (defines system type + environment) |
| `physical_path` | String | Path in the target system (flexible format) |
| `status` | Enum | `active`, `deprecated`, `retired` |
| `notes` | Text | Optional notes |

**Key Benefits:**
- Track the same dataset across different environments (dev, prod)
- Support different contract versions per environment (DEV uses draft v2.0, PROD uses active v1.5)
- Support multiple systems (Unity Catalog, Snowflake, BigQuery, etc.)
- Flexible `physical_path` format per system type

**System Types (from ODCS servers):**
The server type comes from the contract's `servers` array, following the [ODCS Infrastructure & Servers specification](https://bitol-io.github.io/open-data-contract-standard/v3.0.2/infrastructure-servers/). Supported types include: databricks, snowflake, bigquery, postgresql, mysql, s3, kafka, and many more.

## API Endpoints

### CRUD Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/datasets` | List datasets with filters |
| `GET` | `/api/datasets/{id}` | Get dataset details |
| `POST` | `/api/datasets` | Create new dataset |
| `PUT` | `/api/datasets/{id}` | Update dataset |
| `DELETE` | `/api/datasets/{id}` | Delete dataset |

### Query Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/datasets/by-contract/{contract_id}` | Get datasets for a contract |
| `GET` | `/api/datasets/validate-asset/{catalog}/{schema}/{object}` | Validate UC asset exists |

### Contract Assignment

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/datasets/{id}/contract/{contract_id}` | Assign contract |
| `DELETE` | `/api/datasets/{id}/contract` | Remove contract |

### Subscriptions

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/datasets/{id}/subscription` | Check subscription status |
| `POST` | `/api/datasets/{id}/subscribe` | Subscribe to dataset |
| `DELETE` | `/api/datasets/{id}/subscribe` | Unsubscribe |
| `GET` | `/api/datasets/{id}/subscribers` | List subscribers |

### Physical Instances

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/datasets/{id}/instances` | List all instances |
| `GET` | `/api/datasets/{id}/instances/{instance_id}` | Get instance details |
| `POST` | `/api/datasets/{id}/instances` | Add new instance |
| `PUT` | `/api/datasets/{id}/instances/{instance_id}` | Update instance |
| `DELETE` | `/api/datasets/{id}/instances/{instance_id}` | Remove instance |

## Query Parameters

The list endpoint supports these filters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `environment` | String | Filter by environment |
| `status` | String | Filter by status |
| `asset_type` | String | Filter by asset type |
| `contract_id` | String | Filter by contract |
| `owner_team_id` | String | Filter by owner team |
| `project_id` | String | Filter by project |
| `published` | Boolean | Filter by publication status |
| `catalog_name` | String | Filter by catalog |
| `search` | String | Search in name/description |
| `skip` | Integer | Pagination offset |
| `limit` | Integer | Pagination limit (max 1000) |

## Use Cases

### 1. Registering Production Tables

A data engineer registers production tables as Datasets and links them to their governing contracts:

```python
POST /api/datasets
{
  "name": "Customer Transactions - Production",
  "asset_type": "table",
  "catalog_name": "prod_catalog",
  "schema_name": "sales",
  "object_name": "customer_transactions",
  "environment": "prod",
  "contract_id": "contract-uuid",
  "status": "active"
}
```

### 2. Multi-Environment Tracking with Instances

Track the same logical dataset across environments using instances:

```python
POST /api/datasets/{dataset_id}/instances
{
  "contract_id": "contract-v1.5-uuid",      # Active version for prod
  "contract_server_id": "server-prod-uuid", # Prod server from contract
  "physical_path": "prod_catalog.sales.transactions",
  "status": "active"
}

POST /api/datasets/{dataset_id}/instances
{
  "contract_id": "contract-v2.0-uuid",      # Draft version for dev
  "contract_server_id": "server-dev-uuid",  # Dev server from contract
  "physical_path": "dev_catalog.sales.transactions",
  "status": "active"
}
```

### 3. Multi-System Support

The same dataset can have instances in different systems (via different contract servers):

```
Instance 1: Unity Catalog (prod)  → prod_catalog.sales.transactions
Instance 2: Snowflake (analytics) → ANALYTICS_DB.SALES.TRANSACTIONS
Instance 3: BigQuery (reporting)  → project.dataset.transactions
```

### 4. Consumer Discovery

Data consumers browse the Datasets list to find production-ready data:

```
GET /api/datasets?environment=prod&status=active&published=true
```

### 5. Impact Analysis

When a contract changes, find all affected datasets:

```
GET /api/datasets/by-contract/{contract_id}
```

## Permissions

Datasets use the standard RBAC system with these access levels:

| Level | Capabilities |
|-------|--------------|
| `Read-Only` | View datasets, subscribe |
| `Read/Write` | Create, update datasets, manage contracts |
| `Admin` | Delete datasets, view all subscribers |

## Search Integration

Datasets are integrated with the global search system. Each dataset is indexed with:

- Name
- Description
- Environment (as tag)
- Asset type (as tag)
- Custom tags

Users can search for datasets using natural language queries.

## Best Practices

1. **Naming Convention**: Use consistent naming that includes the environment and purpose
   - Good: `Customer Transactions - Production`
   - Bad: `transactions_prod`

2. **Contract Assignment**: Always assign contracts to production datasets for compliance tracking

3. **Environment Tracking**: Create Dataset records for all environments, not just production

4. **Subscription Management**: Encourage consumers to subscribe for change notifications

5. **Version Alignment**: Keep dataset versions in sync with contract versions when applicable

## Related Features

- **[Data Contracts](./data-contracts.md)**: Define the schema and quality requirements
- **[Data Products](./data-products.md)**: Bundle datasets and contracts for consumers
- **[Compliance](./compliance.md)**: Validate datasets against contract requirements
- **[Search](./search.md)**: Discover datasets across the organization

