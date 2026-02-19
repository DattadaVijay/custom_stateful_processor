# LLM Search Tools Reference

This document lists all tools available to the LLM assistant in the conversational search feature.

## Overview

The LLM Tools module (`src/backend/src/tools/`) provides an AI assistant with 41 tools for discovering, analyzing, and managing data assets. These tools are organized into reusable classes that can be used by both the LLM Search feature and the MCP server.

---

## Data Products Tools (Full CRUD)

### 1. `search_data_products`

Search for data products by name, domain, description, or keywords.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query (e.g., 'customer', 'sales transactions') |
| `domain` | string | ❌ | Filter by domain (e.g., 'Customer', 'Sales', 'Finance') |
| `status` | enum | ❌ | Filter by status: `active`, `draft`, `deprecated`, `retired` |

**Returns:** List of matching data products with id, name, domain, description, status, output tables, and version.

---

### 2. `get_data_product`

Get detailed information about a specific data product by its ID.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `product_id` | string | ✅ | The ID of the data product to retrieve |

**Returns:** Data product details including id, name, domain, description, status, version, output_tables, and timestamps.

---

### 3. `create_draft_data_product`

Create a new draft data product. Optionally link to an existing data contract.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Name for the data product (e.g., 'Customer Analytics Product') |
| `description` | string | ✅ | Business description and purpose of the data product |
| `domain` | string | ✅ | Business domain (e.g., 'Customer', 'Sales', 'Finance') |
| `contract_id` | string | ❌ | ID of an existing data contract to link to this product |
| `output_tables` | array | ❌ | List of output table FQNs this product provides |

**Returns:** Created product details with id, name, version, status, and URL.

---

### 4. `update_data_product`

Update an existing data product's properties like domain, description, or status.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `product_id` | string | ✅ | ID of the data product to update |
| `domain` | string | ❌ | New business domain |
| `description` | string | ❌ | New business description/purpose |
| `status` | enum | ❌ | New status: `draft`, `active`, `deprecated` |

**Returns:** Updated product details with id, name, domain, status, and URL.

---

### 5. `delete_data_product`

Delete a data product by its ID. This action cannot be undone.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `product_id` | string | ✅ | The ID of the data product to delete |

**Returns:** Success confirmation with deleted product ID.

---

## Data Contracts Tools (Full CRUD)

### 6. `search_data_contracts`

Search for data contracts by name, domain, description, or keywords.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query (e.g., 'customer', 'sales data') |
| `domain` | string | ❌ | Filter by domain (e.g., 'Customer', 'Sales', 'Finance') |
| `status` | enum | ❌ | Filter by status: `active`, `draft`, `deprecated`, `retired` |

**Returns:** List of matching data contracts with id, name, domain, description, status, and version.

---

### 7. `get_data_contract`

Get detailed information about a specific data contract by its ID.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `contract_id` | string | ✅ | The ID of the data contract to retrieve |

**Returns:** Data contract details including id, name, domain, description, status, version, and timestamps.

---

### 8. `create_draft_data_contract`

Create a new draft data contract based on schema information. The contract will be created in 'draft' status for user review.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Name for the contract (e.g., 'Customer Master Data Contract') |
| `description` | string | ✅ | Business description of what this contract governs |
| `domain` | string | ✅ | Business domain (e.g., 'Customer', 'Sales', 'Finance') |
| `tables` | array | ❌ | List of tables to include in the contract schema |

**Table Object Structure:**
```json
{
  "name": "table_name",
  "full_name": "catalog.schema.table",
  "description": "Table description",
  "columns": [
    {"name": "col1", "type": "STRING", "description": "Column description"}
  ]
}
```

**Returns:** Created contract details with id, name, version, status, and URL.

---

### 9. `update_data_contract`

Update an existing data contract's properties like domain, description, or status.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `contract_id` | string | ✅ | ID of the data contract to update |
| `domain` | string | ❌ | New business domain |
| `description` | string | ❌ | New business description/purpose |
| `status` | enum | ❌ | New status: `draft`, `active`, `deprecated` |

**Returns:** Updated contract details with id, name, domain, status, and URL.

---

### 10. `delete_data_contract`

Delete a data contract by its ID. This action cannot be undone.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `contract_id` | string | ✅ | The ID of the data contract to delete |

**Returns:** Success confirmation with deleted contract ID.

---

## Domains Tools (Full CRUD)

### 11. `search_domains`

Search for data domains by name or description.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query for domains (e.g., 'customer', 'finance') |

**Returns:** List of matching domains with id, name, description, parent_id, and timestamps.

---

### 12. `get_domain`

Get detailed information about a specific data domain by its ID.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `domain_id` | string | ✅ | The ID of the data domain to retrieve |

**Returns:** Domain details including id, name, description, parent hierarchy, and children count.

---

### 13. `create_domain`

Create a new data domain. Domains can be organized hierarchically with parent-child relationships.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Name for the domain (e.g., 'Customer', 'Finance', 'Operations') |
| `description` | string | ❌ | Description of what this domain covers |
| `parent_id` | string | ❌ | ID of the parent domain for hierarchical organization |

**Returns:** Created domain details with id, name, description, and URL.

---

### 14. `update_domain`

Update an existing data domain's name, description, or parent.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `domain_id` | string | ✅ | ID of the domain to update |
| `name` | string | ❌ | New name for the domain |
| `description` | string | ❌ | New description |
| `parent_id` | string | ❌ | New parent domain ID (or null to make it a root domain) |

**Returns:** Updated domain details with id, name, and URL.

---

### 15. `delete_domain`

Delete a data domain by its ID. Note: Domains with children may be affected by cascade rules.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `domain_id` | string | ✅ | The ID of the domain to delete |

**Returns:** Success confirmation with deleted domain name.

---

## Teams Tools (Full CRUD)

### 16. `search_teams`

Search for teams by name, title, or domain.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query for teams (e.g., 'data engineering', 'analytics') |
| `domain_id` | string | ❌ | Optional filter by domain ID |

**Returns:** List of matching teams with id, name, title, description, domain_id, and member count.

---

### 17. `get_team`

Get detailed information about a specific team by its ID, including its members.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `team_id` | string | ✅ | The ID of the team to retrieve |

**Returns:** Team details including id, name, title, description, domain info, member count, and member list.

---

### 18. `create_team`

Create a new team. Teams can be assigned to domains and contain members (users or groups).

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the team (e.g., 'data-engineering-team') |
| `title` | string | ✅ | Display title for the team (e.g., 'Data Engineering Team') |
| `description` | string | ❌ | Description of the team's purpose and responsibilities |
| `domain_id` | string | ❌ | ID of the domain this team belongs to |

**Returns:** Created team details with id, name, title, and URL.

---

### 19. `update_team`

Update an existing team's name, title, description, or domain assignment.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `team_id` | string | ✅ | ID of the team to update |
| `name` | string | ❌ | New unique name for the team |
| `title` | string | ❌ | New display title |
| `description` | string | ❌ | New description |
| `domain_id` | string | ❌ | New domain ID (or null to remove domain assignment) |

**Returns:** Updated team details with id, name, title, and URL.

---

### 20. `delete_team`

Delete a team by its ID. This will also remove all team member associations.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `team_id` | string | ✅ | The ID of the team to delete |

**Returns:** Success confirmation with deleted team name.

---

## Projects Tools (Full CRUD)

### 21. `search_projects`

Search for projects by name, title, or description.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query for projects (e.g., 'customer analytics', 'data platform') |

**Returns:** List of matching projects with id, name, title, description, project_type, and team count.

---

### 22. `get_project`

Get detailed information about a specific project by its ID, including assigned teams.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `project_id` | string | ✅ | The ID of the project to retrieve |

**Returns:** Project details including id, name, title, description, project_type, owner team, team count, and team list.

---

### 23. `create_project`

Create a new project. Projects organize work and can have multiple teams assigned.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the project (e.g., 'customer-analytics-platform') |
| `title` | string | ✅ | Display title for the project (e.g., 'Customer Analytics Platform') |
| `description` | string | ❌ | Description of the project's purpose and scope |
| `project_type` | enum | ❌ | Type of project: `standard`, `admin` |
| `owner_team_id` | string | ❌ | ID of the team that owns this project |
| `team_ids` | array | ❌ | IDs of teams to assign to this project |

**Returns:** Created project details with id, name, title, and URL.

---

### 24. `update_project`

Update an existing project's name, title, description, or owner team.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `project_id` | string | ✅ | ID of the project to update |
| `name` | string | ❌ | New unique name for the project |
| `title` | string | ❌ | New display title |
| `description` | string | ❌ | New description |
| `owner_team_id` | string | ❌ | New owner team ID |

**Returns:** Updated project details with id, name, title, and URL.

---

### 25. `delete_project`

Delete a project by its ID. This will also remove all team assignments for this project.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `project_id` | string | ✅ | The ID of the project to delete |

**Returns:** Success confirmation with deleted project name.

---

## Discovery Tools

### 26. `search_glossary_terms`

Search the knowledge graph for business concepts, terms, and definitions from ontologies and taxonomies.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `term` | string | ✅ | Business term or concept to search for (e.g., 'Customer', 'Revenue') |
| `domain` | string | ❌ | Optional taxonomy/domain filter |

**Returns:** List of matching terms with IRI, name, definition, taxonomy, relevance score, and match type.

---

### 27. `explore_catalog_schema`

List all tables and views in a Unity Catalog schema, including their columns and types. Use this to understand what data assets exist and suggest semantic models or data products.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `catalog` | string | ✅ | Catalog name (e.g., 'demo_cat', 'main') |
| `schema` | string | ✅ | Schema/database name (e.g., 'demo_db', 'default') |
| `include_columns` | boolean | ❌ | Include column details for each table (default: true) |

**Returns:** List of tables/views with name, full_name, table_type, comment, and optionally columns with their types.

---

## Schema & Query Tools

### 28. `get_table_schema`

Get the schema (columns and data types) of a table from a data product.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_fqn` | string | ✅ | Fully qualified table name (catalog.schema.table) |

**Returns:** Table schema with columns (name, type, nullable, comment), table type, and comment.

---

### 29. `execute_analytics_query`

Execute a read-only SQL SELECT query against Databricks tables. Use for aggregations, joins, and filtering.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sql` | string | ✅ | The SQL SELECT query to execute |
| `explanation` | string | ✅ | Brief explanation of what this query does and why |

**Returns:** Query results with columns, rows (limited to 100 in response), row count, and truncation status.

**Limitations:**
- Read-only SELECT queries only
- Results capped at 1000 rows
- Uses user's OBO credentials for access control

---

## Cost Analysis Tools

### 30. `get_data_product_costs`

Get cost information for data products including infrastructure, HR, and storage costs.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `product_id` | string | ❌ | Specific product ID, or omit for all products |
| `aggregate` | boolean | ❌ | If true, return totals; if false, return per-product breakdown (default: false) |

**Returns:** Cost breakdown by product or aggregated totals, with amounts in USD.

---

## Semantic Linking Tools

### 31. `add_semantic_link`

Link a data product or contract to a business term/concept from the knowledge graph.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entity_type` | enum | ✅ | Type of entity: `data_product` or `data_contract` |
| `entity_id` | string | ✅ | ID of the entity to link |
| `concept_iri` | string | ✅ | IRI of the concept from the knowledge graph |
| `concept_label` | string | ✅ | Human-readable label for the concept |
| `relationship_type` | string | ❌ | Type of relationship (default: `relatedTo`) |

**Returns:** Link details with id, entity info, concept info, and relationship type.

---

### 32. `list_semantic_links`

List semantic links (business term associations) for a data product or contract.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entity_type` | enum | ✅ | Type of entity: `data_product` or `data_contract` |
| `entity_id` | string | ✅ | ID of the entity |

**Returns:** List of links with id, IRI, label, relationship type, and creation timestamp.

---

### 33. `remove_semantic_link`

Remove a semantic link from a data product or contract.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `link_id` | string | ✅ | ID of the semantic link to remove |

**Returns:** Success status and removed link ID.

---

## Tags Tools (CRUD + Entity Assignment)

### 34. `search_tags`

Search for tags by name, namespace, or description.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Search query for tags (e.g., 'pii', 'customer', 'sensitive') |
| `namespace` | string | ❌ | Optional filter by namespace name |

**Returns:** List of matching tags with id, name, fully_qualified_name, namespace, description, and status.

---

### 35. `get_tag`

Get detailed information about a specific tag by its ID or fully qualified name.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tag_id` | string | ❌ | The ID of the tag to retrieve |
| `fqn` | string | ❌ | Fully qualified name (namespace::tag_name) - alternative to tag_id |

**Returns:** Tag details including id, name, fqn, namespace, description, status, parent_id, and timestamps.

---

### 36. `create_tag`

Create a new tag. Tags are organized in namespaces and can have hierarchical parent-child relationships.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✅ | Name for the tag (e.g., 'pii', 'customer-data', 'sensitive') |
| `description` | string | ❌ | Description of what this tag represents |
| `namespace_name` | string | ❌ | Namespace for the tag (default: 'default') |
| `parent_id` | string | ❌ | ID of parent tag for hierarchical organization |

**Returns:** Created tag details with id, name, fully_qualified_name.

---

### 37. `update_tag`

Update an existing tag's name, description, or status.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tag_id` | string | ✅ | ID of the tag to update |
| `name` | string | ❌ | New name for the tag |
| `description` | string | ❌ | New description |
| `status` | enum | ❌ | New status: `active`, `deprecated`, `archived` |

**Returns:** Updated tag details with id, name, fully_qualified_name.

---

### 38. `delete_tag`

Delete a tag by its ID. Tags that are parents to other tags cannot be deleted until children are removed.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tag_id` | string | ✅ | The ID of the tag to delete |

**Returns:** Success confirmation.

---

### 39. `list_entity_tags`

List all tags assigned to a specific entity.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entity_type` | enum | ✅ | Type: `data_product`, `data_contract`, `data_domain`, `team`, `project` |
| `entity_id` | string | ✅ | ID of the entity |

**Returns:** List of assigned tags with tag_id, tag_name, fqn, namespace, assigned_value, assigned_by, assigned_at.

---

### 40. `assign_tag_to_entity`

Assign a tag to any entity. Can optionally include an assigned value for key-value tags.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entity_type` | enum | ✅ | Type: `data_product`, `data_contract`, `data_domain`, `team`, `project` |
| `entity_id` | string | ✅ | ID of the entity to tag |
| `tag_id` | string | ✅ | ID of the tag to assign |
| `assigned_value` | string | ❌ | Optional value for the tag assignment |

**Returns:** Assignment details with entity info and tag info.

---

### 41. `remove_tag_from_entity`

Remove a tag assignment from an entity.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entity_type` | enum | ✅ | Type: `data_product`, `data_contract`, `data_domain`, `team`, `project` |
| `entity_id` | string | ✅ | ID of the entity |
| `tag_id` | string | ✅ | ID of the tag to remove |

**Returns:** Success confirmation.

---

## Tool Categories Summary

| Category | Tools | Purpose |
|----------|-------|---------|
| **Data Products** | `search_data_products`, `get_data_product`, `create_draft_data_product`, `update_data_product`, `delete_data_product` | Full CRUD for data products |
| **Data Contracts** | `search_data_contracts`, `get_data_contract`, `create_draft_data_contract`, `update_data_contract`, `delete_data_contract` | Full CRUD for data contracts |
| **Domains** | `search_domains`, `get_domain`, `create_domain`, `update_domain`, `delete_domain` | Full CRUD for data domains |
| **Teams** | `search_teams`, `get_team`, `create_team`, `update_team`, `delete_team` | Full CRUD for teams |
| **Projects** | `search_projects`, `get_project`, `create_project`, `update_project`, `delete_project` | Full CRUD for projects |
| **Discovery** | `search_glossary_terms`, `explore_catalog_schema` | Find and explore data assets |
| **Schema & Query** | `get_table_schema`, `execute_analytics_query` | Inspect schemas and run analytics |
| **Cost Analysis** | `get_data_product_costs` | Analyze data product costs |
| **Semantic Linking** | `add_semantic_link`, `list_semantic_links`, `remove_semantic_link` | Connect assets to business terms |
| **Tags** | `search_tags`, `get_tag`, `create_tag`, `update_tag`, `delete_tag`, `list_entity_tags`, `assign_tag_to_entity`, `remove_tag_from_entity` | Tag management and entity tagging |

---

## Security Notes

- All Unity Catalog operations use **OBO (On-Behalf-Of) credentials** for proper access control
- SQL queries are validated to be **read-only SELECT statements**
- Query results are **capped at 1000 rows** to prevent abuse
- Created assets are always in **draft status** for user review before activation
- Delete operations require appropriate permissions

---

## Architecture

The tools are implemented as reusable classes in `src/backend/src/tools/`:

```
src/backend/src/tools/
├── __init__.py           # Exports all tools and registry
├── base.py               # BaseTool, ToolContext, ToolResult
├── registry.py           # ToolRegistry with format converters
├── data_products.py      # Data product CRUD tools
├── data_contracts.py     # Data contract CRUD tools
├── domains.py            # Domain CRUD tools
├── teams.py              # Team CRUD tools
├── projects.py           # Project CRUD tools
├── semantic_models.py    # Glossary search and semantic linking
├── analytics.py          # Table schema and query execution
├── costs.py              # Cost analysis tools
└── tags.py               # Tag management and entity assignment
```

Tools can provide definitions in multiple formats:
- **OpenAI format**: For OpenAI function calling API
- **MCP format**: For Model Context Protocol servers
