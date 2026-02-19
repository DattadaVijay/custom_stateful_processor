# MCP Server Implementation Plan

## Overview

Implement a Model Context Protocol (MCP) server to expose the Databricks Unity Catalog app's capabilities to AI assistants like Claude. The server will provide tools and resources for querying data products, executing SPARQL queries, searching data contracts, navigating business glossaries, and exploring semantic models.

## Requirements Summary

- **Type**: MCP Server (expose app capabilities to AI assistants)
- **Scope**: Data Products, Data Contracts, Business Glossary, Semantic Models, SPARQL queries
- **Authentication**: MCP-specific tokens with scope-based authorization
- **Primary Use Case**: Enable AI assistants to query data catalog and semantic models
- **Protocol**: JSON-RPC 2.0 over HTTP (SSE support can be added later if needed)

## Architecture

### Component Design

```
Main FastAPI App (Port 8000)          MCP Server (Port 8001)
├─ User REST API                      ├─ JSON-RPC 2.0 Handler
├─ React UI                           ├─ Token Auth Middleware
└─ User/Group Auth                    ├─ Tool Registry
                                      ├─ Resource Provider
         │                            └─ Adapter Layer
         └──── Shared app.state ───────┘
                (managers, db)
                     │
         ┌───────────┴────────────┐
         │  Existing Managers     │
         │  (reused, no changes)  │
         └────────────────────────┘
```

### Key Design Decisions

1. **Adapter Pattern**: Create thin adapter classes (`DataProductsMCPAdapter`, `SemanticModelsMCPAdapter`, etc.) that translate MCP protocol to existing manager calls. This avoids modifying existing business logic.

2. **Separate Port (8001)**: Run MCP server on a different port for traffic isolation, independent monitoring, and rate limiting.

3. **Token-Based Auth**: New `mcp_tokens` database table with bcrypt-hashed tokens, JSON scopes array (`data-products:read`, `sparql:query`, etc.), and expiration/revocation support.

4. **Reuse Existing Logic**: Leverage production-ready implementations:
   - `SemanticModelsManager.query()` for SPARQL (with existing validation, timeout, safety)
   - `DataProductsManager.list_products()` for product filtering
   - `SearchManager` for unified search
   - All existing security and validation logic

## MCP Tools & Resources Specification

### MCP Tools (Callable Operations)

#### 1. query_data_products

**Description**: Search and filter data products with support for status, domain, tags, and pagination.

**Required Scope**: `data-products:read`

**Input Schema** (JSON Schema):
```json
{
  "type": "object",
  "properties": {
    "status": {
      "type": "string",
      "enum": ["DRAFT", "SANDBOX", "PROPOSED", "UNDER_REVIEW", "APPROVED", "ACTIVE", "CERTIFIED", "DEPRECATED", "RETIRED"],
      "description": "Filter by lifecycle status"
    },
    "domain": {
      "type": "string",
      "description": "Filter by business domain (e.g., 'Finance', 'Marketing')"
    },
    "search": {
      "type": "string",
      "description": "Free-text search across name, description, and metadata"
    },
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Filter by tags (e.g., ['pii', 'critical'])"
    },
    "project_id": {
      "type": "string",
      "description": "Filter by Databricks project ID"
    },
    "limit": {
      "type": "integer",
      "default": 50,
      "minimum": 1,
      "maximum": 500,
      "description": "Number of results to return"
    },
    "offset": {
      "type": "integer",
      "default": 0,
      "minimum": 0,
      "description": "Number of results to skip (for pagination)"
    }
  }
}
```

**Response Format**:
```json
{
  "products": [
    {
      "id": "dp-12345",
      "name": "Customer Analytics Dataset",
      "status": "ACTIVE",
      "version": "1.2.0",
      "domain": "Marketing",
      "description": {
        "purpose": "Provides aggregated customer behavior data",
        "usage": "Use for marketing campaign analysis"
      },
      "owner_team_id": "team-marketing",
      "tags": ["customer-data", "analytics"],
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-03-20T14:22:00Z"
    }
  ],
  "total": 156,
  "limit": 50,
  "offset": 0,
  "has_more": true
}
```

**Implementation**: Calls `DataProductsManager.list_products()` with filters

---

#### 2. execute_sparql_query

**Description**: Execute SPARQL queries against the semantic model graph with safety validation.

**Required Scope**: `sparql:query`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "sparql": {
      "type": "string",
      "description": "SPARQL query (SELECT, ASK, DESCRIBE, CONSTRUCT only)",
      "maxLength": 10000
    },
    "max_results": {
      "type": "integer",
      "default": 100,
      "minimum": 1,
      "maximum": 1000,
      "description": "Maximum number of results to return"
    },
    "timeout_seconds": {
      "type": "integer",
      "default": 30,
      "minimum": 1,
      "maximum": 60,
      "description": "Query timeout in seconds"
    }
  },
  "required": ["sparql"]
}
```

**Response Format**:
```json
{
  "results": [
    {
      "concept": "http://example.org/terms#Customer",
      "label": "Customer",
      "comment": "An individual or organization that purchases products"
    }
  ],
  "count": 42,
  "query_time_ms": 245,
  "truncated": false
}
```

**Example SPARQL**:
```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?concept ?label ?comment WHERE {
  { ?concept a rdfs:Class } UNION { ?concept a skos:Concept }
  OPTIONAL { ?concept skos:prefLabel ?label }
  OPTIONAL { ?concept rdfs:comment ?comment }
  FILTER(CONTAINS(LCASE(STR(?label)), "customer"))
}
LIMIT 100
```

**Security**: Uses existing `SPARQLQueryValidator` to block INSERT/DELETE/UPDATE/DROP/CLEAR

**Implementation**: Calls `SemanticModelsManager.query()`

---

#### 3. query_data_contracts

**Description**: Search and filter data contracts by various criteria.

**Required Scope**: `contracts:read`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "search": {
      "type": "string",
      "description": "Search in contract name, description, or content"
    },
    "status": {
      "type": "string",
      "enum": ["DRAFT", "PROPOSED", "ACTIVE", "DEPRECATED"],
      "description": "Filter by contract status"
    },
    "owner": {
      "type": "string",
      "description": "Filter by owner name or team"
    },
    "format": {
      "type": "string",
      "enum": ["YAML", "JSON", "TEXT", "ODCS"],
      "description": "Filter by contract format"
    },
    "limit": {
      "type": "integer",
      "default": 50,
      "minimum": 1,
      "maximum": 500
    },
    "offset": {
      "type": "integer",
      "default": 0,
      "minimum": 0
    }
  }
}
```

**Response Format**:
```json
{
  "contracts": [
    {
      "id": "dc-67890",
      "name": "Customer PII Contract v2",
      "status": "ACTIVE",
      "version": "2.0.0",
      "owner": "Data Governance Team",
      "format": "ODCS",
      "description": "Defines schema and quality rules for customer PII data",
      "created_at": "2024-02-01T09:00:00Z",
      "updated_at": "2024-02-15T11:30:00Z"
    }
  ],
  "total": 87,
  "limit": 50,
  "offset": 0
}
```

**Implementation**: Calls `DataContractsManager.list_contracts()` with filters

---

#### 4. search_glossary_terms

**Description**: Search for semantic concepts and glossary terms across taxonomies.

**Required Scope**: `glossary:read`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Search query for concept labels and descriptions",
      "minLength": 1
    },
    "taxonomy": {
      "type": "string",
      "description": "Filter by taxonomy/ontology name (e.g., 'business-glossary', 'data-governance')"
    },
    "concept_type": {
      "type": "string",
      "enum": ["class", "concept", "individual"],
      "description": "Filter by concept type"
    },
    "limit": {
      "type": "integer",
      "default": 50,
      "minimum": 1,
      "maximum": 500
    }
  },
  "required": ["query"]
}
```

**Response Format**:
```json
{
  "concepts": [
    {
      "iri": "http://ontos.app/terms#Customer",
      "label": "Customer",
      "comment": "An individual or organization that purchases products or services",
      "concept_type": "class",
      "taxonomy": "business-glossary",
      "synonyms": ["Client", "Buyer"],
      "examples": ["John Doe", "Acme Corp"]
    }
  ],
  "total": 23,
  "limit": 50
}
```

**Implementation**: Calls `SemanticModelsManager.search_concepts()`

---

#### 5. get_concept_hierarchy

**Description**: Navigate concept hierarchies to find parent, child, and sibling relationships.

**Required Scope**: `semantic:navigate`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "concept_iri": {
      "type": "string",
      "format": "uri",
      "description": "IRI of the concept to explore (e.g., 'http://ontos.app/terms#Customer')"
    },
    "include_parents": {
      "type": "boolean",
      "default": true,
      "description": "Include parent concepts (broader terms)"
    },
    "include_children": {
      "type": "boolean",
      "default": true,
      "description": "Include child concepts (narrower terms)"
    },
    "include_siblings": {
      "type": "boolean",
      "default": false,
      "description": "Include sibling concepts (same parent)"
    },
    "max_depth": {
      "type": "integer",
      "default": 3,
      "minimum": 1,
      "maximum": 10,
      "description": "Maximum depth for traversal"
    }
  },
  "required": ["concept_iri"]
}
```

**Response Format**:
```json
{
  "concept": {
    "iri": "http://ontos.app/terms#Customer",
    "label": "Customer",
    "comment": "An individual or organization that purchases products"
  },
  "parents": [
    {
      "iri": "http://ontos.app/terms#Party",
      "label": "Party",
      "distance": 1
    }
  ],
  "children": [
    {
      "iri": "http://ontos.app/terms#RetailCustomer",
      "label": "Retail Customer",
      "distance": 1
    },
    {
      "iri": "http://ontos.app/terms#EnterpriseCustomer",
      "label": "Enterprise Customer",
      "distance": 1
    }
  ],
  "siblings": [
    {
      "iri": "http://ontos.app/terms#Supplier",
      "label": "Supplier",
      "shared_parent": "http://ontos.app/terms#Party"
    }
  ]
}
```

**Implementation**: Calls `SemanticModelsManager.get_concept_hierarchy()`

---

#### 6. get_concept_neighbors

**Description**: Discover related concepts through RDF graph relationships.

**Required Scope**: `semantic:navigate`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "concept_iri": {
      "type": "string",
      "format": "uri",
      "description": "IRI of the concept to explore"
    },
    "max_neighbors": {
      "type": "integer",
      "default": 50,
      "minimum": 1,
      "maximum": 200
    }
  },
  "required": ["concept_iri"]
}
```

**Response Format**:
```json
{
  "concept": "http://ontos.app/terms#Customer",
  "outgoing": [
    {
      "predicate": "http://www.w3.org/2000/01/rdf-schema#subClassOf",
      "target": "http://ontos.app/terms#Party",
      "target_label": "Party"
    }
  ],
  "incoming": [
    {
      "predicate": "http://www.w3.org/2000/01/rdf-schema#subClassOf",
      "source": "http://ontos.app/terms#RetailCustomer",
      "source_label": "Retail Customer"
    }
  ],
  "total_outgoing": 3,
  "total_incoming": 7
}
```

**Implementation**: Calls `SemanticModelsManager.get_neighbors()`

---

#### 7. global_search

**Description**: Unified search across all searchable features (data products, contracts, glossary, etc.).

**Required Scope**: `search:read`

**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Search query",
      "minLength": 1
    },
    "feature_types": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": ["data-products", "data-contracts", "business-glossary", "reviews", "compliance"]
      },
      "description": "Filter by feature types (empty = all)"
    },
    "limit": {
      "type": "integer",
      "default": 50,
      "minimum": 1,
      "maximum": 500
    }
  },
  "required": ["query"]
}
```

**Response Format**:
```json
{
  "results": [
    {
      "id": "dp-12345",
      "title": "Customer Analytics Dataset",
      "description": "Provides aggregated customer behavior data",
      "feature_type": "data-products",
      "url": "/data-products/dp-12345",
      "relevance_score": 0.95,
      "tags": ["customer", "analytics"]
    },
    {
      "id": "dc-67890",
      "title": "Customer PII Contract v2",
      "description": "Schema and quality rules for customer PII",
      "feature_type": "data-contracts",
      "url": "/data-contracts/dc-67890",
      "relevance_score": 0.87,
      "tags": ["customer", "pii"]
    }
  ],
  "total": 45,
  "limit": 50
}
```

**Implementation**: Calls `SearchManager.search()`

---

### MCP Resources (Readable Data)

#### 1. product://{id}

**Description**: Retrieve full details of a specific data product.

**Required Scope**: `data-products:read`

**URI Pattern**: `product://<product-id>`

**Example**: `product://dp-12345`

**Content Type**: `application/json`

**Response Format**: Full `DataProduct` object (ODPS v1.0.0 compliant)
```json
{
  "id": "dp-12345",
  "apiVersion": "v1.0.0",
  "kind": "DataProduct",
  "name": "Customer Analytics Dataset",
  "status": "ACTIVE",
  "version": "1.2.0",
  "domain": "Marketing",
  "tenant": "global",
  "description": {
    "purpose": "Provides aggregated customer behavior data for marketing analytics",
    "usage": "Query tables via Unity Catalog, subscribe for updates",
    "limitations": "Data refreshed daily, 30-day retention"
  },
  "owner_team_id": "team-marketing",
  "team": {
    "teamId": "team-marketing",
    "name": "Marketing Analytics",
    "contacts": [
      {
        "name": "Jane Smith",
        "email": "jane.smith@example.com",
        "role": "Product Owner"
      }
    ]
  },
  "outputPorts": [
    {
      "id": "port-1",
      "name": "customer_behavior",
      "type": "table",
      "server": {
        "catalog": "main",
        "schema": "marketing",
        "table": "customer_behavior"
      },
      "dataContract": {
        "contractId": "dc-67890"
      }
    }
  ],
  "tags": ["customer-data", "analytics", "pii"],
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-03-20T14:22:00Z"
}
```

**Implementation**: Calls `DataProductsManager.get_product(id)`

---

#### 2. contract://{id}

**Description**: Retrieve full details of a data contract.

**Required Scope**: `contracts:read`

**URI Pattern**: `contract://<contract-id>`

**Example**: `contract://dc-67890`

**Content Type**: `application/json` or `text/yaml` (based on stored format)

**Response Format**: Full contract definition (ODCS-compliant if format is ODCS)
```json
{
  "id": "dc-67890",
  "name": "Customer PII Contract v2",
  "status": "ACTIVE",
  "version": "2.0.0",
  "format": "ODCS",
  "owner": "Data Governance Team",
  "content": "...contract YAML/JSON...",
  "description": "Defines schema and quality rules for customer PII data",
  "tags": ["pii", "gdpr", "customer"],
  "created_at": "2024-02-01T09:00:00Z",
  "updated_at": "2024-02-15T11:30:00Z"
}
```

**Implementation**: Calls `DataContractsManager.get_contract(id)`

---

#### 3. term://{iri}

**Description**: Retrieve full details of a semantic concept/glossary term.

**Required Scope**: `glossary:read`

**URI Pattern**: `term://<url-encoded-iri>`

**Example**: `term://http%3A%2F%2Fontos.app%2Fterms%23Customer`

**Content Type**: `application/json`

**Response Format**:
```json
{
  "iri": "http://ontos.app/terms#Customer",
  "label": "Customer",
  "comment": "An individual or organization that purchases products or services from the company",
  "concept_type": "class",
  "source_context": "business-glossary",
  "parent_concepts": ["http://ontos.app/terms#Party"],
  "child_concepts": [
    "http://ontos.app/terms#RetailCustomer",
    "http://ontos.app/terms#EnterpriseCustomer"
  ],
  "properties": [
    {
      "predicate": "http://www.w3.org/2004/02/skos/core#altLabel",
      "value": "Client"
    }
  ],
  "synonyms": ["Client", "Buyer"],
  "examples": ["John Doe", "Acme Corporation"],
  "tagged_assets": [
    {
      "entity_type": "data_product",
      "entity_id": "dp-12345",
      "entity_name": "Customer Analytics Dataset"
    }
  ]
}
```

**Implementation**: Calls `SemanticModelsManager.get_concept(iri)`

---

#### 4. taxonomy://{name}

**Description**: Retrieve metadata about a specific taxonomy or ontology.

**Required Scope**: `glossary:read`

**URI Pattern**: `taxonomy://<taxonomy-name>`

**Example**: `taxonomy://business-glossary`

**Content Type**: `application/json`

**Response Format**:
```json
{
  "name": "business-glossary",
  "source": "file:///data/taxonomies/business-glossary.ttl",
  "type": "SKOS",
  "enabled": true,
  "stats": {
    "total_concepts": 456,
    "total_properties": 123,
    "classes": 145,
    "individuals": 89,
    "top_level_concepts": 12
  },
  "namespaces": {
    "default": "http://ontos.app/terms#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
  },
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-03-15T16:45:00Z"
}
```

**Implementation**: Calls `SemanticModelsManager.get_taxonomy_info(name)`

---

### MCP Prompts (Future Enhancement)

**Description**: Pre-configured prompt templates that guide AI assistants in using the MCP tools effectively.

#### Prompt: Data Product Discovery

**Name**: `discover_data_products`

**Description**: Guide for discovering and exploring data products

**Template**:
```
You have access to a Databricks Unity Catalog with data products. To discover products:

1. Use `query_data_products` to search by domain, status, or tags
2. Use `global_search` for free-text search across all features
3. Read full details with the `product://{id}` resource
4. Check associated contracts via the product's outputPorts

Example workflow:
- Search for marketing data: query_data_products(domain="Marketing", status="ACTIVE")
- Get product details: Read resource product://dp-12345
- View the contract: Read resource contract://dc-67890
```

#### Prompt: Semantic Exploration

**Name**: `explore_semantics`

**Description**: Guide for navigating semantic models and glossaries

**Template**:
```
You have access to semantic models with business glossary terms and ontologies. To explore:

1. Search for terms: search_glossary_terms(query="customer")
2. Get full concept details: Read resource term://http%3A%2F%2Fontos.app%2Fterms%23Customer
3. Navigate hierarchy: get_concept_hierarchy(concept_iri="...")
4. Find related concepts: get_concept_neighbors(concept_iri="...")
5. Query with SPARQL for complex relationships: execute_sparql_query(sparql="...")

Example: Find all customer-related concepts and their relationships.
```

#### Prompt: Data Contract Analysis

**Name**: `analyze_contracts`

**Description**: Guide for working with data contracts

**Template**:
```
Data contracts define schemas, quality rules, and SLAs for data products. To analyze:

1. Search contracts: query_data_contracts(search="customer", status="ACTIVE")
2. Read contract: Read resource contract://dc-67890
3. Find products using this contract: query_data_products + filter by contract reference
4. Understand quality requirements and access controls from contract content

Example: Analyze all active contracts for PII data.
```

---

## JSON-RPC 2.0 Protocol Examples

The MCP server implements JSON-RPC 2.0 over HTTP. All requests/responses follow this format.

### 1. Initialize (Handshake)

**Request**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {
      "roots": {
        "listChanged": false
      }
    },
    "clientInfo": {
      "name": "Claude Desktop",
      "version": "1.0.0"
    }
  }
}
```

**Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2024-11-05",
    "capabilities": {
      "tools": {},
      "resources": {}
    },
    "serverInfo": {
      "name": "Databricks UC MCP Server",
      "version": "1.0.0"
    }
  }
}
```

---

### 2. List Tools

**Request**:
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list",
  "params": {}
}
```

**Response** (filtered by token scopes):
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "tools": [
      {
        "name": "query_data_products",
        "description": "Search and filter data products with support for status, domain, tags, and pagination",
        "inputSchema": {
          "type": "object",
          "properties": {
            "status": {"type": "string", "enum": ["DRAFT", "ACTIVE", ...]},
            "domain": {"type": "string"},
            "limit": {"type": "integer", "default": 50}
          }
        }
      },
      {
        "name": "execute_sparql_query",
        "description": "Execute SPARQL queries against the semantic model graph",
        "inputSchema": {
          "type": "object",
          "properties": {
            "sparql": {"type": "string", "maxLength": 10000},
            "max_results": {"type": "integer", "default": 100}
          },
          "required": ["sparql"]
        }
      }
    ]
  }
}
```

---

### 3. Call Tool

**Request** (query_data_products):
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "query_data_products",
    "arguments": {
      "domain": "Marketing",
      "status": "ACTIVE",
      "limit": 10
    }
  }
}
```

**Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"products\": [{\"id\": \"dp-12345\", \"name\": \"Customer Analytics Dataset\", \"status\": \"ACTIVE\", \"domain\": \"Marketing\"}], \"total\": 156, \"limit\": 10, \"offset\": 0}"
      }
    ]
  }
}
```

**Request** (execute_sparql_query):
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "execute_sparql_query",
    "arguments": {
      "sparql": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT ?concept ?label WHERE { ?concept a rdfs:Class . ?concept rdfs:label ?label } LIMIT 10",
      "max_results": 10,
      "timeout_seconds": 30
    }
  }
}
```

**Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"results\": [{\"concept\": \"http://ontos.app/terms#Customer\", \"label\": \"Customer\"}, {\"concept\": \"http://ontos.app/terms#Product\", \"label\": \"Product\"}], \"count\": 10, \"query_time_ms\": 145, \"truncated\": false}"
      }
    ]
  }
}
```

---

### 4. List Resources

**Request**:
```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "resources/list",
  "params": {}
}
```

**Response** (dynamic list based on available data):
```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "result": {
    "resources": [
      {
        "uri": "product://dp-12345",
        "name": "Customer Analytics Dataset",
        "description": "ACTIVE data product in Marketing domain",
        "mimeType": "application/json"
      },
      {
        "uri": "contract://dc-67890",
        "name": "Customer PII Contract v2",
        "description": "ACTIVE contract defining PII data schema",
        "mimeType": "application/json"
      },
      {
        "uri": "term://http%3A%2F%2Fontos.app%2Fterms%23Customer",
        "name": "Customer",
        "description": "Business glossary term from business-glossary taxonomy",
        "mimeType": "application/json"
      },
      {
        "uri": "taxonomy://business-glossary",
        "name": "Business Glossary",
        "description": "Primary business taxonomy with 456 concepts",
        "mimeType": "application/json"
      }
    ]
  }
}
```

---

### 5. Read Resource

**Request**:
```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "resources/read",
  "params": {
    "uri": "product://dp-12345"
  }
}
```

**Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "result": {
    "contents": [
      {
        "uri": "product://dp-12345",
        "mimeType": "application/json",
        "text": "{\"id\": \"dp-12345\", \"apiVersion\": \"v1.0.0\", \"kind\": \"DataProduct\", \"name\": \"Customer Analytics Dataset\", \"status\": \"ACTIVE\", ...full product JSON...}"
      }
    ]
  }
}
```

---

### 6. Error Responses

**Authentication Error** (missing or invalid token):
```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "error": {
    "code": -32001,
    "message": "Authentication failed",
    "data": {
      "detail": "Invalid or expired token",
      "error_type": "AuthenticationError"
    }
  }
}
```

**Authorization Error** (insufficient scope):
```json
{
  "jsonrpc": "2.0",
  "id": 8,
  "error": {
    "code": -32002,
    "message": "Authorization failed",
    "data": {
      "detail": "Token lacks required scope: sparql:query",
      "required_scope": "sparql:query",
      "token_scopes": ["data-products:read"],
      "error_type": "AuthorizationError"
    }
  }
}
```

**Query Timeout**:
```json
{
  "jsonrpc": "2.0",
  "id": 9,
  "error": {
    "code": -32004,
    "message": "Query execution timeout",
    "data": {
      "detail": "SPARQL query exceeded 30 second timeout",
      "timeout_seconds": 30,
      "error_type": "TimeoutError"
    }
  }
}
```

**Resource Not Found**:
```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "error": {
    "code": -32005,
    "message": "Resource not found",
    "data": {
      "detail": "Product with id 'dp-99999' not found",
      "uri": "product://dp-99999",
      "error_type": "NotFoundError"
    }
  }
}
```

**Validation Error** (invalid SPARQL):
```json
{
  "jsonrpc": "2.0",
  "id": 11,
  "error": {
    "code": -32603,
    "message": "Query validation failed",
    "data": {
      "detail": "SPARQL query contains forbidden operation: INSERT",
      "forbidden_operations": ["INSERT", "DELETE", "UPDATE"],
      "error_type": "ValidationError"
    }
  }
}
```

---

## Implementation Phases

**Note**: This is an LLM-assisted implementation with no manual timeline estimates. Phases indicate logical grouping and dependencies, not calendar time.

### Phase 1: Foundation & Token Management

**Database Schema**:
- Create migration: `src/backend/alembic/versions/xxxx_add_mcp_tokens.py`
- Table: `mcp_tokens` (id, token_hash, name, description, created_by, created_at, last_used_at, expires_at, is_active, scopes)

**Token Manager**:
- File: `src/backend/src/controller/mcp_token_manager.py`
- Methods: `generate_token()`, `validate_token()`, `check_scope()`, `revoke_token()`, `list_tokens()`
- Security: bcrypt hashing, show plaintext only once, 90-day default expiration

**Token Management API** (Backend only, no UI initially):
- File: `src/backend/src/routes/mcp_token_routes.py`
- Endpoints:
  - `POST /api/mcp-tokens` - Create token (admin/user)
  - `GET /api/mcp-tokens` - List tokens (admin sees all, users see own)
  - `DELETE /api/mcp-tokens/{id}` - Revoke token
- Note: React UI for token management deferred to future enhancements; use API directly or via curl/Postman initially

### Phase 2: MCP Protocol Core

**Tool Registry**:
- File: `src/backend/src/mcp/tool_registry.py`
- Class: `MCPToolRegistry` with methods for registering tools, listing available tools, and executing with scope checks
- Tool definition includes: name, description, JSON Schema for parameters, required scope

**Resource Provider**:
- File: `src/backend/src/mcp/resource_provider.py`
- Class: `MCPResourceProvider` with handler registration, resource listing, and URI-based reading
- Resource definition includes: URI scheme, name, description, MIME type

**MCP Server**:
- File: `src/backend/src/mcp/server.py`
- Class: `MCPServer` implementing JSON-RPC 2.0 handlers:
  - `initialize` - Handshake and capability negotiation
  - `tools/list` - Return available tools filtered by token scopes
  - `tools/call` - Execute tool with parameter validation
  - `resources/list` - Return available resources
  - `resources/read` - Read resource content by URI
- Authentication middleware: Extract Bearer token, validate, check scopes, update last_used_at

### Phase 3: Adapters

**Base Adapter**:
- File: `src/backend/src/mcp/adapters/base.py`
- Abstract class: `MCPAdapter` with `register_tools()` and `register_resources()` methods

**Data Products Adapter**:
- File: `src/backend/src/mcp/adapters/data_products.py`
- Wraps: `DataProductsManager`
- Tool: `query_data_products` (calls `manager.list_products()` with filters)
- Resource: `product://{id}` (calls `manager.get_product()`)

**Semantic Models Adapter**:
- File: `src/backend/src/mcp/adapters/semantic_models.py`
- Wraps: `SemanticModelsManager`
- Tools:
  - `execute_sparql_query` (calls `manager.query()` with existing validation)
  - `search_glossary_terms` (calls `manager.search_concepts()`)
  - `get_concept_hierarchy` (calls `manager.get_concept_hierarchy()`)
  - `get_concept_neighbors` (calls `manager.get_neighbors()`)
- Resources: `term://{uri}`, `taxonomy://{name}`

**Data Contracts Adapter**:
- File: `src/backend/src/mcp/adapters/data_contracts.py`
- Wraps: `DataContractsManager`
- Tool: `query_data_contracts`
- Resource: `contract://{id}`

**Search Adapter**:
- File: `src/backend/src/mcp/adapters/search.py`
- Wraps: `SearchManager`
- Tool: `global_search`
- Resource: `search://index`

### Phase 4: Integration & Configuration

**Startup Integration**:
- Modify: `src/backend/src/app.py`
- Add MCP server initialization in `startup_event()`:
  - Create `MCPTokenManager`, `MCPToolRegistry`, `MCPResourceProvider`
  - Instantiate all adapters wrapping existing managers from `app.state`
  - Register adapters with registry and provider
  - Create `MCPServer` instance
  - Store in `app.state.mcp_server` and `app.state.mcp_token_manager`

**Configuration**:
- Modify: `src/backend/src/common/config.py`
- Add settings: `MCP_ENABLED`, `MCP_PORT`, `MCP_TOKEN_SECRET`, `MCP_MAX_QUERY_TIMEOUT`, `MCP_MAX_RESULTS_LIMIT`
- Modify: `src/app.yaml`
- Add environment variables and expose port 8001

**Dependencies**:
- Add to `src/backend/requirements.txt`: `sse-starlette>=1.6.0` (for future SSE support)

### Phase 5: Production Hardening

**Error Handling**:
- File: `src/backend/src/mcp/middleware.py`
- Implement JSON-RPC 2.0 error codes:
  - `-32001`: Authentication failed
  - `-32002`: Authorization failed (scope)
  - `-32003`: Rate limit exceeded
  - `-32004`: Query timeout
  - `-32005`: Resource not found

**Logging & Auditing**:
- File: `src/backend/src/mcp/logging.py`
- Structured logs for: tool calls, resource reads, auth failures, scope violations
- Integration with existing `AuditManager`

**Testing**:
- Unit tests: Token manager, tool registry, adapters
- Integration tests: End-to-end MCP protocol flows
- Security tests: Scope enforcement, token expiration, query validation

## Security

### Token Security
- Bcrypt hashing for stored tokens
- Plaintext shown only once at creation
- Configurable expiration (default 90 days)
- Immediate revocation via `is_active` flag
- Audit log all token usage

### Query Safety (SPARQL)
- Reuse existing `SPARQLQueryValidator` from `SemanticModelsManager`
- Enforce 30-60s timeout
- Cap results at 100-1000 rows
- Read-only queries only (SELECT, ASK, DESCRIBE, CONSTRUCT)

### Scope Enforcement
- Check required scope before every tool/resource access
- Return JSON-RPC error code `-32002` if scope missing
- Log all scope violations for security monitoring

## Critical Files to Review Before Implementation

1. `/Users/lars.george/Documents/dev/dbapp/ucapp/src/backend/src/controller/semantic_models_manager.py`
   - SPARQL query execution, validation, timeout logic to reuse

2. `/Users/lars.george/Documents/dev/dbapp/ucapp/src/backend/src/controller/data_products_manager.py`
   - Product filtering and query patterns

3. `/Users/lars.george/Documents/dev/dbapp/ucapp/src/backend/src/common/authorization.py`
   - Permission checking pattern to follow for token scope validation

4. `/Users/lars.george/Documents/dev/dbapp/ucapp/src/backend/src/common/search_interfaces.py`
   - SearchableAsset interface pattern for adapter design

5. `/Users/lars.george/Documents/dev/dbapp/ucapp/src/backend/src/utils/startup_tasks.py`
   - Manager initialization pattern for MCP server startup

## Success Criteria

**Functional**:
- AI assistants can query data products with filters (status, domain, tags)
- AI assistants can execute safe SPARQL queries
- AI assistants can search contracts and glossary terms
- AI assistants can navigate concept hierarchies
- Token management works for creating, listing, and revoking tokens

**Performance**:
- Authentication: <50ms
- Simple queries: <500ms
- Complex SPARQL: timeout after 30-60s
- Overall error rate: <1%

**Security**:
- All tokens hashed and stored securely
- Scopes enforced on all operations
- SPARQL queries validated for safety
- All MCP operations audit logged

## Future Enhancements

1. **Token Management UI**: Admin interface in React frontend for token lifecycle (create, view, revoke tokens with visual scope selection)
2. **SSE Transport**: Add Server-Sent Events support for streaming responses
3. **Rate Limiting**: Implement per-token rate limits (e.g., 100 requests/minute)
4. **Advanced Scopes**: Fine-grained scopes like `data-products:write`, `contracts:create`
5. **MCP Prompts**: Expose pre-configured prompts/templates for common queries
6. **Monitoring Dashboard**: Real-time metrics for MCP usage, performance, errors
