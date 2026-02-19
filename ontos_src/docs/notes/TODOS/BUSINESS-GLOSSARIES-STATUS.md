# Business Glossary / Knowledge System - Implementation Status

**Last Updated:** January 30, 2026

## Overview

The Business Glossary feature has been re-implemented as a unified **Knowledge System** that supports:
- **Custom Glossaries** - User-created business term definitions
- **Taxonomies** - Hierarchical classification systems
- **Ontologies** - Full RDF/OWL semantic models

All knowledge artifacts are stored natively as **RDF triples** in the `rdf_triples` database table, enabling full semantic fidelity while supporting SQL-based querying.

## Architecture

### Data Storage (RDF-Native Approach)

```
┌─────────────────────────────────────────────────────────────┐
│                     rdf_triples table                       │
│  (subject, predicate, object, context, created_at, ...)    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              SemanticModelsManager                          │
│  - In-memory RDFLib graph for SPARQL queries                │
│  - CRUD operations on collections and concepts              │
│  - Import/export RDF formats (Turtle, RDF/XML)              │
└─────────────────────────────────────────────────────────────┘
```

### Named Graphs (Contexts)

| Context Pattern | Purpose |
|----------------|---------|
| `urn:meta:sources` | Collection metadata (hidden from Settings UI) |
| `urn:semantic-links` | App-created semantic links between entities |
| `urn:glossary:<name>` | Custom glossary terms |
| `databricks_ontology` | Bundled Databricks vocabulary |
| `ontos-ontology` | Application-specific vocabulary |
| `odcs-ontology` | Open Data Contract Standard |

### Application Ontology (`ontos-ontology.ttl`)

Extended with governance classes:

| Class | Purpose |
|-------|---------|
| `ontos:KnowledgeCollection` | Container for concepts (glossary, taxonomy, ontology) |
| `ontos:Ownership` | Named reification for concept ownership with roles |

Key properties:
- `ontos:collectionType` - glossary | taxonomy | ontology
- `ontos:scopeLevel` - enterprise | domain | department | team | project | external
- `ontos:parentCollection` - Hierarchical organization
- `ontos:isEditable` - Whether concepts can be modified
- `ontos:status` - Lifecycle state
- `ontos:hasOwner` - Links to Ownership instances

## Backend Implementation

### API Endpoints (`/api/knowledge/...`)

#### Collections
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/collections` | List all collections (with hierarchy option) |
| GET | `/collections/{iri}` | Get collection details |
| POST | `/collections` | Create new collection |
| PATCH | `/collections/{iri}` | Update collection |
| DELETE | `/collections/{iri}` | Delete collection |
| GET | `/collections/{iri}/export` | Export as Turtle or RDF/XML |
| POST | `/collections/{iri}/import` | Import RDF content |

#### Concepts
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/concepts/{iri}` | Get concept details |
| POST | `/concepts` | Create new concept |
| PATCH | `/concepts/{iri}` | Update concept |
| DELETE | `/concepts/{iri}` | Delete concept |

#### Governance
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/concepts/{iri}/owners` | Add owner with role |
| DELETE | `/concepts/{iri}/owners/{email}` | Remove owner |
| POST | `/concepts/{iri}/submit-review` | Submit for review |
| POST | `/concepts/{iri}/publish` | Publish concept |
| POST | `/concepts/{iri}/certify` | Certify concept |
| POST | `/concepts/{iri}/deprecate` | Deprecate concept |
| POST | `/concepts/{iri}/archive` | Archive concept |
| POST | `/concepts/{iri}/promote` | Promote to parent collection |
| POST | `/concepts/{iri}/migrate` | Migrate to different collection |

### Manager Methods (`SemanticModelsManager`)

**Collection Operations:**
- `get_collections()` / `get_collection(iri)`
- `create_collection()` / `update_collection()` / `delete_collection()`
- `get_collections_with_hierarchy()`
- `get_collection_ancestors()` / `get_collection_descendants()`
- `export_collection_as_turtle()` / `export_collection_as_rdfxml()`
- `import_rdf_to_collection()`

**Concept Operations:**
- `get_concept()` / `create_concept()` / `update_concept()` / `delete_concept()`
- `add_concept_owner()` / `remove_concept_owner()`
- `update_concept_status()` (lifecycle transitions)
- `promote_concept()` / `migrate_concept()`

## Frontend Implementation

### Project Structure

```
src/frontend/src/
├── views/
│   └── knowledge.tsx              # Main view (tab-based layout)
│
├── components/knowledge/
│   ├── index.ts                   # Exports
│   ├── collections-tab.tsx        # Collections DataTable
│   ├── concepts-tab.tsx           # Concept tree + detail panel
│   ├── graph-tab.tsx              # Knowledge graph visualization
│   ├── collection-tree.tsx        # Hierarchical collection tree
│   ├── collection-editor-dialog.tsx
│   ├── concept-editor-dialog.tsx
│   ├── node-links-panel.tsx       # Display concept links
│   ├── link-editor-dialog.tsx     # Create/edit links
│   └── promotion-dialog.tsx       # Promote/migrate concepts
│
└── components/semantic-models/
    └── knowledge-graph.tsx        # Cytoscape-based graph viz
```

### UI Features

**Collections Tab:**
- DataTable with sorting, filtering, pagination
- Columns: Name, Type, Scope, Concept Count, Source
- Actions: Edit, Export (Turtle/RDF-XML), Delete
- Create Collection button

**Concepts Tab:**
- Left panel: Searchable concept tree
  - Filter by source (collapsible checkboxes)
  - Group by source toggle
  - Show properties toggle
  - Group by domain toggle
- Right panel: Concept detail view
  - Definition, IRI, type badge, status badge
  - Parent/child concepts (clickable)
  - Domain/Range for properties
  - Synonyms, examples
  - Business owners with roles
  - Incoming/outgoing links panel
  - Edit/Delete buttons (for editable collections)

**Graph Tab:**
- Interactive Cytoscape.js visualization
- Node filtering by source
- Click to select and navigate

### State Management

- `useGlossaryPreferencesStore` (Zustand) - Persists UI preferences:
  - `hiddenSources` - Filtered sources
  - `groupBySource` - Tree grouping mode
  - `showProperties` - Include properties in view
  - `groupByDomain` - Group properties under domain classes
  - `isFilterExpanded` - Filter panel state

## Lifecycle States

```
draft → under_review → approved → published → certified → deprecated → archived
                                      ↓
                              (certification_expires_at)
```

| State | Editable | Description |
|-------|----------|-------------|
| `draft` | Yes | Initial creation, can be freely modified |
| `under_review` | No | Submitted for approval via DataAssetReviewManager |
| `approved` | No | Review passed, ready for publication |
| `published` | No | Available for use, tracked timestamps |
| `certified` | No | Formally certified, has expiration date |
| `deprecated` | No | Marked for retirement, still visible |
| `archived` | No | Hidden from normal views, retained for history |

## Demo Data

Demo collections are loaded via `demo_data.sql`:
- Enterprise Glossary (enterprise scope)
- Demo Business Concepts (enterprise scope, child of Enterprise Glossary)
- Customer Domain Glossary (domain scope, child of Enterprise)
- IoT Domain Glossary (domain scope, child of Enterprise)
- Finance Domain Glossary (domain scope, child of Enterprise)

## What's Working

- [x] RDF-native storage for all semantic data
- [x] Collection CRUD with hierarchy support
- [x] Concept CRUD with full metadata
- [x] Ownership management (multiple owners with roles)
- [x] Lifecycle state management
- [x] Promotion/migration between collections
- [x] Export as Turtle/RDF-XML
- [x] Import RDF files into collections
- [x] Tab-based UI (Collections, Concepts, Graph)
- [x] Concept tree with search and filtering
- [x] Concept detail panel with links display
- [x] Graph visualization (Cytoscape.js)
- [x] Auto-registration of imported RDF sources as collections
- [x] Settings UI hides system contexts
- [x] Dark mode support for badges

## Pending / TODO

### High Priority
- [ ] Full DataAssetReviewManager integration (submit-review callback handling)
- [ ] Approval workflow callbacks (on approve → publish transition)

### Medium Priority
- [ ] Import dialog UI (currently API-only)
- [ ] Version increment on publish
- [ ] Link editor dialog fully wired to API

### Low Priority
- [ ] Demote operation (opposite of promote)
- [ ] Collection-scoped concept listing in UI
- [ ] Missing i18n keys for non-English locales
- [ ] Unit tests for knowledge components
- [ ] E2E tests for knowledge workflows

## Files Reference

### Backend
- `src/backend/src/controller/semantic_models_manager.py` - Main business logic
- `src/backend/src/routes/semantic_models_routes.py` - API endpoints
- `src/backend/src/models/ontology.py` - Pydantic models
- `src/backend/src/db_models/rdf_triples.py` - SQLAlchemy model
- `src/backend/src/repositories/rdf_triples_repository.py` - Database access
- `src/backend/src/data/taxonomies/ontos-ontology.ttl` - Application vocabulary

### Frontend
- `src/frontend/src/views/knowledge.tsx` - Main view
- `src/frontend/src/components/knowledge/*.tsx` - UI components
- `src/frontend/src/types/ontology.ts` - TypeScript types
- `src/frontend/src/stores/glossary-preferences-store.ts` - Zustand store
- `src/frontend/src/i18n/locales/en/semantic-models.json` - Translations
