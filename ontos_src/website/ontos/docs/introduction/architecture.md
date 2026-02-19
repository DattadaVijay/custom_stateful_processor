---
sidebar_position: 2
---
# Architecture

This architecture diagram below illustrates the Ontos App, a full-stack application hosted within the Databricks Platform. It showcases a modern decoupled architecture where a React-based frontend communicates through a Python backend to orchestrate various Databricks services and external data stores.

![Ontos Architecture](/img/architecture.png)

## Core Components

**Frontend (Ontos App UI):** Built with React.js and TypeScript, providing a type-safe, interactive interface for end-users.

**Backend (API Layer):** Utilizes FastAPI, a high-performance Python framework. This layer acts as the central orchestrator, handling requests from the UI and routing them to internal or external services.

## Integration & Data Flow

The FastAPI backend interacts with three primary categories of services:

### Persistence & Analytics

- _OLTP (Online Transactional Processing):_ Lakebase PostgreSQL used for operational application data.

- _DBSQL (Databricks SQL):_ Used for running high-performance analytics and BI queries directly on the lakehouse.

### Administrative & Identity Services

- _Account REST API:_ Manages account-level configurations.

- _ID Federation:_ Handles authentication and identity management, ensuring secure access across the platform.

### Workspace & Metadata Management

- _Workspace REST API:_ The gateway for interacting with Databricks-specific workspace components.

- _Workspace Assets:_ Management of notebooks, libraries, and files.

- _Lakeflow Jobs:_ Orchestration of data engineering pipelines and automated workflows.

- _Unity Catalog:_ The governance layer used for centralized access control, auditing, and lineage across the Databricks metastore.

:rocket: Having now gained a clear understanding of the architecture, let’s move on to the main concepts and definitions for Ontos building blocks. 
