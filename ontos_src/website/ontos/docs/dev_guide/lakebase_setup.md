---
sidebar_position: 2
---

# Lakebase Instance
When deploying to production with Databricks Lakebase, the application uses **OAuth token authentication** instead of passwords.

### How It Works

- **Username detection:** Service principal username is auto-detected at runtime
- **Token generation:** OAuth tokens are automatically generated using the Databricks SDK
- **Token refresh:** Tokens refresh every 50 minutes (before 60-minute expiry)
- **Connection pooling:** Fresh tokens are automatically injected into database connections

### Setup Steps

#### 1. Create Lakebase Instance

Set up a new [Lakebase instance](https://docs.databricks.com/aws/en/oltp/instances/instance) and wait for it to start.

#### 2. Create Database (One-Time Setup)

Connect to your Lakebase instance:
```bash
psql "host=instance-xxx.database.cloud.databricks.com user=<your_email> dbname=postgres port=5432 sslmode=require"
Password: <paste OAuth token>
```

Create the database with open permissions:
```sql
-- Create the database
CREATE DATABASE "app_ontos";

-- Grant CREATE to PUBLIC so the app's service principal can create schemas
GRANT CREATE ON DATABASE "app_ontos" TO PUBLIC;
\q
```

**Why grant to PUBLIC?** The app's service principal is created automatically when the app first connects. By granting to `PUBLIC`, any authenticated role (including the future SP) can create schemas. This avoids needing to know the SP ID before deployment.

#### 3. Deploy Your App

```bash
databricks apps deploy <app-name>
```

On first startup, the app will:
1. Authenticate as its service principal using OAuth
2. Connect to the `app_ontos` database
3. Create the `app_ontos` schema (becomes schema owner automatically)
4. Set default privileges for future tables/sequences
5. Create all application tables

#### 4. Tighten Permissions (Optional)

After successful deployment, you can revoke the broad `PUBLIC` grant:
```sql
-- Get the SP ID from app logs, then run:
REVOKE CREATE ON DATABASE "app_ontos" FROM PUBLIC;
GRANT CREATE ON DATABASE "app_ontos" TO "<service_principal_uuid>";
```

### App Configuration (app.yaml)

Reference the Lakebase instance in your `app.yaml`:

```yaml
- name: "POSTGRES_HOST"
  valueFrom: "database"  # References your Lakebase instance resource
- name: "POSTGRES_PORT"
  value: "5432"
# POSTGRES_USER is auto-detected from service principal - do not set
- name: "POSTGRES_DB"
  value: "app_ontos"
- name: "POSTGRES_DB_SCHEMA"
  value: "app_ontos"
- name: "ENV"
  value: "PROD"  # Triggers OAuth mode (not LOCAL)
```

:::warning SECURITY NOTES

- The `PUBLIC` grant only allows schema creation in the app database
- Only authenticated Databricks principals can connect
- Service principal names are never hardcoded in configuration
- You can optionally tighten permissions after first deploy

:::




