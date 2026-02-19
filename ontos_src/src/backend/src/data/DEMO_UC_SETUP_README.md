# Unity Catalog Demo Data Setup

This directory contains SQL scripts to set up demo data in Unity Catalog for demonstrating Ontos capabilities.

## Quick Start

1. **Run the Setup Script**
   - Open Databricks SQL Editor
   - Load `demo_uc_setup.sql`
   - Execute the entire script

2. **Demo Ontos Features**
   - UC Bulk Import
   - Dataset inference
   - Data Product/Contract management

## What Gets Created

### Catalogs

| Catalog | Description |
|---------|-------------|
| `demo_enterprise` | Main enterprise catalog with business domains |
| `demo_analytics` | Analytics and reporting catalog |

### Schemas & Tables

```
demo_enterprise/
├── sales/
│   ├── customers        (10 rows, PII)
│   ├── products         (10 rows)
│   ├── orders           (10 rows, PII)
│   └── order_items      (13 rows)
├── sales_staging/
│   └── raw_orders       (4 rows, bronze)
├── finance/
│   ├── invoices         (8 rows)
│   ├── payments         (4 rows)
│   └── gl_accounts      (14 rows)
├── hr/
│   ├── departments      (10 rows)
│   └── employees        (10 rows, PII/Salary)
└── reference/
    ├── countries        (10 rows)
    └── product_categories (10 rows)

demo_analytics/
├── reporting/
│   ├── v_sales_summary      (view)
│   ├── v_customer_360       (view)
│   ├── v_product_performance (view)
│   └── v_revenue_by_region  (view)
└── data_science/
    └── v_customer_features  (view)
```

## Tags Applied

The demo applies governance tags to enable Ontos bulk import:

### Tag Taxonomy

| Tag Key | Purpose | Example Values |
|---------|---------|----------------|
| `data-domain` | Business domain | sales, finance, hr, analytics |
| `data-product-name` | Data Product grouping | Customer 360, Order Management |
| `data-contract-name` | Data Contract reference | customer-master, order-transactions |
| `data-quality-tier` | Quality level | bronze, silver, gold |
| `pii` | Contains PII | true, false, derived |
| `data-owner` | Ownership | team@company.com |
| `sla-freshness-hours` | Freshness SLA | 1, 24, 168 |

### Tagged Entities Summary

| Entity Type | Tag | Discovered Values |
|-------------|-----|-------------------|
| **Data Contracts** | `data-contract-name` | customer-master, product-catalog, order-transactions, billing-data, payment-transactions, chart-of-accounts, org-structure, employee-master, reference-data |
| **Data Products** | `data-product-name` | Customer 360, Product Catalog, Order Management, Accounts Receivable, Financial Reporting, Organizational Data, Workforce Analytics, Master Data, Executive Dashboards, Product Analytics, ML Features |
| **Data Domains** | `data-domain` | sales, finance, hr, reference, analytics, data-science |

## Ontos UC Bulk Import Configuration

When configuring the UC Bulk Import job in Ontos, use these entity patterns:

### Data Contracts Pattern
```json
{
  "entity_type": "contract",
  "enabled": true,
  "filter_source": "key",
  "filter_pattern": "^data-contract-name$",
  "key_pattern": "^data-contract-name$",
  "value_extraction_source": "value",
  "value_extraction_pattern": "^(.+)$"
}
```

### Data Products Pattern
```json
{
  "entity_type": "product",
  "enabled": true,
  "filter_source": "key",
  "filter_pattern": "^data-product-name$",
  "key_pattern": "^data-product-name$",
  "value_extraction_source": "value",
  "value_extraction_pattern": "^(.+)$"
}
```

### Data Domains Pattern
```json
{
  "entity_type": "domain",
  "enabled": true,
  "filter_source": "key",
  "filter_pattern": "^data-domain$",
  "key_pattern": "^data-domain$",
  "value_extraction_source": "value",
  "value_extraction_pattern": "^(.+)$"
}
```

### Complete Patterns Array (for Job Configuration)
```json
[
  {
    "entity_type": "contract",
    "enabled": true,
    "key_pattern": "^data-contract-name$",
    "value_extraction_source": "value",
    "value_extraction_pattern": "^(.+)$"
  },
  {
    "entity_type": "product",
    "enabled": true,
    "key_pattern": "^data-product-name$",
    "value_extraction_source": "value",
    "value_extraction_pattern": "^(.+)$"
  },
  {
    "entity_type": "domain",
    "enabled": true,
    "key_pattern": "^data-domain$",
    "value_extraction_source": "value",
    "value_extraction_pattern": "^(.+)$"
  }
]
```

## Demo Script

### Step 1: Setup Unity Catalog Data

1. Open Databricks SQL Editor
2. Run `demo_uc_setup.sql`
3. Verify with the queries at the end of the script

### Step 2: Verify Tags

```sql
-- See all tags that Ontos will discover
SELECT 
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE catalog_name LIKE 'demo%'
ORDER BY tag_name, tag_value;
```

### Step 3: Run Ontos UC Bulk Import

1. Navigate to Ontos Settings → Jobs
2. Create or run the "UC Bulk Import" job
3. Configure with the patterns above
4. Use these parameters:
   - `default_catalog`: `demo_enterprise`
   - `conflict_strategy`: `skip` (for re-runs)
   - `dry_run`: `true` (first run to preview)

### Step 4: Verify Import

After the job completes:

1. **Data Domains** - Check Ontos Data Domains view
2. **Data Contracts** - Check Ontos Data Contracts view
3. **Data Products** - Check Ontos Data Products view

### Step 5: Demo Dataset Inference

1. Navigate to Data Contracts or Data Products
2. Click "Infer from UC" or similar action
3. Select tables from `demo_enterprise` catalog
4. Show how schema, comments, and tags are imported

## Data Relationships

```
                    ┌─────────────────┐
                    │    countries    │
                    │   (reference)   │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   customers     │ │   employees     │ │    invoices     │
│    (sales)      │ │     (hr)        │ │   (finance)     │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         │                   ▼                   │
         │          ┌─────────────────┐          │
         │          │  departments    │          │
         │          │     (hr)        │          │
         │          └─────────────────┘          │
         │                                       │
         ▼                                       ▼
┌─────────────────┐                    ┌─────────────────┐
│     orders      │                    │    payments     │
│    (sales)      │                    │   (finance)     │
└────────┬────────┘                    └─────────────────┘
         │
         ▼
┌─────────────────┐
│   order_items   │───────────┐
│    (sales)      │           │
└─────────────────┘           ▼
                     ┌─────────────────┐
                     │    products     │
                     │    (sales)      │
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │product_categories│
                     │  (reference)    │
                     └─────────────────┘
```

## Cleanup

To remove all demo data:

```sql
DROP CATALOG IF EXISTS demo_enterprise CASCADE;
DROP CATALOG IF EXISTS demo_analytics CASCADE;
```

## Troubleshooting

### Tags Not Showing

If tags don't appear in `system.information_schema.*_tags`:
- Ensure you have `SELECT` permission on system tables
- Wait a few seconds for catalog refresh
- Try `REFRESH CATALOG demo_enterprise`

### Permission Errors

Required permissions:
- `CREATE CATALOG` on metastore
- `USE CATALOG` on created catalogs
- `CREATE SCHEMA` on catalogs
- `CREATE TABLE` on schemas

### Bulk Import Errors

If UC Bulk Import fails:
- Check Databricks job logs
- Verify Spark session can access system tables
- Ensure service principal has required permissions

## Notes

- All sample data is fictional
- PII data is clearly marked for governance demos
- Foreign key constraints demonstrate data modeling
- Tags follow a consistent taxonomy for discovery

