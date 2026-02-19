# MDM Demo Data

This directory contains demo data for testing the Master Data Management (MDM) feature.

## Files

### SQL DDL Files

Run these files in order on your Databricks workspace:

1. **`01_create_schema.sql`**
   - Creates the `app_demo_data` catalog and `mdm_demo` schema
   - Run this first

2. **`02_create_customer_master.sql`**
   - Creates the `customer_master` table (golden records)
   - Inserts 100 sample master customer records
   - This is the MDM master table

3. **`03_create_crm_customers.sql`**
   - Creates the `crm_customers` table (source system)
   - Inserts 50 sample CRM customer records including:
     - Exact matches to master records
     - Fuzzy matches (name variations, typos, formatting differences)
     - Duplicates within CRM (same person, multiple records)
     - New records not in master
     - Records with data quality issues

### ODCS Data Contract Files

4. **`customer_master_contract.yaml`**
   - ODCS contract for the Customer Master table
   - Defines the golden record schema
   - Contains MDM configuration in `customProperties`:

   | Property | Description |
   |----------|-------------|
   | `mdmEntityType` | "customer" - entity being managed |
   | `mdmRole` | "master" - this is the golden record |
   | `mdmMatchingRules` | Rules for finding matches (email exact, name fuzzy, etc.) |
   | `mdmSurvivorshipRules` | How to merge conflicting values (most_recent, most_trusted, most_complete) |
   | `mdmConfidenceThreshold` | 0.80 - minimum confidence for a valid match |
   | `mdmReviewThreshold` | 0.90 - matches above this go to review |
   | `mdmAutoMergeAbove` | 0.95 - high-confidence matches auto-merge |

5. **`crm_customers_contract.yaml`**
   - ODCS contract for the CRM Customers source table
   - Defines the CRM system schema
   - Contains MDM source configuration in `customProperties`:

   | Property | Description |
   |----------|-------------|
   | `mdmEntityType` | "customer" - same entity type as master |
   | `mdmRole` | "source" - this is a source system |
   | `mdmSourceSystem` | "CRM" - name of the source |
   | `mdmSourcePriority` | 2 - priority for survivorship rules |
   | `mdmKeyColumn` | "crm_customer_id" - unique identifier |
   | `mdmColumnMappings` | Pre-defined source→master column mappings |
   | `mdmTransformations` | Data transformations (concat names, normalize phone) |
   | `mdmExclusionRules` | Rules to exclude test/null records |
   | `mdmChangeDetection` | Incremental change detection config |

   **Note**: When linking a source contract, the UI automatically loads `mdmKeyColumn`, 
   `mdmSourcePriority`, and `mdmColumnMappings` from the contract's customProperties!

## Unity Catalog Locations

- **Catalog**: `app_demo_data`
- **Schema**: `mdm_demo`
- **Master Table**: `app_demo_data.mdm_demo.customer_master`
- **Source Table**: `app_demo_data.mdm_demo.crm_customers`

## Usage

### 1. Set up the database

```sql
-- Run in Databricks SQL or a notebook
-- Execute files in order:
-- 01_create_schema.sql
-- 02_create_customer_master.sql
-- 03_create_crm_customers.sql
```

### 2. Load the contracts into the app

The ODCS contracts can be loaded into the application through:
- The Data Contracts UI (import YAML)
- Direct API call to `/api/data-contracts`
- Demo data loader during startup

### 3. Create an MDM Configuration

In the MDM UI (`/master-data`):

1. Click **"New MDM Configuration"**
2. Fill in the form:
   | Field | Value | Description |
   |-------|-------|-------------|
   | Name | `Customer MDM` | A descriptive name |
   | Master Contract | `Customer Master Contract` | Select from dropdown |
   | Entity Type | `Customer` | The type of entity being managed |
   | Matching Rules | (pre-populated from contract) | Rules for identifying matches |
   | Survivorship Rules | (pre-populated from contract) | Rules for merging values |
3. Click **"Create"**

### 4. Link Source Contract (CRM)

After creating the configuration:

1. Click on the newly created **"Customer MDM"** configuration
2. In the Sources section, click **"Link Source"**
3. Fill in the dialog:

   | Field | Value | Description |
   |-------|-------|-------------|
   | **Source Contract** | `CRM Customers Contract` | Select from dropdown |
   | **Key Column** | `crm_id` | The unique identifier in the source |
   | **Priority** | `1` | Higher = preferred for survivorship |

4. Add **Column Mappings** (Source → Master):

   **Option A: Auto-load from Contract** (Recommended)
   - If the source contract has `mdmColumnMappings` in its `customProperties`, 
     mappings are automatically loaded when you select the contract!
   - The CRM Customers contract includes pre-defined mappings.

   **Option B: Suggest Mappings**
   - Click the **"Suggest"** button (magic wand icon)
   - Uses fuzzy string matching to find similar column names
   - Review and adjust the suggestions as needed

   **Option C: Manual Entry**
   - Click the source column dropdown, select the CRM column
   - Click the master column dropdown, select the corresponding master column
   - Click the **+** button to add the mapping
   
   Required mappings (if not auto-loaded):
   | Source (CRM) | Master | Notes |
   |--------------|--------|-------|
   | `email_address` | `email` | Different column names |
   | `phone_number` | `phone` | Different column names |
   | `street_address` | `address_line1` | Different column names |
   | `state_code` | `state` | Different column names |
   | `zip` | `postal_code` | Different column names |
   | `country_code` | `country` | Different column names |
   | `industry_type` | `industry` | Different column names |

5. Click **"Link Source"**

### 5. Run MDM Matching

1. With the configuration selected, click **"Start Matching"**
2. The matching job runs in the background (check Jobs in Settings)
3. When complete, match candidates appear in the "Match Runs" table
4. Review match candidates by confidence score:
   - **Auto-merge** (≥0.95): High confidence matches
   - **Review** (0.8-0.95): Require human verification
   - **Reject** (<0.8): Low confidence, likely false positives

### 6. Review and Approve Matches

1. Click **"Create Review"** on a completed match run
2. Enter the reviewer email (data steward)
3. A review request is created in **Asset Review** (`/data-asset-reviews`)
4. The reviewer can:
   - **Approve** individual matches
   - **Reject** false positives
   - **Edit** match details
5. Click **"Merge Approved"** to merge accepted matches into master

## Expected Match Results

When running MDM matching, you should see approximately:

| Category | Count | Description |
|----------|-------|-------------|
| Exact Matches | ~10 | Email matches (CRM-1001 to CRM-1010) |
| Fuzzy Matches | ~15 | Name/address variations |
| Internal Duplicates | ~5 | CRM duplicates (John Smith variants) |
| New Records | ~15 | Not in master (CRM-1019 onwards) |
| Data Quality Issues | ~2 | Test/null records to exclude |

## Matching Examples

### Exact Match
- **Master**: GOLD-001, John Smith, john.smith@email.com
- **CRM**: CRM-1001, John Smith, john.smith@email.com
- **Result**: High confidence exact match on email

### Fuzzy Match (Name Variation)
- **Master**: GOLD-003, Michael Brown, mbrown@gmail.com
- **CRM**: CRM-1003, Mike Brown, mbrown@gmail.com
- **Result**: Fuzzy match on name, exact on email

### Fuzzy Match (Formatting)
- **Master**: GOLD-009, James Taylor, jtaylor@finance.com
- **CRM**: CRM-1009, Jim Taylor, james.taylor@premier-financial.com
- **Result**: Fuzzy match requiring review

### Duplicate Detection
- **CRM**: CRM-1001, CRM-1016, CRM-1017, CRM-1041, CRM-1042
- **Result**: All map to same master record GOLD-001

### New Record
- **CRM**: CRM-1019, Alexander Thompson, athompson@newtech.io
- **Result**: No match in master, flagged as new record

