# MDM Demo Data - Vendor Master

This directory contains demo data for testing the Master Data Management (MDM) feature with **Vendor** entities.

## Use Case

This demo showcases vendor/supplier MDM matching between two systems:
- **SAP ERP** - Primary master system with vendor account data
- **Contract Management** - Secondary source with supplier contract data

### Key MDM Challenges Demonstrated

1. **Name Variations**: Company suffixes (Ltd, Limited, GmbH, B.V., AB, Inc)
2. **Fuzzy Matching**: Abbreviations (HP vs Hewlett-Packard, IBM Deutschland)
3. **Country Code Mapping**: SAP uses 2-letter codes (DE, SE) vs full names (Germany, Sweden)
4. **Special Characters**: Handling of ü, ö, ä, æ in vendor names
5. **Cross-Reference IDs**: Different vendor IDs between systems

## Files

### SQL DDL Files

Run these files in order on your Databricks workspace:

1. **`01_create_schema.sql`**
   - Creates the `app_demo_data` catalog and `mdm_vendor_demo` schema
   - Run this first

2. **`02_create_vendor_master.sql`**
   - Creates the `vendor_master` table (SAP ERP golden records)
   - Inserts 50 sample SAP vendor records
   - This is the MDM master table

3. **`03_create_contract_vendors.sql`**
   - Creates the `contract_vendors` table (Contract Management source)
   - Inserts 40 sample contract vendor records including:
     - Exact matches to SAP vendor records
     - Fuzzy matches (name variations, abbreviations)
     - New vendors not in SAP master
     - Records with data quality issues

### ODCS Data Contract Files

4. **`vendor_master_contract.yaml`**
   - ODCS contract for the Vendor Master table (SAP ERP)
   - Defines the golden record schema
   - Contains MDM configuration in `customProperties`:

   | Property | Description |
   |----------|-------------|
   | `mdmEntityType` | "vendor" - entity being managed |
   | `mdmRole` | "master" - this is the golden record |
   | `mdmMatchingRules` | Rules for finding matches (name fuzzy, country mapping) |
   | `mdmSurvivorshipRules` | How to merge conflicting values |
   | `mdmConfidenceThreshold` | 0.80 - minimum confidence for a valid match |
   | `mdmReviewThreshold` | 0.90 - matches above this go to review |
   | `mdmAutoMergeAbove` | 0.95 - high-confidence matches auto-merge |

5. **`contract_vendors_contract.yaml`**
   - ODCS contract for the Contract Vendors source table
   - Defines the contract management system schema
   - Contains MDM source configuration in `customProperties`:

   | Property | Description |
   |----------|-------------|
   | `mdmEntityType` | "vendor" - same entity type as master |
   | `mdmRole` | "source" - this is a source system |
   | `mdmSourceSystem` | "CONTRACT_MGMT" - name of the source |
   | `mdmSourcePriority` | 2 - priority for survivorship rules |
   | `mdmKeyColumn` | "external_vendor_id" - unique identifier |
   | `mdmColumnMappings` | Pre-defined source→master column mappings |
   | `mdmTransformations` | Data transformations (country code normalization) |

## Unity Catalog Locations

- **Catalog**: `app_demo_data`
- **Schema**: `mdm_vendor_demo`
- **Master Table**: `app_demo_data.mdm_vendor_demo.vendor_master`
- **Source Table**: `app_demo_data.mdm_vendor_demo.contract_vendors`

## Usage

### 1. Set up the database

```sql
-- Run in Databricks SQL or a notebook
-- Execute files in order:
-- 01_create_schema.sql
-- 02_create_vendor_master.sql
-- 03_create_contract_vendors.sql
```

### 2. Create an MDM Configuration

In the MDM UI (`/master-data`):

1. Click **"New MDM Configuration"**
2. Fill in the form:
   | Field | Value | Description |
   |-------|-------|-------------|
   | Name | `Vendor MDM` | A descriptive name |
   | Master Contract | `Vendor Master Contract (SAP)` | Select from dropdown |
   | Entity Type | `Vendor` | The type of entity being managed |
3. Click **"Create"**

### 3. Link Source Contract (Contract Management)

After creating the configuration:

1. Click on the newly created **"Vendor MDM"** configuration
2. In the Sources section, click **"Link Source"**
3. Fill in the dialog:

   | Field | Value | Description |
   |-------|-------|-------------|
   | **Source Contract** | `Contract Vendors Contract` | Select from dropdown |
   | **Key Column** | `external_vendor_id` | The unique identifier in the source |
   | **Priority** | `2` | Lower priority than SAP master |

4. Column mappings will auto-load from contract's `mdmColumnMappings`

## Expected Match Results

When running MDM matching, you should see approximately:

| Category | Count | Description |
|----------|-------|-------------|
| Exact Matches | ~8 | Name exact matches (Microsoft, IBM, Capgemini) |
| Fuzzy Matches | ~12 | Name variations (HP/Hewlett-Packard, abbreviations) |
| Country Mapping | ~10 | Matches requiring country code→name mapping |
| New Records | ~8 | Not in SAP master (new contract vendors) |
| Data Quality Issues | ~2 | Missing/test records to exclude |

## Matching Examples

### Exact Match
- **SAP**: V-001, Microsoft Ireland Operations Ltd, IE
- **Contract**: CNT-001, Microsoft Ireland Operations Ltd., Ireland
- **Result**: High confidence exact match on normalized name

### Fuzzy Match (Abbreviation)
- **SAP**: V-003, Hewlett-Packard Sverige AB, SE
- **Contract**: CNT-003, HP PPS Sverige AB, Sweden
- **Result**: Fuzzy match requiring review (HP = Hewlett-Packard)

### Country Mapping Match
- **SAP**: V-005, IBM Deutschland GmbH, DE
- **Contract**: CNT-005, IBM Deutschland GmbH, Germany
- **Result**: Match with country code mapping (DE → Germany)

### Name + Country Match
- **SAP**: V-010, Basware AB, SE
- **Contract**: CNT-010, Basware AB, Sweden
- **Result**: Exact name match with country verification

### New Record (No Match)
- **Contract**: CNT-035, New Supplier Inc, United States
- **Result**: No match in SAP master, flagged as new vendor

## Recommended MDM Match Rules

| Rule | Match On | Confidence |
|------|----------|------------|
| Rule 1: Exact Match | vendor_name = vendor_name (exact) | 100% |
| Rule 2: Fuzzy Name | SOUNDEX or Levenshtein < 3 | 85-95% |
| Rule 3: Name + Country | Similar name + country mapping | 90-95% |
| Rule 4: Vendor ID Cross-Ref | external_vendor_id in SAP vendor_id | 95-100% |

## Reference Data

The demo includes a country code mapping table for matching:

| SAP Code | Contract Name |
|----------|---------------|
| DE | Germany |
| SE | Sweden |
| NL | Netherlands |
| GB | United Kingdom |
| IE | Ireland |
| US | United States |
| FR | France |
| CH | Switzerland |
| AT | Austria |
| NO | Norway |
| DK | Denmark |
| FI | Finland |

