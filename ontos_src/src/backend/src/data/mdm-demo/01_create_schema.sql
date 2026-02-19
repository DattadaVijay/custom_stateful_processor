-- ============================================================================
-- MDM Demo: Schema Setup
-- ============================================================================
-- This script creates the catalog and schema for the MDM demo.
-- Run this first before creating the tables.
-- ============================================================================

-- Create the catalog (if not exists)
CREATE CATALOG IF NOT EXISTS app_demo_data;

-- Use the catalog
USE CATALOG app_demo_data;

-- Create the schema for MDM demo
CREATE SCHEMA IF NOT EXISTS mdm_demo
COMMENT 'Schema for Master Data Management demo tables';

-- Grant permissions (adjust principals as needed)
-- GRANT ALL PRIVILEGES ON SCHEMA app_demo_data.mdm_demo TO `data-engineers`;
-- GRANT SELECT ON SCHEMA app_demo_data.mdm_demo TO `data-consumers`;

