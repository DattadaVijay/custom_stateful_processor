-- ============================================================================
-- MDM Vendor Demo: Schema Setup
-- ============================================================================
-- This script creates the catalog and schema for the Vendor MDM demo.
-- Run this first before creating the tables.
-- ============================================================================

-- Create the catalog (if not exists)
CREATE CATALOG IF NOT EXISTS app_demo_data;

-- Use the catalog
USE CATALOG app_demo_data;

-- Create the schema for Vendor MDM demo
CREATE SCHEMA IF NOT EXISTS mdm_vendor_demo
COMMENT 'Schema for Vendor Master Data Management demo tables';

-- Create country code reference table for matching
CREATE TABLE IF NOT EXISTS mdm_vendor_demo.country_code_mapping (
    iso_code STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2 country code',
    country_name STRING NOT NULL COMMENT 'Full country name',
    alternate_names ARRAY<STRING> COMMENT 'Alternative spellings/names'
)
USING DELTA
COMMENT 'Reference table for country code to name mapping';

-- Insert country mappings used in vendor data
INSERT INTO mdm_vendor_demo.country_code_mapping VALUES
    ('DE', 'Germany', ARRAY('Deutschland', 'DE', 'DEU')),
    ('SE', 'Sweden', ARRAY('Sverige', 'SE', 'SWE')),
    ('NL', 'Netherlands', ARRAY('Nederland', 'Holland', 'NL', 'NLD')),
    ('GB', 'United Kingdom', ARRAY('UK', 'Great Britain', 'England', 'GB', 'GBR')),
    ('IE', 'Ireland', ARRAY('Eire', 'IE', 'IRL')),
    ('US', 'United States', ARRAY('USA', 'America', 'United States of America', 'US')),
    ('FR', 'France', ARRAY('FR', 'FRA')),
    ('CH', 'Switzerland', ARRAY('Schweiz', 'Suisse', 'Svizzera', 'CH', 'CHE')),
    ('AT', 'Austria', ARRAY('Österreich', 'AT', 'AUT')),
    ('NO', 'Norway', ARRAY('Norge', 'NO', 'NOR')),
    ('DK', 'Denmark', ARRAY('Danmark', 'DK', 'DNK')),
    ('FI', 'Finland', ARRAY('Suomi', 'FI', 'FIN'));

-- Grant permissions (adjust principals as needed)
-- GRANT ALL PRIVILEGES ON SCHEMA app_demo_data.mdm_vendor_demo TO `data-engineers`;
-- GRANT SELECT ON SCHEMA app_demo_data.mdm_vendor_demo TO `data-consumers`;

