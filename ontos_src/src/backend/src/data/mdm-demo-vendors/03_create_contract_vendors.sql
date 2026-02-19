-- ============================================================================
-- MDM Vendor Demo: Contract Vendors Table (Contract Management Source)
-- ============================================================================
-- This represents vendor/supplier data from a Contract Management system
-- that needs to be matched against the SAP Vendor Master.
-- Contains matches, variations, and new vendors for MDM testing.
-- ============================================================================

USE CATALOG app_demo_data;
USE SCHEMA mdm_vendor_demo;

-- Drop existing table if it exists (for demo reset purposes)
DROP TABLE IF EXISTS contract_vendors;

-- Create the contract vendors source table
CREATE TABLE contract_vendors (
    -- Contract Management identifier
    external_vendor_id STRING NOT NULL COMMENT 'Supplier ID from contract system',
    
    -- Vendor name fields
    vendor_name STRING COMMENT 'Full supplier name',
    vendor_name_alt STRING COMMENT 'Alternative name/trading name',
    
    -- Location (different structure from SAP)
    country STRING COMMENT 'Full country name',
    city STRING COMMENT 'City name',
    address STRING COMMENT 'Full address',
    
    -- Contract-specific fields
    relationship_type STRING COMMENT 'Contract relationship: strategic, preferred, standard, spot',
    status STRING COMMENT 'Vendor status: active, inactive, pending, blocked',
    vendor_category STRING COMMENT 'Supplier category',
    
    -- Contract information
    contract_count INT COMMENT 'Number of active contracts',
    total_contract_value DOUBLE COMMENT 'Total value of contracts',
    contract_currency STRING COMMENT 'Primary contract currency',
    first_contract_date DATE COMMENT 'Date of first contract',
    last_contract_date DATE COMMENT 'Date of most recent contract',
    
    -- Contact information
    primary_contact STRING COMMENT 'Primary contact name',
    contact_email STRING COMMENT 'Contact email address',
    contact_phone STRING COMMENT 'Contact phone number',
    
    -- Metadata
    created_date TIMESTAMP COMMENT 'Record creation timestamp',
    modified_date TIMESTAMP COMMENT 'Last modification timestamp'
)
USING DELTA
COMMENT 'Contract Management Vendor Data - Source for MDM matching'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'mdm-source-system' = 'CONTRACT_MGMT',
    'mdm-entity-type' = 'vendor'
);

-- Insert 40 contract vendor records:
-- - Some exact matches to SAP vendor records
-- - Some fuzzy matches (name variations, abbreviations)
-- - Some new vendors not in SAP
-- - Some with data quality issues

INSERT INTO contract_vendors VALUES
    -- Exact/close matches to SAP records (with country name instead of code)
    ('CNT-001', 'Microsoft Ireland Operations Ltd.', 'Microsoft EMEA', 'Ireland', 'Dublin', 'One Microsoft Place, South County Business Park, Dublin 18', 'strategic', 'active', 'Software', 5, 2500000.00, 'EUR', '2020-01-15', '2024-06-01', 'John Murphy', 'jmurphy@microsoft.com', '+353-1-706-3100', '2020-01-10 10:00:00', '2024-06-15 14:30:00'),
    ('CNT-002', 'HP PPS Sverige AB', 'HP Sweden', 'Sweden', 'Stockholm', 'Kronborgsgränd 1, 164 99 Stockholm', 'preferred', 'active', 'Hardware', 3, 850000.00, 'SEK', '2021-03-20', '2024-05-15', 'Erik Lindqvist', 'elindqvist@hp.com', '+46-8-524-910-10', '2021-03-15 09:15:00', '2024-05-20 11:45:00'),
    ('CNT-003', 'Hewlett-Packard Enterprise AB', 'HPE Sweden', 'Sweden', 'Stockholm', 'Kronborgsgränd 1, Stockholm', 'strategic', 'active', 'IT Infrastructure', 4, 1200000.00, 'SEK', '2019-06-10', '2024-06-10', 'Anna Svensson', 'asvensson@hpe.com', '+46-8-524-910-20', '2019-06-05 11:30:00', '2024-06-15 16:20:00'),
    ('CNT-004', 'IBM Deutschland GmbH', 'IBM Germany', 'Germany', 'Ehningen', 'IBM-Allee 1, 71139 Ehningen', 'strategic', 'active', 'IT Services', 8, 3500000.00, 'EUR', '2018-02-01', '2024-06-20', 'Hans Müller', 'hmueller@de.ibm.com', '+49-7034-15-100', '2018-01-25 08:00:00', '2024-06-22 09:15:00'),
    ('CNT-005', 'Capgemini Sverige AB', NULL, 'Sweden', 'Stockholm', 'Luntmakargatan 18, 111 22 Stockholm', 'preferred', 'active', 'Consulting', 6, 2100000.00, 'SEK', '2019-09-15', '2024-05-30', 'Lars Persson', 'lpersson@capgemini.com', '+46-8-536-100-10', '2019-09-10 14:20:00', '2024-06-01 10:45:00'),
    
    -- Fuzzy matches (name variations, abbreviations, different legal suffixes)
    ('CNT-006', 'SAP Deutschland SE & Co. KG', 'SAP Germany', 'Germany', 'Walldorf', 'Dietmar-Hopp-Allee 16', 'strategic', 'active', 'Software', 10, 5000000.00, 'EUR', '2017-01-01', '2024-06-25', 'Klaus Schmidt', 'kschmidt@sap.com', '+49-6227-7-100', '2017-01-01 10:00:00', '2024-06-25 14:30:00'),
    ('CNT-007', 'Oracle Netherlands B.V.', 'Oracle NL', 'Netherlands', 'Utrecht', 'Herculesplein 21, 3584 BA Utrecht', 'preferred', 'active', 'Software', 4, 1800000.00, 'EUR', '2020-04-01', '2024-04-28', 'Jan de Vries', 'jdevries@oracle.com', '+31-30-663-6400', '2020-03-25 09:30:00', '2024-05-01 10:20:00'),
    ('CNT-008', 'Cisco Systems GmbH', 'Cisco Germany', 'Germany', 'Garching', 'Parkring 20, 85748 Garching', 'strategic', 'active', 'Networking', 7, 2800000.00, 'EUR', '2018-04-15', '2024-05-25', 'Thomas Weber', 'tweber@cisco.com', '+49-89-357-1200', '2018-04-10 11:00:00', '2024-05-28 16:40:00'),
    ('CNT-009', 'Dell Technologies Deutschland', 'Dell Germany', 'Germany', 'Frankfurt', 'Unterschweinstiege 10, Frankfurt', 'preferred', 'active', 'Hardware', 5, 1500000.00, 'EUR', '2019-05-01', '2024-05-22', 'Michael Braun', 'mbraun@dell.com', '+49-69-9792-100', '2019-04-25 08:15:00', '2024-05-25 09:55:00'),
    ('CNT-010', 'Basware AB', 'Basware Sweden', 'Sweden', 'Stockholm', 'Sveavägen 17, Stockholm', 'standard', 'active', 'Software', 2, 450000.00, 'SEK', '2022-05-15', '2024-05-20', 'Sofia Nilsson', 'snilsson@basware.com', '+46-8-587-100-10', '2022-05-10 13:40:00', '2024-05-22 13:10:00'),
    
    -- More fuzzy matches with name variations
    ('CNT-011', 'Accenture Sweden AB', 'Accenture Nordic', 'Sweden', 'Stockholm', 'Kungsgatan 55, Stockholm', 'strategic', 'active', 'Consulting', 9, 4200000.00, 'SEK', '2018-06-01', '2024-06-18', 'Maria Andersson', 'mandersson@accenture.com', '+46-8-775-30-10', '2018-05-25 10:30:00', '2024-06-20 11:25:00'),
    ('CNT-012', 'Deloitte Sweden AB', 'Deloitte Nordic', 'Sweden', 'Stockholm', 'Rehnsgatan 11, 113 79 Stockholm', 'preferred', 'active', 'Audit & Advisory', 5, 1900000.00, 'SEK', '2019-06-15', '2024-05-15', 'Johan Eriksson', 'jeriksson@deloitte.se', '+46-8-506-700-10', '2019-06-10 09:00:00', '2024-05-18 15:35:00'),
    ('CNT-013', 'KPMG Sweden AB', 'KPMG Nordic', 'Sweden', 'Stockholm', 'Vasagatan 16, Stockholm', 'preferred', 'active', 'Audit & Advisory', 4, 1600000.00, 'SEK', '2020-07-01', '2024-05-12', 'Karin Ström', 'kstrom@kpmg.se', '+46-8-723-91-10', '2020-06-25 14:15:00', '2024-05-15 10:50:00'),
    ('CNT-014', 'PricewaterhouseCoopers AB', 'PwC Sweden', 'Sweden', 'Stockholm', 'Torsgatan 21, Stockholm', 'preferred', 'active', 'Audit & Advisory', 3, 1200000.00, 'SEK', '2021-07-15', '2024-05-10', 'Anders Olsson', 'aolsson@pwc.se', '+46-10-212-40-10', '2021-07-10 11:30:00', '2024-05-12 14:20:00'),
    ('CNT-015', 'EY Sweden AB', 'Ernst & Young Sweden', 'Sweden', 'Stockholm', 'Hamngatan 26, Stockholm', 'standard', 'active', 'Audit & Advisory', 2, 800000.00, 'SEK', '2022-08-01', '2024-05-08', 'Eva Johansson', 'ejohansson@se.ey.com', '+46-8-520-590-10', '2022-07-25 08:45:00', '2024-05-10 12:45:00'),
    
    -- UK vendors with country name variations
    ('CNT-016', 'British Telecom Group PLC', 'BT Business', 'United Kingdom', 'London', '81 Newgate Street, London', 'preferred', 'active', 'Telecommunications', 3, 950000.00, 'GBP', '2020-08-15', '2024-05-05', 'James Smith', 'jsmith@bt.com', '+44-20-7356-5100', '2020-08-10 10:00:00', '2024-05-08 16:15:00'),
    ('CNT-017', 'Vodafone Limited', 'Vodafone UK Business', 'United Kingdom', 'Newbury', 'The Connection, Newbury', 'standard', 'active', 'Telecommunications', 2, 650000.00, 'GBP', '2021-09-01', '2024-05-02', 'Sarah Williams', 'swilliams@vodafone.co.uk', '+44-1635-33300', '2021-08-25 09:15:00', '2024-05-05 09:30:00'),
    ('CNT-018', 'TCS UK Limited', 'Tata Consultancy Services', 'United Kingdom', 'London', '19 Victoria Street, London', 'strategic', 'active', 'IT Services', 6, 2400000.00, 'GBP', '2019-09-15', '2024-04-30', 'Raj Patel', 'rpatel@tcs.com', '+44-20-7618-4150', '2019-09-10 13:30:00', '2024-05-02 11:40:00'),
    ('CNT-019', 'Wipro Technologies Ltd', 'Wipro UK', 'United Kingdom', 'London', '1 Ropemaker Street, London', 'preferred', 'active', 'IT Services', 4, 1700000.00, 'GBP', '2020-10-01', '2024-04-28', 'Priya Kumar', 'pkumar@wipro.com', '+44-20-7618-4250', '2020-09-25 11:00:00', '2024-05-01 15:55:00'),
    ('CNT-020', 'Infosys Limited UK', 'Infosys BPO', 'United Kingdom', 'London', '36-38 Cornhill, London', 'standard', 'active', 'IT Services', 3, 1100000.00, 'GBP', '2021-10-15', '2024-04-25', 'Anil Sharma', 'asharma@infosys.com', '+44-20-7618-4350', '2021-10-10 09:45:00', '2024-04-28 10:25:00'),
    
    -- Nordic vendors
    ('CNT-021', 'TietoEVRY Sweden AB', 'Tieto Sweden', 'Sweden', 'Stockholm', 'Kronborgsgränd 11, Stockholm', 'preferred', 'active', 'IT Services', 4, 1400000.00, 'SEK', '2020-11-01', '2024-04-22', 'Oscar Lindgren', 'olindgren@tietoevry.com', '+46-10-481-00-10', '2020-10-25 10:20:00', '2024-04-25 14:10:00'),
    ('CNT-022', 'Ericsson AB', 'Telefonaktiebolaget Ericsson', 'Sweden', 'Stockholm', 'Torshamnsgatan 21, Stockholm', 'strategic', 'active', 'Telecommunications', 8, 4500000.00, 'SEK', '2017-11-15', '2024-06-20', 'Björn Karlsson', 'bkarlsson@ericsson.com', '+46-10-719-00-10', '2017-11-10 08:30:00', '2024-06-22 09:45:00'),
    ('CNT-023', 'Nokia Corporation', 'Nokia Networks Finland', 'Finland', 'Espoo', 'Karakaari 7, Espoo', 'strategic', 'active', 'Telecommunications', 6, 3200000.00, 'EUR', '2018-12-01', '2024-06-18', 'Mikko Virtanen', 'mvirtanen@nokia.com', '+358-10-448-8100', '2018-11-25 11:15:00', '2024-06-20 13:30:00'),
    ('CNT-024', 'Nets A/S', 'Nets Denmark', 'Denmark', 'Ballerup', 'Lautrupbjerg 10, Ballerup', 'preferred', 'active', 'Payment Services', 3, 900000.00, 'DKK', '2021-12-15', '2024-04-15', 'Lars Jensen', 'ljensen@nets.eu', '+45-44-68-44-70', '2021-12-10 14:40:00', '2024-04-18 12:15:00'),
    ('CNT-025', 'Visma Software AS', 'Visma Norway', 'Norway', 'Oslo', 'Karenslyst allé 56, Oslo', 'standard', 'active', 'Software', 2, 550000.00, 'NOK', '2022-01-01', '2024-04-12', 'Ole Hansen', 'ohansen@visma.com', '+47-46-40-40-10', '2021-12-25 09:00:00', '2024-04-15 16:40:00'),
    
    -- German industrial vendors
    ('CNT-026', 'Siemens Deutschland AG', 'Siemens Germany', 'Germany', 'München', 'Werner-von-Siemens-Str. 1, Munich', 'strategic', 'active', 'Industrial', 12, 6500000.00, 'EUR', '2016-01-15', '2024-06-25', 'Friedrich Bauer', 'fbauer@siemens.com', '+49-89-636-100', '2016-01-10 10:30:00', '2024-06-28 10:55:00'),
    ('CNT-027', 'Robert Bosch GmbH', 'Bosch Germany', 'Germany', 'Stuttgart', 'Robert-Bosch-Platz 1, Stuttgart', 'strategic', 'active', 'Industrial', 9, 4800000.00, 'EUR', '2017-01-20', '2024-06-22', 'Wolfgang Richter', 'wrichter@bosch.com', '+49-711-400-100', '2017-01-15 08:15:00', '2024-06-25 15:20:00'),
    ('CNT-028', 'Continental Automotive GmbH', 'Continental Germany', 'Germany', 'Hannover', 'Vahrenwalder Straße 9, Hannover', 'preferred', 'active', 'Automotive', 5, 2200000.00, 'EUR', '2019-02-01', '2024-06-18', 'Markus Fischer', 'mfischer@continental.com', '+49-511-938-100', '2019-01-25 11:45:00', '2024-06-20 11:35:00'),
    
    -- New vendors not in SAP master
    ('CNT-029', 'CloudFlare Inc', 'Cloudflare Europe', 'United States', 'San Francisco', '101 Townsend St, San Francisco, CA', 'standard', 'active', 'IT Security', 2, 350000.00, 'USD', '2023-03-01', '2024-06-01', 'Emily Watson', 'ewatson@cloudflare.com', '+1-415-123-4567', '2023-02-25 10:00:00', '2024-06-05 14:15:00'),
    ('CNT-030', 'Datadog Inc', 'Datadog EMEA', 'United States', 'New York', '620 8th Avenue, New York, NY', 'preferred', 'active', 'IT Monitoring', 3, 520000.00, 'USD', '2022-06-15', '2024-05-30', 'Michael Brown', 'mbrown@datadoghq.com', '+1-646-123-4568', '2022-06-10 11:30:00', '2024-06-02 16:45:00'),
    ('CNT-031', 'Snowflake Computing', 'Snowflake EMEA', 'United States', 'Bozeman', '106 E Babcock St, Bozeman, MT', 'strategic', 'active', 'Data Platform', 4, 980000.00, 'USD', '2022-01-10', '2024-06-15', 'Jennifer Lee', 'jlee@snowflake.com', '+1-406-123-4569', '2022-01-05 09:15:00', '2024-06-18 12:30:00'),
    ('CNT-032', 'HashiCorp Inc', 'HashiCorp EMEA', 'United States', 'San Francisco', '101 Second Street, San Francisco', 'standard', 'active', 'DevOps Tools', 2, 280000.00, 'USD', '2023-04-01', '2024-05-20', 'David Chen', 'dchen@hashicorp.com', '+1-415-123-4570', '2023-03-25 13:20:00', '2024-05-25 14:55:00'),
    ('CNT-033', 'Confluent Inc', 'Confluent Europe', 'United States', 'Mountain View', '899 W Evelyn Ave, Mountain View', 'preferred', 'active', 'Data Streaming', 3, 450000.00, 'USD', '2022-09-01', '2024-06-10', 'Sarah Johnson', 'sjohnson@confluent.io', '+1-650-123-4571', '2022-08-25 08:30:00', '2024-06-12 10:40:00'),
    
    -- Duplicates within contract system (same vendor, multiple entries)
    ('CNT-034', 'Basware Corporation', 'Basware Oyj', 'Finland', 'Espoo', 'Linnoitustie 2, Espoo', 'preferred', 'active', 'Software', 3, 680000.00, 'EUR', '2021-03-10', '2024-04-15', 'Pekka Korhonen', 'pkorhonen@basware.com', '+358-9-8791-0', '2021-03-05 10:00:00', '2024-04-18 15:10:00'),
    ('CNT-035', 'Capgemini SE', 'Capgemini Group', 'France', 'Paris', '11 rue de Tilsitt, Paris', 'strategic', 'active', 'Consulting', 7, 3800000.00, 'EUR', '2018-05-01', '2024-06-20', 'Pierre Martin', 'pmartin@capgemini.com', '+33-1-47-54-50-00', '2018-04-25 09:30:00', '2024-06-22 11:20:00'),
    
    -- Records with data quality issues
    ('CNT-036', NULL, 'Unknown Supplier', 'Unknown', 'Unknown', 'Address Unknown', 'standard', 'pending', 'Uncategorized', 0, 0.00, 'EUR', NULL, NULL, NULL, 'unknown@test.com', NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    ('CNT-037', 'Test Vendor Ltd', 'Test Company', 'Test Country', 'Test City', 'Test Address', 'standard', 'inactive', 'Test', 0, 0.00, 'EUR', NULL, NULL, 'Test Contact', 'test@test.test', '000-0000', '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    
    -- More new legitimate vendors
    ('CNT-038', 'Stripe Inc', 'Stripe Payments Europe', 'Ireland', 'Dublin', '1 Grand Canal Street Lower, Dublin', 'preferred', 'active', 'Payment Processing', 4, 750000.00, 'EUR', '2021-08-01', '2024-06-15', 'Liam Murphy', 'lmurphy@stripe.com', '+353-1-234-5678', '2021-07-25 10:30:00', '2024-06-18 14:45:00'),
    ('CNT-039', 'Twilio Inc', 'Twilio Europe', 'United States', 'San Francisco', '375 Beale St, San Francisco', 'standard', 'active', 'Communications API', 2, 320000.00, 'USD', '2023-01-15', '2024-05-25', 'Rachel Kim', 'rkim@twilio.com', '+1-415-123-4572', '2023-01-10 11:45:00', '2024-05-28 12:30:00'),
    ('CNT-040', 'Okta Inc', 'Okta EMEA', 'United States', 'San Francisco', '100 First Street, San Francisco', 'preferred', 'active', 'Identity Management', 3, 480000.00, 'USD', '2022-04-01', '2024-06-05', 'Daniel Park', 'dpark@okta.com', '+1-415-123-4573', '2022-03-25 09:00:00', '2024-06-08 15:55:00');

-- Verify the data
SELECT COUNT(*) as total_records FROM contract_vendors;

-- Show statistics about the data
SELECT 
    status,
    COUNT(*) as count
FROM contract_vendors
GROUP BY status
ORDER BY count DESC;

-- Show relationship types
SELECT 
    relationship_type,
    COUNT(*) as count
FROM contract_vendors
GROUP BY relationship_type
ORDER BY count DESC;

