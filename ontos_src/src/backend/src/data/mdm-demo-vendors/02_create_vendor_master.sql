-- ============================================================================
-- MDM Vendor Demo: Vendor Master Table (SAP ERP)
-- ============================================================================
-- This is the "golden record" master table for vendor data from SAP ERP.
-- It contains the authoritative vendor records from the primary system.
-- ============================================================================

USE CATALOG app_demo_data;
USE SCHEMA mdm_vendor_demo;

-- Drop existing table if it exists (for demo reset purposes)
DROP TABLE IF EXISTS vendor_master;

-- Create the vendor master table (SAP ERP structure)
CREATE TABLE vendor_master (
    -- Primary identifier
    vendor_id STRING NOT NULL COMMENT 'SAP vendor account number',
    
    -- Vendor name fields
    vendor_name STRING COMMENT 'Primary vendor name',
    vendor_name_2 STRING COMMENT 'Secondary name field (legal form, etc.)',
    
    -- Location
    country_code STRING COMMENT 'ISO 3166-1 alpha-2 country code',
    city STRING COMMENT 'City name',
    postal_code STRING COMMENT 'Postal/ZIP code',
    street_address STRING COMMENT 'Street address',
    
    -- SAP-specific fields
    company_code STRING COMMENT 'SAP company code',
    purchasing_org STRING COMMENT 'Purchasing organization',
    account_group STRING COMMENT 'Vendor account group',
    payment_terms STRING COMMENT 'Payment terms code',
    
    -- Classification
    industry_code STRING COMMENT 'Industry classification code',
    vendor_type STRING COMMENT 'Vendor type: supplier, service, contractor',
    
    -- Contact information
    contact_person STRING COMMENT 'Primary contact name',
    phone STRING COMMENT 'Primary phone number',
    email STRING COMMENT 'Primary email address',
    website STRING COMMENT 'Company website',
    
    -- Financial
    currency STRING COMMENT 'Default currency code',
    tax_number STRING COMMENT 'Tax identification number',
    duns_number STRING COMMENT 'D&B DUNS number',
    
    -- Metadata
    source_systems ARRAY<STRING> COMMENT 'List of source systems',
    confidence_score DOUBLE COMMENT 'Data quality confidence score (0-1)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    last_merged_at TIMESTAMP COMMENT 'Last MDM merge timestamp'
)
USING DELTA
COMMENT 'Vendor Master Data - SAP ERP Golden Records'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'data-product-name' = 'Vendor Master',
    'data-product-domain' = 'Procurement',
    'mdm-entity-type' = 'vendor',
    'mdm-role' = 'master'
);

-- Insert 50 sample SAP vendor records
-- Includes vendors that will match with contract management system
INSERT INTO vendor_master VALUES
    -- Major IT vendors (will have matches in contract system)
    ('V-001', 'Microsoft Ireland Operations Ltd', NULL, 'IE', 'Dublin', 'D18 P521', 'One Microsoft Place, South County Business Park', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'John Murphy', '+353-1-706-3000', 'procurement@microsoft.ie', 'www.microsoft.com', 'EUR', 'IE8256796U', '985695123', ARRAY('SAP_ERP'), 0.98, '2023-01-15 10:30:00', '2024-06-20 14:45:00', '2024-06-20 14:45:00'),
    ('V-002', 'HP PPS Sverige AB', NULL, 'SE', 'Stockholm', '164 99', 'Kronborgsgränd 1', '1000', 'PO01', 'ZVEN', 'NET45', 'IT01', 'supplier', 'Erik Lindqvist', '+46-8-524-910-00', 'sales@hp.se', 'www.hp.com', 'SEK', 'SE556066-7083', '345678901', ARRAY('SAP_ERP'), 0.95, '2023-02-10 09:15:00', '2024-06-18 11:20:00', '2024-06-18 11:20:00'),
    ('V-003', 'Hewlett-Packard Sverige AB', 'Enterprise Division', 'SE', 'Stockholm', '164 99', 'Kronborgsgränd 1', '1000', 'PO01', 'ZVEN', 'NET45', 'IT01', 'supplier', 'Anna Svensson', '+46-8-524-910-01', 'enterprise@hp.se', 'www.hp.com/enterprise', 'SEK', 'SE556066-7084', '345678902', ARRAY('SAP_ERP'), 0.94, '2023-02-12 11:45:00', '2024-06-15 16:30:00', '2024-06-15 16:30:00'),
    ('V-004', 'IBM Deutschland GmbH', NULL, 'DE', 'Ehningen', '71139', 'IBM-Allee 1', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Hans Müller', '+49-7034-15-0', 'info@de.ibm.com', 'www.ibm.com/de', 'EUR', 'DE113891176', '234567890', ARRAY('SAP_ERP'), 0.97, '2023-03-01 08:00:00', '2024-06-12 09:15:00', '2024-06-12 09:15:00'),
    ('V-005', 'Capgemini Sverige AB', NULL, 'SE', 'Stockholm', '111 22', 'Luntmakargatan 18', '1000', 'PO01', 'ZCON', 'NET30', 'IT02', 'service', 'Lars Persson', '+46-8-536-100-00', 'contact@capgemini.se', 'www.capgemini.com', 'SEK', 'SE556274-6168', '456789012', ARRAY('SAP_ERP'), 0.96, '2023-03-15 14:20:00', '2024-06-10 10:45:00', '2024-06-10 10:45:00'),
    
    -- European vendors
    ('V-006', 'SAP SE', NULL, 'DE', 'Walldorf', '69190', 'Dietmar-Hopp-Allee 16', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Klaus Schmidt', '+49-6227-7-0', 'info@sap.com', 'www.sap.com', 'EUR', 'DE143424646', '123456789', ARRAY('SAP_ERP'), 0.99, '2023-01-01 10:00:00', '2024-06-20 14:30:00', '2024-06-20 14:30:00'),
    ('V-007', 'Oracle Nederland B.V.', NULL, 'NL', 'Utrecht', '3584 BA', 'Herculesplein 21', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Jan de Vries', '+31-30-663-6363', 'info@oracle.nl', 'www.oracle.com', 'EUR', 'NL004503027B01', '567890123', ARRAY('SAP_ERP'), 0.94, '2023-04-01 09:30:00', '2024-05-28 10:20:00', '2024-05-28 10:20:00'),
    ('V-008', 'Cisco Systems Germany GmbH', NULL, 'DE', 'Garching', '85748', 'Parkring 20', '1000', 'PO01', 'ZVEN', 'NET30', 'IT03', 'supplier', 'Thomas Weber', '+49-89-357-1100', 'info@cisco.de', 'www.cisco.com', 'EUR', 'DE812398230', '678901234', ARRAY('SAP_ERP'), 0.95, '2023-04-15 11:00:00', '2024-05-25 16:40:00', '2024-05-25 16:40:00'),
    ('V-009', 'Dell Technologies GmbH', NULL, 'DE', 'Frankfurt', '60549', 'Unterschweinstiege 10', '1000', 'PO01', 'ZVEN', 'NET45', 'IT01', 'supplier', 'Michael Braun', '+49-69-9792-0', 'vertrieb@dell.com', 'www.dell.de', 'EUR', 'DE812841558', '789012345', ARRAY('SAP_ERP'), 0.93, '2023-05-01 08:15:00', '2024-05-22 09:55:00', '2024-05-22 09:55:00'),
    ('V-010', 'Basware AB', NULL, 'SE', 'Stockholm', '111 46', 'Sveavägen 17', '1000', 'PO01', 'ZVEN', 'NET30', 'IT04', 'supplier', 'Sofia Nilsson', '+46-8-587-100-00', 'info@basware.se', 'www.basware.com', 'SEK', 'SE556494-6652', '890123456', ARRAY('SAP_ERP'), 0.92, '2023-05-15 13:40:00', '2024-05-20 13:10:00', '2024-05-20 13:10:00'),
    
    -- Consulting and services
    ('V-011', 'Accenture AB', NULL, 'SE', 'Stockholm', '111 87', 'Kungsgatan 55', '1000', 'PO01', 'ZCON', 'NET30', 'IT02', 'service', 'Maria Andersson', '+46-8-775-30-00', 'info@accenture.se', 'www.accenture.com', 'SEK', 'SE556224-4073', '901234567', ARRAY('SAP_ERP'), 0.96, '2023-06-01 10:30:00', '2024-05-18 11:25:00', '2024-05-18 11:25:00'),
    ('V-012', 'Deloitte AB', NULL, 'SE', 'Stockholm', '113 79', 'Rehnsgatan 11', '1000', 'PO01', 'ZCON', 'NET30', 'IT02', 'service', 'Johan Eriksson', '+46-8-506-700-00', 'info@deloitte.se', 'www.deloitte.se', 'SEK', 'SE556271-5551', '012345678', ARRAY('SAP_ERP'), 0.95, '2023-06-15 09:00:00', '2024-05-15 15:35:00', '2024-05-15 15:35:00'),
    ('V-013', 'KPMG AB', NULL, 'SE', 'Stockholm', '104 25', 'Vasagatan 16', '1000', 'PO01', 'ZCON', 'NET30', 'FIN1', 'service', 'Karin Ström', '+46-8-723-91-00', 'info@kpmg.se', 'www.kpmg.se', 'SEK', 'SE556043-4465', '112233445', ARRAY('SAP_ERP'), 0.94, '2023-07-01 14:15:00', '2024-05-12 10:50:00', '2024-05-12 10:50:00'),
    ('V-014', 'PwC Sverige AB', 'Öhrlings PricewaterhouseCoopers', 'SE', 'Stockholm', '113 97', 'Torsgatan 21', '1000', 'PO01', 'ZCON', 'NET30', 'FIN1', 'service', 'Anders Olsson', '+46-10-212-40-00', 'kontakt@pwc.se', 'www.pwc.se', 'SEK', 'SE556047-2689', '223344556', ARRAY('SAP_ERP'), 0.93, '2023-07-15 11:30:00', '2024-05-10 14:20:00', '2024-05-10 14:20:00'),
    ('V-015', 'Ernst & Young AB', 'EY Sweden', 'SE', 'Stockholm', '102 50', 'Hamngatan 26', '1000', 'PO01', 'ZCON', 'NET30', 'FIN1', 'service', 'Eva Johansson', '+46-8-520-590-00', 'info@se.ey.com', 'www.ey.com/se', 'SEK', 'SE556053-5428', '334455667', ARRAY('SAP_ERP'), 0.94, '2023-08-01 08:45:00', '2024-05-08 12:45:00', '2024-05-08 12:45:00'),
    
    -- UK vendors
    ('V-016', 'British Telecom PLC', 'BT Group', 'GB', 'London', 'EC1A 7AJ', '81 Newgate Street', '1000', 'PO01', 'ZVEN', 'NET30', 'TEL1', 'supplier', 'James Smith', '+44-20-7356-5000', 'business@bt.com', 'www.bt.com', 'GBP', 'GB162714445', '445566778', ARRAY('SAP_ERP'), 0.92, '2023-08-15 10:00:00', '2024-05-05 16:15:00', '2024-05-05 16:15:00'),
    ('V-017', 'Vodafone UK Ltd', NULL, 'GB', 'Newbury', 'RG14 2PZ', 'The Connection', '1000', 'PO01', 'ZVEN', 'NET30', 'TEL1', 'supplier', 'Sarah Williams', '+44-1onal635-33251', 'business@vodafone.co.uk', 'www.vodafone.co.uk', 'GBP', 'GB569953277', '556677889', ARRAY('SAP_ERP'), 0.91, '2023-09-01 09:15:00', '2024-05-02 09:30:00', '2024-05-02 09:30:00'),
    ('V-018', 'Tata Consultancy Services UK Ltd', 'TCS UK', 'GB', 'London', 'SW1E 5JL', '19 Victoria Street', '1000', 'PO01', 'ZCON', 'NET45', 'IT02', 'service', 'Raj Patel', '+44-20-7618-4100', 'enquiries@tcs.com', 'www.tcs.com', 'GBP', 'GB689394984', '667788990', ARRAY('SAP_ERP'), 0.95, '2023-09-15 13:30:00', '2024-04-30 11:40:00', '2024-04-30 11:40:00'),
    ('V-019', 'Wipro UK Ltd', NULL, 'GB', 'London', 'EC2V 7HN', '1 Ropemaker Street', '1000', 'PO01', 'ZCON', 'NET45', 'IT02', 'service', 'Priya Kumar', '+44-20-7618-4200', 'info@wipro.com', 'www.wipro.com', 'GBP', 'GB744536285', '778899001', ARRAY('SAP_ERP'), 0.93, '2023-10-01 11:00:00', '2024-04-28 15:55:00', '2024-04-28 15:55:00'),
    ('V-020', 'Infosys BPO UK Ltd', NULL, 'GB', 'London', 'EC2R 8AH', '36-38 Cornhill', '1000', 'PO01', 'ZCON', 'NET45', 'IT02', 'service', 'Anil Sharma', '+44-20-7618-4300', 'ukinfo@infosys.com', 'www.infosys.com', 'GBP', 'GB821475312', '889900112', ARRAY('SAP_ERP'), 0.94, '2023-10-15 09:45:00', '2024-04-25 10:25:00', '2024-04-25 10:25:00'),
    
    -- Nordic vendors
    ('V-021', 'Tieto Sweden AB', 'TietoEVRY', 'SE', 'Stockholm', '164 40', 'Kronborgsgränd 11', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Oscar Lindgren', '+46-10-481-00-00', 'info@tietoevry.se', 'www.tietoevry.com', 'SEK', 'SE556055-7745', '990011223', ARRAY('SAP_ERP'), 0.93, '2023-11-01 10:20:00', '2024-04-22 14:10:00', '2024-04-22 14:10:00'),
    ('V-022', 'Ericsson AB', 'Telefonaktiebolaget LM Ericsson', 'SE', 'Stockholm', '164 83', 'Torshamnsgatan 21', '1000', 'PO01', 'ZVEN', 'NET30', 'TEL1', 'supplier', 'Björn Karlsson', '+46-10-719-00-00', 'info@ericsson.com', 'www.ericsson.com', 'SEK', 'SE556016-0680', '001122334', ARRAY('SAP_ERP'), 0.98, '2023-11-15 08:30:00', '2024-04-20 09:45:00', '2024-04-20 09:45:00'),
    ('V-023', 'Nokia Solutions & Networks OY', 'Nokia Finland', 'FI', 'Espoo', '02610', 'Karakaari 7', '1000', 'PO01', 'ZVEN', 'NET30', 'TEL1', 'supplier', 'Mikko Virtanen', '+358-10-448-8000', 'info@nokia.com', 'www.nokia.com', 'EUR', 'FI01124477', '112233446', ARRAY('SAP_ERP'), 0.97, '2023-12-01 11:15:00', '2024-04-18 13:30:00', '2024-04-18 13:30:00'),
    ('V-024', 'Nets Denmark A/S', NULL, 'DK', 'Ballerup', '2750', 'Lautrupbjerg 10', '1000', 'PO01', 'ZVEN', 'NET30', 'FIN2', 'supplier', 'Lars Jensen', '+45-44-68-44-68', 'info@nets.eu', 'www.nets.eu', 'DKK', 'DK20016175', '223344557', ARRAY('SAP_ERP'), 0.92, '2023-12-15 14:40:00', '2024-04-15 12:15:00', '2024-04-15 12:15:00'),
    ('V-025', 'Visma AS', NULL, 'NO', 'Oslo', '0107', 'Karenslyst allé 56', '1000', 'PO01', 'ZVEN', 'NET30', 'IT04', 'supplier', 'Ole Hansen', '+47-46-40-40-00', 'info@visma.com', 'www.visma.com', 'NOK', 'NO983048408', '334455668', ARRAY('SAP_ERP'), 0.94, '2024-01-01 09:00:00', '2024-04-12 16:40:00', '2024-04-12 16:40:00'),
    
    -- German vendors
    ('V-026', 'Siemens AG', 'Siemens Deutschland', 'DE', 'München', '80333', 'Werner-von-Siemens-Str. 1', '1000', 'PO01', 'ZVEN', 'NET30', 'IND1', 'supplier', 'Friedrich Bauer', '+49-89-636-00', 'info@siemens.com', 'www.siemens.com', 'EUR', 'DE129274202', '445566779', ARRAY('SAP_ERP'), 0.99, '2024-01-15 10:30:00', '2024-04-10 10:55:00', '2024-04-10 10:55:00'),
    ('V-027', 'Bosch GmbH', 'Robert Bosch GmbH', 'DE', 'Stuttgart', '70469', 'Robert-Bosch-Platz 1', '1000', 'PO01', 'ZVEN', 'NET30', 'IND1', 'supplier', 'Wolfgang Richter', '+49-711-400-0', 'info@bosch.com', 'www.bosch.com', 'EUR', 'DE812293249', '556677880', ARRAY('SAP_ERP'), 0.98, '2024-01-20 08:15:00', '2024-04-08 15:20:00', '2024-04-08 15:20:00'),
    ('V-028', 'Continental AG', NULL, 'DE', 'Hannover', '30165', 'Vahrenwalder Straße 9', '1000', 'PO01', 'ZVEN', 'NET30', 'IND1', 'supplier', 'Markus Fischer', '+49-511-938-01', 'info@continental.com', 'www.continental.com', 'EUR', 'DE811154058', '667788991', ARRAY('SAP_ERP'), 0.96, '2024-02-01 11:45:00', '2024-04-05 11:35:00', '2024-04-05 11:35:00'),
    ('V-029', 'BASF SE', NULL, 'DE', 'Ludwigshafen', '67056', 'Carl-Bosch-Straße 38', '1000', 'PO01', 'ZVEN', 'NET30', 'CHE1', 'supplier', 'Peter Wagner', '+49-621-60-0', 'info@basf.com', 'www.basf.com', 'EUR', 'DE812035738', '778899002', ARRAY('SAP_ERP'), 0.97, '2024-02-15 09:30:00', '2024-04-02 14:50:00', '2024-04-02 14:50:00'),
    ('V-030', 'Bayer AG', NULL, 'DE', 'Leverkusen', '51373', 'Kaiser-Wilhelm-Allee', '1000', 'PO01', 'ZVEN', 'NET30', 'CHE1', 'supplier', 'Stefan Schulz', '+49-214-30-1', 'info@bayer.com', 'www.bayer.com', 'EUR', 'DE811455800', '889900113', ARRAY('SAP_ERP'), 0.96, '2024-03-01 14:20:00', '2024-03-30 10:05:00', '2024-03-30 10:05:00'),
    
    -- Swiss vendors
    ('V-031', 'ABB Ltd', 'ABB Schweiz AG', 'CH', 'Zürich', '8050', 'Affolternstrasse 44', '1000', 'PO01', 'ZVEN', 'NET30', 'IND1', 'supplier', 'Bruno Keller', '+41-43-317-7111', 'info@ch.abb.com', 'www.abb.com', 'CHF', 'CHE-106.395.576', '990011224', ARRAY('SAP_ERP'), 0.97, '2024-03-10 10:00:00', '2024-03-28 13:25:00', '2024-03-28 13:25:00'),
    ('V-032', 'Nestlé S.A.', NULL, 'CH', 'Vevey', '1800', 'Avenue Nestlé 55', '1000', 'PO01', 'ZVEN', 'NET30', 'FOD1', 'supplier', 'François Blanc', '+41-21-924-1111', 'info@nestle.com', 'www.nestle.com', 'CHF', 'CHE-116.268.917', '001122335', ARRAY('SAP_ERP'), 0.98, '2024-03-15 13:45:00', '2024-03-25 12:40:00', '2024-03-25 12:40:00'),
    ('V-033', 'Roche Holding AG', NULL, 'CH', 'Basel', '4070', 'Grenzacherstrasse 124', '1000', 'PO01', 'ZVEN', 'NET30', 'PHA1', 'supplier', 'Marc Dubois', '+41-61-688-1111', 'info@roche.com', 'www.roche.com', 'CHF', 'CHE-107.639.898', '112233447', ARRAY('SAP_ERP'), 0.97, '2024-03-20 08:30:00', '2024-03-22 15:55:00', '2024-03-22 15:55:00'),
    ('V-034', 'Novartis AG', NULL, 'CH', 'Basel', '4002', 'Lichtstrasse 35', '1000', 'PO01', 'ZVEN', 'NET30', 'PHA1', 'supplier', 'Andreas Meier', '+41-61-324-1111', 'info@novartis.com', 'www.novartis.com', 'CHF', 'CHE-112.099.295', '223344558', ARRAY('SAP_ERP'), 0.98, '2024-03-25 11:15:00', '2024-03-20 09:20:00', '2024-03-20 09:20:00'),
    ('V-035', 'UBS AG', 'UBS Switzerland AG', 'CH', 'Zürich', '8098', 'Bahnhofstrasse 45', '1000', 'PO01', 'ZVEN', 'NET30', 'FIN1', 'supplier', 'Thomas Brunner', '+41-44-234-1111', 'info@ubs.com', 'www.ubs.com', 'CHF', 'CHE-106.876.766', '334455669', ARRAY('SAP_ERP'), 0.96, '2024-04-01 09:00:00', '2024-03-18 11:45:00', '2024-03-18 11:45:00'),
    
    -- More IT vendors
    ('V-036', 'Amazon Web Services EMEA SARL', 'AWS Luxembourg', 'LU', 'Luxembourg', 'L-1150', '38 avenue John F. Kennedy', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Pierre Martin', '+352-26-73-30-00', 'aws-europe@amazon.com', 'www.aws.amazon.com', 'EUR', 'LU20260743', '445566770', ARRAY('SAP_ERP'), 0.95, '2024-04-05 10:30:00', '2024-06-20 14:45:00', '2024-06-20 14:45:00'),
    ('V-037', 'Google Cloud EMEA Ltd', NULL, 'IE', 'Dublin', 'D04 E5W5', 'Gordon House, Barrow Street', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Liam O''Brien', '+353-1-543-1000', 'cloud@google.com', 'www.cloud.google.com', 'EUR', 'IE3695455WH', '556677881', ARRAY('SAP_ERP'), 0.94, '2024-04-10 14:15:00', '2024-06-18 11:20:00', '2024-06-18 11:20:00'),
    ('V-038', 'Salesforce.com Germany GmbH', NULL, 'DE', 'München', '80636', 'Erika-Mann-Straße 31', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Julia Hoffmann', '+49-89-42094-0', 'info@salesforce.de', 'www.salesforce.com', 'EUR', 'DE271880927', '667788992', ARRAY('SAP_ERP'), 0.93, '2024-04-15 08:45:00', '2024-06-15 16:30:00', '2024-06-15 16:30:00'),
    ('V-039', 'ServiceNow Netherlands B.V.', NULL, 'NL', 'Amsterdam', '1082 MD', 'Beechavenue 54', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Willem van der Berg', '+31-20-262-0900', 'info@servicenow.nl', 'www.servicenow.com', 'EUR', 'NL856227048B01', '778899003', ARRAY('SAP_ERP'), 0.92, '2024-04-20 11:30:00', '2024-06-12 09:15:00', '2024-06-12 09:15:00'),
    ('V-040', 'Atlassian Pty Ltd', 'Atlassian UK', 'GB', 'London', 'EC2A 4BX', '341 Old Street', '1000', 'PO01', 'ZVEN', 'NET30', 'IT01', 'supplier', 'Emma Thompson', '+44-20-3859-5700', 'sales@atlassian.com', 'www.atlassian.com', 'GBP', 'GB263466003', '889900114', ARRAY('SAP_ERP'), 0.91, '2024-04-25 09:15:00', '2024-06-10 10:45:00', '2024-06-10 10:45:00'),
    
    -- Additional vendors for variety
    ('V-041', 'Schneider Electric SE', NULL, 'FR', 'Rueil-Malmaison', '92500', '35 rue Joseph Monier', '1000', 'PO01', 'ZVEN', 'NET30', 'IND1', 'supplier', 'Jean Dupont', '+33-1-41-29-70-00', 'info@schneider-electric.com', 'www.schneider-electric.com', 'EUR', 'FR56542048574', '990011225', ARRAY('SAP_ERP'), 0.96, '2024-05-01 10:00:00', '2024-06-08 13:20:00', '2024-06-08 13:20:00'),
    ('V-042', 'Philips Electronics Nederland B.V.', NULL, 'NL', 'Amsterdam', '1096 BC', 'Amstelplein 2', '1000', 'PO01', 'ZVEN', 'NET30', 'ELE1', 'supplier', 'Pieter de Jong', '+31-20-590-5000', 'info@philips.com', 'www.philips.com', 'EUR', 'NL002226463B01', '001122336', ARRAY('SAP_ERP'), 0.95, '2024-05-05 13:30:00', '2024-06-05 15:50:00', '2024-06-05 15:50:00'),
    ('V-043', 'Volvo Cars Sweden AB', NULL, 'SE', 'Göteborg', '405 31', 'Assar Gabrielssons Väg', '1000', 'PO01', 'ZVEN', 'NET30', 'AUT1', 'supplier', 'Gustav Åberg', '+46-31-59-00-00', 'info@volvocars.com', 'www.volvocars.com', 'SEK', 'SE556074-3089', '112233448', ARRAY('SAP_ERP'), 0.97, '2024-05-10 08:15:00', '2024-06-03 11:30:00', '2024-06-03 11:30:00'),
    ('V-044', 'Scania AB', NULL, 'SE', 'Södertälje', '151 87', 'Vagnmakarvägen 1', '1000', 'PO01', 'ZVEN', 'NET30', 'AUT1', 'supplier', 'Henrik Axelsson', '+46-8-553-810-00', 'info@scania.com', 'www.scania.com', 'SEK', 'SE556051-7610', '223344559', ARRAY('SAP_ERP'), 0.96, '2024-05-15 11:45:00', '2024-06-01 14:15:00', '2024-06-01 14:15:00'),
    ('V-045', 'IKEA of Sweden AB', NULL, 'SE', 'Älmhult', '343 81', 'Tulpanvägen 8', '1000', 'PO01', 'ZVEN', 'NET30', 'RET1', 'supplier', 'Ingvar Lindqvist', '+46-476-81-00-00', 'business@ikea.com', 'www.ikea.com', 'SEK', 'SE556074-7561', '334455660', ARRAY('SAP_ERP'), 0.98, '2024-05-20 14:30:00', '2024-05-28 10:20:00', '2024-05-28 10:20:00'),
    
    -- More consulting firms
    ('V-046', 'McKinsey & Company Inc Sweden AB', NULL, 'SE', 'Stockholm', '111 44', 'Birger Jarlsgatan 25', '1000', 'PO01', 'ZCON', 'NET30', 'CON1', 'service', 'Carl Bergström', '+46-8-796-80-00', 'stockholm@mckinsey.com', 'www.mckinsey.com', 'SEK', 'SE556162-0456', '445566771', ARRAY('SAP_ERP'), 0.94, '2024-05-25 09:00:00', '2024-05-25 16:40:00', '2024-05-25 16:40:00'),
    ('V-047', 'Boston Consulting Group Nordic AB', 'BCG Sweden', 'SE', 'Stockholm', '111 46', 'Kungsträdgårdsgatan 10', '1000', 'PO01', 'ZCON', 'NET30', 'CON1', 'service', 'Erik Larsson', '+46-8-736-90-00', 'stockholm@bcg.com', 'www.bcg.com', 'SEK', 'SE556445-9292', '556677882', ARRAY('SAP_ERP'), 0.93, '2024-06-01 10:30:00', '2024-05-22 09:55:00', '2024-05-22 09:55:00'),
    ('V-048', 'Bain & Company Sweden AB', NULL, 'SE', 'Stockholm', '111 37', 'Drottninggatan 25', '1000', 'PO01', 'ZCON', 'NET30', 'CON1', 'service', 'Anna Holm', '+46-8-611-00-00', 'stockholm@bain.com', 'www.bain.com', 'SEK', 'SE556569-8781', '667788993', ARRAY('SAP_ERP'), 0.92, '2024-06-05 13:15:00', '2024-05-20 13:10:00', '2024-05-20 13:10:00'),
    ('V-049', 'Roland Berger GmbH', NULL, 'DE', 'München', '80538', 'Sederanger 1', '1000', 'PO01', 'ZCON', 'NET30', 'CON1', 'service', 'Matthias Gruber', '+49-89-9230-0', 'info@rolandberger.com', 'www.rolandberger.com', 'EUR', 'DE147514020', '778899004', ARRAY('SAP_ERP'), 0.91, '2024-06-10 08:45:00', '2024-05-18 11:25:00', '2024-05-18 11:25:00'),
    ('V-050', 'Oliver Wyman GmbH', NULL, 'DE', 'Frankfurt', '60329', 'Taunusanlage 17', '1000', 'PO01', 'ZCON', 'NET30', 'CON1', 'service', 'Sabine Hartmann', '+49-69-971-73-0', 'info@oliverwyman.com', 'www.oliverwyman.com', 'EUR', 'DE113766892', '889900115', ARRAY('SAP_ERP'), 0.90, '2024-06-15 11:00:00', '2024-05-15 15:35:00', '2024-05-15 15:35:00');

-- Verify the data
SELECT COUNT(*) as total_records FROM vendor_master;

