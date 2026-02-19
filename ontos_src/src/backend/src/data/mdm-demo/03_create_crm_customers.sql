-- ============================================================================
-- MDM Demo: CRM Customers Table (Source System)
-- ============================================================================
-- This represents customer data from a CRM system that needs to be matched
-- against the Customer Master. Contains some duplicates, variations, and
-- new records for MDM testing.
-- ============================================================================

USE CATALOG app_demo_data;
USE SCHEMA mdm_demo;

-- Drop existing table if it exists (for demo reset purposes)
DROP TABLE IF EXISTS crm_customers;

-- Create the CRM customers source table
CREATE TABLE crm_customers (
    -- CRM-specific identifier
    crm_customer_id STRING NOT NULL COMMENT 'CRM system customer ID',
    
    -- Customer attributes (note: slightly different schema from master)
    first_name STRING COMMENT 'Customer first name',
    last_name STRING COMMENT 'Customer last name',
    email_address STRING COMMENT 'Email address',
    phone_number STRING COMMENT 'Phone number',
    
    -- Address (different structure)
    street_address STRING COMMENT 'Full street address',
    city STRING COMMENT 'City',
    state_province STRING COMMENT 'State or province',
    zip_code STRING COMMENT 'ZIP or postal code',
    country_code STRING COMMENT 'Country code',
    
    -- CRM-specific fields
    company STRING COMMENT 'Company name',
    account_type STRING COMMENT 'Account type: lead, prospect, customer',
    lead_source STRING COMMENT 'How the customer was acquired',
    last_contact_date DATE COMMENT 'Last contact date',
    
    -- Metadata
    created_date TIMESTAMP COMMENT 'Record creation timestamp',
    modified_date TIMESTAMP COMMENT 'Last modification timestamp'
)
USING DELTA
COMMENT 'CRM System Customer Data - Source for MDM matching'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'mdm-source-system' = 'crm',
    'mdm-entity-type' = 'customer'
);

-- Insert 50 CRM records:
-- - Some exact matches to master records
-- - Some fuzzy matches (name variations, typos)
-- - Some new records not in master
-- - Some duplicates within CRM itself

INSERT INTO crm_customers VALUES
    -- Exact/close matches to master records (GOLD-001 to GOLD-010)
    ('CRM-1001', 'John', 'Smith', 'john.smith@email.com', '555-0101', '123 Main St Apt 4B', 'New York', 'NY', '10001', 'US', 'Acme Corp', 'customer', 'referral', '2024-06-15', '2024-01-15 10:30:00', '2024-06-15 14:30:00'),
    ('CRM-1002', 'Sarah', 'Johnson', 'sarah.j@company.com', '555-0102', '456 Oak Avenue', 'Los Angeles', 'CA', '90210', 'US', 'TechStart', 'customer', 'web', '2024-06-10', '2024-02-01 09:00:00', '2024-06-10 11:45:00'),
    ('CRM-1003', 'Mike', 'Brown', 'mbrown@gmail.com', '5550103', '789 Pine Road Suite 200', 'Chicago', 'IL', '60601', 'US', 'Global Industries LLC', 'customer', 'trade show', '2024-05-28', '2024-01-20 14:20:00', '2024-05-28 16:15:00'),
    ('CRM-1004', 'Emily', 'Davis', 'emily.davis@outlook.com', '+1-555-0104', '321 Elm Street', 'Houston', 'TX', '77001', 'US', NULL, 'prospect', 'cold call', '2024-06-01', '2024-03-10 08:45:00', '2024-06-01 09:30:00'),
    ('CRM-1005', 'Robert', 'Wilson', 'r.wilson@enterprise.com', '555.0105', '654 Maple Drive Floor 5', 'Phoenix', 'AZ', '85001', 'US', 'Enterprise Solutions Inc', 'customer', 'partner', '2024-06-18', '2024-02-15 11:00:00', '2024-06-18 10:20:00'),
    
    -- Fuzzy matches (name variations, typos, different email domains)
    ('CRM-1006', 'Jennifer', 'Martinez', 'j.martinez@dataflow.io', '(555) 010-6', '987 Cedar Lane', 'Philadelphia', 'PA', '19101', 'US', 'DataFlow', 'customer', 'web', '2024-05-15', '2024-03-01 10:10:00', '2024-05-15 13:40:00'),
    ('CRM-1007', 'Dave', 'Anderson', 'david.anderson@corp.net', '5550107', '147 Birch Way Unit 3', 'San Antonio', 'TX', '78201', 'US', 'CloudBase', 'prospect', 'referral', '2024-04-20', '2024-02-20 16:35:00', '2024-04-20 15:10:00'),
    ('CRM-1008', 'Lisa', 'Thomas', 'l.thomas@startup.co', '555-0108', '258 Spruce Avenue', 'San Diego', 'CA', '92101', 'US', 'Innovation Labs Inc', 'customer', 'web', '2024-06-05', '2024-03-15 12:50:00', '2024-06-05 11:55:00'),
    ('CRM-1009', 'Jim', 'Taylor', 'james.taylor@premier-financial.com', '555 0109', '369 Walnut St Ste 800', 'Dallas', 'TX', '75201', 'US', 'Premier Financial Group', 'customer', 'trade show', '2024-06-12', '2024-04-01 09:25:00', '2024-06-12 14:30:00'),
    ('CRM-1010', 'Pat', 'White', 'patricia.white@healthfirst.org', '555-0110', '741 Hickory Road', 'San Jose', 'CA', '95101', 'US', 'HealthFirst', 'customer', 'web', '2024-05-20', '2024-04-10 15:40:00', '2024-05-20 10:45:00'),
    
    -- More fuzzy matches with data quality issues
    ('CRM-1011', 'Chris', 'Lee', 'clee@mediastream.tv', '555-0111', '852 Ash Court #12', 'Austin', 'TX', '78701', 'US', 'MediaStream', 'lead', 'social media', '2024-04-10', '2024-04-25 11:15:00', '2024-04-10 16:20:00'),
    ('CRM-1012', 'N.', 'Harris', 'nancy.harris@retailmax.com', '5550112', '963 Poplar Boulevard', 'Jacksonville', 'FL', '32099', 'US', 'RetailMax Corporation', 'customer', 'referral', '2024-06-08', '2024-05-01 08:30:00', '2024-06-08 09:40:00'),
    ('CRM-1013', 'Daniel', 'Clark', 'dan.clark@greenenergy.co', '+1(555)0113', '159 Sycamore Way Fl 2', 'Fort Worth', 'TX', '76101', 'US', 'GreenEnergy', 'prospect', 'web', '2024-03-25', '2024-05-10 14:55:00', '2024-03-25 13:35:00'),
    ('CRM-1014', 'Karen', 'Lewis', 'klewis@edutech.org', '555-0114', '357 Chestnut Dr', 'Columbus', 'OH', '43201', 'US', 'EduTech Academy', 'customer', 'partner', '2024-05-30', '2024-05-15 10:20:00', '2024-05-30 11:10:00'),
    ('CRM-1015', 'Mark', 'Robinson', 'mrobinson@fastfreight.net', '555-0115', '468 Willow Ln Ste 150', 'Charlotte', 'NC', '28201', 'US', 'FastFreight', 'customer', 'cold call', '2024-06-20', '2024-05-20 16:45:00', '2024-06-20 15:20:00'),
    
    -- Duplicates within CRM (same person, different records)
    ('CRM-1016', 'John', 'Smith', 'jsmith@acme.com', '555-0101', '123 Main Street', 'New York', 'NY', '10001', 'US', 'Acme Corporation', 'lead', 'trade show', '2024-01-10', '2024-01-05 09:00:00', '2024-01-10 10:15:00'),
    ('CRM-1017', 'J.', 'Smith', 'john.smith@email.com', '5550101', '123 Main St 4B', 'New York', 'NY', '10001', 'US', 'Acme', 'prospect', 'web', '2024-02-20', '2024-02-18 14:00:00', '2024-02-20 11:30:00'),
    ('CRM-1018', 'Sara', 'Johnson', 'sarah.johnson@techstart.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'US', 'TechStart Inc', 'lead', 'referral', '2024-03-05', '2024-03-01 08:45:00', '2024-03-05 09:20:00'),
    
    -- New records not in master (should be flagged as new)
    ('CRM-1019', 'Alexander', 'Thompson', 'athompson@newtech.io', '555-0201', '111 Innovation Way', 'Seattle', 'WA', '98101', 'US', 'NewTech Innovations', 'customer', 'web', '2024-06-25', '2024-06-20 10:00:00', '2024-06-25 14:15:00'),
    ('CRM-1020', 'Samantha', 'Williams', 'swilliams@cloudnine.com', '555-0202', '222 Cloud Street', 'San Francisco', 'CA', '94102', 'US', 'CloudNine Solutions', 'prospect', 'social media', '2024-06-22', '2024-06-15 11:30:00', '2024-06-22 16:45:00'),
    ('CRM-1021', 'Marcus', 'Chen', 'mchen@datawise.ai', '555-0203', '333 AI Boulevard', 'Palo Alto', 'CA', '94301', 'US', 'DataWise AI', 'lead', 'trade show', '2024-06-18', '2024-06-10 09:15:00', '2024-06-18 12:30:00'),
    ('CRM-1022', 'Elena', 'Rodriguez', 'erodriguez@globalfin.com', '555-0204', '444 Finance Plaza', 'Miami', 'FL', '33131', 'US', 'GlobalFin Partners', 'customer', 'referral', '2024-06-28', '2024-06-25 08:00:00', '2024-06-28 09:45:00'),
    ('CRM-1023', 'Brandon', 'Kim', 'bkim@startup.vc', '555-0205', '555 Venture Road', 'Austin', 'TX', '78702', 'US', 'Startup VC Fund', 'prospect', 'cold call', '2024-06-15', '2024-06-01 14:20:00', '2024-06-15 15:35:00'),
    ('CRM-1024', 'Olivia', 'Patel', 'opatel@healthtech.med', '555-0206', '666 Medical Drive', 'Boston', 'MA', '02102', 'US', 'HealthTech Medical', 'customer', 'partner', '2024-06-30', '2024-06-28 10:45:00', '2024-06-30 11:20:00'),
    ('CRM-1025', 'Tyler', 'Jackson', 'tjackson@ecogreen.org', '555-0207', '777 Green Lane', 'Portland', 'OR', '97202', 'US', 'EcoGreen Foundation', 'lead', 'web', '2024-06-12', '2024-06-05 13:00:00', '2024-06-12 14:40:00'),
    
    -- More matches to master records (GOLD-021 to GOLD-030)
    ('CRM-1026', 'George', 'Lopez', 'g.lopez@globaltravel.go', '555-0121', '24 Juniper Way Suite 400', 'Boston', 'MA', '02101', 'US', 'GlobalTravel', 'customer', 'web', '2024-04-28', '2024-04-15 12:15:00', '2024-04-28 11:55:00'),
    ('CRM-1027', 'Helen', 'Hill', 'hhill@safeguard-insurance.safe', '5550122', '135 Laurel Blvd', 'El Paso', 'TX', '79901', 'US', 'SafeGuard Ins', 'customer', 'referral', '2024-04-20', '2024-04-10 14:40:00', '2024-04-20 15:30:00'),
    ('CRM-1028', 'Ed', 'Scott', 'edward.scott@connectnet.net', '(555) 012-3', '246 Fir Lane 7C', 'Detroit', 'MI', '48201', 'US', 'ConnectNet', 'prospect', 'trade show', '2024-04-15', '2024-04-05 10:05:00', '2024-04-15 10:50:00'),
    ('CRM-1029', 'Donna', 'Green', 'd.green@primeproperty.estate', '555-0124', '357 Aspen Road', 'Nashville', 'TN', '37201', 'US', 'PrimeProperty', 'customer', 'cold call', '2024-04-10', '2024-04-01 16:30:00', '2024-04-10 14:25:00'),
    ('CRM-1030', 'Kenneth', 'Adams', 'ken.adams@secureshield.pro', '555 012 5', '468 Alder Ave Floor 6', 'Portland', 'OR', '97201', 'US', 'SecureShield', 'customer', 'partner', '2024-04-05', '2024-03-25 09:55:00', '2024-04-05 09:30:00'),
    
    -- More new records
    ('CRM-1031', 'Victoria', 'Chang', 'vchang@techpioneer.com', '555-0208', '888 Pioneer Street', 'San Jose', 'CA', '95102', 'US', 'TechPioneer Labs', 'lead', 'social media', '2024-06-08', '2024-06-01 08:30:00', '2024-06-08 09:15:00'),
    ('CRM-1032', 'Nathan', 'Singh', 'nsingh@datastream.io', '555-0209', '999 Stream Avenue', 'Atlanta', 'GA', '30302', 'US', 'DataStream Analytics', 'prospect', 'web', '2024-06-05', '2024-05-28 11:00:00', '2024-06-05 12:30:00'),
    ('CRM-1033', 'Rachel', 'Nguyen', 'rnguyen@smartsolutions.biz', '555-0210', '100 Smart Way', 'Chicago', 'IL', '60602', 'US', 'Smart Solutions Inc', 'customer', 'trade show', '2024-06-02', '2024-05-25 14:15:00', '2024-06-02 15:45:00'),
    ('CRM-1034', 'Derek', 'Okonkwo', 'dokonkwo@innovatech.tech', '555-0211', '200 Tech Park', 'Dallas', 'TX', '75202', 'US', 'InnovaTech Systems', 'lead', 'referral', '2024-05-30', '2024-05-20 09:30:00', '2024-05-30 10:20:00'),
    ('CRM-1035', 'Sophia', 'Martinez', 'smartinez@futureforward.co', '555-0212', '300 Future Blvd', 'Phoenix', 'AZ', '85002', 'US', 'FutureForward Corp', 'prospect', 'cold call', '2024-05-25', '2024-05-15 13:45:00', '2024-05-25 14:55:00'),
    
    -- More fuzzy matches to master (GOLD-041 to GOLD-050)
    ('CRM-1036', 'Gary', 'Morris', 'g.morris@cloudtech.cloud', '555-0141', '24 Saffron Court Ste 100', 'Raleigh', 'NC', '27601', 'US', 'CloudTech', 'customer', 'web', '2024-03-08', '2024-02-28 16:35:00', '2024-03-08 16:10:00'),
    ('CRM-1037', 'Angela', 'Rogers', 'angela.rogers@smilebright.smile', '5550142', '135 Paprika Way', 'Omaha', 'NE', '68101', 'US', 'SmileBright', 'customer', 'partner', '2024-03-05', '2024-02-25 09:00:00', '2024-03-05 09:35:00'),
    ('CRM-1038', 'Nick', 'Reed', 'nreed@sparkleclean.auto', '555-014-3', '246 Cayenne Street #12', 'Miami', 'FL', '33101', 'US', 'SparkleClean', 'prospect', 'referral', '2024-03-02', '2024-02-20 13:25:00', '2024-03-02 13:00:00'),
    ('CRM-1039', 'Stephanie', 'Cook', 's.cook@zenyoga.zen', '555.0144', '357 Cardamom Blvd', 'Oakland', 'CA', '94601', 'US', 'ZenYoga', 'customer', 'social media', '2024-02-28', '2024-02-15 11:50:00', '2024-02-28 12:15:00'),
    ('CRM-1040', 'Eric', 'Morgan', 'e.morgan@snapshot.snap', '+1-555-0145', '468 Ginger Lane Fl 7', 'Minneapolis', 'MN', '55401', 'US', 'SnapShot Photo', 'lead', 'trade show', '2024-02-25', '2024-02-10 15:15:00', '2024-02-25 15:40:00'),
    
    -- Records with data quality issues
    ('CRM-1041', 'JOHN', 'SMITH', 'JOHN.SMITH@EMAIL.COM', '5550101', '123 MAIN ST', 'NEW YORK', 'NY', '10001', 'US', 'ACME CORP', 'customer', 'web', '2024-06-25', '2024-06-20 10:00:00', '2024-06-25 10:30:00'),
    ('CRM-1042', 'john', 'smith', 'johnsmith@gmail.com', '555.010.1', '123 main street', 'new york', 'ny', '10001', 'us', 'acme', 'lead', 'referral', '2024-06-28', '2024-06-25 08:15:00', '2024-06-28 08:45:00'),
    ('CRM-1043', NULL, 'Unknown Customer', 'unknown@test.com', '555-9999', '999 Unknown Ave', 'Unknown City', 'XX', '00000', 'US', NULL, 'lead', 'import', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    ('CRM-1044', 'Test', 'Record', 'test@test.test', '000-0000', 'Test Address', 'Test City', 'TS', '12345', 'US', 'Test Company', 'lead', 'test', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
    
    -- More new legitimate records
    ('CRM-1045', 'Amanda', 'Foster', 'afoster@greentech.eco', '555-0213', '400 Eco Drive', 'Denver', 'CO', '80202', 'US', 'GreenTech Solutions', 'customer', 'web', '2024-05-20', '2024-05-10 10:30:00', '2024-05-20 11:15:00'),
    ('CRM-1046', 'Jonathan', 'Park', 'jpark@medicalai.health', '555-0214', '500 Health Way', 'Houston', 'TX', '77002', 'US', 'MedicalAI Inc', 'prospect', 'trade show', '2024-05-15', '2024-05-05 14:00:00', '2024-05-15 14:45:00'),
    ('CRM-1047', 'Michelle', 'Lee', 'mlee@financeplus.fin', '555-0215', '600 Finance Plaza', 'New York', 'NY', '10002', 'US', 'FinancePlus Partners', 'customer', 'partner', '2024-05-10', '2024-05-01 09:45:00', '2024-05-10 10:20:00'),
    ('CRM-1048', 'Christopher', 'Wang', 'cwang@roboticslab.ai', '555-0216', '700 Robot Lane', 'San Francisco', 'CA', '94103', 'US', 'Robotics Lab AI', 'lead', 'social media', '2024-05-05', '2024-04-25 13:20:00', '2024-05-05 14:05:00'),
    ('CRM-1049', 'Jennifer', 'Brown', 'jbrown@cloudservices.net', '555-0217', '800 Cloud Street', 'Seattle', 'WA', '98102', 'US', 'Cloud Services Pro', 'prospect', 'web', '2024-05-01', '2024-04-20 11:50:00', '2024-05-01 12:35:00'),
    ('CRM-1050', 'William', 'Davis', 'wdavis@dataanalytics.io', '555-0218', '900 Data Drive', 'Austin', 'TX', '78703', 'US', 'Data Analytics Hub', 'customer', 'referral', '2024-04-25', '2024-04-15 08:30:00', '2024-04-25 09:15:00');

-- Verify the data
SELECT COUNT(*) as total_records FROM crm_customers;

-- Show some statistics about the data
SELECT 
    account_type,
    COUNT(*) as count
FROM crm_customers
GROUP BY account_type
ORDER BY count DESC;

