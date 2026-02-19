-- ============================================================================
-- MDM Demo: Customer Master Table
-- ============================================================================
-- This is the "golden record" master table for customer data.
-- It contains deduplicated, merged customer records from various sources.
-- ============================================================================

USE CATALOG app_demo_data;
USE SCHEMA mdm_demo;

-- Drop existing table if it exists (for demo reset purposes)
DROP TABLE IF EXISTS customer_master;

-- Create the customer master table
CREATE TABLE customer_master (
    -- Primary identifier
    golden_id STRING NOT NULL COMMENT 'Unique golden record identifier',
    
    -- Core customer attributes
    customer_name STRING COMMENT 'Full customer name',
    email STRING COMMENT 'Primary email address',
    phone STRING COMMENT 'Primary phone number',
    
    -- Address information
    address_line1 STRING COMMENT 'Street address line 1',
    address_line2 STRING COMMENT 'Street address line 2 (apt, suite, etc.)',
    city STRING COMMENT 'City name',
    state STRING COMMENT 'State or province code',
    postal_code STRING COMMENT 'Postal/ZIP code',
    country STRING COMMENT 'Country code (ISO 3166-1 alpha-2)',
    
    -- Business attributes
    company_name STRING COMMENT 'Company or organization name',
    industry STRING COMMENT 'Industry classification',
    customer_type STRING COMMENT 'Customer type: individual, business, enterprise',
    
    -- Metadata
    source_systems ARRAY<STRING> COMMENT 'List of source systems contributing to this record',
    confidence_score DOUBLE COMMENT 'Overall data quality confidence score (0-1)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    last_merged_at TIMESTAMP COMMENT 'Last MDM merge timestamp'
)
USING DELTA
COMMENT 'Customer Master Data - Golden records for customer entity'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'data-product-name' = 'Customer Master',
    'data-product-domain' = 'Customer',
    'mdm-entity-type' = 'customer',
    'mdm-role' = 'master'
);

-- Insert 100 sample master records
INSERT INTO customer_master VALUES
    ('GOLD-001', 'John Smith', 'john.smith@email.com', '+1-555-0101', '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'US', 'Acme Corp', 'Technology', 'business', ARRAY('CRM', 'ERP'), 0.95, '2024-01-15 10:30:00', '2024-06-20 14:45:00', '2024-06-20 14:45:00'),
    ('GOLD-002', 'Sarah Johnson', 'sarah.j@company.com', '+1-555-0102', '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90210', 'US', 'TechStart Inc', 'Technology', 'enterprise', ARRAY('CRM', 'Marketing'), 0.92, '2024-01-16 09:15:00', '2024-06-18 11:20:00', '2024-06-18 11:20:00'),
    ('GOLD-003', 'Michael Brown', 'mbrown@gmail.com', '+1-555-0103', '789 Pine Rd', 'Suite 200', 'Chicago', 'IL', '60601', 'US', 'Global Industries', 'Manufacturing', 'business', ARRAY('ERP'), 0.88, '2024-01-17 11:45:00', '2024-06-15 16:30:00', '2024-06-15 16:30:00'),
    ('GOLD-004', 'Emily Davis', 'emily.davis@outlook.com', '+1-555-0104', '321 Elm St', NULL, 'Houston', 'TX', '77001', 'US', NULL, NULL, 'individual', ARRAY('CRM'), 0.90, '2024-01-18 08:00:00', '2024-06-12 09:15:00', '2024-06-12 09:15:00'),
    ('GOLD-005', 'Robert Wilson', 'rwilson@enterprise.com', '+1-555-0105', '654 Maple Dr', 'Floor 5', 'Phoenix', 'AZ', '85001', 'US', 'Enterprise Solutions', 'Consulting', 'enterprise', ARRAY('CRM', 'ERP', 'Marketing'), 0.97, '2024-01-19 14:20:00', '2024-06-10 10:45:00', '2024-06-10 10:45:00'),
    ('GOLD-006', 'Jennifer Martinez', 'jmartinez@tech.io', '+1-555-0106', '987 Cedar Ln', NULL, 'Philadelphia', 'PA', '19101', 'US', 'DataFlow LLC', 'Technology', 'business', ARRAY('CRM'), 0.91, '2024-01-20 10:10:00', '2024-06-08 13:20:00', '2024-06-08 13:20:00'),
    ('GOLD-007', 'David Anderson', 'danderson@corp.net', '+1-555-0107', '147 Birch Way', 'Unit 3', 'San Antonio', 'TX', '78201', 'US', 'CloudBase Systems', 'Technology', 'enterprise', ARRAY('ERP', 'Marketing'), 0.89, '2024-01-21 16:35:00', '2024-06-05 15:50:00', '2024-06-05 15:50:00'),
    ('GOLD-008', 'Lisa Thomas', 'lthomas@startup.co', '+1-555-0108', '258 Spruce Ave', NULL, 'San Diego', 'CA', '92101', 'US', 'Innovation Labs', 'Research', 'business', ARRAY('CRM', 'ERP'), 0.94, '2024-01-22 12:50:00', '2024-06-03 11:30:00', '2024-06-03 11:30:00'),
    ('GOLD-009', 'James Taylor', 'jtaylor@finance.com', '+1-555-0109', '369 Walnut St', 'Suite 800', 'Dallas', 'TX', '75201', 'US', 'Premier Financial', 'Finance', 'enterprise', ARRAY('CRM', 'ERP', 'Finance'), 0.96, '2024-01-23 09:25:00', '2024-06-01 14:15:00', '2024-06-01 14:15:00'),
    ('GOLD-010', 'Patricia White', 'pwhite@health.org', '+1-555-0110', '741 Hickory Rd', NULL, 'San Jose', 'CA', '95101', 'US', 'HealthFirst Network', 'Healthcare', 'business', ARRAY('CRM'), 0.87, '2024-01-24 15:40:00', '2024-05-28 10:20:00', '2024-05-28 10:20:00'),
    ('GOLD-011', 'Christopher Lee', 'clee@media.tv', '+1-555-0111', '852 Ash Ct', 'Apt 12', 'Austin', 'TX', '78701', 'US', 'MediaStream Inc', 'Media', 'business', ARRAY('Marketing'), 0.93, '2024-01-25 11:15:00', '2024-05-25 16:40:00', '2024-05-25 16:40:00'),
    ('GOLD-012', 'Nancy Harris', 'nharris@retail.com', '+1-555-0112', '963 Poplar Blvd', NULL, 'Jacksonville', 'FL', '32099', 'US', 'RetailMax Corp', 'Retail', 'enterprise', ARRAY('CRM', 'ERP'), 0.91, '2024-01-26 08:30:00', '2024-05-22 09:55:00', '2024-05-22 09:55:00'),
    ('GOLD-013', 'Daniel Clark', 'dclark@energy.co', '+1-555-0113', '159 Sycamore Way', 'Floor 2', 'Fort Worth', 'TX', '76101', 'US', 'GreenEnergy Solutions', 'Energy', 'business', ARRAY('ERP'), 0.86, '2024-01-27 14:55:00', '2024-05-20 13:10:00', '2024-05-20 13:10:00'),
    ('GOLD-014', 'Karen Lewis', 'klewis@edu.org', '+1-555-0114', '357 Chestnut Dr', NULL, 'Columbus', 'OH', '43201', 'US', 'EduTech Academy', 'Education', 'business', ARRAY('CRM', 'Marketing'), 0.90, '2024-01-28 10:20:00', '2024-05-18 11:25:00', '2024-05-18 11:25:00'),
    ('GOLD-015', 'Mark Robinson', 'mrobinson@logistics.net', '+1-555-0115', '468 Willow Ln', 'Suite 150', 'Charlotte', 'NC', '28201', 'US', 'FastFreight Logistics', 'Logistics', 'enterprise', ARRAY('CRM', 'ERP', 'Logistics'), 0.95, '2024-01-29 16:45:00', '2024-05-15 15:35:00', '2024-05-15 15:35:00'),
    ('GOLD-016', 'Betty Walker', 'bwalker@legal.law', '+1-555-0116', '579 Beech Rd', NULL, 'San Francisco', 'CA', '94101', 'US', 'Walker & Associates', 'Legal', 'business', ARRAY('CRM'), 0.88, '2024-01-30 09:10:00', '2024-05-12 10:50:00', '2024-05-12 10:50:00'),
    ('GOLD-017', 'Steven Hall', 'shall@auto.com', '+1-555-0117', '680 Magnolia Ave', 'Unit 8', 'Indianapolis', 'IN', '46201', 'US', 'AutoTech Motors', 'Automotive', 'business', ARRAY('ERP', 'Marketing'), 0.92, '2024-01-31 13:35:00', '2024-05-10 14:20:00', '2024-05-10 14:20:00'),
    ('GOLD-018', 'Dorothy Young', 'dyoung@pharma.rx', '+1-555-0118', '791 Redwood Ct', NULL, 'Seattle', 'WA', '98101', 'US', 'BioPharm Research', 'Pharmaceutical', 'enterprise', ARRAY('CRM', 'ERP'), 0.94, '2024-02-01 11:00:00', '2024-05-08 12:45:00', '2024-05-08 12:45:00'),
    ('GOLD-019', 'Paul King', 'pking@construct.build', '+1-555-0119', '802 Cypress St', 'Floor 3', 'Denver', 'CO', '80201', 'US', 'BuildRight Construction', 'Construction', 'business', ARRAY('ERP'), 0.85, '2024-02-02 15:25:00', '2024-05-05 16:15:00', '2024-05-05 16:15:00'),
    ('GOLD-020', 'Sandra Wright', 'swright@food.eat', '+1-555-0120', '913 Palm Dr', NULL, 'Washington', 'DC', '20001', 'US', 'FreshFood Distributors', 'Food & Beverage', 'business', ARRAY('CRM', 'Marketing'), 0.89, '2024-02-03 08:50:00', '2024-05-02 09:30:00', '2024-05-02 09:30:00'),
    ('GOLD-021', 'George Lopez', 'glopez@travel.go', '+1-555-0121', '024 Juniper Way', 'Suite 400', 'Boston', 'MA', '02101', 'US', 'GlobalTravel Agency', 'Travel', 'enterprise', ARRAY('CRM', 'ERP', 'Marketing'), 0.96, '2024-02-04 12:15:00', '2024-04-30 11:40:00', '2024-04-30 11:40:00'),
    ('GOLD-022', 'Helen Hill', 'hhill@insurance.safe', '+1-555-0122', '135 Laurel Blvd', NULL, 'El Paso', 'TX', '79901', 'US', 'SafeGuard Insurance', 'Insurance', 'business', ARRAY('CRM'), 0.91, '2024-02-05 14:40:00', '2024-04-28 15:55:00', '2024-04-28 15:55:00'),
    ('GOLD-023', 'Edward Scott', 'escott@telecom.net', '+1-555-0123', '246 Fir Ln', 'Apt 7C', 'Detroit', 'MI', '48201', 'US', 'ConnectNet Telecom', 'Telecommunications', 'enterprise', ARRAY('ERP', 'Billing'), 0.93, '2024-02-06 10:05:00', '2024-04-25 10:25:00', '2024-04-25 10:25:00'),
    ('GOLD-024', 'Donna Green', 'dgreen@real.estate', '+1-555-0124', '357 Aspen Rd', NULL, 'Nashville', 'TN', '37201', 'US', 'PrimeProperty Realty', 'Real Estate', 'business', ARRAY('CRM', 'Marketing'), 0.87, '2024-02-07 16:30:00', '2024-04-22 14:10:00', '2024-04-22 14:10:00'),
    ('GOLD-025', 'Kenneth Adams', 'kadams@security.pro', '+1-555-0125', '468 Alder Ave', 'Floor 6', 'Portland', 'OR', '97201', 'US', 'SecureShield Systems', 'Security', 'business', ARRAY('CRM', 'ERP'), 0.90, '2024-02-08 09:55:00', '2024-04-20 09:45:00', '2024-04-20 09:45:00'),
    ('GOLD-026', 'Carol Baker', 'cbaker@fashion.style', '+1-555-0126', '579 Holly Ct', NULL, 'Oklahoma City', 'OK', '73101', 'US', 'StyleHouse Fashion', 'Fashion', 'business', ARRAY('Marketing'), 0.88, '2024-02-09 13:20:00', '2024-04-18 13:30:00', '2024-04-18 13:30:00'),
    ('GOLD-027', 'Ronald Nelson', 'rnelson@sport.fit', '+1-555-0127', '680 Ivy St', 'Suite 75', 'Las Vegas', 'NV', '89101', 'US', 'FitLife Sports', 'Sports & Fitness', 'enterprise', ARRAY('CRM', 'ERP'), 0.94, '2024-02-10 11:45:00', '2024-04-15 12:15:00', '2024-04-15 12:15:00'),
    ('GOLD-028', 'Michelle Carter', 'mcarter@beauty.glow', '+1-555-0128', '791 Moss Way', NULL, 'Louisville', 'KY', '40201', 'US', 'GlowBeauty Products', 'Beauty', 'business', ARRAY('CRM', 'Marketing'), 0.86, '2024-02-11 15:10:00', '2024-04-12 16:40:00', '2024-04-12 16:40:00'),
    ('GOLD-029', 'Anthony Mitchell', 'amitchell@event.plan', '+1-555-0129', '802 Sage Dr', 'Unit 22', 'Baltimore', 'MD', '21201', 'US', 'EventPro Planners', 'Events', 'business', ARRAY('CRM'), 0.89, '2024-02-12 08:35:00', '2024-04-10 10:55:00', '2024-04-10 10:55:00'),
    ('GOLD-030', 'Margaret Perez', 'mperez@clean.eco', '+1-555-0130', '913 Thyme Blvd', NULL, 'Milwaukee', 'WI', '53201', 'US', 'EcoClean Services', 'Environmental', 'business', ARRAY('ERP'), 0.91, '2024-02-13 14:00:00', '2024-04-08 15:20:00', '2024-04-08 15:20:00'),
    ('GOLD-031', 'Joshua Roberts', 'jroberts@game.play', '+1-555-0131', '024 Basil Ln', 'Apt 15', 'Albuquerque', 'NM', '87101', 'US', 'GameZone Entertainment', 'Gaming', 'business', ARRAY('CRM', 'Marketing'), 0.93, '2024-02-14 10:25:00', '2024-04-05 11:35:00', '2024-04-05 11:35:00'),
    ('GOLD-032', 'Laura Turner', 'lturner@pet.care', '+1-555-0132', '135 Oregano Ave', NULL, 'Tucson', 'AZ', '85701', 'US', 'PetCare Plus', 'Pet Services', 'business', ARRAY('CRM'), 0.87, '2024-02-15 16:50:00', '2024-04-02 14:50:00', '2024-04-02 14:50:00'),
    ('GOLD-033', 'Brian Phillips', 'bphillips@music.beat', '+1-555-0133', '246 Parsley Rd', 'Suite 30', 'Fresno', 'CA', '93701', 'US', 'BeatWave Records', 'Music', 'enterprise', ARRAY('CRM', 'ERP', 'Marketing'), 0.95, '2024-02-16 09:15:00', '2024-03-30 10:05:00', '2024-03-30 10:05:00'),
    ('GOLD-034', 'Sharon Campbell', 'scampbell@art.create', '+1-555-0134', '357 Cilantro Ct', NULL, 'Sacramento', 'CA', '95801', 'US', 'Creative Arts Studio', 'Arts', 'business', ARRAY('CRM'), 0.84, '2024-02-17 13:40:00', '2024-03-28 13:25:00', '2024-03-28 13:25:00'),
    ('GOLD-035', 'Kevin Parker', 'kparker@home.decor', '+1-555-0135', '468 Dill Way', 'Floor 4', 'Long Beach', 'CA', '90801', 'US', 'HomeStyle Decor', 'Home & Garden', 'business', ARRAY('Marketing'), 0.88, '2024-02-18 11:05:00', '2024-03-25 12:40:00', '2024-03-25 12:40:00'),
    ('GOLD-036', 'Kimberly Evans', 'kevans@child.care', '+1-555-0136', '579 Mint St', NULL, 'Kansas City', 'MO', '64101', 'US', 'BrightStart Childcare', 'Childcare', 'business', ARRAY('CRM', 'ERP'), 0.90, '2024-02-19 15:30:00', '2024-03-22 15:55:00', '2024-03-22 15:55:00'),
    ('GOLD-037', 'Jason Edwards', 'jedwards@fitness.gym', '+1-555-0137', '680 Chive Blvd', 'Unit 5', 'Mesa', 'AZ', '85201', 'US', 'PowerFit Gyms', 'Fitness', 'enterprise', ARRAY('CRM', 'Marketing'), 0.92, '2024-02-20 08:55:00', '2024-03-20 09:20:00', '2024-03-20 09:20:00'),
    ('GOLD-038', 'Deborah Collins', 'dcollins@book.read', '+1-555-0138', '791 Fennel Ln', NULL, 'Virginia Beach', 'VA', '23450', 'US', 'BookWorld Publishers', 'Publishing', 'business', ARRAY('CRM'), 0.86, '2024-02-21 12:20:00', '2024-03-18 11:45:00', '2024-03-18 11:45:00'),
    ('GOLD-039', 'Ryan Stewart', 'rstewart@coffee.brew', '+1-555-0139', '802 Cumin Ave', 'Apt 9', 'Atlanta', 'GA', '30301', 'US', 'BrewMaster Coffee', 'Food & Beverage', 'business', ARRAY('ERP', 'Marketing'), 0.89, '2024-02-22 14:45:00', '2024-03-15 14:10:00', '2024-03-15 14:10:00'),
    ('GOLD-040', 'Amanda Sanchez', 'asanchez@spa.relax', '+1-555-0140', '913 Turmeric Rd', NULL, 'Colorado Springs', 'CO', '80901', 'US', 'Serenity Spa', 'Wellness', 'business', ARRAY('CRM'), 0.91, '2024-02-23 10:10:00', '2024-03-12 10:35:00', '2024-03-12 10:35:00'),
    ('GOLD-041', 'Gary Morris', 'gmorris@tech.cloud', '+1-555-0141', '024 Saffron Ct', 'Suite 100', 'Raleigh', 'NC', '27601', 'US', 'CloudTech Innovations', 'Technology', 'enterprise', ARRAY('CRM', 'ERP', 'Cloud'), 0.97, '2024-02-24 16:35:00', '2024-03-10 16:25:00', '2024-03-10 16:25:00'),
    ('GOLD-042', 'Angela Rogers', 'arogers@dental.smile', '+1-555-0142', '135 Paprika Way', NULL, 'Omaha', 'NE', '68101', 'US', 'SmileBright Dental', 'Healthcare', 'business', ARRAY('CRM', 'ERP'), 0.88, '2024-02-25 09:00:00', '2024-03-08 09:50:00', '2024-03-08 09:50:00'),
    ('GOLD-043', 'Nicholas Reed', 'nreed@car.wash', '+1-555-0143', '246 Cayenne St', 'Unit 12', 'Miami', 'FL', '33101', 'US', 'SparkleClean Auto', 'Automotive', 'business', ARRAY('Marketing'), 0.85, '2024-02-26 13:25:00', '2024-03-05 13:15:00', '2024-03-05 13:15:00'),
    ('GOLD-044', 'Stephanie Cook', 'scook@yoga.zen', '+1-555-0144', '357 Cardamom Blvd', NULL, 'Oakland', 'CA', '94601', 'US', 'ZenYoga Studios', 'Fitness', 'business', ARRAY('CRM', 'Marketing'), 0.90, '2024-02-27 11:50:00', '2024-03-02 12:30:00', '2024-03-02 12:30:00'),
    ('GOLD-045', 'Eric Morgan', 'emorgan@photo.snap', '+1-555-0145', '468 Ginger Ln', 'Floor 7', 'Minneapolis', 'MN', '55401', 'US', 'SnapShot Photography', 'Photography', 'business', ARRAY('CRM'), 0.87, '2024-02-28 15:15:00', '2024-02-28 15:55:00', '2024-02-28 15:55:00'),
    ('GOLD-046', 'Melissa Bell', 'mbell@florist.bloom', '+1-555-0146', '579 Nutmeg Ave', NULL, 'Tulsa', 'OK', '74101', 'US', 'BloomFloral Designs', 'Floral', 'business', ARRAY('Marketing'), 0.84, '2024-03-01 08:40:00', '2024-06-22 10:20:00', '2024-06-22 10:20:00'),
    ('GOLD-047', 'Timothy Murphy', 'tmurphy@plumb.fix', '+1-555-0147', '680 Allspice Rd', 'Suite 25', 'Arlington', 'TX', '76001', 'US', 'QuickFix Plumbing', 'Home Services', 'business', ARRAY('ERP'), 0.89, '2024-03-02 12:05:00', '2024-06-20 14:45:00', '2024-06-20 14:45:00'),
    ('GOLD-048', 'Rebecca Rivera', 'rrivera@dance.move', '+1-555-0148', '791 Clove Ct', NULL, 'Wichita', 'KS', '67201', 'US', 'MoveIt Dance Academy', 'Dance', 'business', ARRAY('CRM', 'Marketing'), 0.92, '2024-03-03 14:30:00', '2024-06-18 11:35:00', '2024-06-18 11:35:00'),
    ('GOLD-049', 'Jeffrey Cooper', 'jcooper@electric.power', '+1-555-0149', '802 Mace Way', 'Apt 3D', 'New Orleans', 'LA', '70112', 'US', 'PowerUp Electric', 'Utilities', 'business', ARRAY('ERP'), 0.86, '2024-03-04 10:55:00', '2024-06-15 16:10:00', '2024-06-15 16:10:00'),
    ('GOLD-050', 'Jessica Richardson', 'jrichardson@bakery.sweet', '+1-555-0150', '913 Anise St', NULL, 'Cleveland', 'OH', '44101', 'US', 'SweetTreats Bakery', 'Food & Beverage', 'business', ARRAY('CRM', 'Marketing'), 0.91, '2024-03-05 16:20:00', '2024-06-12 09:25:00', '2024-06-12 09:25:00'),
    ('GOLD-051', 'Larry Cox', 'lcox@lawn.care', '+1-555-0151', '024 Vanilla Blvd', 'Unit 18', 'Bakersfield', 'CA', '93301', 'US', 'GreenLawn Services', 'Landscaping', 'business', ARRAY('ERP'), 0.85, '2024-03-06 09:45:00', '2024-06-10 10:55:00', '2024-06-10 10:55:00'),
    ('GOLD-052', 'Amy Howard', 'ahoward@nail.salon', '+1-555-0152', '135 Cinnamon Ln', NULL, 'Tampa', 'FL', '33601', 'US', 'NailArt Studio', 'Beauty', 'business', ARRAY('CRM'), 0.88, '2024-03-07 13:10:00', '2024-06-08 13:40:00', '2024-06-08 13:40:00'),
    ('GOLD-053', 'Scott Ward', 'sward@hvac.cool', '+1-555-0153', '246 Cocoa Ave', 'Floor 1', 'Honolulu', 'HI', '96801', 'US', 'CoolBreeze HVAC', 'Home Services', 'enterprise', ARRAY('CRM', 'ERP'), 0.93, '2024-03-08 15:35:00', '2024-06-05 15:20:00', '2024-06-05 15:20:00'),
    ('GOLD-054', 'Heather Torres', 'htorres@tailor.fit', '+1-555-0154', '357 Mocha Rd', NULL, 'Aurora', 'CO', '80010', 'US', 'PerfectFit Tailoring', 'Fashion', 'business', ARRAY('CRM'), 0.86, '2024-03-09 08:00:00', '2024-06-03 11:55:00', '2024-06-03 11:55:00'),
    ('GOLD-055', 'Raymond Peterson', 'rpeterson@roof.top', '+1-555-0155', '468 Hazel Ct', 'Suite 50', 'Anaheim', 'CA', '92801', 'US', 'TopNotch Roofing', 'Construction', 'business', ARRAY('ERP'), 0.90, '2024-03-10 11:25:00', '2024-06-01 14:30:00', '2024-06-01 14:30:00'),
    ('GOLD-056', 'Christina Gray', 'cgray@print.press', '+1-555-0156', '579 Olive Way', NULL, 'Santa Ana', 'CA', '92701', 'US', 'PrintPress Solutions', 'Printing', 'business', ARRAY('Marketing'), 0.87, '2024-03-11 14:50:00', '2024-05-28 10:05:00', '2024-05-28 10:05:00'),
    ('GOLD-057', 'Dennis Ramirez', 'dramirez@move.truck', '+1-555-0157', '680 Pecan St', 'Apt 21', 'Corpus Christi', 'TX', '78401', 'US', 'QuickMove Trucking', 'Logistics', 'enterprise', ARRAY('CRM', 'ERP', 'Logistics'), 0.94, '2024-03-12 10:15:00', '2024-05-25 16:50:00', '2024-05-25 16:50:00'),
    ('GOLD-058', 'Kathleen James', 'kjames@clean.home', '+1-555-0158', '791 Almond Blvd', NULL, 'Riverside', 'CA', '92501', 'US', 'HomeSparkle Cleaning', 'Home Services', 'business', ARRAY('CRM'), 0.89, '2024-03-13 16:40:00', '2024-05-22 09:40:00', '2024-05-22 09:40:00'),
    ('GOLD-059', 'Arthur Watson', 'awatson@paint.color', '+1-555-0159', '802 Cashew Ln', 'Unit 7', 'Lexington', 'KY', '40501', 'US', 'ColorSplash Painters', 'Home Services', 'business', ARRAY('ERP', 'Marketing'), 0.86, '2024-03-14 09:05:00', '2024-05-20 13:25:00', '2024-05-20 13:25:00'),
    ('GOLD-060', 'Frances Brooks', 'fbrooks@tutoring.learn', '+1-555-0160', '913 Pistachio Ave', NULL, 'Stockton', 'CA', '95201', 'US', 'LearnSmart Tutoring', 'Education', 'business', ARRAY('CRM', 'Marketing'), 0.91, '2024-03-15 13:30:00', '2024-05-18 11:55:00', '2024-05-18 11:55:00'),
    ('GOLD-061', 'Wayne Kelly', 'wkelly@pest.control', '+1-555-0161', '024 Macadamia Rd', 'Floor 2', 'Pittsburgh', 'PA', '15201', 'US', 'BugBuster Pest Control', 'Home Services', 'business', ARRAY('ERP'), 0.84, '2024-03-16 11:55:00', '2024-05-15 15:15:00', '2024-05-15 15:15:00'),
    ('GOLD-062', 'Teresa Sanders', 'tsanders@senior.care', '+1-555-0162', '135 Brazil Ct', NULL, 'Anchorage', 'AK', '99501', 'US', 'GoldenYears Care', 'Healthcare', 'enterprise', ARRAY('CRM', 'ERP'), 0.95, '2024-03-17 15:20:00', '2024-05-12 10:40:00', '2024-05-12 10:40:00'),
    ('GOLD-063', 'Russell Price', 'rprice@security.guard', '+1-555-0163', '246 Walnut Way', 'Suite 80', 'Cincinnati', 'OH', '45201', 'US', 'ShieldForce Security', 'Security', 'business', ARRAY('CRM', 'ERP'), 0.92, '2024-03-18 08:45:00', '2024-05-10 14:05:00', '2024-05-10 14:05:00'),
    ('GOLD-064', 'Janet Bennett', 'jbennett@catering.feast', '+1-555-0164', '357 Chestnut St', NULL, 'Newark', 'NJ', '07101', 'US', 'FeastMaster Catering', 'Food & Beverage', 'business', ARRAY('Marketing'), 0.88, '2024-03-19 12:10:00', '2024-05-08 12:30:00', '2024-05-08 12:30:00'),
    ('GOLD-065', 'Gregory Wood', 'gwood@furniture.craft', '+1-555-0165', '468 Oak Blvd', 'Apt 14', 'Toledo', 'OH', '43601', 'US', 'CraftWood Furniture', 'Furniture', 'business', ARRAY('CRM', 'ERP'), 0.90, '2024-03-20 14:35:00', '2024-05-05 16:55:00', '2024-05-05 16:55:00'),
    ('GOLD-066', 'Diane Barnes', 'dbarnes@jewelry.shine', '+1-555-0166', '579 Pine Ln', NULL, 'St. Louis', 'MO', '63101', 'US', 'ShineGem Jewelry', 'Jewelry', 'business', ARRAY('CRM'), 0.85, '2024-03-21 10:00:00', '2024-05-02 09:20:00', '2024-05-02 09:20:00'),
    ('GOLD-067', 'Henry Ross', 'hross@window.clean', '+1-555-0167', '680 Maple Ave', 'Unit 33', 'Plano', 'TX', '75023', 'US', 'CrystalClear Windows', 'Home Services', 'business', ARRAY('ERP'), 0.87, '2024-03-22 16:25:00', '2024-04-30 11:10:00', '2024-04-30 11:10:00'),
    ('GOLD-068', 'Ruth Henderson', 'rhenderson@daycare.kids', '+1-555-0168', '791 Elm Rd', NULL, 'Lincoln', 'NE', '68501', 'US', 'HappyKids Daycare', 'Childcare', 'enterprise', ARRAY('CRM', 'ERP', 'Marketing'), 0.93, '2024-03-23 09:50:00', '2024-04-28 15:35:00', '2024-04-28 15:35:00'),
    ('GOLD-069', 'Albert Coleman', 'acoleman@tire.shop', '+1-555-0169', '802 Birch Ct', 'Floor 1', 'Irvine', 'CA', '92602', 'US', 'TireMax Auto Center', 'Automotive', 'business', ARRAY('CRM'), 0.89, '2024-03-24 13:15:00', '2024-04-25 10:50:00', '2024-04-25 10:50:00'),
    ('GOLD-070', 'Judith Jenkins', 'jjenkins@alterations.sew', '+1-555-0170', '913 Cedar Way', NULL, 'Chula Vista', 'CA', '91910', 'US', 'SewPerfect Alterations', 'Fashion', 'business', ARRAY('Marketing'), 0.86, '2024-03-25 11:40:00', '2024-04-22 14:25:00', '2024-04-22 14:25:00'),
    ('GOLD-071', 'Willie Perry', 'wperry@locksmith.key', '+1-555-0171', '024 Spruce St', 'Suite 15', 'Scottsdale', 'AZ', '85251', 'US', 'KeyMaster Locksmith', 'Home Services', 'business', ARRAY('ERP'), 0.84, '2024-03-26 15:05:00', '2024-04-20 09:55:00', '2024-04-20 09:55:00'),
    ('GOLD-072', 'Rose Powell', 'rpowell@massage.relax', '+1-555-0172', '135 Ash Blvd', NULL, 'Laredo', 'TX', '78040', 'US', 'RelaxZone Massage', 'Wellness', 'business', ARRAY('CRM', 'Marketing'), 0.91, '2024-03-27 08:30:00', '2024-04-18 13:15:00', '2024-04-18 13:15:00'),
    ('GOLD-073', 'Jack Long', 'jlong@carpet.clean', '+1-555-0173', '246 Poplar Ln', 'Apt 6', 'Gilbert', 'AZ', '85234', 'US', 'FreshCarpet Cleaners', 'Home Services', 'business', ARRAY('ERP'), 0.88, '2024-03-28 12:55:00', '2024-04-15 12:40:00', '2024-04-15 12:40:00'),
    ('GOLD-074', 'Shirley Butler', 'sbutler@gift.wrap', '+1-555-0174', '357 Sycamore Ave', NULL, 'North Las Vegas', 'NV', '89030', 'US', 'GiftWrap Boutique', 'Retail', 'business', ARRAY('CRM'), 0.85, '2024-03-29 14:20:00', '2024-04-12 16:05:00', '2024-04-12 16:05:00'),
    ('GOLD-075', 'Ralph Simmons', 'rsimmons@aquarium.fish', '+1-555-0175', '468 Willow Rd', 'Unit 9', 'Glendale', 'AZ', '85301', 'US', 'AquaWorld Fish Shop', 'Pet Services', 'business', ARRAY('Marketing'), 0.87, '2024-03-30 10:45:00', '2024-04-10 11:25:00', '2024-04-10 11:25:00'),
    ('GOLD-076', 'Gloria Foster', 'gfoster@antique.old', '+1-555-0176', '579 Hickory Ct', NULL, 'Madison', 'WI', '53701', 'US', 'TimelessAntiques', 'Retail', 'business', ARRAY('CRM', 'ERP'), 0.90, '2024-03-31 16:10:00', '2024-04-08 15:50:00', '2024-04-08 15:50:00'),
    ('GOLD-077', 'Eugene Gonzales', 'egonzales@bike.ride', '+1-555-0177', '680 Cedar Blvd', 'Floor 3', 'Lubbock', 'TX', '79401', 'US', 'CycleZone Bikes', 'Sports & Fitness', 'business', ARRAY('CRM', 'Marketing'), 0.92, '2024-04-01 09:35:00', '2024-04-05 10:15:00', '2024-04-05 10:15:00'),
    ('GOLD-078', 'Evelyn Nelson', 'enelson@frame.art', '+1-555-0178', '791 Walnut Way', NULL, 'Reno', 'NV', '89501', 'US', 'ArtFrame Gallery', 'Arts', 'business', ARRAY('CRM'), 0.86, '2024-04-02 13:00:00', '2024-06-25 14:30:00', '2024-06-25 14:30:00'),
    ('GOLD-079', 'Roy Bryant', 'rbryant@sign.make', '+1-555-0179', '802 Chestnut Ln', 'Suite 40', 'Chesapeake', 'VA', '23320', 'US', 'SignWorks Studios', 'Printing', 'business', ARRAY('ERP', 'Marketing'), 0.89, '2024-04-03 11:25:00', '2024-06-22 09:55:00', '2024-06-22 09:55:00'),
    ('GOLD-080', 'Ann Alexander', 'aalexander@pool.splash', '+1-555-0180', '913 Oak St', NULL, 'Norfolk', 'VA', '23501', 'US', 'SplashPool Services', 'Home Services', 'enterprise', ARRAY('CRM', 'ERP'), 0.94, '2024-04-04 15:50:00', '2024-06-20 13:25:00', '2024-06-20 13:25:00'),
    ('GOLD-081', 'Jesse Russell', 'jrussell@garage.park', '+1-555-0181', '024 Pine Ave', 'Apt 28', 'Fremont', 'CA', '94536', 'US', 'ParkSmart Garages', 'Real Estate', 'business', ARRAY('ERP'), 0.87, '2024-04-05 08:15:00', '2024-06-18 11:50:00', '2024-06-18 11:50:00'),
    ('GOLD-082', 'Virginia Griffin', 'vgriffin@makeup.glam', '+1-555-0182', '135 Maple Rd', NULL, 'Irving', 'TX', '75060', 'US', 'GlamBeauty Cosmetics', 'Beauty', 'business', ARRAY('CRM', 'Marketing'), 0.90, '2024-04-06 12:40:00', '2024-06-15 16:15:00', '2024-06-15 16:15:00'),
    ('GOLD-083', 'Lawrence Diaz', 'ldiaz@shoe.repair', '+1-555-0183', '246 Elm Ct', 'Unit 4', 'San Bernardino', 'CA', '92401', 'US', 'SoleRepair Shoes', 'Retail', 'business', ARRAY('CRM'), 0.85, '2024-04-07 14:05:00', '2024-06-12 10:40:00', '2024-06-12 10:40:00'),
    ('GOLD-084', 'Jean Hayes', 'jhayes@quilt.sew', '+1-555-0184', '357 Birch Way', NULL, 'Garland', 'TX', '75040', 'US', 'QuiltCraft Creations', 'Arts', 'business', ARRAY('Marketing'), 0.88, '2024-04-08 10:30:00', '2024-06-10 14:05:00', '2024-06-10 14:05:00'),
    ('GOLD-085', 'Fred Myers', 'fmyers@fence.build', '+1-555-0185', '468 Spruce Blvd', 'Floor 5', 'Henderson', 'NV', '89014', 'US', 'FencePro Builders', 'Construction', 'business', ARRAY('ERP'), 0.89, '2024-04-09 16:55:00', '2024-06-08 12:30:00', '2024-06-08 12:30:00'),
    ('GOLD-086', 'Alice Ford', 'aford@candle.glow', '+1-555-0186', '579 Ash Ln', NULL, 'Fort Wayne', 'IN', '46801', 'US', 'CandleGlow Scents', 'Retail', 'business', ARRAY('CRM', 'Marketing'), 0.86, '2024-04-10 09:20:00', '2024-06-05 15:55:00', '2024-06-05 15:55:00'),
    ('GOLD-087', 'Martin Hamilton', 'mhamilton@gutter.clean', '+1-555-0187', '680 Poplar Ave', 'Suite 60', 'Hialeah', 'FL', '33010', 'US', 'GutterGuard Services', 'Home Services', 'business', ARRAY('ERP'), 0.84, '2024-04-11 13:45:00', '2024-06-03 11:20:00', '2024-06-03 11:20:00'),
    ('GOLD-088', 'Marie Graham', 'mgraham@pottery.clay', '+1-555-0188', '791 Sycamore Rd', NULL, 'Boise', 'ID', '83701', 'US', 'ClayWorks Pottery', 'Arts', 'business', ARRAY('CRM'), 0.91, '2024-04-12 11:10:00', '2024-06-01 14:45:00', '2024-06-01 14:45:00'),
    ('GOLD-089', 'Carl Sullivan', 'csullivan@glass.repair', '+1-555-0189', '802 Willow Ct', 'Apt 17', 'Tacoma', 'WA', '98401', 'US', 'ClearView Glass', 'Home Services', 'enterprise', ARRAY('CRM', 'ERP'), 0.93, '2024-04-13 15:35:00', '2024-05-28 10:10:00', '2024-05-28 10:10:00'),
    ('GOLD-090', 'Cheryl Wallace', 'cwallace@herb.garden', '+1-555-0190', '913 Hickory Way', NULL, 'Modesto', 'CA', '95350', 'US', 'HerbGarden Fresh', 'Food & Beverage', 'business', ARRAY('Marketing'), 0.87, '2024-04-14 08:00:00', '2024-05-25 16:35:00', '2024-05-25 16:35:00'),
    ('GOLD-091', 'Philip West', 'pwest@chimney.sweep', '+1-555-0191', '024 Cedar St', 'Unit 11', 'Des Moines', 'IA', '50301', 'US', 'SweepMaster Chimney', 'Home Services', 'business', ARRAY('ERP'), 0.85, '2024-04-15 12:25:00', '2024-05-22 09:50:00', '2024-05-22 09:50:00'),
    ('GOLD-092', 'Beverly Cole', 'bcole@fabric.store', '+1-555-0192', '135 Walnut Blvd', NULL, 'Spokane', 'WA', '99201', 'US', 'FabricLand Supplies', 'Retail', 'business', ARRAY('CRM', 'ERP'), 0.90, '2024-04-16 14:50:00', '2024-05-20 13:15:00', '2024-05-20 13:15:00'),
    ('GOLD-093', 'Keith West', 'kwest@boat.repair', '+1-555-0193', '246 Chestnut Ave', 'Floor 8', 'Baton Rouge', 'LA', '70801', 'US', 'MarineFix Boats', 'Marine', 'business', ARRAY('CRM'), 0.88, '2024-04-17 10:15:00', '2024-05-18 11:40:00', '2024-05-18 11:40:00'),
    ('GOLD-094', 'Julia Hunt', 'jhunt@balloon.decor', '+1-555-0194', '357 Oak Ln', NULL, 'Fargo', 'ND', '58102', 'US', 'BalloonParty Decor', 'Events', 'business', ARRAY('Marketing'), 0.86, '2024-04-18 16:40:00', '2024-05-15 15:05:00', '2024-05-15 15:05:00'),
    ('GOLD-095', 'Harold Owens', 'howens@shred.docs', '+1-555-0195', '468 Pine Rd', 'Suite 90', 'Rochester', 'NY', '14601', 'US', 'SecureShred Documents', 'Business Services', 'enterprise', ARRAY('CRM', 'ERP'), 0.94, '2024-04-19 09:05:00', '2024-05-12 10:30:00', '2024-05-12 10:30:00'),
    ('GOLD-096', 'Lillian Stone', 'lstone@knit.craft', '+1-555-0196', '579 Maple Ct', NULL, 'Little Rock', 'AR', '72201', 'US', 'KnitCraft Yarns', 'Arts', 'business', ARRAY('CRM'), 0.87, '2024-04-20 13:30:00', '2024-05-10 14:55:00', '2024-05-10 14:55:00'),
    ('GOLD-097', 'Billy Crawford', 'bcrawford@appliance.fix', '+1-555-0197', '680 Elm Way', 'Apt 5', 'Salt Lake City', 'UT', '84101', 'US', 'AppliancePro Repairs', 'Home Services', 'business', ARRAY('ERP', 'Marketing'), 0.89, '2024-04-21 11:55:00', '2024-05-08 12:20:00', '2024-05-08 12:20:00'),
    ('GOLD-098', 'Mildred Ortiz', 'mortiz@notary.sign', '+1-555-0198', '791 Birch St', NULL, 'Grand Rapids', 'MI', '49501', 'US', 'SignSecure Notary', 'Legal', 'business', ARRAY('CRM'), 0.91, '2024-04-22 15:20:00', '2024-05-05 16:45:00', '2024-05-05 16:45:00'),
    ('GOLD-099', 'Howard Warren', 'hwarren@deck.build', '+1-555-0199', '802 Spruce Ave', 'Unit 13', 'Knoxville', 'TN', '37901', 'US', 'DeckMaster Builders', 'Construction', 'business', ARRAY('ERP'), 0.85, '2024-04-23 08:45:00', '2024-05-02 09:10:00', '2024-05-02 09:10:00'),
    ('GOLD-100', 'Norma Hawkins', 'nhawkins@trophy.award', '+1-555-0200', '913 Ash Blvd', NULL, 'Providence', 'RI', '02901', 'US', 'TrophyTime Awards', 'Retail', 'business', ARRAY('CRM', 'Marketing'), 0.92, '2024-04-24 12:10:00', '2024-04-30 11:35:00', '2024-04-30 11:35:00');

-- Verify the data
SELECT COUNT(*) as total_records FROM customer_master;

