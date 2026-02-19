-- =============================================================================
-- ONTOS DEMO: Unity Catalog Enterprise Data Setup
-- =============================================================================
-- This script creates a realistic enterprise data environment in Unity Catalog
-- with catalogs, schemas, tables, views, sample data, tags, comments, and
-- foreign key relationships.
--
-- Purpose: Demonstrate Ontos capabilities including:
--   - UC Bulk Import (discovering tagged assets)
--   - Dataset inference from Unity Catalog
--   - Data Product and Contract management
--
-- Usage: Run this script in Databricks SQL Editor before demoing Ontos
-- =============================================================================

-- ============================================================================
-- STEP 1: CREATE CATALOGS
-- ============================================================================
-- We create separate catalogs for different business domains

CREATE CATALOG IF NOT EXISTS demo_enterprise
COMMENT 'Enterprise demo catalog for Ontos demonstration';

CREATE CATALOG IF NOT EXISTS demo_analytics
COMMENT 'Analytics and reporting catalog for Ontos demonstration';

-- ============================================================================
-- STEP 2: CREATE SCHEMAS
-- ============================================================================

-- Sales domain schemas
CREATE SCHEMA IF NOT EXISTS demo_enterprise.sales
COMMENT 'Sales domain - Customer orders, products, and revenue data';

CREATE SCHEMA IF NOT EXISTS demo_enterprise.sales_staging
COMMENT 'Sales staging area - Raw and intermediate data';

-- Finance domain schemas
CREATE SCHEMA IF NOT EXISTS demo_enterprise.finance
COMMENT 'Finance domain - Transactions, accounts, and billing';

-- HR domain schemas
CREATE SCHEMA IF NOT EXISTS demo_enterprise.hr
COMMENT 'Human Resources - Employee and organizational data';

-- Shared/Reference data
CREATE SCHEMA IF NOT EXISTS demo_enterprise.reference
COMMENT 'Shared reference data - Lookups, codes, and master data';

-- Analytics schemas
CREATE SCHEMA IF NOT EXISTS demo_analytics.reporting
COMMENT 'Reporting layer - Aggregated views for BI tools';

CREATE SCHEMA IF NOT EXISTS demo_analytics.data_science
COMMENT 'Data Science - Feature stores and ML datasets';

-- ============================================================================
-- STEP 3: CREATE REFERENCE/MASTER DATA TABLES
-- ============================================================================

USE CATALOG demo_enterprise;
USE SCHEMA reference;

-- Countries reference table
CREATE OR REPLACE TABLE countries (
    country_code STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2 country code',
    country_name STRING NOT NULL COMMENT 'Full country name',
    region STRING COMMENT 'Geographic region',
    currency_code STRING COMMENT 'ISO 4217 currency code',
    created_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp',
    CONSTRAINT countries_pk PRIMARY KEY (country_code)
)
COMMENT 'Master data for countries and regions'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'public'
);

INSERT INTO countries VALUES
    ('US', 'United States', 'North America', 'USD', current_timestamp()),
    ('GB', 'United Kingdom', 'Europe', 'GBP', current_timestamp()),
    ('DE', 'Germany', 'Europe', 'EUR', current_timestamp()),
    ('FR', 'France', 'Europe', 'EUR', current_timestamp()),
    ('JP', 'Japan', 'Asia Pacific', 'JPY', current_timestamp()),
    ('AU', 'Australia', 'Asia Pacific', 'AUD', current_timestamp()),
    ('CA', 'Canada', 'North America', 'CAD', current_timestamp()),
    ('BR', 'Brazil', 'South America', 'BRL', current_timestamp()),
    ('IN', 'India', 'Asia Pacific', 'INR', current_timestamp()),
    ('SG', 'Singapore', 'Asia Pacific', 'SGD', current_timestamp());

-- Product categories reference
CREATE OR REPLACE TABLE product_categories (
    category_id STRING NOT NULL COMMENT 'Unique category identifier',
    category_name STRING NOT NULL COMMENT 'Category display name',
    parent_category_id STRING COMMENT 'Parent category for hierarchy',
    description STRING COMMENT 'Category description',
    is_active BOOLEAN DEFAULT true COMMENT 'Whether category is active',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT product_categories_pk PRIMARY KEY (category_id)
)
COMMENT 'Product category hierarchy'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'internal'
);

INSERT INTO product_categories VALUES
    ('ELEC', 'Electronics', NULL, 'Electronic devices and accessories', true, current_timestamp()),
    ('ELEC-PHONES', 'Mobile Phones', 'ELEC', 'Smartphones and mobile devices', true, current_timestamp()),
    ('ELEC-LAPTOPS', 'Laptops', 'ELEC', 'Portable computers', true, current_timestamp()),
    ('ELEC-ACCESS', 'Accessories', 'ELEC', 'Electronic accessories', true, current_timestamp()),
    ('CLOTH', 'Clothing', NULL, 'Apparel and fashion', true, current_timestamp()),
    ('CLOTH-MEN', 'Men''s Clothing', 'CLOTH', 'Men''s apparel', true, current_timestamp()),
    ('CLOTH-WOMEN', 'Women''s Clothing', 'CLOTH', 'Women''s apparel', true, current_timestamp()),
    ('HOME', 'Home & Garden', NULL, 'Home improvement and garden', true, current_timestamp()),
    ('SPORTS', 'Sports & Outdoors', NULL, 'Sports equipment and outdoor gear', true, current_timestamp()),
    ('BOOKS', 'Books & Media', NULL, 'Books, music, and digital media', true, current_timestamp());

-- ============================================================================
-- STEP 4: CREATE SALES DOMAIN TABLES
-- ============================================================================

USE SCHEMA sales;

-- Customers table
CREATE OR REPLACE TABLE customers (
    customer_id STRING NOT NULL COMMENT 'Unique customer identifier (UUID)',
    email STRING NOT NULL COMMENT 'Customer email address - PII',
    first_name STRING COMMENT 'Customer first name - PII',
    last_name STRING COMMENT 'Customer last name - PII',
    phone STRING COMMENT 'Phone number - PII',
    country_code STRING COMMENT 'Customer country',
    customer_segment STRING COMMENT 'Customer segment: RETAIL, BUSINESS, ENTERPRISE',
    loyalty_tier STRING COMMENT 'Loyalty program tier: BRONZE, SILVER, GOLD, PLATINUM',
    total_lifetime_value DECIMAL(12,2) COMMENT 'Total customer lifetime value',
    created_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'Account creation date',
    updated_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'Last update timestamp',
    is_active BOOLEAN DEFAULT true COMMENT 'Whether customer account is active',
    CONSTRAINT customers_pk PRIMARY KEY (customer_id)
)
COMMENT 'Customer master data - Contains PII, handle with care'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'confidential',
    'pii_columns' = 'email,first_name,last_name,phone',
    'retention_days' = '2555'
);

-- Generate sample customers
INSERT INTO customers VALUES
    ('CUST-001', 'john.smith@example.com', 'John', 'Smith', '+1-555-0101', 'US', 'RETAIL', 'GOLD', 15420.50, current_timestamp(), current_timestamp(), true),
    ('CUST-002', 'emma.wilson@example.com', 'Emma', 'Wilson', '+1-555-0102', 'US', 'BUSINESS', 'PLATINUM', 89230.00, current_timestamp(), current_timestamp(), true),
    ('CUST-003', 'hans.mueller@example.de', 'Hans', 'Mueller', '+49-30-12345', 'DE', 'RETAIL', 'SILVER', 3250.75, current_timestamp(), current_timestamp(), true),
    ('CUST-004', 'marie.dubois@example.fr', 'Marie', 'Dubois', '+33-1-23456789', 'FR', 'ENTERPRISE', 'PLATINUM', 245000.00, current_timestamp(), current_timestamp(), true),
    ('CUST-005', 'yuki.tanaka@example.jp', 'Yuki', 'Tanaka', '+81-3-1234-5678', 'JP', 'RETAIL', 'BRONZE', 890.25, current_timestamp(), current_timestamp(), true),
    ('CUST-006', 'sarah.jones@example.com', 'Sarah', 'Jones', '+1-555-0106', 'US', 'RETAIL', 'SILVER', 4560.00, current_timestamp(), current_timestamp(), true),
    ('CUST-007', 'raj.patel@example.in', 'Raj', 'Patel', '+91-22-12345678', 'IN', 'BUSINESS', 'GOLD', 28900.00, current_timestamp(), current_timestamp(), true),
    ('CUST-008', 'chen.wei@example.sg', 'Wei', 'Chen', '+65-6123-4567', 'SG', 'ENTERPRISE', 'PLATINUM', 156000.00, current_timestamp(), current_timestamp(), true),
    ('CUST-009', 'olivia.brown@example.au', 'Olivia', 'Brown', '+61-2-1234-5678', 'AU', 'RETAIL', 'BRONZE', 1200.50, current_timestamp(), current_timestamp(), true),
    ('CUST-010', 'james.taylor@example.ca', 'James', 'Taylor', '+1-416-555-0110', 'CA', 'BUSINESS', 'GOLD', 45600.00, current_timestamp(), current_timestamp(), true);

-- Products table
CREATE OR REPLACE TABLE products (
    product_id STRING NOT NULL COMMENT 'Unique product identifier (SKU)',
    product_name STRING NOT NULL COMMENT 'Product display name',
    category_id STRING COMMENT 'Product category reference',
    description STRING COMMENT 'Product description',
    unit_price DECIMAL(10,2) NOT NULL COMMENT 'Current unit price',
    cost_price DECIMAL(10,2) COMMENT 'Cost price for margin calculation',
    stock_quantity INT DEFAULT 0 COMMENT 'Current stock level',
    supplier_id STRING COMMENT 'Primary supplier identifier',
    is_active BOOLEAN DEFAULT true COMMENT 'Whether product is available for sale',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT products_pk PRIMARY KEY (product_id)
)
COMMENT 'Product catalog with pricing and inventory'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'internal'
);

INSERT INTO products VALUES
    ('SKU-1001', 'iPhone 15 Pro 256GB', 'ELEC-PHONES', 'Latest Apple smartphone with A17 chip', 1199.00, 850.00, 150, 'SUP-APPLE', true, current_timestamp(), current_timestamp()),
    ('SKU-1002', 'Samsung Galaxy S24 Ultra', 'ELEC-PHONES', 'Premium Android flagship phone', 1299.00, 920.00, 120, 'SUP-SAMSUNG', true, current_timestamp(), current_timestamp()),
    ('SKU-1003', 'MacBook Pro 14" M3', 'ELEC-LAPTOPS', 'Professional laptop with M3 chip', 1999.00, 1450.00, 75, 'SUP-APPLE', true, current_timestamp(), current_timestamp()),
    ('SKU-1004', 'Dell XPS 15', 'ELEC-LAPTOPS', 'Premium Windows ultrabook', 1799.00, 1300.00, 60, 'SUP-DELL', true, current_timestamp(), current_timestamp()),
    ('SKU-1005', 'AirPods Pro 2nd Gen', 'ELEC-ACCESS', 'Wireless noise-cancelling earbuds', 249.00, 150.00, 300, 'SUP-APPLE', true, current_timestamp(), current_timestamp()),
    ('SKU-2001', 'Premium Wool Blazer', 'CLOTH-MEN', 'Italian wool business blazer', 450.00, 180.00, 45, 'SUP-FASHION1', true, current_timestamp(), current_timestamp()),
    ('SKU-2002', 'Silk Evening Dress', 'CLOTH-WOMEN', 'Elegant silk evening wear', 680.00, 220.00, 25, 'SUP-FASHION1', true, current_timestamp(), current_timestamp()),
    ('SKU-3001', 'Smart Home Hub', 'HOME', 'Central smart home controller', 299.00, 120.00, 200, 'SUP-TECH', true, current_timestamp(), current_timestamp()),
    ('SKU-4001', 'Professional Tennis Racket', 'SPORTS', 'Tournament-grade tennis racket', 350.00, 140.00, 80, 'SUP-SPORTS', true, current_timestamp(), current_timestamp()),
    ('SKU-5001', 'Business Strategy Collection', 'BOOKS', 'Set of 5 business bestsellers', 89.00, 35.00, 500, 'SUP-BOOKS', true, current_timestamp(), current_timestamp());

-- Orders table
CREATE OR REPLACE TABLE orders (
    order_id STRING NOT NULL COMMENT 'Unique order identifier',
    customer_id STRING NOT NULL COMMENT 'Reference to customer',
    order_date TIMESTAMP NOT NULL COMMENT 'Order placement timestamp',
    status STRING NOT NULL COMMENT 'Order status: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED',
    shipping_address STRING COMMENT 'Delivery address - PII',
    billing_address STRING COMMENT 'Billing address - PII',
    subtotal DECIMAL(12,2) COMMENT 'Order subtotal before tax',
    tax_amount DECIMAL(12,2) COMMENT 'Tax amount',
    shipping_cost DECIMAL(10,2) COMMENT 'Shipping charges',
    total_amount DECIMAL(12,2) NOT NULL COMMENT 'Total order amount',
    currency_code STRING DEFAULT 'USD' COMMENT 'Order currency',
    payment_method STRING COMMENT 'Payment method used',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT orders_pk PRIMARY KEY (order_id),
    CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)
COMMENT 'Customer orders - Contains shipping/billing PII'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'confidential',
    'pii_columns' = 'shipping_address,billing_address'
);

INSERT INTO orders VALUES
    ('ORD-2024-001', 'CUST-001', '2024-01-15 10:30:00', 'DELIVERED', '123 Main St, New York, NY 10001', '123 Main St, New York, NY 10001', 1448.00, 130.32, 15.00, 1593.32, 'USD', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-002', 'CUST-002', '2024-01-16 14:22:00', 'DELIVERED', '456 Oak Ave, Los Angeles, CA 90001', '456 Oak Ave, Los Angeles, CA 90001', 3998.00, 359.82, 0.00, 4357.82, 'USD', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-003', 'CUST-003', '2024-01-18 09:15:00', 'SHIPPED', 'Hauptstr. 42, 10115 Berlin', 'Hauptstr. 42, 10115 Berlin', 1199.00, 227.81, 25.00, 1451.81, 'EUR', 'PAYPAL', current_timestamp(), current_timestamp()),
    ('ORD-2024-004', 'CUST-004', '2024-01-20 16:45:00', 'CONFIRMED', '15 Rue de Rivoli, 75001 Paris', '15 Rue de Rivoli, 75001 Paris', 2679.00, 535.80, 0.00, 3214.80, 'EUR', 'BANK_TRANSFER', current_timestamp(), current_timestamp()),
    ('ORD-2024-005', 'CUST-005', '2024-01-22 11:30:00', 'DELIVERED', '1-2-3 Shibuya, Tokyo 150-0002', '1-2-3 Shibuya, Tokyo 150-0002', 249.00, 24.90, 12.00, 285.90, 'JPY', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-006', 'CUST-006', '2024-01-25 08:00:00', 'PENDING', '789 Pine St, Chicago, IL 60601', '789 Pine St, Chicago, IL 60601', 450.00, 40.50, 10.00, 500.50, 'USD', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-007', 'CUST-007', '2024-02-01 13:20:00', 'DELIVERED', 'Bandra West, Mumbai 400050', 'Bandra West, Mumbai 400050', 1799.00, 323.82, 50.00, 2172.82, 'INR', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-008', 'CUST-008', '2024-02-05 10:00:00', 'SHIPPED', '10 Marina Bay, Singapore 018956', '10 Marina Bay, Singapore 018956', 5997.00, 479.76, 0.00, 6476.76, 'SGD', 'BANK_TRANSFER', current_timestamp(), current_timestamp()),
    ('ORD-2024-009', 'CUST-002', '2024-02-10 15:30:00', 'DELIVERED', '456 Oak Ave, Los Angeles, CA 90001', '456 Oak Ave, Los Angeles, CA 90001', 89.00, 8.01, 5.00, 102.01, 'USD', 'CREDIT_CARD', current_timestamp(), current_timestamp()),
    ('ORD-2024-010', 'CUST-001', '2024-02-15 09:45:00', 'CONFIRMED', '123 Main St, New York, NY 10001', '123 Main St, New York, NY 10001', 2248.00, 202.32, 0.00, 2450.32, 'USD', 'CREDIT_CARD', current_timestamp(), current_timestamp());

-- Order items table
CREATE OR REPLACE TABLE order_items (
    order_item_id STRING NOT NULL COMMENT 'Unique order item identifier',
    order_id STRING NOT NULL COMMENT 'Reference to order',
    product_id STRING NOT NULL COMMENT 'Reference to product',
    quantity INT NOT NULL COMMENT 'Quantity ordered',
    unit_price DECIMAL(10,2) NOT NULL COMMENT 'Price at time of order',
    discount_percent DECIMAL(5,2) DEFAULT 0 COMMENT 'Discount applied',
    line_total DECIMAL(12,2) NOT NULL COMMENT 'Line item total',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT order_items_pk PRIMARY KEY (order_item_id),
    CONSTRAINT order_items_order_fk FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT order_items_product_fk FOREIGN KEY (product_id) REFERENCES products(product_id)
)
COMMENT 'Individual items within orders'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'internal'
);

INSERT INTO order_items VALUES
    ('OI-001-1', 'ORD-2024-001', 'SKU-1001', 1, 1199.00, 0.00, 1199.00, current_timestamp()),
    ('OI-001-2', 'ORD-2024-001', 'SKU-1005', 1, 249.00, 0.00, 249.00, current_timestamp()),
    ('OI-002-1', 'ORD-2024-002', 'SKU-1003', 2, 1999.00, 0.00, 3998.00, current_timestamp()),
    ('OI-003-1', 'ORD-2024-003', 'SKU-1001', 1, 1199.00, 0.00, 1199.00, current_timestamp()),
    ('OI-004-1', 'ORD-2024-004', 'SKU-2002', 1, 680.00, 0.00, 680.00, current_timestamp()),
    ('OI-004-2', 'ORD-2024-004', 'SKU-1003', 1, 1999.00, 0.00, 1999.00, current_timestamp()),
    ('OI-005-1', 'ORD-2024-005', 'SKU-1005', 1, 249.00, 0.00, 249.00, current_timestamp()),
    ('OI-006-1', 'ORD-2024-006', 'SKU-2001', 1, 450.00, 0.00, 450.00, current_timestamp()),
    ('OI-007-1', 'ORD-2024-007', 'SKU-1004', 1, 1799.00, 0.00, 1799.00, current_timestamp()),
    ('OI-008-1', 'ORD-2024-008', 'SKU-1003', 3, 1999.00, 0.00, 5997.00, current_timestamp()),
    ('OI-009-1', 'ORD-2024-009', 'SKU-5001', 1, 89.00, 0.00, 89.00, current_timestamp()),
    ('OI-010-1', 'ORD-2024-010', 'SKU-1003', 1, 1999.00, 0.00, 1999.00, current_timestamp()),
    ('OI-010-2', 'ORD-2024-010', 'SKU-1005', 1, 249.00, 0.00, 249.00, current_timestamp());

-- ============================================================================
-- STEP 5: CREATE FINANCE DOMAIN TABLES
-- ============================================================================

USE SCHEMA finance;

-- Invoices table
CREATE OR REPLACE TABLE invoices (
    invoice_id STRING NOT NULL COMMENT 'Unique invoice identifier',
    order_id STRING COMMENT 'Related order reference',
    customer_id STRING NOT NULL COMMENT 'Customer reference',
    invoice_date DATE NOT NULL COMMENT 'Invoice issue date',
    due_date DATE NOT NULL COMMENT 'Payment due date',
    subtotal DECIMAL(12,2) NOT NULL COMMENT 'Subtotal before tax',
    tax_amount DECIMAL(12,2) NOT NULL COMMENT 'Tax amount',
    total_amount DECIMAL(12,2) NOT NULL COMMENT 'Total invoice amount',
    currency_code STRING DEFAULT 'USD' COMMENT 'Invoice currency',
    status STRING NOT NULL COMMENT 'Invoice status: DRAFT, SENT, PAID, OVERDUE, CANCELLED',
    payment_date DATE COMMENT 'Date payment received',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT invoices_pk PRIMARY KEY (invoice_id)
)
COMMENT 'Customer invoices for billing and accounts receivable'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'confidential'
);

INSERT INTO invoices VALUES
    ('INV-2024-001', 'ORD-2024-001', 'CUST-001', '2024-01-15', '2024-02-15', 1448.00, 130.32, 1593.32, 'USD', 'PAID', '2024-01-20', current_timestamp(), current_timestamp()),
    ('INV-2024-002', 'ORD-2024-002', 'CUST-002', '2024-01-16', '2024-02-16', 3998.00, 359.82, 4357.82, 'USD', 'PAID', '2024-01-25', current_timestamp(), current_timestamp()),
    ('INV-2024-003', 'ORD-2024-003', 'CUST-003', '2024-01-18', '2024-02-18', 1199.00, 227.81, 1451.81, 'EUR', 'SENT', NULL, current_timestamp(), current_timestamp()),
    ('INV-2024-004', 'ORD-2024-004', 'CUST-004', '2024-01-20', '2024-02-20', 2679.00, 535.80, 3214.80, 'EUR', 'SENT', NULL, current_timestamp(), current_timestamp()),
    ('INV-2024-005', 'ORD-2024-005', 'CUST-005', '2024-01-22', '2024-02-22', 249.00, 24.90, 285.90, 'JPY', 'PAID', '2024-01-28', current_timestamp(), current_timestamp()),
    ('INV-2024-006', 'ORD-2024-006', 'CUST-006', '2024-01-25', '2024-02-25', 450.00, 40.50, 500.50, 'USD', 'DRAFT', NULL, current_timestamp(), current_timestamp()),
    ('INV-2024-007', 'ORD-2024-007', 'CUST-007', '2024-02-01', '2024-03-01', 1799.00, 323.82, 2172.82, 'INR', 'PAID', '2024-02-10', current_timestamp(), current_timestamp()),
    ('INV-2024-008', 'ORD-2024-008', 'CUST-008', '2024-02-05', '2024-03-05', 5997.00, 479.76, 6476.76, 'SGD', 'SENT', NULL, current_timestamp(), current_timestamp());

-- Payments table
CREATE OR REPLACE TABLE payments (
    payment_id STRING NOT NULL COMMENT 'Unique payment identifier',
    invoice_id STRING NOT NULL COMMENT 'Related invoice',
    payment_date TIMESTAMP NOT NULL COMMENT 'Payment timestamp',
    amount DECIMAL(12,2) NOT NULL COMMENT 'Payment amount',
    currency_code STRING NOT NULL COMMENT 'Payment currency',
    payment_method STRING NOT NULL COMMENT 'Payment method: CREDIT_CARD, BANK_TRANSFER, PAYPAL, etc.',
    reference_number STRING COMMENT 'External payment reference',
    status STRING NOT NULL COMMENT 'Payment status: PENDING, COMPLETED, FAILED, REFUNDED',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT payments_pk PRIMARY KEY (payment_id),
    CONSTRAINT payments_invoice_fk FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id)
)
COMMENT 'Payment transactions for invoices'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'confidential'
);

INSERT INTO payments VALUES
    ('PAY-001', 'INV-2024-001', '2024-01-20 14:30:00', 1593.32, 'USD', 'CREDIT_CARD', 'TXN-CC-12345', 'COMPLETED', current_timestamp()),
    ('PAY-002', 'INV-2024-002', '2024-01-25 10:15:00', 4357.82, 'USD', 'CREDIT_CARD', 'TXN-CC-12346', 'COMPLETED', current_timestamp()),
    ('PAY-003', 'INV-2024-005', '2024-01-28 09:00:00', 285.90, 'JPY', 'CREDIT_CARD', 'TXN-CC-12347', 'COMPLETED', current_timestamp()),
    ('PAY-004', 'INV-2024-007', '2024-02-10 11:45:00', 2172.82, 'INR', 'CREDIT_CARD', 'TXN-CC-12348', 'COMPLETED', current_timestamp());

-- General Ledger accounts
CREATE OR REPLACE TABLE gl_accounts (
    account_id STRING NOT NULL COMMENT 'General ledger account ID',
    account_name STRING NOT NULL COMMENT 'Account name',
    account_type STRING NOT NULL COMMENT 'Account type: ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE',
    parent_account_id STRING COMMENT 'Parent account for hierarchy',
    is_active BOOLEAN DEFAULT true COMMENT 'Whether account is active',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT gl_accounts_pk PRIMARY KEY (account_id)
)
COMMENT 'Chart of accounts for financial reporting'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'internal'
);

INSERT INTO gl_accounts VALUES
    ('1000', 'Assets', 'ASSET', NULL, true, current_timestamp()),
    ('1100', 'Cash and Cash Equivalents', 'ASSET', '1000', true, current_timestamp()),
    ('1200', 'Accounts Receivable', 'ASSET', '1000', true, current_timestamp()),
    ('1300', 'Inventory', 'ASSET', '1000', true, current_timestamp()),
    ('2000', 'Liabilities', 'LIABILITY', NULL, true, current_timestamp()),
    ('2100', 'Accounts Payable', 'LIABILITY', '2000', true, current_timestamp()),
    ('2200', 'Accrued Expenses', 'LIABILITY', '2000', true, current_timestamp()),
    ('3000', 'Equity', 'EQUITY', NULL, true, current_timestamp()),
    ('4000', 'Revenue', 'REVENUE', NULL, true, current_timestamp()),
    ('4100', 'Product Sales', 'REVENUE', '4000', true, current_timestamp()),
    ('4200', 'Service Revenue', 'REVENUE', '4000', true, current_timestamp()),
    ('5000', 'Expenses', 'EXPENSE', NULL, true, current_timestamp()),
    ('5100', 'Cost of Goods Sold', 'EXPENSE', '5000', true, current_timestamp()),
    ('5200', 'Operating Expenses', 'EXPENSE', '5000', true, current_timestamp());

-- ============================================================================
-- STEP 6: CREATE HR DOMAIN TABLES
-- ============================================================================

USE SCHEMA hr;

-- Departments table
CREATE OR REPLACE TABLE departments (
    department_id STRING NOT NULL COMMENT 'Unique department identifier',
    department_name STRING NOT NULL COMMENT 'Department name',
    parent_department_id STRING COMMENT 'Parent department for hierarchy',
    cost_center STRING COMMENT 'Cost center code',
    manager_employee_id STRING COMMENT 'Department manager',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT departments_pk PRIMARY KEY (department_id)
)
COMMENT 'Organizational departments and structure'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'internal'
);

INSERT INTO departments VALUES
    ('DEPT-EXEC', 'Executive', NULL, 'CC-1000', NULL, current_timestamp(), current_timestamp()),
    ('DEPT-SALES', 'Sales', 'DEPT-EXEC', 'CC-2000', 'EMP-002', current_timestamp(), current_timestamp()),
    ('DEPT-SALES-NA', 'Sales - North America', 'DEPT-SALES', 'CC-2100', 'EMP-003', current_timestamp(), current_timestamp()),
    ('DEPT-SALES-EMEA', 'Sales - EMEA', 'DEPT-SALES', 'CC-2200', NULL, current_timestamp(), current_timestamp()),
    ('DEPT-SALES-APAC', 'Sales - APAC', 'DEPT-SALES', 'CC-2300', NULL, current_timestamp(), current_timestamp()),
    ('DEPT-ENG', 'Engineering', 'DEPT-EXEC', 'CC-3000', 'EMP-004', current_timestamp(), current_timestamp()),
    ('DEPT-ENG-PLATFORM', 'Platform Engineering', 'DEPT-ENG', 'CC-3100', NULL, current_timestamp(), current_timestamp()),
    ('DEPT-ENG-DATA', 'Data Engineering', 'DEPT-ENG', 'CC-3200', NULL, current_timestamp(), current_timestamp()),
    ('DEPT-FIN', 'Finance', 'DEPT-EXEC', 'CC-4000', 'EMP-005', current_timestamp(), current_timestamp()),
    ('DEPT-HR', 'Human Resources', 'DEPT-EXEC', 'CC-5000', NULL, current_timestamp(), current_timestamp());

-- Employees table (PII sensitive)
CREATE OR REPLACE TABLE employees (
    employee_id STRING NOT NULL COMMENT 'Unique employee identifier',
    email STRING NOT NULL COMMENT 'Corporate email - PII',
    first_name STRING NOT NULL COMMENT 'First name - PII',
    last_name STRING NOT NULL COMMENT 'Last name - PII',
    phone STRING COMMENT 'Work phone - PII',
    department_id STRING COMMENT 'Department reference',
    job_title STRING COMMENT 'Job title',
    manager_id STRING COMMENT 'Direct manager employee ID',
    hire_date DATE COMMENT 'Employment start date',
    employment_type STRING COMMENT 'FULL_TIME, PART_TIME, CONTRACT',
    salary DECIMAL(12,2) COMMENT 'Annual salary - Highly confidential',
    currency_code STRING DEFAULT 'USD' COMMENT 'Salary currency',
    country_code STRING COMMENT 'Work location country',
    is_active BOOLEAN DEFAULT true COMMENT 'Current employment status',
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    CONSTRAINT employees_pk PRIMARY KEY (employee_id),
    CONSTRAINT employees_dept_fk FOREIGN KEY (department_id) REFERENCES departments(department_id)
)
COMMENT 'Employee master data - Contains PII and salary information'
TBLPROPERTIES (
    'quality' = 'gold',
    'data_classification' = 'highly_confidential',
    'pii_columns' = 'email,first_name,last_name,phone,salary',
    'retention_days' = '3650'
);

INSERT INTO employees VALUES
    ('EMP-001', 'ceo@company.com', 'Alexandra', 'Johnson', '+1-555-1001', 'DEPT-EXEC', 'Chief Executive Officer', NULL, '2018-01-15', 'FULL_TIME', 450000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-002', 'sales.vp@company.com', 'Michael', 'Chen', '+1-555-1002', 'DEPT-SALES', 'VP of Sales', 'EMP-001', '2019-03-01', 'FULL_TIME', 280000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-003', 'na.sales@company.com', 'Jennifer', 'Williams', '+1-555-1003', 'DEPT-SALES-NA', 'Director of NA Sales', 'EMP-002', '2020-06-15', 'FULL_TIME', 185000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-004', 'cto@company.com', 'David', 'Kim', '+1-555-1004', 'DEPT-ENG', 'Chief Technology Officer', 'EMP-001', '2018-02-01', 'FULL_TIME', 380000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-005', 'cfo@company.com', 'Sarah', 'Martinez', '+1-555-1005', 'DEPT-FIN', 'Chief Financial Officer', 'EMP-001', '2019-01-10', 'FULL_TIME', 320000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-006', 'data.eng1@company.com', 'Robert', 'Brown', '+1-555-1006', 'DEPT-ENG-DATA', 'Senior Data Engineer', 'EMP-004', '2021-04-01', 'FULL_TIME', 165000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-007', 'platform.eng1@company.com', 'Emily', 'Davis', '+1-555-1007', 'DEPT-ENG-PLATFORM', 'Platform Engineer', 'EMP-004', '2022-01-15', 'FULL_TIME', 145000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-008', 'sales.rep1@company.com', 'James', 'Wilson', '+1-555-1008', 'DEPT-SALES-NA', 'Sales Representative', 'EMP-003', '2023-02-01', 'FULL_TIME', 85000.00, 'USD', 'US', true, current_timestamp(), current_timestamp()),
    ('EMP-009', 'emea.sales@company.de', 'Klaus', 'Schmidt', '+49-30-5551009', 'DEPT-SALES-EMEA', 'EMEA Sales Manager', 'EMP-002', '2021-07-01', 'FULL_TIME', 120000.00, 'EUR', 'DE', true, current_timestamp(), current_timestamp()),
    ('EMP-010', 'apac.sales@company.sg', 'Li', 'Wong', '+65-6555-1010', 'DEPT-SALES-APAC', 'APAC Sales Manager', 'EMP-002', '2022-03-15', 'FULL_TIME', 150000.00, 'SGD', 'SG', true, current_timestamp(), current_timestamp());

-- ============================================================================
-- STEP 7: CREATE STAGING TABLES
-- ============================================================================

USE SCHEMA sales_staging;

-- Raw orders from source systems
CREATE OR REPLACE TABLE raw_orders (
    source_system STRING COMMENT 'Source system identifier',
    raw_order_id STRING COMMENT 'Order ID from source',
    raw_data STRING COMMENT 'Raw JSON payload',
    ingestion_timestamp TIMESTAMP DEFAULT current_timestamp() COMMENT 'When data was ingested',
    processing_status STRING DEFAULT 'NEW' COMMENT 'Processing status: NEW, PROCESSING, PROCESSED, ERROR',
    error_message STRING COMMENT 'Error details if processing failed'
)
COMMENT 'Raw order data from various source systems'
TBLPROPERTIES (
    'quality' = 'bronze',
    'data_classification' = 'internal',
    'retention_days' = '90'
);

INSERT INTO raw_orders VALUES
    ('ECOMMERCE', 'EC-001', '{"customer_email":"test1@example.com","items":[{"sku":"SKU-1001","qty":1}],"total":1199.00}', current_timestamp(), 'PROCESSED', NULL),
    ('ECOMMERCE', 'EC-002', '{"customer_email":"test2@example.com","items":[{"sku":"SKU-1003","qty":2}],"total":3998.00}', current_timestamp(), 'PROCESSED', NULL),
    ('POS', 'POS-001', '{"store_id":"STORE-NYC-01","items":[{"sku":"SKU-1005","qty":3}],"total":747.00}', current_timestamp(), 'NEW', NULL),
    ('PARTNER', 'PARTNER-001', '{"partner_id":"AMZN","order_ref":"AMZN-12345","total":2500.00}', current_timestamp(), 'ERROR', 'Invalid partner_id format');

-- ============================================================================
-- STEP 8: CREATE ANALYTICS VIEWS
-- ============================================================================

USE CATALOG demo_analytics;
USE SCHEMA reporting;

-- Sales summary view
CREATE OR REPLACE VIEW v_sales_summary AS
SELECT 
    DATE_TRUNC('month', o.order_date) AS order_month,
    c.country_code,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM demo_enterprise.sales.orders o
JOIN demo_enterprise.sales.customers c ON o.customer_id = c.customer_id
GROUP BY DATE_TRUNC('month', o.order_date), c.country_code;

ALTER VIEW v_sales_summary SET TBLPROPERTIES (
    'data_classification' = 'internal'
);
COMMENT ON VIEW v_sales_summary IS 'Monthly sales summary by country for executive reporting';

-- Customer 360 view
CREATE OR REPLACE VIEW v_customer_360 AS
SELECT 
    c.customer_id,
    c.customer_segment,
    c.loyalty_tier,
    c.country_code,
    co.country_name,
    co.region,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(current_date(), MAX(o.order_date)) AS days_since_last_order
FROM demo_enterprise.sales.customers c
LEFT JOIN demo_enterprise.sales.orders o ON c.customer_id = o.customer_id
LEFT JOIN demo_enterprise.reference.countries co ON c.country_code = co.country_code
GROUP BY c.customer_id, c.customer_segment, c.loyalty_tier, c.country_code, co.country_name, co.region;

ALTER VIEW v_customer_360 SET TBLPROPERTIES (
    'data_classification' = 'confidential'
);
COMMENT ON VIEW v_customer_360 IS 'Comprehensive customer view with order history and segmentation - Contains aggregated customer data';

-- Product performance view
CREATE OR REPLACE VIEW v_product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    pc.category_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.line_total) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(oi.unit_price) AS avg_selling_price,
    p.unit_price AS current_price,
    p.cost_price,
    (p.unit_price - p.cost_price) / p.unit_price * 100 AS margin_percent
FROM demo_enterprise.sales.products p
LEFT JOIN demo_enterprise.sales.order_items oi ON p.product_id = oi.product_id
LEFT JOIN demo_enterprise.sales.orders o ON oi.order_id = o.order_id
LEFT JOIN demo_enterprise.reference.product_categories pc ON p.category_id = pc.category_id
GROUP BY p.product_id, p.product_name, pc.category_name, p.unit_price, p.cost_price;

ALTER VIEW v_product_performance SET TBLPROPERTIES (
    'data_classification' = 'internal'
);
COMMENT ON VIEW v_product_performance IS 'Product performance metrics including revenue, units sold, and margins';

-- Revenue by region
CREATE OR REPLACE VIEW v_revenue_by_region AS
SELECT 
    co.region,
    co.country_name,
    DATE_TRUNC('quarter', o.order_date) AS quarter,
    SUM(o.total_amount) AS total_revenue,
    COUNT(o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count
FROM demo_enterprise.sales.orders o
JOIN demo_enterprise.sales.customers c ON o.customer_id = c.customer_id
JOIN demo_enterprise.reference.countries co ON c.country_code = co.country_code
GROUP BY co.region, co.country_name, DATE_TRUNC('quarter', o.order_date);

ALTER VIEW v_revenue_by_region SET TBLPROPERTIES (
    'data_classification' = 'internal'
);
COMMENT ON VIEW v_revenue_by_region IS 'Quarterly revenue breakdown by geographic region';

-- Data Science schema views
USE SCHEMA data_science;

-- Customer features for ML
CREATE OR REPLACE VIEW v_customer_features AS
SELECT 
    c.customer_id,
    c.customer_segment,
    c.loyalty_tier,
    c.country_code,
    c.total_lifetime_value,
    DATEDIFF(current_date(), c.created_at) AS account_age_days,
    COUNT(DISTINCT o.order_id) AS order_count,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
    COALESCE(MAX(o.total_amount), 0) AS max_order_value,
    COALESCE(MIN(o.total_amount), 0) AS min_order_value,
    COALESCE(DATEDIFF(current_date(), MAX(o.order_date)), 9999) AS recency_days,
    CASE 
        WHEN c.loyalty_tier = 'PLATINUM' THEN 4
        WHEN c.loyalty_tier = 'GOLD' THEN 3
        WHEN c.loyalty_tier = 'SILVER' THEN 2
        ELSE 1
    END AS loyalty_score
FROM demo_enterprise.sales.customers c
LEFT JOIN demo_enterprise.sales.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_segment, c.loyalty_tier, c.country_code, 
         c.total_lifetime_value, c.created_at;

ALTER VIEW v_customer_features SET TBLPROPERTIES (
    'data_classification' = 'internal'
);
COMMENT ON VIEW v_customer_features IS 'Customer feature set for machine learning models - churn prediction, segmentation';

-- ============================================================================
-- STEP 9: ADD UNITY CATALOG TAGS
-- ============================================================================
-- Tags enable Ontos UC Bulk Import to discover and create entities automatically

-- Tag the enterprise catalog
ALTER CATALOG demo_enterprise SET TAGS ('ontos-managed' = 'true', 'data-domain' = 'enterprise');
ALTER CATALOG demo_analytics SET TAGS ('ontos-managed' = 'true', 'data-domain' = 'analytics');

-- Tag schemas with domain information
ALTER SCHEMA demo_enterprise.sales SET TAGS (
    'data-domain' = 'sales',
    'data-product-name' = 'Sales Operations',
    'data-owner' = 'sales-team@company.com'
);

ALTER SCHEMA demo_enterprise.sales_staging SET TAGS (
    'data-domain' = 'sales',
    'environment' = 'staging'
);

ALTER SCHEMA demo_enterprise.finance SET TAGS (
    'data-domain' = 'finance',
    'data-product-name' = 'Financial Services',
    'data-owner' = 'finance-team@company.com'
);

ALTER SCHEMA demo_enterprise.hr SET TAGS (
    'data-domain' = 'hr',
    'data-product-name' = 'Human Resources',
    'data-owner' = 'hr-team@company.com',
    'data-classification' = 'highly-confidential'
);

ALTER SCHEMA demo_enterprise.reference SET TAGS (
    'data-domain' = 'reference',
    'data-product-name' = 'Master Data',
    'data-owner' = 'data-governance@company.com'
);

ALTER SCHEMA demo_analytics.reporting SET TAGS (
    'data-domain' = 'analytics',
    'data-product-name' = 'Business Intelligence',
    'data-owner' = 'bi-team@company.com'
);

ALTER SCHEMA demo_analytics.data_science SET TAGS (
    'data-domain' = 'analytics',
    'data-product-name' = 'ML Features',
    'data-owner' = 'ds-team@company.com'
);

-- Tag tables for Data Contracts and Products

-- Customers - Core sales data contract
ALTER TABLE demo_enterprise.sales.customers SET TAGS (
    'data-contract-name' = 'customer-master',
    'data-product-name' = 'Customer 360',
    'data-domain' = 'sales',
    'pii' = 'true',
    'data-quality-tier' = 'gold',
    'sla-freshness-hours' = '24'
);

-- Products catalog
ALTER TABLE demo_enterprise.sales.products SET TAGS (
    'data-contract-name' = 'product-catalog',
    'data-product-name' = 'Product Catalog',
    'data-domain' = 'sales',
    'data-quality-tier' = 'gold'
);

-- Orders - Transactional data
ALTER TABLE demo_enterprise.sales.orders SET TAGS (
    'data-contract-name' = 'order-transactions',
    'data-product-name' = 'Order Management',
    'data-domain' = 'sales',
    'pii' = 'true',
    'data-quality-tier' = 'gold',
    'sla-freshness-hours' = '1'
);

-- Order items
ALTER TABLE demo_enterprise.sales.order_items SET TAGS (
    'data-contract-name' = 'order-transactions',
    'data-product-name' = 'Order Management',
    'data-domain' = 'sales',
    'data-quality-tier' = 'gold'
);

-- Finance tables
ALTER TABLE demo_enterprise.finance.invoices SET TAGS (
    'data-contract-name' = 'billing-data',
    'data-product-name' = 'Accounts Receivable',
    'data-domain' = 'finance',
    'data-quality-tier' = 'gold',
    'sla-freshness-hours' = '24'
);

ALTER TABLE demo_enterprise.finance.payments SET TAGS (
    'data-contract-name' = 'payment-transactions',
    'data-product-name' = 'Accounts Receivable',
    'data-domain' = 'finance',
    'data-quality-tier' = 'gold',
    'sla-freshness-hours' = '1'
);

ALTER TABLE demo_enterprise.finance.gl_accounts SET TAGS (
    'data-contract-name' = 'chart-of-accounts',
    'data-product-name' = 'Financial Reporting',
    'data-domain' = 'finance',
    'data-quality-tier' = 'gold'
);

-- HR tables
ALTER TABLE demo_enterprise.hr.departments SET TAGS (
    'data-contract-name' = 'org-structure',
    'data-product-name' = 'Organizational Data',
    'data-domain' = 'hr',
    'data-quality-tier' = 'gold'
);

ALTER TABLE demo_enterprise.hr.employees SET TAGS (
    'data-contract-name' = 'employee-master',
    'data-product-name' = 'Workforce Analytics',
    'data-domain' = 'hr',
    'pii' = 'true',
    'sensitive' = 'true',
    'data-quality-tier' = 'gold',
    'restricted-access' = 'true'
);

-- Reference tables
ALTER TABLE demo_enterprise.reference.countries SET TAGS (
    'data-contract-name' = 'reference-data',
    'data-product-name' = 'Master Data',
    'data-domain' = 'reference',
    'data-quality-tier' = 'gold'
);

ALTER TABLE demo_enterprise.reference.product_categories SET TAGS (
    'data-contract-name' = 'reference-data',
    'data-product-name' = 'Master Data',
    'data-domain' = 'reference',
    'data-quality-tier' = 'gold'
);

-- Staging tables
ALTER TABLE demo_enterprise.sales_staging.raw_orders SET TAGS (
    'data-domain' = 'sales',
    'data-quality-tier' = 'bronze',
    'environment' = 'staging',
    'retention-days' = '90'
);

-- Analytics views
ALTER VIEW demo_analytics.reporting.v_sales_summary SET TAGS (
    'data-product-name' = 'Executive Dashboards',
    'data-domain' = 'analytics',
    'data-quality-tier' = 'gold',
    'refresh-frequency' = 'daily'
);

ALTER VIEW demo_analytics.reporting.v_customer_360 SET TAGS (
    'data-product-name' = 'Customer 360',
    'data-domain' = 'analytics',
    'data-quality-tier' = 'gold',
    'pii' = 'derived'
);

ALTER VIEW demo_analytics.reporting.v_product_performance SET TAGS (
    'data-product-name' = 'Product Analytics',
    'data-domain' = 'analytics',
    'data-quality-tier' = 'gold'
);

ALTER VIEW demo_analytics.reporting.v_revenue_by_region SET TAGS (
    'data-product-name' = 'Executive Dashboards',
    'data-domain' = 'analytics',
    'data-quality-tier' = 'gold'
);

ALTER VIEW demo_analytics.data_science.v_customer_features SET TAGS (
    'data-product-name' = 'ML Features',
    'data-domain' = 'data-science',
    'data-quality-tier' = 'gold',
    'ml-feature-store' = 'true'
);

-- ============================================================================
-- STEP 10: VERIFICATION QUERIES
-- ============================================================================
-- Run these to verify the setup

-- Check all catalogs
SELECT catalog_name, comment 
FROM system.information_schema.catalogs 
WHERE catalog_name LIKE 'demo%';

-- Check schemas
SELECT catalog_name, schema_name, comment 
FROM system.information_schema.schemata 
WHERE catalog_name LIKE 'demo%';

-- Check tables and their row counts
SELECT 
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    t.comment
FROM system.information_schema.tables t
WHERE t.table_catalog LIKE 'demo%'
ORDER BY t.table_catalog, t.table_schema, t.table_name;

-- Check all tags (for Ontos bulk import)
SELECT 
    catalog_name,
    tag_name,
    tag_value
FROM system.information_schema.catalog_tags
WHERE catalog_name LIKE 'demo%';

SELECT 
    catalog_name,
    schema_name,
    tag_name,
    tag_value
FROM system.information_schema.schema_tags
WHERE catalog_name LIKE 'demo%';

SELECT 
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE catalog_name LIKE 'demo%'
ORDER BY catalog_name, schema_name, table_name, tag_name;

-- Summary of discovered entities for Ontos import
SELECT 'Data Contracts' AS entity_type, COUNT(DISTINCT tag_value) AS count
FROM system.information_schema.table_tags
WHERE catalog_name LIKE 'demo%' AND tag_name = 'data-contract-name'
UNION ALL
SELECT 'Data Products', COUNT(DISTINCT tag_value)
FROM system.information_schema.table_tags
WHERE catalog_name LIKE 'demo%' AND tag_name = 'data-product-name'
UNION ALL
SELECT 'Data Domains', COUNT(DISTINCT tag_value)
FROM system.information_schema.table_tags
WHERE catalog_name LIKE 'demo%' AND tag_name = 'data-domain';

-- ============================================================================
-- CLEANUP SCRIPT (Run this to remove demo data)
-- ============================================================================
-- Uncomment and run these commands to clean up:
--
-- DROP CATALOG IF EXISTS demo_enterprise CASCADE;
-- DROP CATALOG IF EXISTS demo_analytics CASCADE;
--
-- ============================================================================

SELECT 'Demo setup complete!' AS status, 
       'Use Ontos UC Bulk Import to discover tagged entities' AS next_step;

