/*
Purpose:
    Initialize the analytical database schema for the DataCo Supply Chain case study.

What this script does:
    - Creates the analytics database
    - Defines dimension tables (Geo, Customer Geo, Product)
    - Defines the central fact table with enforced relationships

Execution Notes:
    - Run this script once before loading any data
    - Dimension tables must be created before the fact table
    - This script assumes SQL Server
*/

-- =============================================================================
-- STEP 1: CREATE DATABASE
-- =============================================================================
CREATE DATABASE DataCo_Analytics;
GO

-- =============================================================================
-- STEP 2: SET DATABASE CONTEXT
-- =============================================================================
-- Prevents accidental table creation in system databases
USE DataCo_Analytics;
GO

-- =============================================================================
-- STEP 3: CREATE DIMENSION TABLES
-- =============================================================================
-- Dimensions must exist before the fact table due to foreign key constraints

CREATE TABLE Dim_Geo (
    geo_id INT PRIMARY KEY,
    order_country NVARCHAR(100),
    order_state NVARCHAR(100),
    order_region NVARCHAR(100),
    market NVARCHAR(50)
);

CREATE TABLE Dim_Customer_Geo (
    customer_geo_id INT PRIMARY KEY,
    customer_country NVARCHAR(100),
    customer_state NVARCHAR(100)
);

CREATE TABLE Dim_Product (
    product_key INT PRIMARY KEY,
    product_name NVARCHAR(255),
    category_name NVARCHAR(100),
    department_name NVARCHAR(100)
);

-- =============================================================================
-- STEP 4: CREATE FACT TABLE
-- =============================================================================
-- Central analytical table containing transactional and derived metrics

CREATE TABLE Fact_Sales (
    -- IDENTIFIERS & KEYS
    order_id INT IDENTITY(10000001, 1) 
        PRIMARY KEY CLUSTERED,

    geo_id INT 
        FOREIGN KEY REFERENCES Dim_Geo(geo_id),

    customer_geo_id INT 
        FOREIGN KEY REFERENCES Dim_Customer_Geo(customer_geo_id),

    product_key INT 
        FOREIGN KEY REFERENCES Dim_Product(product_key),

    -- TIME DIMENSIONS
    order_year INT,
    order_month INT,
    order_day INT,
    day_name_str NVARCHAR(20),
    order_day_type NVARCHAR(20),

    -- LOGISTICS & OPERATIONS
    type NVARCHAR(50),
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    shipping_delta INT,
    delivery_class NVARCHAR(50),
    shipping_mode_clean NVARCHAR(50),
    order_status NVARCHAR(50),
    customer_segment NVARCHAR(50),

    -- FINANCIAL METRICS
    order_item_quantity INT,
    order_item_product_price DECIMAL(18, 4),
    order_item_discount_rate DECIMAL(18, 4),
    order_item_profit_ratio DECIMAL(18, 4),
    gross_sales DECIMAL(18, 4),
    discount_amount DECIMAL(18, 4),
    net_revenue DECIMAL(18, 4),
    order_profit_amount DECIMAL(18, 4),
    total_cost DECIMAL(18, 4),
    actual_unit_cost DECIMAL(18, 4),

    -- RISK & STRATEGY FLAGS
    is_profit_bleeder BIT,
    markup_pct DECIMAL(18, 4),
    margin_leakage_pct DECIMAL(18, 4),
    price_segment NVARCHAR(50),
    trade_route NVARCHAR(255),

    -- ANALYTICAL SUPPORT METRICS
    state_order_count INT,
    state_density_class NVARCHAR(50),

    -- AUDIT
    load_timestamp DATETIME DEFAULT GETDATE()
);
GO