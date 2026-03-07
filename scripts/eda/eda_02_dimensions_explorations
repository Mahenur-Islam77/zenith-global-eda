/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 2 -- Dimensions Exploration
================================================================================
  Purpose : Understand the shape of every dimension -- what categories exist,
            how many unique values are present, and what the allowed domain
            looks like for every descriptive / categorical column.

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold

  Run Order:
    2.1  dim_customer  -- cardinality of categorical columns
    2.2  dim_customer  -- enumerate all domain values
    2.3  dim_product   -- category hierarchy
    2.4  dim_product   -- product lines and maintenance
    2.5  dim_store     -- full store landscape
    2.6  fact_sales    -- delivery status domain
    2.7  fact_returns  -- return reason domain
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 2.1  dim_customer: Cardinality of All Categorical Columns
-- ============================================================================
-- High cardinality (many distinct values) --> identifier-like column
-- Low cardinality  (few distinct values)  --> good for grouping / filtering
-- ============================================================================

SELECT 'gender'         AS dimension_column, COUNT(DISTINCT gender)         AS distinct_values FROM gold.dim_customer
UNION ALL
SELECT 'marital_status',                     COUNT(DISTINCT marital_status)  FROM gold.dim_customer
UNION ALL
SELECT 'age_group',                          COUNT(DISTINCT age_group)       FROM gold.dim_customer
UNION ALL
SELECT 'city',                               COUNT(DISTINCT city)            FROM gold.dim_customer
UNION ALL
SELECT 'country',                            COUNT(DISTINCT country)         FROM gold.dim_customer
UNION ALL
SELECT 'continent',                          COUNT(DISTINCT continent)       FROM gold.dim_customer
ORDER BY distinct_values DESC;


-- ============================================================================
-- 2.2  dim_customer: Enumerate All Domain Values per Categorical Column
-- ============================================================================
-- Reveals dirty data: e.g., 'M', 'Male', 'MALE' appearing as 3 distinct values
-- when they should all map to a single standardised value.
-- ============================================================================

-- Gender values
SELECT DISTINCT gender          AS gender_value
FROM gold.dim_customer
ORDER BY gender;

-- Marital status values
SELECT DISTINCT marital_status  AS marital_status_value
FROM gold.dim_customer
ORDER BY marital_status;

-- Age group bands (should follow defined cohorts)
SELECT DISTINCT age_group       AS age_group_value
FROM gold.dim_customer
ORDER BY age_group;

-- Continent values
SELECT DISTINCT continent       AS continent_value
FROM gold.dim_customer
ORDER BY continent;

-- Country values
SELECT DISTINCT country         AS country_value
FROM gold.dim_customer
ORDER BY country;


-- ============================================================================
-- 2.3  dim_product: Full Category Hierarchy (Category --> Subcategory)
-- ============================================================================
-- Shows how many products sit under each subcategory and the cost range
-- within each group -- useful for spotting misclassified products.
-- ============================================================================

SELECT
    category,
    subcategory,
    COUNT(product_id)       AS product_count,
    MIN(cost)               AS min_cost,
    MAX(cost)               AS max_cost,
    ROUND(AVG(cost), 2)     AS avg_cost
FROM gold.dim_product
GROUP BY
    category,
    subcategory
ORDER BY
    category,
    subcategory;


-- ============================================================================
-- 2.4  dim_product: Product Lines and Maintenance Requirements
-- ============================================================================
-- Distinct product line values
-- Cross-tab: shows maintenance_required split per category
-- ============================================================================

-- All distinct product line values
SELECT DISTINCT product_line        AS product_line_value
FROM gold.dim_product
ORDER BY product_line;

-- All distinct maintenance required values
SELECT DISTINCT maintenance_required AS maintenance_value
FROM gold.dim_product
ORDER BY maintenance_required;

-- Cross-tab: how many products per category require maintenance?
SELECT
    category,
    maintenance_required,
    COUNT(*)                AS product_count
FROM gold.dim_product
GROUP BY
    category,
    maintenance_required
ORDER BY
    category,
    maintenance_required;


-- ============================================================================
-- 2.5  dim_store: Full Store Landscape
-- ============================================================================
-- Understand the channel mix (Flagship / Retail / Online / Outlet)
-- and how stores are distributed across regions.
-- ============================================================================

-- Distinct store types
SELECT DISTINCT store_type  AS store_type_value
FROM gold.dim_store
ORDER BY store_type;

-- Distinct regions
SELECT DISTINCT region      AS region_value
FROM gold.dim_store
ORDER BY region;

-- Store count per type and region
SELECT
    store_type,
    region,
    COUNT(store_id)         AS store_count
FROM gold.dim_store
GROUP BY
    store_type,
    region
ORDER BY
    store_type,
    region;


-- ============================================================================
-- 2.6  fact_sales: Delivery Status Domain
-- ============================================================================
-- Confirms only valid values exist: 'On Time', 'Late', 'Unknown'
-- Also shows the split -- what percentage of orders ship on time?
-- ============================================================================

SELECT
    delivery_status,
    COUNT(*)                                        AS order_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
    , 2)                                            AS pct_of_total
FROM gold.fact_sales
GROUP BY delivery_status
ORDER BY order_count DESC;


-- ============================================================================
-- 2.7  fact_returns: Return Reason Domain
-- ============================================================================
-- Enumerate all return reason values.
-- Expected reasons: Defective, Size Mismatch, Unsatisfied, Wrong Item
-- ============================================================================

SELECT
    return_reason,
    COUNT(*)                                        AS return_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
    , 2)                                            AS pct_of_total
FROM gold.fact_returns
GROUP BY return_reason
ORDER BY return_count DESC;
