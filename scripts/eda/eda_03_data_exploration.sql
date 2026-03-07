/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 3 -- Data Exploration (Quality & Integrity)
================================================================================
  Purpose : Validate data quality -- check for NULLs, duplicates, date range
            validity, unrealistic values, and referential integrity between
            fact and dimension tables. Sign off on data before any aggregation.

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold

  Run Order:
    3.1  Date range exploration       (fact_sales, fact_returns)
    3.2  NULL audit -- dim_customer
    3.3  NULL audit -- dim_product
    3.4  NULL audit -- fact_sales
    3.5  NULL audit -- fact_returns
    3.6  Referential integrity checks (orphan records)
    3.7  Duplicate detection          (business key uniqueness)
    3.8  Age data sanity check        (unrealistic birthdate/age values)
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 3.1  Date Range Exploration
-- ============================================================================
-- Confirms temporal coverage of both fact tables aligns to 2023-2024.
-- span_months tells us how many months of history are available.
-- ============================================================================

-- Sales date range
SELECT
    MIN(order_date)                         AS earliest_order,
    MAX(order_date)                         AS latest_order,
    DATEDIFF(MONTH,
        MIN(order_date),
        MAX(order_date))                    AS span_months,
    COUNT(DISTINCT YEAR(order_date))        AS years_covered,
    COUNT(DISTINCT
        CAST(YEAR(order_date) AS VARCHAR) + '-' +
        RIGHT('0' + CAST(MONTH(order_date) AS VARCHAR), 2)
    )                                       AS distinct_year_months
FROM gold.fact_sales;

-- Returns date range
SELECT
    MIN(return_date)                        AS earliest_return,
    MAX(return_date)                        AS latest_return,
    DATEDIFF(MONTH,
        MIN(return_date),
        MAX(return_date))                   AS span_months
FROM gold.fact_returns;


-- ============================================================================
-- 3.2  NULL Audit: dim_customer
-- ============================================================================
-- COUNT(*) - COUNT(col) gives NULLs for each column.
-- NULLs in key fields signal data gaps from the CRM / ERP source systems.
-- ============================================================================

SELECT
    'customer_id'       AS column_name, COUNT(*) - COUNT(customer_id)      AS null_count FROM gold.dim_customer
UNION ALL SELECT 'first_name',          COUNT(*) - COUNT(first_name)       FROM gold.dim_customer
UNION ALL SELECT 'last_name',           COUNT(*) - COUNT(last_name)        FROM gold.dim_customer
UNION ALL SELECT 'gender',              COUNT(*) - COUNT(gender)           FROM gold.dim_customer
UNION ALL SELECT 'marital_status',      COUNT(*) - COUNT(marital_status)   FROM gold.dim_customer
UNION ALL SELECT 'birthdate',           COUNT(*) - COUNT(birthdate)        FROM gold.dim_customer
UNION ALL SELECT 'age',                 COUNT(*) - COUNT(age)              FROM gold.dim_customer
UNION ALL SELECT 'city',                COUNT(*) - COUNT(city)             FROM gold.dim_customer
UNION ALL SELECT 'country',             COUNT(*) - COUNT(country)          FROM gold.dim_customer
UNION ALL SELECT 'continent',           COUNT(*) - COUNT(continent)        FROM gold.dim_customer
ORDER BY null_count DESC;


-- ============================================================================
-- 3.3  NULL Audit: dim_product
-- ============================================================================
-- cost NULLs would break margin calculations.
-- category / subcategory NULLs would cause products to fall into 'Unknown'.
-- ============================================================================

SELECT
    'product_id'            AS column_name, COUNT(*) - COUNT(product_id)            AS null_count FROM gold.dim_product
UNION ALL SELECT 'product_name',            COUNT(*) - COUNT(product_name)          FROM gold.dim_product
UNION ALL SELECT 'cost',                    COUNT(*) - COUNT(cost)                  FROM gold.dim_product
UNION ALL SELECT 'category',               COUNT(*) - COUNT(category)              FROM gold.dim_product
UNION ALL SELECT 'subcategory',             COUNT(*) - COUNT(subcategory)           FROM gold.dim_product
UNION ALL SELECT 'product_line',            COUNT(*) - COUNT(product_line)          FROM gold.dim_product
UNION ALL SELECT 'maintenance_required',    COUNT(*) - COUNT(maintenance_required)  FROM gold.dim_product
ORDER BY null_count DESC;


-- ============================================================================
-- 3.4  NULL Audit: fact_sales
-- ============================================================================
-- FK column NULLs (product_id, customer_id, store_id) would break all joins.
-- Measure NULLs (sales_amount, quantity) would silently undercount revenue.
-- ============================================================================

SELECT
    'order_number'      AS column_name, COUNT(*) - COUNT(order_number)    AS null_count FROM gold.fact_sales
UNION ALL SELECT 'product_id',          COUNT(*) - COUNT(product_id)      FROM gold.fact_sales
UNION ALL SELECT 'customer_id',         COUNT(*) - COUNT(customer_id)     FROM gold.fact_sales
UNION ALL SELECT 'store_id',            COUNT(*) - COUNT(store_id)        FROM gold.fact_sales
UNION ALL SELECT 'order_date',          COUNT(*) - COUNT(order_date)      FROM gold.fact_sales
UNION ALL SELECT 'shipping_date',       COUNT(*) - COUNT(shipping_date)   FROM gold.fact_sales
UNION ALL SELECT 'due_date',            COUNT(*) - COUNT(due_date)        FROM gold.fact_sales
UNION ALL SELECT 'quantity',            COUNT(*) - COUNT(quantity)        FROM gold.fact_sales
UNION ALL SELECT 'price',               COUNT(*) - COUNT(price)           FROM gold.fact_sales
UNION ALL SELECT 'sales_amount',        COUNT(*) - COUNT(sales_amount)    FROM gold.fact_sales
ORDER BY null_count DESC;


-- ============================================================================
-- 3.5  NULL Audit: fact_returns
-- ============================================================================
-- return_amount NULLs would understate the total value refunded.
-- order_number NULLs would prevent linking returns back to sales.
-- ============================================================================

SELECT
    'return_id'         AS column_name, COUNT(*) - COUNT(return_id)       AS null_count FROM gold.fact_returns
UNION ALL SELECT 'order_number',        COUNT(*) - COUNT(order_number)    FROM gold.fact_returns
UNION ALL SELECT 'return_date',         COUNT(*) - COUNT(return_date)     FROM gold.fact_returns
UNION ALL SELECT 'return_reason',       COUNT(*) - COUNT(return_reason)   FROM gold.fact_returns
UNION ALL SELECT 'return_amount',       COUNT(*) - COUNT(return_amount)   FROM gold.fact_returns
ORDER BY null_count DESC;


-- ============================================================================
-- 3.6  Referential Integrity Checks (Orphan Records)
-- ============================================================================
-- An orphan is a FK value in the fact table that has NO matching row in its
-- dimension. Orphans cause rows to silently disappear in LEFT JOINs and
-- return NULL dimension attributes in INNER JOINs.
-- Expected result: 0 orphans in a clean warehouse.
-- ============================================================================

-- Orphan customers in fact_sales (no matching dim_customer)
SELECT COUNT(*) AS orphan_customers
FROM gold.fact_sales fs
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_customer dc
    WHERE dc.customer_id = fs.customer_id
);

-- Orphan products in fact_sales (no matching dim_product)
SELECT COUNT(*) AS orphan_products
FROM gold.fact_sales fs
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_product dp
    WHERE dp.product_id = fs.product_id
);

-- Orphan stores in fact_sales (no matching dim_store)
SELECT COUNT(*) AS orphan_stores
FROM gold.fact_sales fs
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_store ds
    WHERE ds.store_id = fs.store_id
);

-- Orphan returns in fact_returns (no matching order in fact_sales)
SELECT COUNT(*) AS orphan_returns
FROM gold.fact_returns fr
WHERE NOT EXISTS (
    SELECT 1 FROM gold.fact_sales fs
    WHERE fs.order_number = fr.order_number
);


-- ============================================================================
-- 3.7  Duplicate Detection (Business Key Uniqueness)
-- ============================================================================
-- Each business key should appear exactly once.
-- Any count > 1 is a data quality defect requiring investigation.
-- ============================================================================

-- Duplicate customer IDs in dim_customer
SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Duplicate product IDs in dim_product
SELECT
    product_id,
    COUNT(*) AS occurrences
FROM gold.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Duplicate order numbers in fact_sales
SELECT
    order_number,
    COUNT(*) AS occurrences
FROM gold.fact_sales
GROUP BY order_number
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Duplicate return IDs in fact_returns
SELECT
    return_id,
    COUNT(*) AS occurrences
FROM gold.fact_returns
GROUP BY return_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;


-- ============================================================================
-- 3.8  Age Data Sanity Check
-- ============================================================================
-- Ages < 18 or > 100 likely indicate bad birthdate data in the source CRM.
-- Also confirms how many customers have a NULL birthdate (no age computable).
-- ============================================================================

SELECT
    MIN(age)                                        AS youngest_customer,
    MAX(age)                                        AS oldest_customer,
    ROUND(AVG(CAST(age AS FLOAT)), 1)               AS avg_age,
    -- Flag potentially bad records
    SUM(CASE WHEN age < 18  THEN 1 ELSE 0 END)      AS age_under_18,
    SUM(CASE WHEN age > 100 THEN 1 ELSE 0 END)      AS age_over_100,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END)    AS age_null
FROM gold.dim_customer;
