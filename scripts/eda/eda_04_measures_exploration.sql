/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 4 -- Measures Exploration
================================================================================
  Purpose : Compute descriptive statistics for every numeric measure in the
            gold layer. Understand scale, spread, and central tendency before
            building any aggregation or dashboard.

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold

  Run Order:
    4.1  sales_amount  -- full statistical profile with percentiles
    4.2  price & quantity -- summary statistics
    4.3  return_amount -- statistical summary + return rate vs revenue
    4.4  product cost  -- statistical profile by category
    4.5  delivery lead time -- shipping and due-date statistics
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 4.1  sales_amount: Full Statistical Profile with Percentiles
-- ============================================================================
-- PERCENT_RANK() gives a 0-1 percentile position for each row.
-- We then use conditional MAX to extract P25 / P50 / P75 breakpoints.
-- CV% (Coefficient of Variation) = StdDev / Mean * 100
--   Low CV%  (<30%)  --> revenue is consistently distributed
--   High CV% (>100%) --> a few very large orders dominate the total
-- ============================================================================

SELECT
    COUNT(*)                                    AS total_orders,
    ROUND(SUM(sales_amount), 2)                 AS total_revenue,
    ROUND(MIN(sales_amount), 2)                 AS min_order_value,
    ROUND(MAX(sales_amount), 2)                 AS max_order_value,
    ROUND(AVG(sales_amount), 2)                 AS avg_order_value,
    ROUND(STDEV(sales_amount), 2)               AS stddev_order_value,
    -- Coefficient of Variation: how spread out is the revenue?
    ROUND(
        STDEV(sales_amount)
        / NULLIF(AVG(sales_amount), 0) * 100
    , 2)                                        AS cv_pct,
    -- Percentile breakpoints via PERCENT_RANK
    ROUND(MAX(CASE WHEN rn_pct <= 25 THEN sales_amount END), 2) AS p25,
    ROUND(MAX(CASE WHEN rn_pct <= 50 THEN sales_amount END), 2) AS p50_median,
    ROUND(MAX(CASE WHEN rn_pct <= 75 THEN sales_amount END), 2) AS p75
FROM (
    SELECT
        sales_amount,
        PERCENT_RANK() OVER (ORDER BY sales_amount) * 100  AS rn_pct
    FROM gold.fact_sales
) t;


-- ============================================================================
-- 4.2  Price and Quantity: Summary Statistics
-- ============================================================================
-- unit price: what is the typical selling price of an individual item?
-- quantity  : how many units are typically sold per order?
-- effective_price_per_unit: does quantity discounting reduce the unit rate?
-- ============================================================================

SELECT
    -- Unit price analysis
    ROUND(MIN(price), 2)                        AS min_unit_price,
    ROUND(MAX(price), 2)                        AS max_unit_price,
    ROUND(AVG(price), 2)                        AS avg_unit_price,
    ROUND(STDEV(price), 2)                      AS stddev_price,

    -- Quantity per order analysis
    MIN(quantity)                               AS min_qty_per_order,
    MAX(quantity)                               AS max_qty_per_order,
    ROUND(AVG(CAST(quantity AS FLOAT)), 2)      AS avg_qty_per_order,
    SUM(quantity)                               AS total_units_sold,

    -- Derived: effective revenue per unit (accounts for discounts)
    ROUND(
        SUM(sales_amount) / NULLIF(SUM(quantity), 0)
    , 2)                                        AS effective_price_per_unit
FROM gold.fact_sales;


-- ============================================================================
-- 4.3  return_amount: Statistical Summary + Return Rate vs Revenue
-- ============================================================================
-- Compares the total value returned against total revenue.
-- A high return_rate_pct indicates a significant financial leakage.
-- ============================================================================

-- Statistical profile of individual return transactions
SELECT
    COUNT(*)                                    AS total_return_transactions,
    ROUND(SUM(return_amount), 2)                AS total_returned_value,
    ROUND(MIN(return_amount), 2)                AS min_return_amount,
    ROUND(MAX(return_amount), 2)                AS max_return_amount,
    ROUND(AVG(return_amount), 2)                AS avg_return_amount,
    ROUND(STDEV(return_amount), 2)              AS stddev_return_amount
FROM gold.fact_returns;

-- Returns as % of gross sales revenue
SELECT
    ROUND(
        (SELECT SUM(return_amount) FROM gold.fact_returns)
        / NULLIF((SELECT SUM(sales_amount) FROM gold.fact_sales), 0)
        * 100
    , 2)                                        AS return_rate_pct;


-- ============================================================================
-- 4.4  Product Cost: Statistical Profile by Category
-- ============================================================================
-- stddev_cost reveals price dispersion within a category.
-- A high stddev in 'Bikes' is expected (budget vs premium SKUs).
-- A high stddev in 'Accessories' may indicate miscoded products.
-- ============================================================================

SELECT
    category,
    COUNT(product_id)                           AS product_count,
    ROUND(MIN(cost), 2)                         AS min_cost,
    ROUND(MAX(cost), 2)                         AS max_cost,
    ROUND(AVG(cost), 2)                         AS avg_cost,
    ROUND(STDEV(cost), 2)                       AS stddev_cost,
    -- Cost range within category
    ROUND(MAX(cost) - MIN(cost), 2)             AS cost_range
FROM gold.dim_product
GROUP BY category
ORDER BY avg_cost DESC;


-- ============================================================================
-- 4.5  Delivery Lead Time: Shipping and Due-Date Statistics
-- ============================================================================
-- days_to_ship: how long from order to dispatch?
-- days_to_due : how long until payment / delivery is due?
-- Large stddev in days_to_ship signals inconsistent fulfilment performance.
-- ============================================================================

SELECT
    -- Days from order to shipping
    MIN(days_to_ship)                           AS min_days_to_ship,
    MAX(days_to_ship)                           AS max_days_to_ship,
    ROUND(AVG(CAST(days_to_ship AS FLOAT)), 1)  AS avg_days_to_ship,
    ROUND(STDEV(days_to_ship), 1)               AS stddev_days_to_ship,

    -- Days from order to due date
    MIN(days_to_due)                            AS min_days_to_due,
    MAX(days_to_due)                            AS max_days_to_due,
    ROUND(AVG(CAST(days_to_due AS FLOAT)), 1)   AS avg_days_to_due,
    ROUND(STDEV(days_to_due), 1)                AS stddev_days_to_due
FROM gold.fact_sales
WHERE
    days_to_ship IS NOT NULL    -- exclude orders without a shipping date
    AND days_to_due IS NOT NULL;
