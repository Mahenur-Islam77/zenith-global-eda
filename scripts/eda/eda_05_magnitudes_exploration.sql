/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 5 -- Magnitudes
================================================================================
  Purpose : Quantify the scale and volume of the business across every
            analytical dimension. Reveals which segments drive the most value,
            volume, and customer engagement.

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold

  Run Order:
    5.1  Revenue by year (YoY scale)
    5.2  Revenue by product category
    5.3  Revenue by store type / channel
    5.4  Revenue by continent and country
    5.5  Returns magnitude by product category
    5.6  Customer volume by demographic segment (age group, gender, marital)
    5.7  Monthly order volume trend
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 5.1  Total Revenue by Year (Year-over-Year Scale)
-- ============================================================================
-- The most fundamental business KPI.
-- Compare 2023 vs 2024 to understand growth trajectory.
-- ============================================================================

SELECT
    order_year,
    COUNT(DISTINCT order_number)            AS total_orders,
    SUM(quantity)                           AS total_units_sold,
    ROUND(SUM(sales_amount), 2)             AS total_revenue,
    ROUND(AVG(sales_amount), 2)             AS avg_order_value
FROM gold.fact_sales
GROUP BY order_year
ORDER BY order_year;


-- ============================================================================
-- 5.2  Revenue by Product Category
-- ============================================================================
-- Which category carries the most weight in the business?
-- revenue_pct shows each category's contribution to the total.
-- ============================================================================

SELECT
    dp.category,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    SUM(fs.quantity)                        AS units_sold,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(
        SUM(fs.sales_amount) * 100.0
        / SUM(SUM(fs.sales_amount)) OVER ()
    , 2)                                    AS revenue_pct
FROM gold.fact_sales fs
JOIN gold.dim_product dp
    ON fs.product_id = dp.product_id
GROUP BY dp.category
ORDER BY total_revenue DESC;


-- ============================================================================
-- 5.3  Revenue by Store Type (Sales Channel)
-- ============================================================================
-- Compares Online vs Retail vs Flagship vs Outlet performance.
-- avg_order_value shows whether online buyers spend more per order.
-- ============================================================================

SELECT
    ds.store_type,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    SUM(fs.quantity)                        AS units_sold,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(AVG(fs.sales_amount), 2)          AS avg_order_value,
    ROUND(
        SUM(fs.sales_amount) * 100.0
        / SUM(SUM(fs.sales_amount)) OVER ()
    , 2)                                    AS revenue_pct
FROM gold.fact_sales fs
JOIN gold.dim_store ds
    ON fs.store_id = ds.store_id
GROUP BY ds.store_type
ORDER BY total_revenue DESC;


-- ============================================================================
-- 5.4  Revenue by Geography (Continent and Country)
-- ============================================================================
-- Two separate queries: continent-level rollup and country-level detail.
-- Together they reveal which markets drive the most revenue.
-- ============================================================================

-- By continent
SELECT
    dc.continent,
    COUNT(DISTINCT dc.customer_id)          AS unique_customers,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(
        SUM(fs.sales_amount) * 100.0
        / SUM(SUM(fs.sales_amount)) OVER ()
    , 2)                                    AS revenue_pct
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
    ON fs.customer_id = dc.customer_id
GROUP BY dc.continent
ORDER BY total_revenue DESC;

-- By country (detailed breakdown)
SELECT
    dc.country,
    dc.continent,
    COUNT(DISTINCT dc.customer_id)          AS unique_customers,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(AVG(fs.sales_amount), 2)          AS avg_order_value
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
    ON fs.customer_id = dc.customer_id
GROUP BY
    dc.country,
    dc.continent
ORDER BY total_revenue DESC;


-- ============================================================================
-- 5.5  Returns Magnitude by Product Category
-- ============================================================================
-- Links: fact_returns --> fact_sales --> dim_product
-- return_value_pct reveals which categories generate the most financial
-- leakage through returns.
-- ============================================================================

SELECT
    dp.category,
    COUNT(fr.return_id)                     AS total_returns,
    ROUND(SUM(fr.return_amount), 2)         AS total_returned_value,
    ROUND(AVG(fr.return_amount), 2)         AS avg_return_amount,
    ROUND(
        SUM(fr.return_amount) * 100.0
        / SUM(SUM(fr.return_amount)) OVER ()
    , 2)                                    AS return_value_pct
FROM gold.fact_returns fr
JOIN gold.fact_sales  fs  ON fr.order_number = fs.order_number
JOIN gold.dim_product dp  ON fs.product_id   = dp.product_id
GROUP BY dp.category
ORDER BY total_returns DESC;


-- ============================================================================
-- 5.6  Customer Volume by Demographic Segment
-- ============================================================================
-- Combines dim_customer with fact_sales to show which demographic group
-- generates the most customers and the most revenue.
-- ============================================================================

-- By age group
SELECT
    dc.age_group,
    COUNT(DISTINCT dc.customer_id)          AS customer_count,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(AVG(fs.sales_amount), 2)          AS avg_order_value
FROM gold.dim_customer dc
LEFT JOIN gold.fact_sales fs
    ON dc.customer_id = fs.customer_id
GROUP BY dc.age_group
ORDER BY total_revenue DESC;

-- By gender
SELECT
    dc.gender,
    COUNT(DISTINCT dc.customer_id)          AS customer_count,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue,
    ROUND(AVG(fs.sales_amount), 2)          AS avg_order_value
FROM gold.dim_customer dc
LEFT JOIN gold.fact_sales fs
    ON dc.customer_id = fs.customer_id
GROUP BY dc.gender
ORDER BY total_revenue DESC;

-- By marital status
SELECT
    dc.marital_status,
    COUNT(DISTINCT dc.customer_id)          AS customer_count,
    COUNT(DISTINCT fs.order_number)         AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)          AS total_revenue
FROM gold.dim_customer dc
LEFT JOIN gold.fact_sales fs
    ON dc.customer_id = fs.customer_id
GROUP BY dc.marital_status
ORDER BY total_revenue DESC;


-- ============================================================================
-- 5.7  Monthly Order Volume Trend
-- ============================================================================
-- Shows the volume of orders placed each month across the full date range.
-- Useful for spotting seasonality, peaks, and quiet periods in the business.
-- ============================================================================

SELECT
    order_year,
    order_month,
    order_month_name,
    COUNT(DISTINCT order_number)            AS total_orders,
    SUM(quantity)                           AS units_sold,
    ROUND(SUM(sales_amount), 2)             AS monthly_revenue
FROM gold.fact_sales
GROUP BY
    order_year,
    order_month,
    order_month_name
ORDER BY
    order_year,
    order_month;
