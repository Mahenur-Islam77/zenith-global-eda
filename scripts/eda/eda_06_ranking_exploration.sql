/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 6 -- Ranking
================================================================================
  Purpose : Identify top and bottom performers across products, customers,
            stores, geographies, and time using SQL window functions.

  Window Functions Used:
    DENSE_RANK()   -- ranks with no gaps (1,2,2,3 not 1,2,2,4). Use for
                      leaderboards where ties should share a rank.
    RANK()         -- ranks with gaps after ties (1,2,2,4). Rarely preferred.
    ROW_NUMBER()   -- unique sequential position even for ties. Use when you
                      need exactly N rows (e.g., strict Top 10).
    PARTITION BY   -- resets the rank counter within each group (e.g., rank
                      products inside their own category independently).

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold

  Run Order:
    6.1  Top 10 best-selling products by revenue
    6.2  Bottom 10 lowest-performing products
    6.3  Top 10 highest-value customers (Customer Lifetime Value)
    6.4  Product rank within category (PARTITION BY)
    6.5  Store ranking by revenue and delivery performance
    6.6  Monthly revenue ranking (best and worst months)
    6.7  Countries ranked by return rate
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 6.1  Top 10 Best-Selling Products by Revenue
-- ============================================================================
-- DENSE_RANK handles ties: two products with the same revenue share rank 1,
-- and the next product gets rank 2 (not rank 3).
-- ============================================================================

SELECT TOP 10
    dp.product_name,
    dp.category,
    dp.subcategory,
    SUM(fs.quantity)                            AS units_sold,
    ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) DESC
    )                                           AS revenue_rank
FROM gold.fact_sales fs
JOIN gold.dim_product dp
    ON fs.product_id = dp.product_id
GROUP BY
    dp.product_name,
    dp.category,
    dp.subcategory
ORDER BY revenue_rank;


-- ============================================================================
-- 6.2  Bottom 10 Lowest-Performing Products
-- ============================================================================
-- Ranks by ASC revenue to surface the weakest products.
-- These are candidates for discontinuation, repricing, or promotion review.
-- ============================================================================

SELECT TOP 10
    dp.product_name,
    dp.category,
    dp.subcategory,
    SUM(fs.quantity)                            AS units_sold,
    ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) ASC
    )                                           AS low_revenue_rank
FROM gold.fact_sales fs
JOIN gold.dim_product dp
    ON fs.product_id = dp.product_id
GROUP BY
    dp.product_name,
    dp.category,
    dp.subcategory
ORDER BY low_revenue_rank;


-- ============================================================================
-- 6.3  Top 10 Highest-Value Customers (Customer Lifetime Value)
-- ============================================================================
-- Lifetime revenue = sum of all sales_amount for a customer across 2023-2024.
-- Also shows order frequency and average spend per order.
-- ============================================================================

SELECT TOP 10
    dc.customer_id,
    dc.full_name,
    dc.country,
    dc.age_group,
    dc.gender,
    COUNT(DISTINCT fs.order_number)             AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)              AS lifetime_revenue,
    ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) DESC
    )                                           AS clv_rank
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
    ON fs.customer_id = dc.customer_id
GROUP BY
    dc.customer_id,
    dc.full_name,
    dc.country,
    dc.age_group,
    dc.gender
ORDER BY clv_rank;


-- ============================================================================
-- 6.4  Product Rank Within Category (PARTITION BY category)
-- ============================================================================
-- PARTITION BY resets the rank counter for each category independently.
-- Result: you see rank 1, 2, 3 ... inside Bikes AND rank 1, 2, 3 inside
-- Accessories simultaneously -- all in one result set.
-- ============================================================================

SELECT
    dp.category,
    dp.subcategory,
    dp.product_name,
    ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
    SUM(fs.quantity)                            AS units_sold,
    -- Global rank across all products
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) DESC
    )                                           AS global_rank,
    -- Rank reset per category
    DENSE_RANK() OVER (
        PARTITION BY dp.category
        ORDER BY SUM(fs.sales_amount) DESC
    )                                           AS rank_within_category
FROM gold.fact_sales fs
JOIN gold.dim_product dp
    ON fs.product_id = dp.product_id
GROUP BY
    dp.category,
    dp.subcategory,
    dp.product_name
ORDER BY
    dp.category,
    rank_within_category;


-- ============================================================================
-- 6.5  Store Ranking by Revenue and Delivery Performance
-- ============================================================================
-- Two independent DENSE_RANK windows in one query:
--   revenue_rank    : ordered by total revenue DESC (higher = better)
--   delivery_rank   : ordered by on-time rate DESC (higher = better)
-- A store ranked #1 for revenue but #20 for delivery is commercially strong
-- but operationally weak -- a key insight for management.
-- ============================================================================

SELECT
    ds.store_name,
    ds.store_type,
    ds.region,
    COUNT(DISTINCT fs.order_number)             AS total_orders,
    ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
    -- On-time delivery rate %
    ROUND(
        SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
        * 100.0 / COUNT(*)
    , 2)                                        AS on_time_pct,
    -- Revenue rank (1 = highest revenue)
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) DESC
    )                                           AS revenue_rank,
    -- Delivery rank (1 = best on-time rate)
    DENSE_RANK() OVER (
        ORDER BY
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) DESC
    )                                           AS delivery_rank
FROM gold.fact_sales fs
JOIN gold.dim_store ds
    ON fs.store_id = ds.store_id
GROUP BY
    ds.store_name,
    ds.store_type,
    ds.region
ORDER BY revenue_rank;


-- ============================================================================
-- 6.6  Monthly Revenue Ranking (Best and Worst Months)
-- ============================================================================
-- Two DENSE_RANK windows:
--   overall_rank      : ranks every year-month across the full dataset
--   rank_within_year  : ranks months inside their own year (PARTITION BY year)
-- ============================================================================

SELECT
    order_year,
    order_month,
    order_month_name,
    COUNT(DISTINCT order_number)                AS total_orders,
    ROUND(SUM(sales_amount), 2)                 AS monthly_revenue,
    -- Rank across all months in the dataset
    DENSE_RANK() OVER (
        ORDER BY SUM(sales_amount) DESC
    )                                           AS overall_rank,
    -- Rank within the same year only
    DENSE_RANK() OVER (
        PARTITION BY order_year
        ORDER BY SUM(sales_amount) DESC
    )                                           AS rank_within_year
FROM gold.fact_sales
GROUP BY
    order_year,
    order_month,
    order_month_name
ORDER BY
    order_year,
    order_month;


-- ============================================================================
-- 6.7  Countries Ranked by Return Rate
-- ============================================================================
-- return_rate_pct = returns / orders * 100
-- NULLIF prevents divide-by-zero for countries with zero orders.
-- High return rates highlight logistics or product-market fit problems
-- in specific geographies.
-- ============================================================================

SELECT
    dc.country,
    dc.continent,
    COUNT(DISTINCT fs.order_number)             AS total_orders,
    COUNT(DISTINCT fr.return_id)                AS total_returns,
    ROUND(
        COUNT(DISTINCT fr.return_id) * 100.0
        / NULLIF(COUNT(DISTINCT fs.order_number), 0)
    , 2)                                        AS return_rate_pct,
    -- Rank countries: highest return rate = rank 1 (worst performer)
    DENSE_RANK() OVER (
        ORDER BY
            COUNT(DISTINCT fr.return_id) * 100.0
            / NULLIF(COUNT(DISTINCT fs.order_number), 0) DESC
    )                                           AS return_rank
FROM gold.fact_sales fs
JOIN gold.dim_customer dc
    ON fs.customer_id  = dc.customer_id
LEFT JOIN gold.fact_returns fr
    ON fs.order_number = fr.order_number
GROUP BY
    dc.country,
    dc.continent
ORDER BY return_rank;
