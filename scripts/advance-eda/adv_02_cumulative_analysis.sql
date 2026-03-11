/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 2: Cumulative Analysis
================================================================================
  Purpose : Build running totals, cumulative percentages, and moving averages
            to understand growth trajectory and year-to-date performance.
            Cumulative views reveal how quickly targets are reached and
            where acceleration or deceleration occurs.

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: SUM() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
              AVG() OVER (ROWS BETWEEN N PRECEDING AND CURRENT ROW),
              PARTITION BY, conditional aggregation, CTEs, NULLIF

  Sections:
    2.1  Cumulative revenue by month
    2.2  Cumulative revenue as % of annual total
    2.3  Year-to-date (YTD) revenue comparison: 2023 vs 2024
    2.4  Cumulative units sold over time
    2.5  3-month and 6-month moving average revenue
    2.6  Cumulative new customer acquisition
    2.7  Cumulative return value vs cumulative revenue (leakage trend)
    2.8  Cumulative revenue by product category
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 2.1  Cumulative Revenue by Month
-- ============================================================================
-- Each month's row shows: the monthly figure AND the running total
-- from the beginning of the dataset up to and including that month.
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is the standard frame.
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        COUNT(DISTINCT order_number)            AS monthly_orders,
        ROUND(SUM(sales_amount), 2)             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_orders,
    monthly_revenue,
    -- Running total revenue from month 1 through current month
    ROUND(SUM(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS cumulative_revenue,
    -- Running total orders
    SUM(monthly_orders) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS cumulative_orders
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 2.2  Cumulative Revenue as % of Annual Total
-- ============================================================================
-- For each month, shows what % of that year's total revenue has been
-- accumulated so far. Reveals how evenly distributed revenue is across the year.
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    -- Running total within the year only (PARTITION BY order_year)
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS ytd_revenue,
    -- Annual total for this year (full year sum)
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY order_year
    ), 2)                                       AS annual_total,
    -- % of the annual total accumulated so far
    ROUND(
        SUM(monthly_revenue) OVER (
            PARTITION BY order_year
            ORDER BY order_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0
        / NULLIF(SUM(monthly_revenue) OVER (PARTITION BY order_year), 0)
    , 2)                                        AS pct_of_annual_total
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 2.3  Year-to-Date (YTD) Comparison: 2023 vs 2024 Side by Side
-- ============================================================================
-- Compares the cumulative performance of both years month by month.
-- A 2024 YTD > 2023 YTD at the same month = the business is outpacing last year.
-- ============================================================================

WITH monthly_by_year AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
),
ytd AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        monthly_revenue,
        -- YTD cumulative per year
        ROUND(SUM(monthly_revenue) OVER (
            PARTITION BY order_year
            ORDER BY order_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2)                                   AS ytd_revenue
    FROM monthly_by_year
)
SELECT
    order_month,
    order_month_name,
    -- 2023 column
    MAX(CASE WHEN order_year = 2023 THEN monthly_revenue END) AS monthly_2023,
    MAX(CASE WHEN order_year = 2023 THEN ytd_revenue     END) AS ytd_2023,
    -- 2024 column
    MAX(CASE WHEN order_year = 2024 THEN monthly_revenue END) AS monthly_2024,
    MAX(CASE WHEN order_year = 2024 THEN ytd_revenue     END) AS ytd_2024,
    -- YTD gap: 2024 vs 2023
    ROUND(
        MAX(CASE WHEN order_year = 2024 THEN ytd_revenue END)
        - MAX(CASE WHEN order_year = 2023 THEN ytd_revenue END)
    , 2)                                        AS ytd_gap,
    -- % ahead or behind 2023
    ROUND(
        (
            MAX(CASE WHEN order_year = 2024 THEN ytd_revenue END)
            - MAX(CASE WHEN order_year = 2023 THEN ytd_revenue END)
        )
        / NULLIF(MAX(CASE WHEN order_year = 2023 THEN ytd_revenue END), 0) * 100
    , 2)                                        AS ytd_growth_pct
FROM ytd
GROUP BY order_month, order_month_name
ORDER BY order_month;


-- ============================================================================
-- 2.4  Cumulative Units Sold Over Time
-- ============================================================================
-- Tracks cumulative volume (units, not revenue) across the full date range.
-- Useful for inventory and demand planning context.
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        SUM(quantity)                           AS monthly_units
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_units,
    -- Cumulative units across all time
    SUM(monthly_units) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS cumulative_units,
    -- Cumulative units within the year
    SUM(monthly_units) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS ytd_units,
    -- Monthly units as % of cumulative total
    ROUND(
        monthly_units * 100.0
        / NULLIF(SUM(monthly_units) OVER (), 0)
    , 2)                                        AS pct_of_total_units
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 2.5  3-Month and 6-Month Moving Average Revenue
-- ============================================================================
-- Moving averages smooth volatility and expose the true trend direction.
-- Comparing short MA (3m) to long MA (6m) shows acceleration or deceleration.
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    -- 3-month moving average
    ROUND(AVG(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                       AS ma_3month,
    -- 6-month moving average
    ROUND(AVG(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ), 2)                                       AS ma_6month,
    -- Gap between short and long MA: positive = short trend above long trend (bullish)
    ROUND(
        AVG(monthly_revenue) OVER (
            ORDER BY order_year, order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
        - AVG(monthly_revenue) OVER (
            ORDER BY order_year, order_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        )
    , 2)                                        AS ma_gap_3_vs_6
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 2.6  Cumulative New Customer Acquisition
-- ============================================================================
-- Counts new customers per month (first-ever order) and builds
-- a cumulative total to show how the customer base grew over time.
-- ============================================================================

WITH first_orders AS (
    -- One row per customer: the month of their FIRST purchase
    SELECT
        customer_id,
        YEAR(MIN(order_date))                   AS acq_year,
        MONTH(MIN(order_date))                  AS acq_month,
        DATENAME(MONTH, MIN(order_date))        AS acq_month_name
    FROM gold.fact_sales
    GROUP BY customer_id
),
monthly_acquisition AS (
    SELECT
        acq_year,
        acq_month,
        acq_month_name,
        COUNT(customer_id)                      AS new_customers
    FROM first_orders
    GROUP BY acq_year, acq_month, acq_month_name
)
SELECT
    acq_year,
    acq_month,
    acq_month_name,
    new_customers,
    -- Cumulative customer base growth over the full dataset
    SUM(new_customers) OVER (
        ORDER BY acq_year, acq_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS cumulative_customers,
    -- Cumulative within the same year
    SUM(new_customers) OVER (
        PARTITION BY acq_year
        ORDER BY acq_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS ytd_new_customers
FROM monthly_acquisition
ORDER BY acq_year, acq_month;


-- ============================================================================
-- 2.7  Cumulative Return Value vs Cumulative Revenue (Leakage Trend)
-- ============================================================================
-- Shows the cumulative financial leakage from returns alongside cumulative
-- revenue. The gap between the two lines reveals net revenue trajectory.
-- ============================================================================

WITH monthly_sales AS (
    SELECT
        order_year AS yr, order_month AS mo, order_month_name,
        ROUND(SUM(sales_amount), 2) AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
),
monthly_returns AS (
    SELECT
        return_year AS yr, return_month AS mo,
        ROUND(SUM(return_amount), 2) AS monthly_returns
    FROM gold.fact_returns
    GROUP BY return_year, return_month
),
combined AS (
    SELECT
        ms.yr, ms.mo, ms.order_month_name,
        ms.monthly_revenue,
        COALESCE(mr.monthly_returns, 0) AS monthly_returns
    FROM monthly_sales ms
    LEFT JOIN monthly_returns mr ON ms.yr = mr.yr AND ms.mo = mr.mo
)
SELECT
    yr                                          AS order_year,
    mo                                          AS order_month,
    order_month_name,
    monthly_revenue,
    monthly_returns,
    -- Cumulative gross revenue
    ROUND(SUM(monthly_revenue) OVER (
        ORDER BY yr, mo
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS cumulative_revenue,
    -- Cumulative returns (financial leakage)
    ROUND(SUM(monthly_returns) OVER (
        ORDER BY yr, mo
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS cumulative_returns,
    -- Cumulative NET revenue (revenue minus returns)
    ROUND(
        SUM(monthly_revenue) OVER (
            ORDER BY yr, mo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        - SUM(monthly_returns) OVER (
            ORDER BY yr, mo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    , 2)                                        AS cumulative_net_revenue
FROM combined
ORDER BY yr, mo;


-- ============================================================================
-- 2.8  Cumulative Revenue by Product Category
-- ============================================================================
-- Separate running totals per category (using PARTITION BY category).
-- Shows which categories accumulate revenue faster — steeper slope = faster.
-- ============================================================================

WITH cat_monthly AS (
    SELECT
        dp.category,
        fs.order_year,
        fs.order_month,
        fs.order_month_name,
        ROUND(SUM(fs.sales_amount), 2)          AS monthly_revenue
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.category, fs.order_year, fs.order_month, fs.order_month_name
)
SELECT
    category,
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    -- Cumulative revenue per category from day one
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY category
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                       AS category_cumulative_revenue
FROM cat_monthly
ORDER BY category, order_year, order_month;
