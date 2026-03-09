/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 1: Change Over Time
================================================================================
  Purpose : Track how key business metrics evolve across months, quarters,
            and years. Identify trends, seasonality, and growth trajectories
            using window functions (LAG, LEAD) and date-based aggregations.

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: LAG(), LEAD(), CTEs, date functions, conditional aggregation,
              CASE, NULLIF, ROUND

  Sections:
    1.1  Monthly revenue trend (2023-2024)
    1.2  Month-over-month (MoM) growth rate
    1.3  Year-over-year (YoY) comparison -- same month across years
    1.4  Quarterly revenue trend
    1.5  3-month rolling average revenue
    1.6  New customer acquisition trend (first-order month)
    1.7  Monthly return volume trend
    1.8  Product category revenue trend over time
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 1.1  Monthly Revenue Trend (2023-2024)
-- ============================================================================
-- Baseline: How much revenue was generated each month?
-- Shows the raw trend line before any growth calculations.
-- ============================================================================

SELECT
    order_year,
    order_month,
    order_month_name,
    COUNT(DISTINCT order_number)        AS total_orders,
    SUM(quantity)                       AS units_sold,
    ROUND(SUM(sales_amount), 2)         AS monthly_revenue,
    ROUND(AVG(sales_amount), 2)         AS avg_order_value
FROM gold.fact_sales
GROUP BY
    order_year,
    order_month,
    order_month_name
ORDER BY
    order_year,
    order_month;


-- ============================================================================
-- 1.2  Month-over-Month (MoM) Growth Rate
-- ============================================================================
-- LAG(1) brings the previous month's revenue into the current row.
-- MoM % = (current - previous) / previous * 100
-- Positive = growth, Negative = decline
-- ============================================================================

WITH monthly_revenue AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)                             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
),
mom_calc AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        monthly_revenue,
        -- Bring previous month's revenue into current row
        LAG(monthly_revenue) OVER (
            ORDER BY order_year, order_month
        )                                                       AS prev_month_revenue
    FROM monthly_revenue
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    prev_month_revenue,
    -- Absolute change in revenue
    ROUND(monthly_revenue - prev_month_revenue, 2)              AS mom_change,
    -- Percentage change (NULLIF prevents divide-by-zero)
    ROUND(
        (monthly_revenue - prev_month_revenue)
        / NULLIF(prev_month_revenue, 0) * 100
    , 2)                                                        AS mom_growth_pct,
    -- Flag: growing or declining month
    CASE
        WHEN monthly_revenue > prev_month_revenue THEN 'Growth'
        WHEN monthly_revenue < prev_month_revenue THEN 'Decline'
        ELSE 'Flat'
    END                                                         AS trend_direction
FROM mom_calc
ORDER BY order_year, order_month;


-- ============================================================================
-- 1.3  Year-over-Year (YoY) Comparison -- Same Month Across Years
-- ============================================================================
-- Aligns each month of 2024 against the same month of 2023.
-- Uses conditional aggregation to pivot both years into one row per month.
-- ============================================================================

WITH monthly_by_year AS (
    SELECT
        order_month,
        order_month_name,
        -- Revenue for 2023
        ROUND(SUM(CASE WHEN order_year = 2023 THEN sales_amount ELSE 0 END), 2) AS revenue_2023,
        -- Revenue for 2024
        ROUND(SUM(CASE WHEN order_year = 2024 THEN sales_amount ELSE 0 END), 2) AS revenue_2024
    FROM gold.fact_sales
    GROUP BY order_month, order_month_name
)
SELECT
    order_month,
    order_month_name,
    revenue_2023,
    revenue_2024,
    -- Absolute YoY change
    ROUND(revenue_2024 - revenue_2023, 2)                       AS yoy_change,
    -- YoY growth percentage
    ROUND(
        (revenue_2024 - revenue_2023)
        / NULLIF(revenue_2023, 0) * 100
    , 2)                                                        AS yoy_growth_pct,
    -- Best performing year for that month
    CASE
        WHEN revenue_2024 > revenue_2023 THEN '2024 Better'
        WHEN revenue_2024 < revenue_2023 THEN '2023 Better'
        ELSE 'Equal'
    END                                                         AS winning_year
FROM monthly_by_year
ORDER BY order_month;


-- ============================================================================
-- 1.4  Quarterly Revenue Trend
-- ============================================================================
-- Groups months into Q1-Q4 and shows quarterly performance per year.
-- Useful for identifying seasonal patterns across business quarters.
-- ============================================================================

WITH quarterly AS (
    SELECT
        order_year,
        -- Map month number to quarter label
        CASE
            WHEN order_month BETWEEN 1 AND 3  THEN 'Q1'
            WHEN order_month BETWEEN 4 AND 6  THEN 'Q2'
            WHEN order_month BETWEEN 7 AND 9  THEN 'Q3'
            WHEN order_month BETWEEN 10 AND 12 THEN 'Q4'
        END                                                     AS quarter,
        -- Numeric quarter for ordering
        CEILING(order_month / 3.0)                              AS quarter_num,
        sales_amount,
        order_number,
        quantity
    FROM gold.fact_sales
)
SELECT
    order_year,
    quarter,
    COUNT(DISTINCT order_number)                                AS total_orders,
    SUM(quantity)                                               AS units_sold,
    ROUND(SUM(sales_amount), 2)                                 AS quarterly_revenue,
    ROUND(AVG(sales_amount), 2)                                 AS avg_order_value,
    -- QoQ change using LAG within the same year (PARTITION BY year)
    ROUND(
        SUM(sales_amount)
        - LAG(SUM(sales_amount)) OVER (
            PARTITION BY order_year
            ORDER BY quarter_num
        )
    , 2)                                                        AS qoq_change
FROM quarterly
GROUP BY order_year, quarter, quarter_num
ORDER BY order_year, quarter_num;


-- ============================================================================
-- 1.5  3-Month Rolling Average Revenue
-- ============================================================================
-- Smooths out short-term fluctuations to reveal the underlying trend.
-- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW = current + 2 previous months.
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)                             AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    -- 3-month rolling average (includes current + 2 prior months)
    ROUND(AVG(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                       AS rolling_3m_avg,
    -- 6-month rolling average
    ROUND(AVG(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ), 2)                                                       AS rolling_6m_avg,
    -- Difference from rolling average (above or below trend?)
    ROUND(
        monthly_revenue
        - AVG(monthly_revenue) OVER (
            ORDER BY order_year, order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
    , 2)                                                        AS deviation_from_trend
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 1.6  New Customer Acquisition Trend (First-Order Month)
-- ============================================================================
-- Identifies when each customer placed their very first order.
-- Tracks how many NEW customers were acquired each month.
-- ============================================================================

WITH first_orders AS (
    -- Find the first order date for every customer
    SELECT
        customer_id,
        MIN(order_date)                                         AS first_order_date,
        YEAR(MIN(order_date))                                   AS acquisition_year,
        MONTH(MIN(order_date))                                  AS acquisition_month,
        DATENAME(MONTH, MIN(order_date))                        AS acquisition_month_name
    FROM gold.fact_sales
    GROUP BY customer_id
)
SELECT
    acquisition_year,
    acquisition_month,
    acquisition_month_name,
    COUNT(customer_id)                                          AS new_customers,
    -- Running total of customer base growth
    SUM(COUNT(customer_id)) OVER (
        ORDER BY acquisition_year, acquisition_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                           AS cumulative_customer_base
FROM first_orders
GROUP BY acquisition_year, acquisition_month, acquisition_month_name
ORDER BY acquisition_year, acquisition_month;


-- ============================================================================
-- 1.7  Monthly Return Volume Trend
-- ============================================================================
-- Tracks returns over time and compares the return volume to sales volume.
-- A rising return rate over time is a quality or satisfaction warning signal.
-- ============================================================================

WITH monthly_sales AS (
    SELECT
        order_year   AS yr,
        order_month  AS mo,
        order_month_name,
        COUNT(DISTINCT order_number)    AS orders,
        ROUND(SUM(sales_amount), 2)     AS revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
),
monthly_returns AS (
    SELECT
        return_year  AS yr,
        return_month AS mo,
        COUNT(return_id)                AS return_count,
        ROUND(SUM(return_amount), 2)    AS return_value
    FROM gold.fact_returns
    GROUP BY return_year, return_month
)
SELECT
    ms.yr                               AS order_year,
    ms.mo                               AS order_month,
    ms.order_month_name,
    ms.orders                           AS total_orders,
    ms.revenue                          AS monthly_revenue,
    COALESCE(mr.return_count, 0)        AS monthly_returns,
    COALESCE(mr.return_value, 0)        AS monthly_return_value,
    -- Return rate as % of orders
    ROUND(
        COALESCE(mr.return_count, 0) * 100.0
        / NULLIF(ms.orders, 0)
    , 2)                                AS return_rate_pct,
    -- Return value as % of revenue
    ROUND(
        COALESCE(mr.return_value, 0) * 100.0
        / NULLIF(ms.revenue, 0)
    , 2)                                AS return_value_pct
FROM monthly_sales ms
LEFT JOIN monthly_returns mr
    ON ms.yr = mr.yr AND ms.mo = mr.mo
ORDER BY ms.yr, ms.mo;


-- ============================================================================
-- 1.8  Product Category Revenue Trend Over Time
-- ============================================================================
-- Shows how each category's monthly revenue evolved across 2023-2024.
-- Reveals which categories are growing and which are losing momentum.
-- ============================================================================

WITH category_monthly AS (
    SELECT
        fs.order_year,
        fs.order_month,
        fs.order_month_name,
        dp.category,
        ROUND(SUM(fs.sales_amount), 2)                          AS category_revenue
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY
        fs.order_year,
        fs.order_month,
        fs.order_month_name,
        dp.category
)
SELECT
    order_year,
    order_month,
    order_month_name,
    category,
    category_revenue,
    -- Category's revenue as % of that month's total
    ROUND(
        category_revenue * 100.0
        / SUM(category_revenue) OVER (PARTITION BY order_year, order_month)
    , 2)                                                        AS pct_of_monthly_revenue,
    -- MoM change for this specific category
    ROUND(
        category_revenue
        - LAG(category_revenue) OVER (
            PARTITION BY category
            ORDER BY order_year, order_month
        )
    , 2)                                                        AS category_mom_change
FROM category_monthly
ORDER BY order_year, order_month, category;
