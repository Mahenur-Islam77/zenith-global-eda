/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 5: Data Segmentation
================================================================================
  Purpose : Divide customers, products, stores, and markets into meaningful
            segments based on behavioral and performance characteristics.
            Segmentation enables targeted strategy: different actions for
            different groups rather than one-size-fits-all approaches.

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: CTEs, CASE-based classification, NTILE(), DENSE_RANK(),
              DATEDIFF(), multi-attribute scoring, correlated subqueries

  Sections:
    5.1  RFM Customer Segmentation (Recency, Frequency, Monetary)
    5.2  Customer Lifetime Value (CLV) tiers
    5.3  Product performance tiers (Hero / Contributor / Laggard)
    5.4  Store performance tiers (Top / Mid / Bottom)
    5.5  Order size segmentation (Small / Medium / Large / Whale)
    5.6  Geographic revenue tiers (High / Mid / Low value markets)
    5.7  Customer activity segmentation (Active / At-Risk / Lost)
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 5.1  RFM Customer Segmentation
-- ============================================================================
-- RFM is the gold standard for customer segmentation:
--   R = Recency  -- how recently did the customer buy? (lower days = better)
--   F = Frequency -- how often do they buy? (more orders = better)
--   M = Monetary -- how much do they spend? (higher = better)
--
-- Each dimension is scored 1-5 using NTILE(5).
-- A combined RFM score then maps customers to named segments.
-- ============================================================================

WITH customer_rfm_raw AS (
    -- Step 1: Compute raw R, F, M values per customer
    SELECT
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.age_group,
        dc.gender,
        -- Recency: days since last purchase (lower = more recent = better)
        DATEDIFF(DAY, MAX(fs.order_date), '2025-01-01')         AS recency_days,
        -- Frequency: number of distinct orders
        COUNT(DISTINCT fs.order_number)                         AS frequency,
        -- Monetary: total lifetime spend
        ROUND(SUM(fs.sales_amount), 2)                          AS monetary
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.age_group,
        dc.gender
),
rfm_scores AS (
    -- Step 2: Score each dimension using NTILE(5)
    -- Recency: NTILE reversed (low recency days = high score)
    SELECT
        customer_id,
        full_name,
        country,
        age_group,
        gender,
        recency_days,
        frequency,
        monetary,
        -- R score: 5 = most recent, 1 = oldest (reverse sort)
        NTILE(5) OVER (ORDER BY recency_days ASC)               AS r_score,
        -- F score: 5 = most frequent
        NTILE(5) OVER (ORDER BY frequency DESC)                 AS f_score,
        -- M score: 5 = highest spender
        NTILE(5) OVER (ORDER BY monetary DESC)                  AS m_score
    FROM customer_rfm_raw
),
rfm_segments AS (
    -- Step 3: Combine scores into a composite and assign segment label
    SELECT
        customer_id,
        full_name,
        country,
        age_group,
        gender,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        -- Combined RFM score (simple average of all three)
        ROUND((r_score + f_score + m_score) / 3.0, 2)          AS rfm_score,
        -- Named segment based on score combination
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 4 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                  THEN 'Recent Customers'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Potential Loyalists'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'At Risk - High Value'
            WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
            WHEN r_score <= 1 AND f_score <= 2 AND m_score <= 2 THEN 'Lost Customers'
            WHEN m_score >= 4 AND f_score <= 2                  THEN 'Big Spenders (Low Freq)'
            ELSE 'Casual Buyers'
        END                                                     AS rfm_segment
    FROM rfm_scores
)
SELECT
    customer_id,
    full_name,
    country,
    age_group,
    gender,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_score,
    rfm_segment
FROM rfm_segments
ORDER BY rfm_score DESC, monetary DESC;

-- RFM Segment Summary
WITH customer_rfm_raw AS (
    SELECT
        fs.customer_id,
        DATEDIFF(DAY, MAX(fs.order_date), '2025-01-01')         AS recency_days,
        COUNT(DISTINCT fs.order_number)                         AS frequency,
        ROUND(SUM(fs.sales_amount), 2)                          AS monetary
    FROM gold.fact_sales fs
    GROUP BY fs.customer_id
),
rfm_scores AS (
    SELECT customer_id, recency_days, frequency, monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC)               AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)                 AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)                  AS m_score
    FROM customer_rfm_raw
),
rfm_segments AS (
    SELECT customer_id, monetary,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 4 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                  THEN 'Recent Customers'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Potential Loyalists'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'At Risk - High Value'
            WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
            WHEN r_score <= 1 AND f_score <= 2 AND m_score <= 2 THEN 'Lost Customers'
            WHEN m_score >= 4 AND f_score <= 2                  THEN 'Big Spenders (Low Freq)'
            ELSE 'Casual Buyers'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT
    rfm_segment,
    COUNT(customer_id)                      AS customer_count,
    ROUND(SUM(monetary), 2)                 AS segment_revenue,
    ROUND(AVG(monetary), 2)                 AS avg_customer_revenue,
    ROUND(COUNT(customer_id) * 100.0 / SUM(COUNT(customer_id)) OVER (), 2) AS pct_of_customers,
    ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2)           AS pct_of_revenue
FROM rfm_segments
GROUP BY rfm_segment
ORDER BY segment_revenue DESC;


-- ============================================================================
-- 5.2  Customer Lifetime Value (CLV) Tiers
-- ============================================================================
-- Segments customers into 4 value tiers using NTILE(4):
--   Tier 1 (top 25%) = VIP
--   Tier 2 = High Value
--   Tier 3 = Mid Value
--   Tier 4 (bottom 25%) = Low Value
-- ============================================================================

WITH clv AS (
    SELECT
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.continent,
        dc.age_group,
        dc.gender,
        dc.marital_status,
        COUNT(DISTINCT fs.order_number)         AS total_orders,
        SUM(fs.quantity)                        AS total_units,
        ROUND(SUM(fs.sales_amount), 2)          AS lifetime_revenue,
        ROUND(AVG(fs.sales_amount), 2)          AS avg_order_value,
        MIN(fs.order_date)                      AS first_order_date,
        MAX(fs.order_date)                      AS last_order_date,
        DATEDIFF(DAY, MIN(fs.order_date), MAX(fs.order_date)) AS customer_lifespan_days
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY
        fs.customer_id, dc.full_name, dc.country, dc.continent,
        dc.age_group, dc.gender, dc.marital_status
)
SELECT
    customer_id,
    full_name,
    country,
    continent,
    age_group,
    gender,
    total_orders,
    total_units,
    lifetime_revenue,
    avg_order_value,
    customer_lifespan_days,
    -- CLV tier using NTILE (1 = VIP, 4 = Low)
    NTILE(4) OVER (ORDER BY lifetime_revenue DESC)              AS clv_ntile,
    CASE NTILE(4) OVER (ORDER BY lifetime_revenue DESC)
        WHEN 1 THEN 'VIP (Top 25%)'
        WHEN 2 THEN 'High Value'
        WHEN 3 THEN 'Mid Value'
        WHEN 4 THEN 'Low Value (Bottom 25%)'
    END                                                         AS clv_tier,
    -- Revenue percentile position
    ROUND(PERCENT_RANK() OVER (ORDER BY lifetime_revenue) * 100, 1) AS revenue_percentile
FROM clv
ORDER BY lifetime_revenue DESC;


-- ============================================================================
-- 5.3  Product Performance Tiers (Hero / Contributor / Laggard)
-- ============================================================================
-- Segments all products into 3 tiers based on combined revenue and volume.
-- Hero products = top sellers; Laggards = candidates for review.
-- ============================================================================

WITH product_metrics AS (
    SELECT
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        dp.cost,
        ROUND(SUM(fs.sales_amount), 2)                          AS total_revenue,
        SUM(fs.quantity)                                        AS total_units,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        -- Revenue rank globally
        DENSE_RANK() OVER (ORDER BY SUM(fs.sales_amount) DESC)  AS revenue_rank
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY
        dp.product_id, dp.product_name, dp.category,
        dp.subcategory, dp.cost
),
product_tiers AS (
    SELECT
        *,
        -- Revenue percentile
        ROUND(PERCENT_RANK() OVER (ORDER BY total_revenue) * 100, 1) AS revenue_percentile,
        -- Volume percentile
        ROUND(PERCENT_RANK() OVER (ORDER BY total_units) * 100, 1)   AS volume_percentile
    FROM product_metrics
)
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    cost,
    total_revenue,
    total_units,
    total_orders,
    revenue_rank,
    revenue_percentile,
    volume_percentile,
    -- Performance tier based on combined revenue and volume percentile
    CASE
        WHEN revenue_percentile >= 75 AND volume_percentile >= 75 THEN 'Hero'
        WHEN revenue_percentile >= 50 OR  volume_percentile >= 50 THEN 'Contributor'
        WHEN revenue_percentile >= 25                             THEN 'Average'
        ELSE                                                           'Laggard'
    END                                                         AS performance_tier
FROM product_tiers
ORDER BY revenue_rank;


-- ============================================================================
-- 5.4  Store Performance Tiers (Top / Mid / Bottom)
-- ============================================================================
-- Classifies all 100 stores into 3 performance tiers using NTILE(3).
-- Based on combined revenue and delivery rate scoring.
-- ============================================================================

WITH store_metrics AS (
    SELECT
        ds.store_id,
        ds.store_name,
        ds.store_type,
        ds.region,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)                          AS total_revenue,
        ROUND(
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                                    AS on_time_pct
    FROM gold.fact_sales fs
    JOIN gold.dim_store ds ON fs.store_id = ds.store_id
    GROUP BY ds.store_id, ds.store_name, ds.store_type, ds.region
)
SELECT
    store_id,
    store_name,
    store_type,
    region,
    total_orders,
    total_revenue,
    on_time_pct,
    -- Revenue tier: 1 = Top, 3 = Bottom
    NTILE(3) OVER (ORDER BY total_revenue DESC)                 AS revenue_tier_num,
    CASE NTILE(3) OVER (ORDER BY total_revenue DESC)
        WHEN 1 THEN 'Top Tier'
        WHEN 2 THEN 'Mid Tier'
        WHEN 3 THEN 'Bottom Tier'
    END                                                         AS revenue_tier,
    -- Delivery tier: 1 = Best delivery, 3 = Worst
    NTILE(3) OVER (ORDER BY on_time_pct DESC)                   AS delivery_tier_num,
    CASE NTILE(3) OVER (ORDER BY on_time_pct DESC)
        WHEN 1 THEN 'High Delivery'
        WHEN 2 THEN 'Average Delivery'
        WHEN 3 THEN 'Low Delivery'
    END                                                         AS delivery_tier
FROM store_metrics
ORDER BY total_revenue DESC;


-- ============================================================================
-- 5.5  Order Size Segmentation (Small / Medium / Large / Whale)
-- ============================================================================
-- Segments individual orders by sales_amount size.
-- Useful for understanding the distribution of transaction values.
-- ============================================================================

WITH order_segments AS (
    SELECT
        fs.order_number,
        fs.customer_id,
        fs.order_date,
        fs.order_year,
        dp.category,
        ds.store_type,
        fs.quantity,
        ROUND(fs.sales_amount, 2)                               AS order_value,
        -- Segment by order value
        CASE
            WHEN fs.sales_amount <   100 THEN 'Micro (< $100)'
            WHEN fs.sales_amount <   500 THEN 'Small ($100-$499)'
            WHEN fs.sales_amount <  2000 THEN 'Medium ($500-$1,999)'
            WHEN fs.sales_amount < 10000 THEN 'Large ($2,000-$9,999)'
            ELSE                              'Whale ($10,000+)'
        END                                                     AS order_segment,
        CASE
            WHEN fs.sales_amount <   100 THEN 1
            WHEN fs.sales_amount <   500 THEN 2
            WHEN fs.sales_amount <  2000 THEN 3
            WHEN fs.sales_amount < 10000 THEN 4
            ELSE                              5
        END                                                     AS segment_sort
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    JOIN gold.dim_store   ds ON fs.store_id   = ds.store_id
)
SELECT
    order_segment,
    COUNT(order_number)                                         AS order_count,
    SUM(quantity)                                               AS units_sold,
    ROUND(SUM(order_value), 2)                                  AS total_revenue,
    ROUND(AVG(order_value), 2)                                  AS avg_order_value,
    ROUND(MIN(order_value), 2)                                  AS min_value,
    ROUND(MAX(order_value), 2)                                  AS max_value,
    -- Volume % of all orders
    ROUND(COUNT(order_number) * 100.0 / SUM(COUNT(order_number)) OVER (), 2) AS pct_of_orders,
    -- Revenue % of total
    ROUND(SUM(order_value) * 100.0 / SUM(SUM(order_value)) OVER (), 2)       AS pct_of_revenue
FROM order_segments
GROUP BY order_segment, segment_sort
ORDER BY segment_sort;


-- ============================================================================
-- 5.6  Geographic Revenue Tiers (High / Mid / Low Value Markets)
-- ============================================================================
-- Classifies all countries into 3 revenue tiers and calculates
-- each tier's collective contribution to global revenue.
-- ============================================================================

WITH country_revenue AS (
    SELECT
        dc.continent,
        dc.country,
        ROUND(SUM(fs.sales_amount), 2)                          AS country_revenue,
        COUNT(DISTINCT dc.customer_id)                          AS unique_customers,
        COUNT(DISTINCT fs.order_number)                         AS total_orders
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dc.continent, dc.country
)
SELECT
    continent,
    country,
    unique_customers,
    total_orders,
    country_revenue,
    -- Revenue tier using NTILE(3): 1=High, 3=Low
    NTILE(3) OVER (ORDER BY country_revenue DESC)               AS revenue_tier_num,
    CASE NTILE(3) OVER (ORDER BY country_revenue DESC)
        WHEN 1 THEN 'High Value Market'
        WHEN 2 THEN 'Mid Value Market'
        WHEN 3 THEN 'Low Value Market'
    END                                                         AS market_tier,
    -- Country's share of global revenue
    ROUND(
        country_revenue * 100.0
        / NULLIF(SUM(country_revenue) OVER (), 0)
    , 2)                                                        AS pct_of_global_revenue,
    -- Revenue per customer (market efficiency)
    ROUND(country_revenue / NULLIF(unique_customers, 0), 2)     AS revenue_per_customer
FROM country_revenue
ORDER BY country_revenue DESC;


-- ============================================================================
-- 5.7  Customer Activity Segmentation (Active / At-Risk / Lost)
-- ============================================================================
-- Classifies customers by recency of their last purchase relative
-- to the reference date (2025-01-01).
-- ============================================================================

WITH customer_activity AS (
    SELECT
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.age_group,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)                          AS lifetime_revenue,
        MAX(fs.order_date)                                      AS last_order_date,
        DATEDIFF(DAY, MAX(fs.order_date), '2025-01-01')         AS days_since_last_order
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY fs.customer_id, dc.full_name, dc.country, dc.age_group
)
SELECT
    customer_id,
    full_name,
    country,
    age_group,
    total_orders,
    lifetime_revenue,
    last_order_date,
    days_since_last_order,
    -- Activity segment based on recency
    CASE
        WHEN days_since_last_order <= 90  THEN 'Active (< 90 days)'
        WHEN days_since_last_order <= 180 THEN 'Cooling (90-180 days)'
        WHEN days_since_last_order <= 365 THEN 'At-Risk (180-365 days)'
        ELSE                                   'Lost (> 365 days)'
    END                                                         AS activity_segment,
    -- Combined risk level (high-value at-risk = top priority)
    CASE
        WHEN days_since_last_order > 180 AND lifetime_revenue >= 5000 THEN 'HIGH PRIORITY WINBACK'
        WHEN days_since_last_order > 180 AND lifetime_revenue >= 2000 THEN 'Medium Priority Winback'
        WHEN days_since_last_order <= 90                              THEN 'Nurture'
        ELSE 'Monitor'
    END                                                         AS crm_action
FROM customer_activity
ORDER BY days_since_last_order DESC, lifetime_revenue DESC;
