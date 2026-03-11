/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 3: Performance Analysis
================================================================================
  Purpose : Benchmark every entity (products, customers, stores, countries,
            months) against meaningful reference points -- their group average,
            overall average, or prior period. Performance analysis answers:
            "Is this entity above or below expectations?"

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: AVG() OVER (PARTITION BY), DENSE_RANK(), CASE benchmarking,
              CTEs, subqueries, LAG(), NULLIF, conditional classification

  Sections:
    3.1  Products above vs below their category average revenue
    3.2  Stores vs overall average revenue and delivery rate
    3.3  Countries vs continent average revenue
    3.4  Customers above vs below average lifetime spend
    3.5  Monthly revenue vs annual average (seasonal performance)
    3.6  Product return rate vs category average return rate
    3.7  Store delivery performance vs overall benchmark
    3.8  Subcategory performance vs parent category average
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 3.1  Products Above vs Below Their Category Average Revenue
-- ============================================================================
-- For every product, compute total revenue and compare it to the average
-- revenue of all products in the same category.
-- This surfaces over-performers and under-performers within each category.
-- ============================================================================

WITH product_revenue AS (
    SELECT
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        ROUND(SUM(fs.sales_amount), 2)              AS product_revenue,
        SUM(fs.quantity)                            AS units_sold
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory
)
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    product_revenue,
    units_sold,
    -- Average revenue of all products in the same category
    ROUND(AVG(product_revenue) OVER (PARTITION BY category), 2) AS category_avg_revenue,
    -- Gap vs category average
    ROUND(
        product_revenue
        - AVG(product_revenue) OVER (PARTITION BY category)
    , 2)                                            AS gap_vs_category_avg,
    -- % above or below category average
    ROUND(
        (product_revenue - AVG(product_revenue) OVER (PARTITION BY category))
        / NULLIF(AVG(product_revenue) OVER (PARTITION BY category), 0) * 100
    , 2)                                            AS pct_vs_category_avg,
    -- Classification label
    CASE
        WHEN product_revenue > AVG(product_revenue) OVER (PARTITION BY category) * 1.20 THEN 'High Performer'
        WHEN product_revenue < AVG(product_revenue) OVER (PARTITION BY category) * 0.80 THEN 'Under Performer'
        ELSE 'Average'
    END                                             AS performance_tier,
    -- Rank within category
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY product_revenue DESC
    )                                               AS rank_in_category
FROM product_revenue
ORDER BY category, rank_in_category;


-- ============================================================================
-- 3.2  Stores vs Overall Average Revenue and Delivery Rate
-- ============================================================================
-- Each store is measured against two benchmarks simultaneously:
--   1. Revenue benchmark  (overall average store revenue)
--   2. Delivery benchmark (overall average on-time delivery rate)
-- Quadrant classification reveals operational vs commercial performance gaps.
-- ============================================================================

WITH store_metrics AS (
    SELECT
        ds.store_id,
        ds.store_name,
        ds.store_type,
        ds.region,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
        -- On-time delivery rate
        ROUND(
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                        AS on_time_pct
    FROM gold.fact_sales fs
    JOIN gold.dim_store ds ON fs.store_id = ds.store_id
    GROUP BY ds.store_id, ds.store_name, ds.store_type, ds.region
)
SELECT
    store_name,
    store_type,
    region,
    total_orders,
    total_revenue,
    on_time_pct,
    -- Overall benchmarks (no PARTITION BY = entire dataset)
    ROUND(AVG(total_revenue) OVER (), 2)            AS benchmark_avg_revenue,
    ROUND(AVG(on_time_pct)   OVER (), 2)            AS benchmark_avg_ontime_pct,
    -- Revenue gap vs benchmark
    ROUND(total_revenue - AVG(total_revenue) OVER (), 2) AS revenue_vs_benchmark,
    -- Delivery gap vs benchmark
    ROUND(on_time_pct - AVG(on_time_pct) OVER (), 2)    AS delivery_vs_benchmark,
    -- Performance quadrant classification
    CASE
        WHEN total_revenue >= AVG(total_revenue) OVER () AND on_time_pct >= AVG(on_time_pct) OVER ()
            THEN 'Star  (High Revenue + High Delivery)'
        WHEN total_revenue >= AVG(total_revenue) OVER () AND on_time_pct <  AVG(on_time_pct) OVER ()
            THEN 'Commercial Strong / Ops Weak'
        WHEN total_revenue <  AVG(total_revenue) OVER () AND on_time_pct >= AVG(on_time_pct) OVER ()
            THEN 'Ops Strong / Commercial Weak'
        ELSE
            'Needs Improvement (Low Revenue + Low Delivery)'
    END                                             AS performance_quadrant
FROM store_metrics
ORDER BY total_revenue DESC;


-- ============================================================================
-- 3.3  Countries vs Continent Average Revenue
-- ============================================================================
-- Each country is benchmarked against the average country revenue within
-- its continent. A country underperforming its continental average is a
-- candidate for targeted sales or marketing investment.
-- ============================================================================

WITH country_revenue AS (
    SELECT
        dc.continent,
        dc.country,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
        COUNT(DISTINCT dc.customer_id)              AS unique_customers
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dc.continent, dc.country
)
SELECT
    continent,
    country,
    total_orders,
    unique_customers,
    total_revenue,
    -- Average country revenue within the same continent
    ROUND(AVG(total_revenue) OVER (PARTITION BY continent), 2)  AS continent_avg_revenue,
    -- Gap vs continent peers
    ROUND(
        total_revenue - AVG(total_revenue) OVER (PARTITION BY continent)
    , 2)                                            AS gap_vs_continent_avg,
    -- % vs continent average
    ROUND(
        (total_revenue - AVG(total_revenue) OVER (PARTITION BY continent))
        / NULLIF(AVG(total_revenue) OVER (PARTITION BY continent), 0) * 100
    , 2)                                            AS pct_vs_continent_avg,
    -- Country rank within its continent
    DENSE_RANK() OVER (
        PARTITION BY continent
        ORDER BY total_revenue DESC
    )                                               AS rank_in_continent,
    CASE
        WHEN total_revenue > AVG(total_revenue) OVER (PARTITION BY continent)
            THEN 'Above Continental Average'
        ELSE 'Below Continental Average'
    END                                             AS performance_vs_continent
FROM country_revenue
ORDER BY continent, rank_in_continent;


-- ============================================================================
-- 3.4  Customers Above vs Below Average Lifetime Spend
-- ============================================================================
-- Every customer is compared to the overall average customer lifetime value.
-- Segments customers into value tiers for CRM and retention strategy.
-- ============================================================================

WITH customer_clv AS (
    SELECT
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.age_group,
        dc.gender,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)              AS lifetime_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
        -- Date of most recent purchase
        MAX(fs.order_date)                          AS last_order_date,
        -- Days since last purchase (recency)
        DATEDIFF(DAY, MAX(fs.order_date), '2025-01-01') AS recency_days
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY
        fs.customer_id,
        dc.full_name,
        dc.country,
        dc.age_group,
        dc.gender
)
SELECT
    customer_id,
    full_name,
    country,
    age_group,
    gender,
    total_orders,
    lifetime_revenue,
    avg_order_value,
    recency_days,
    -- Overall average CLV across all customers
    ROUND(AVG(lifetime_revenue) OVER (), 2)         AS overall_avg_clv,
    -- Gap vs overall average
    ROUND(lifetime_revenue - AVG(lifetime_revenue) OVER (), 2) AS clv_gap_vs_avg,
    -- % vs average
    ROUND(
        (lifetime_revenue - AVG(lifetime_revenue) OVER ())
        / NULLIF(AVG(lifetime_revenue) OVER (), 0) * 100
    , 2)                                            AS pct_vs_avg_clv,
    -- CLV performance label
    CASE
        WHEN lifetime_revenue >= AVG(lifetime_revenue) OVER () * 2   THEN 'VIP (2x+ avg)'
        WHEN lifetime_revenue >= AVG(lifetime_revenue) OVER () * 1.5 THEN 'High Value'
        WHEN lifetime_revenue >= AVG(lifetime_revenue) OVER ()       THEN 'Above Average'
        WHEN lifetime_revenue >= AVG(lifetime_revenue) OVER () * 0.5 THEN 'Below Average'
        ELSE 'Low Value'
    END                                             AS clv_tier
FROM customer_clv
ORDER BY lifetime_revenue DESC;


-- ============================================================================
-- 3.5  Monthly Revenue vs Annual Average (Seasonal Performance)
-- ============================================================================
-- Compares each month to the annual average to identify peak and off-peak
-- seasons. A month above the annual average is "in season"; below is "off peak".
-- ============================================================================

WITH monthly AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        ROUND(SUM(sales_amount), 2)                 AS monthly_revenue
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_revenue,
    -- Annual average for the same year
    ROUND(AVG(monthly_revenue) OVER (PARTITION BY order_year), 2) AS annual_monthly_avg,
    -- Gap vs annual average
    ROUND(
        monthly_revenue - AVG(monthly_revenue) OVER (PARTITION BY order_year)
    , 2)                                            AS gap_vs_annual_avg,
    -- % above or below annual average
    ROUND(
        (monthly_revenue - AVG(monthly_revenue) OVER (PARTITION BY order_year))
        / NULLIF(AVG(monthly_revenue) OVER (PARTITION BY order_year), 0) * 100
    , 2)                                            AS pct_vs_annual_avg,
    -- Seasonal classification
    CASE
        WHEN monthly_revenue > AVG(monthly_revenue) OVER (PARTITION BY order_year) * 1.15 THEN 'Peak Season'
        WHEN monthly_revenue < AVG(monthly_revenue) OVER (PARTITION BY order_year) * 0.85 THEN 'Off-Peak Season'
        ELSE 'Normal'
    END                                             AS seasonality_label,
    -- Rank within the year (1 = best revenue month)
    DENSE_RANK() OVER (
        PARTITION BY order_year
        ORDER BY monthly_revenue DESC
    )                                               AS monthly_rank_in_year
FROM monthly
ORDER BY order_year, order_month;


-- ============================================================================
-- 3.6  Product Return Rate vs Category Average Return Rate
-- ============================================================================
-- Computes each product's individual return rate and compares it to the
-- average return rate of all products in its category.
-- High return rate vs category average = product-level quality issue.
-- ============================================================================

WITH product_sales AS (
    SELECT
        fs.product_id,
        dp.product_name,
        dp.category,
        COUNT(DISTINCT fs.order_number)             AS total_orders
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY fs.product_id, dp.product_name, dp.category
),
product_returns AS (
    SELECT
        fs.product_id,
        COUNT(DISTINCT fr.return_id)                AS total_returns
    FROM gold.fact_returns fr
    JOIN gold.fact_sales fs ON fr.order_number = fs.order_number
    GROUP BY fs.product_id
),
product_return_rates AS (
    SELECT
        ps.product_id,
        ps.product_name,
        ps.category,
        ps.total_orders,
        COALESCE(pr.total_returns, 0)               AS total_returns,
        ROUND(
            COALESCE(pr.total_returns, 0) * 100.0
            / NULLIF(ps.total_orders, 0)
        , 2)                                        AS return_rate_pct
    FROM product_sales ps
    LEFT JOIN product_returns pr ON ps.product_id = pr.product_id
)
SELECT
    product_name,
    category,
    total_orders,
    total_returns,
    return_rate_pct,
    -- Average return rate for products in the same category
    ROUND(AVG(return_rate_pct) OVER (PARTITION BY category), 2) AS category_avg_return_rate,
    -- Gap vs category average
    ROUND(
        return_rate_pct - AVG(return_rate_pct) OVER (PARTITION BY category)
    , 2)                                            AS gap_vs_category_avg,
    CASE
        WHEN return_rate_pct > AVG(return_rate_pct) OVER (PARTITION BY category) * 1.5
            THEN 'High Risk - Investigate'
        WHEN return_rate_pct > AVG(return_rate_pct) OVER (PARTITION BY category)
            THEN 'Above Category Average'
        ELSE 'Within Normal Range'
    END                                             AS return_risk_flag
FROM product_return_rates
ORDER BY category, return_rate_pct DESC;


-- ============================================================================
-- 3.7  Store Delivery Performance vs Overall Benchmark
-- ============================================================================
-- Compares each store's on-time delivery percentage against both the
-- overall average AND its own store type average (peer benchmark).
-- ============================================================================

WITH store_delivery AS (
    SELECT
        ds.store_id,
        ds.store_name,
        ds.store_type,
        ds.region,
        COUNT(*)                                    AS total_shipments,
        ROUND(
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                        AS on_time_pct,
        ROUND(AVG(CAST(fs.days_to_ship AS FLOAT)), 1) AS avg_days_to_ship
    FROM gold.fact_sales fs
    JOIN gold.dim_store ds ON fs.store_id = ds.store_id
    WHERE fs.days_to_ship IS NOT NULL
    GROUP BY ds.store_id, ds.store_name, ds.store_type, ds.region
)
SELECT
    store_name,
    store_type,
    region,
    total_shipments,
    on_time_pct,
    avg_days_to_ship,
    -- Overall network benchmark
    ROUND(AVG(on_time_pct) OVER (), 2)              AS network_avg_ontime,
    -- Peer benchmark: average among stores of the same type
    ROUND(AVG(on_time_pct) OVER (PARTITION BY store_type), 2) AS store_type_avg_ontime,
    -- Gap vs network
    ROUND(on_time_pct - AVG(on_time_pct) OVER (), 2) AS gap_vs_network,
    -- Gap vs store type peer
    ROUND(on_time_pct - AVG(on_time_pct) OVER (PARTITION BY store_type), 2) AS gap_vs_peer,
    CASE
        WHEN on_time_pct >= AVG(on_time_pct) OVER () THEN 'Above Network Avg'
        ELSE 'Below Network Avg'
    END                                             AS vs_network_flag
FROM store_delivery
ORDER BY on_time_pct DESC;


-- ============================================================================
-- 3.8  Subcategory Performance vs Parent Category Average
-- ============================================================================
-- Reveals which subcategories are driving a category and which are dragging.
-- Uses AVG OVER (PARTITION BY category) as the intra-category benchmark.
-- ============================================================================

WITH subcat_revenue AS (
    SELECT
        dp.category,
        dp.subcategory,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        SUM(fs.quantity)                            AS units_sold,
        ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.category, dp.subcategory
)
SELECT
    category,
    subcategory,
    total_orders,
    units_sold,
    total_revenue,
    avg_order_value,
    -- Average subcategory revenue within the parent category
    ROUND(AVG(total_revenue) OVER (PARTITION BY category), 2)   AS category_avg_subcat_revenue,
    ROUND(
        total_revenue - AVG(total_revenue) OVER (PARTITION BY category)
    , 2)                                            AS gap_vs_category_avg,
    ROUND(
        (total_revenue - AVG(total_revenue) OVER (PARTITION BY category))
        / NULLIF(AVG(total_revenue) OVER (PARTITION BY category), 0) * 100
    , 2)                                            AS pct_vs_category_avg,
    -- Revenue share within parent category
    ROUND(
        total_revenue * 100.0
        / NULLIF(SUM(total_revenue) OVER (PARTITION BY category), 0)
    , 2)                                            AS pct_of_category_revenue,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY total_revenue DESC
    )                                               AS rank_in_category
FROM subcat_revenue
ORDER BY category, rank_in_category;
