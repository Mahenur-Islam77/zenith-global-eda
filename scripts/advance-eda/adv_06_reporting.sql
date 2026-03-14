/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 6: Reporting
================================================================================
  Purpose : Produce polished, self-contained analytical reports using
            advanced SQL. Each report combines multiple CTEs, window
            functions, and aggregations into a single query that could
            feed a dashboard, slide deck, or stakeholder briefing.

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: Multi-CTE pipelines, conditional aggregation, window functions,
              ROLLUP, subqueries, LAG, DENSE_RANK, PERCENT_RANK, NULLIF

  Sections:
    6.1  Executive Summary Report  -- all core KPIs in one query
    6.2  Monthly Performance Report -- full trend with MoM and YoY
    6.3  Product Performance Report -- hero products + laggards
    6.4  Customer Insights Report   -- demographics + CLV + RFM
    6.5  Store Operations Report    -- revenue + delivery + ranking
    6.6  Returns Deep-Dive Report   -- reason + category + trend
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 6.1  Executive Summary Report
-- ============================================================================
-- A single query returning all top-level KPIs:
--   Revenue, Orders, AOV, Units Sold, Unique Customers, Return Rate,
--   On-Time Delivery %, Top Category, Top Country — side by side.
-- Designed to answer: "How is the business performing overall?"
-- ============================================================================

WITH
-- Total sales KPIs
sales_kpis AS (
    SELECT
        COUNT(DISTINCT order_number)            AS total_orders,
        COUNT(DISTINCT customer_id)             AS unique_customers,
        SUM(quantity)                           AS total_units_sold,
        ROUND(SUM(sales_amount), 2)             AS total_revenue,
        ROUND(AVG(sales_amount), 2)             AS avg_order_value,
        ROUND(MAX(sales_amount), 2)             AS largest_order,
        COUNT(DISTINCT product_id)              AS active_products,
        COUNT(DISTINCT store_id)                AS active_stores
    FROM gold.fact_sales
),
-- Delivery KPIs
delivery_kpis AS (
    SELECT
        ROUND(
            SUM(CASE WHEN delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                    AS on_time_delivery_pct,
        ROUND(AVG(CAST(days_to_ship AS FLOAT)), 1) AS avg_days_to_ship
    FROM gold.fact_sales
    WHERE days_to_ship IS NOT NULL
),
-- Returns KPIs
returns_kpis AS (
    SELECT
        COUNT(return_id)                        AS total_returns,
        ROUND(SUM(return_amount), 2)            AS total_return_value,
        ROUND(AVG(return_amount), 2)            AS avg_return_value
    FROM gold.fact_returns
),
-- Return rate
return_rate AS (
    SELECT
        ROUND(
            (SELECT COUNT(*) FROM gold.fact_returns) * 100.0
            / NULLIF((SELECT COUNT(DISTINCT order_number) FROM gold.fact_sales), 0)
        , 2)                                    AS return_rate_pct
),
-- Top performing category
top_category AS (
    SELECT TOP 1
        dp.category                             AS top_category,
        ROUND(SUM(fs.sales_amount), 2)          AS top_category_revenue
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.category
    ORDER BY SUM(fs.sales_amount) DESC
),
-- Top performing country
top_country AS (
    SELECT TOP 1
        dc.country                              AS top_country,
        ROUND(SUM(fs.sales_amount), 2)          AS top_country_revenue
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY dc.country
    ORDER BY SUM(fs.sales_amount) DESC
),
-- YoY revenue growth
yoy AS (
    SELECT
        ROUND(SUM(CASE WHEN order_year = 2023 THEN sales_amount END), 2) AS revenue_2023,
        ROUND(SUM(CASE WHEN order_year = 2024 THEN sales_amount END), 2) AS revenue_2024
    FROM gold.fact_sales
)
SELECT
    -- Revenue
    s.total_revenue,
    s.total_orders,
    s.avg_order_value,
    s.largest_order,
    s.total_units_sold,
    s.unique_customers,
    s.active_products,
    s.active_stores,
    -- Delivery
    d.on_time_delivery_pct,
    d.avg_days_to_ship,
    -- Returns
    r.total_returns,
    r.total_return_value,
    r.avg_return_value,
    rr.return_rate_pct,
    -- Top performers
    tc.top_category,
    tc.top_category_revenue,
    tco.top_country,
    tco.top_country_revenue,
    -- YoY growth
    y.revenue_2023,
    y.revenue_2024,
    ROUND((y.revenue_2024 - y.revenue_2023) / NULLIF(y.revenue_2023, 0) * 100, 2) AS yoy_growth_pct
FROM sales_kpis      s
CROSS JOIN delivery_kpis  d
CROSS JOIN returns_kpis   r
CROSS JOIN return_rate    rr
CROSS JOIN top_category   tc
CROSS JOIN top_country    tco
CROSS JOIN yoy            y;


-- ============================================================================
-- 6.2  Monthly Performance Report
-- ============================================================================
-- Complete monthly trend report combining:
--   Revenue, Orders, AOV, MoM growth, YoY comparison, Cumulative total,
--   Monthly rank within year, Return volume
-- One row per month — ready to power a time-series dashboard.
-- ============================================================================

WITH monthly_sales AS (
    SELECT
        order_year,
        order_month,
        order_month_name,
        COUNT(DISTINCT order_number)                    AS monthly_orders,
        SUM(quantity)                                   AS monthly_units,
        ROUND(SUM(sales_amount), 2)                     AS monthly_revenue,
        ROUND(AVG(sales_amount), 2)                     AS monthly_aov
    FROM gold.fact_sales
    GROUP BY order_year, order_month, order_month_name
),
monthly_returns AS (
    SELECT
        return_year, return_month,
        COUNT(return_id)                                AS monthly_returns,
        ROUND(SUM(return_amount), 2)                    AS monthly_return_value
    FROM gold.fact_returns
    GROUP BY return_year, return_month
),
combined AS (
    SELECT
        ms.*,
        COALESCE(mr.monthly_returns, 0)                 AS monthly_returns,
        COALESCE(mr.monthly_return_value, 0)            AS monthly_return_value
    FROM monthly_sales ms
    LEFT JOIN monthly_returns mr
        ON ms.order_year = mr.return_year
        AND ms.order_month = mr.return_month
)
SELECT
    order_year,
    order_month,
    order_month_name,
    monthly_orders,
    monthly_units,
    monthly_revenue,
    monthly_aov,
    monthly_returns,
    monthly_return_value,
    -- Return rate
    ROUND(monthly_returns * 100.0 / NULLIF(monthly_orders, 0), 2) AS return_rate_pct,
    -- MoM revenue change
    ROUND(
        monthly_revenue
        - LAG(monthly_revenue) OVER (ORDER BY order_year, order_month)
    , 2)                                                AS mom_revenue_change,
    -- MoM growth %
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_year, order_month))
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY order_year, order_month), 0) * 100
    , 2)                                                AS mom_growth_pct,
    -- YoY: same month prior year
    ROUND(
        monthly_revenue
        - LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year)
    , 2)                                                AS yoy_revenue_change,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year))
        / NULLIF(LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year), 0) * 100
    , 2)                                                AS yoy_growth_pct,
    -- Cumulative revenue
    ROUND(SUM(monthly_revenue) OVER (
        ORDER BY order_year, order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                               AS cumulative_revenue,
    -- YTD cumulative
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                               AS ytd_revenue,
    -- Rank within year (1 = best revenue month)
    DENSE_RANK() OVER (
        PARTITION BY order_year
        ORDER BY monthly_revenue DESC
    )                                                   AS rank_in_year,
    -- Season label
    CASE
        WHEN monthly_revenue > AVG(monthly_revenue) OVER (PARTITION BY order_year) * 1.15 THEN 'Peak'
        WHEN monthly_revenue < AVG(monthly_revenue) OVER (PARTITION BY order_year) * 0.85 THEN 'Off-Peak'
        ELSE 'Normal'
    END                                                 AS season_label
FROM combined
ORDER BY order_year, order_month;


-- ============================================================================
-- 6.3  Product Performance Report
-- ============================================================================
-- Comprehensive product report combining:
--   Revenue, units, rank, category avg comparison, return rate, cost margin proxy
-- ============================================================================

WITH product_sales AS (
    SELECT
        fs.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        dp.cost,
        dp.product_line,
        dp.maintenance_required,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        SUM(fs.quantity)                            AS total_units,
        ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
        ROUND(AVG(fs.price), 2)                     AS avg_selling_price
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY
        fs.product_id, dp.product_name, dp.category,
        dp.subcategory, dp.cost, dp.product_line, dp.maintenance_required
),
product_returns AS (
    SELECT
        fs.product_id,
        COUNT(fr.return_id)                         AS total_returns,
        ROUND(SUM(fr.return_amount), 2)             AS total_return_value
    FROM gold.fact_returns fr
    JOIN gold.fact_sales fs ON fr.order_number = fs.order_number
    GROUP BY fs.product_id
)
SELECT
    ps.product_id,
    ps.product_name,
    ps.category,
    ps.subcategory,
    ps.cost,
    ps.product_line,
    ps.maintenance_required,
    ps.total_orders,
    ps.total_units,
    ps.total_revenue,
    ps.avg_selling_price,
    -- Margin proxy (selling price vs cost)
    ROUND(ps.avg_selling_price - ps.cost, 2)                AS margin_proxy,
    ROUND((ps.avg_selling_price - ps.cost)
        / NULLIF(ps.avg_selling_price, 0) * 100, 2)         AS margin_pct,
    -- Returns
    COALESCE(pr.total_returns, 0)                           AS total_returns,
    COALESCE(pr.total_return_value, 0)                      AS total_return_value,
    ROUND(COALESCE(pr.total_returns, 0) * 100.0
        / NULLIF(ps.total_orders, 0), 2)                    AS return_rate_pct,
    -- Revenue vs category average
    ROUND(AVG(ps.total_revenue) OVER (PARTITION BY ps.category), 2) AS category_avg_revenue,
    ROUND(ps.total_revenue - AVG(ps.total_revenue) OVER (PARTITION BY ps.category), 2) AS gap_vs_cat_avg,
    -- Rankings
    DENSE_RANK() OVER (ORDER BY ps.total_revenue DESC)      AS global_rank,
    DENSE_RANK() OVER (
        PARTITION BY ps.category
        ORDER BY ps.total_revenue DESC
    )                                                       AS rank_in_category,
    -- Revenue % of global total
    ROUND(ps.total_revenue * 100.0
        / NULLIF(SUM(ps.total_revenue) OVER (), 0), 2)      AS pct_of_global_revenue,
    -- Performance label
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY ps.total_revenue) >= 0.90 THEN 'Hero'
        WHEN PERCENT_RANK() OVER (ORDER BY ps.total_revenue) >= 0.50 THEN 'Contributor'
        WHEN PERCENT_RANK() OVER (ORDER BY ps.total_revenue) >= 0.25 THEN 'Average'
        ELSE 'Laggard'
    END                                                     AS product_tier
FROM product_sales ps
LEFT JOIN product_returns pr ON ps.product_id = pr.product_id
ORDER BY ps.total_revenue DESC;


-- ============================================================================
-- 6.4  Customer Insights Report
-- ============================================================================
-- Full customer-level report combining:
--   CLV, order frequency, recency, demographic info, RFM score, tier
-- ============================================================================

WITH customer_metrics AS (
    SELECT
        fs.customer_id,
        dc.full_name,
        dc.gender,
        dc.age_group,
        dc.marital_status,
        dc.city,
        dc.country,
        dc.continent,
        -- Purchase metrics
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        SUM(fs.quantity)                            AS total_units,
        ROUND(SUM(fs.sales_amount), 2)              AS lifetime_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
        MIN(fs.order_date)                          AS first_order_date,
        MAX(fs.order_date)                          AS last_order_date,
        -- Recency in days
        DATEDIFF(DAY, MAX(fs.order_date), '2025-01-01') AS recency_days,
        -- Customer lifespan
        DATEDIFF(DAY, MIN(fs.order_date), MAX(fs.order_date)) AS lifespan_days
    FROM gold.fact_sales fs
    JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
    GROUP BY
        fs.customer_id, dc.full_name, dc.gender, dc.age_group,
        dc.marital_status, dc.city, dc.country, dc.continent
)
SELECT
    customer_id,
    full_name,
    gender,
    age_group,
    marital_status,
    city,
    country,
    continent,
    total_orders,
    total_units,
    lifetime_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    recency_days,
    lifespan_days,
    -- Revenue vs overall average
    ROUND(AVG(lifetime_revenue) OVER (), 2)         AS overall_avg_revenue,
    ROUND(lifetime_revenue - AVG(lifetime_revenue) OVER (), 2) AS gap_vs_avg,
    -- Global CLV rank
    DENSE_RANK() OVER (ORDER BY lifetime_revenue DESC) AS clv_rank,
    -- CLV tier
    CASE NTILE(4) OVER (ORDER BY lifetime_revenue DESC)
        WHEN 1 THEN 'VIP'
        WHEN 2 THEN 'High Value'
        WHEN 3 THEN 'Mid Value'
        WHEN 4 THEN 'Low Value'
    END                                             AS clv_tier,
    -- RFM scores (quick version)
    NTILE(5) OVER (ORDER BY recency_days ASC)       AS r_score,
    NTILE(5) OVER (ORDER BY total_orders DESC)      AS f_score,
    NTILE(5) OVER (ORDER BY lifetime_revenue DESC)  AS m_score,
    -- Activity status
    CASE
        WHEN recency_days <= 90  THEN 'Active'
        WHEN recency_days <= 180 THEN 'Cooling'
        WHEN recency_days <= 365 THEN 'At-Risk'
        ELSE                          'Lost'
    END                                             AS activity_status
FROM customer_metrics
ORDER BY lifetime_revenue DESC;


-- ============================================================================
-- 6.5  Store Operations Report
-- ============================================================================
-- Complete per-store report: revenue, orders, delivery KPIs, ranking,
-- peer comparison, and performance quadrant classification.
-- ============================================================================

WITH store_ops AS (
    SELECT
        ds.store_id,
        ds.store_name,
        ds.store_type,
        ds.region,
        COUNT(DISTINCT fs.order_number)             AS total_orders,
        COUNT(DISTINCT fs.customer_id)              AS unique_customers,
        SUM(fs.quantity)                            AS units_sold,
        ROUND(SUM(fs.sales_amount), 2)              AS total_revenue,
        ROUND(AVG(fs.sales_amount), 2)              AS avg_order_value,
        -- Delivery metrics
        ROUND(
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                        AS on_time_pct,
        ROUND(AVG(CAST(fs.days_to_ship AS FLOAT)), 1) AS avg_days_to_ship,
        SUM(CASE WHEN fs.delivery_status = 'Late'    THEN 1 ELSE 0 END) AS late_orders,
        SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1 ELSE 0 END) AS on_time_orders
    FROM gold.fact_sales fs
    JOIN gold.dim_store ds ON fs.store_id = ds.store_id
    WHERE fs.days_to_ship IS NOT NULL
    GROUP BY ds.store_id, ds.store_name, ds.store_type, ds.region
)
SELECT
    store_id,
    store_name,
    store_type,
    region,
    total_orders,
    unique_customers,
    units_sold,
    total_revenue,
    avg_order_value,
    on_time_pct,
    avg_days_to_ship,
    late_orders,
    on_time_orders,
    -- Network benchmarks
    ROUND(AVG(total_revenue) OVER (), 2)                AS network_avg_revenue,
    ROUND(AVG(on_time_pct)   OVER (), 2)                AS network_avg_ontime,
    -- Store type peer benchmarks
    ROUND(AVG(total_revenue) OVER (PARTITION BY store_type), 2) AS peer_avg_revenue,
    ROUND(AVG(on_time_pct)   OVER (PARTITION BY store_type), 2) AS peer_avg_ontime,
    -- Revenue rank globally and within store type
    DENSE_RANK() OVER (ORDER BY total_revenue DESC)     AS global_revenue_rank,
    DENSE_RANK() OVER (
        PARTITION BY store_type
        ORDER BY total_revenue DESC
    )                                                   AS rank_within_type,
    -- Revenue % of total
    ROUND(
        total_revenue * 100.0
        / NULLIF(SUM(total_revenue) OVER (), 0)
    , 2)                                                AS pct_of_total_revenue,
    -- Performance quadrant
    CASE
        WHEN total_revenue >= AVG(total_revenue) OVER () AND on_time_pct >= AVG(on_time_pct) OVER ()
            THEN 'Star'
        WHEN total_revenue >= AVG(total_revenue) OVER () AND on_time_pct <  AVG(on_time_pct) OVER ()
            THEN 'Revenue Leader / Ops Risk'
        WHEN total_revenue <  AVG(total_revenue) OVER () AND on_time_pct >= AVG(on_time_pct) OVER ()
            THEN 'Ops Reliable / Growth Needed'
        ELSE 'Underperformer'
    END                                                 AS quadrant,
    -- Revenue tier
    CASE NTILE(3) OVER (ORDER BY total_revenue DESC)
        WHEN 1 THEN 'Top Tier'
        WHEN 2 THEN 'Mid Tier'
        WHEN 3 THEN 'Bottom Tier'
    END                                                 AS revenue_tier
FROM store_ops
ORDER BY global_revenue_rank;


-- ============================================================================
-- 6.6  Returns Deep-Dive Report
-- ============================================================================
-- Comprehensive returns analysis:
--   By reason, category, country, month, and return amount distribution.
-- Answers: "Why are customers returning? Where? When? How much?"
-- ============================================================================

WITH returns_enriched AS (
    -- Enrich fact_returns with context from fact_sales and dimensions
    SELECT
        fr.return_id,
        fr.order_number,
        fr.return_date,
        fr.return_year,
        fr.return_month,
        fr.return_month_name,
        fr.return_reason,
        fr.return_amount,
        fs.order_date,
        fs.sales_amount                             AS original_sale_amount,
        fs.quantity                                 AS original_quantity,
        fs.delivery_status,
        dp.category,
        dp.subcategory,
        dp.product_name,
        dc.country,
        dc.continent,
        dc.age_group,
        ds.store_type,
        -- Days between order and return
        DATEDIFF(DAY, fs.order_date, fr.return_date) AS days_to_return,
        -- Return amount as % of original sale
        ROUND(
            fr.return_amount * 100.0
            / NULLIF(fs.sales_amount, 0)
        , 2)                                        AS return_pct_of_sale
    FROM gold.fact_returns fr
    JOIN gold.fact_sales   fs ON fr.order_number = fs.order_number
    JOIN gold.dim_product  dp ON fs.product_id   = dp.product_id
    JOIN gold.dim_customer dc ON fs.customer_id  = dc.customer_id
    JOIN gold.dim_store    ds ON fs.store_id     = ds.store_id
)

-- Summary: returns by reason and category
SELECT
    return_reason,
    category,
    COUNT(return_id)                                AS return_count,
    ROUND(SUM(return_amount), 2)                    AS total_return_value,
    ROUND(AVG(return_amount), 2)                    AS avg_return_value,
    ROUND(AVG(days_to_return), 1)                   AS avg_days_to_return,
    ROUND(AVG(return_pct_of_sale), 2)               AS avg_pct_of_original_sale,
    -- % of total returns
    ROUND(COUNT(return_id) * 100.0
        / NULLIF(SUM(COUNT(return_id)) OVER (), 0), 2) AS pct_of_all_returns,
    -- % of category returns vs all category returns
    ROUND(COUNT(return_id) * 100.0
        / NULLIF(SUM(COUNT(return_id)) OVER (PARTITION BY category), 0), 2) AS pct_within_category,
    -- Rank by return count within reason
    DENSE_RANK() OVER (
        PARTITION BY return_reason
        ORDER BY COUNT(return_id) DESC
    )                                               AS rank_in_reason
FROM returns_enriched
GROUP BY return_reason, category
ORDER BY return_reason, return_count DESC;

-- Returns by country and delivery status
SELECT
    country,
    continent,
    delivery_status,
    COUNT(return_id)                                AS return_count,
    ROUND(SUM(return_amount), 2)                    AS total_return_value,
    ROUND(AVG(days_to_return), 1)                   AS avg_days_to_return,
    -- Country return rate
    ROUND(COUNT(return_id) * 100.0
        / NULLIF(SUM(COUNT(return_id)) OVER (PARTITION BY country), 0), 2) AS pct_of_country_returns,
    DENSE_RANK() OVER (ORDER BY COUNT(return_id) DESC) AS global_rank
FROM returns_enriched
GROUP BY country, continent, delivery_status
ORDER BY return_count DESC;
