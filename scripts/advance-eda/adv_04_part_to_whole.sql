/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE -- ADVANCED EDA
  Analysis 4: Part-to-Whole Analysis
================================================================================
  Purpose : Understand how individual segments contribute to the total.
            Part-to-whole analysis answers: "What percentage of the whole
            does each piece represent?" -- enabling composition analysis,
            channel mix, and revenue attribution.

  Database : Zenith_Global_Data_Warehouse
  Schema   : gold
  Techniques: SUM() OVER () for grand totals, SUM() OVER (PARTITION BY)
              for group totals, CASE-based buckets, CTEs, hierarchical %

  Sections:
    4.1  Revenue % by product category (category mix)
    4.2  Revenue % by continent and country (geographic hierarchy)
    4.3  Subcategory as % of parent category revenue
    4.4  Store type channel mix (% of total orders and revenue)
    4.5  Return reason distribution (% of total returns)
    4.6  Customer demographic revenue contribution (age + gender)
    4.7  Product cost tier distribution (% of catalog by price band)
    4.8  Delivery status composition by store type
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 4.1  Revenue % by Product Category (Category Mix)
-- ============================================================================
-- Answers: "What share of total revenue does each category own?"
-- SUM(...) OVER () without PARTITION BY = grand total across all rows.
-- ============================================================================

WITH category_totals AS (
    SELECT
        dp.category,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        SUM(fs.quantity)                                        AS units_sold,
        ROUND(SUM(fs.sales_amount), 2)                          AS category_revenue,
        ROUND(AVG(fs.sales_amount), 2)                          AS avg_order_value
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.category
)
SELECT
    category,
    total_orders,
    units_sold,
    category_revenue,
    avg_order_value,
    -- Revenue % of grand total
    ROUND(
        category_revenue * 100.0
        / NULLIF(SUM(category_revenue) OVER (), 0)
    , 2)                                                        AS pct_of_total_revenue,
    -- Orders % of grand total
    ROUND(
        total_orders * 100.0
        / NULLIF(SUM(total_orders) OVER (), 0)
    , 2)                                                        AS pct_of_total_orders,
    -- Units % of grand total
    ROUND(
        units_sold * 100.0
        / NULLIF(SUM(units_sold) OVER (), 0)
    , 2)                                                        AS pct_of_total_units,
    -- Revenue rank
    DENSE_RANK() OVER (ORDER BY category_revenue DESC)          AS revenue_rank
FROM category_totals
ORDER BY category_revenue DESC;


-- ============================================================================
-- 4.2  Revenue % by Continent and Country (Geographic Hierarchy)
-- ============================================================================
-- Two-level part-to-whole:
--   Level 1: Each continent as % of global revenue
--   Level 2: Each country as % of its continent revenue AND global revenue
-- ============================================================================

WITH geo_revenue AS (
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
    -- Country as % of global revenue
    ROUND(
        country_revenue * 100.0
        / NULLIF(SUM(country_revenue) OVER (), 0)
    , 2)                                                        AS pct_of_global_revenue,
    -- Country as % of its continent's revenue (PARTITION BY continent)
    ROUND(
        country_revenue * 100.0
        / NULLIF(SUM(country_revenue) OVER (PARTITION BY continent), 0)
    , 2)                                                        AS pct_of_continent_revenue,
    -- Continent total
    ROUND(SUM(country_revenue) OVER (PARTITION BY continent), 2) AS continent_revenue,
    -- Continent as % of global
    ROUND(
        SUM(country_revenue) OVER (PARTITION BY continent) * 100.0
        / NULLIF(SUM(country_revenue) OVER (), 0)
    , 2)                                                        AS continent_pct_of_global,
    -- Country rank within its continent
    DENSE_RANK() OVER (
        PARTITION BY continent
        ORDER BY country_revenue DESC
    )                                                           AS rank_in_continent
FROM geo_revenue
ORDER BY continent, country_revenue DESC;


-- ============================================================================
-- 4.3  Subcategory as % of Parent Category Revenue
-- ============================================================================
-- Shows the internal composition of each category.
-- "Road Bikes" as 65% of the Bikes category = Bikes is heavily road-focused.
-- ============================================================================

WITH subcat_revenue AS (
    SELECT
        dp.category,
        dp.subcategory,
        ROUND(SUM(fs.sales_amount), 2)                          AS subcat_revenue,
        COUNT(DISTINCT fs.order_number)                         AS subcat_orders,
        SUM(fs.quantity)                                        AS subcat_units
    FROM gold.fact_sales fs
    JOIN gold.dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.category, dp.subcategory
)
SELECT
    category,
    subcategory,
    subcat_orders,
    subcat_units,
    subcat_revenue,
    -- Subcategory as % of its parent category
    ROUND(
        subcat_revenue * 100.0
        / NULLIF(SUM(subcat_revenue) OVER (PARTITION BY category), 0)
    , 2)                                                        AS pct_of_category,
    -- Subcategory as % of global revenue
    ROUND(
        subcat_revenue * 100.0
        / NULLIF(SUM(subcat_revenue) OVER (), 0)
    , 2)                                                        AS pct_of_global,
    -- Category total for reference
    ROUND(SUM(subcat_revenue) OVER (PARTITION BY category), 2) AS category_total_revenue,
    -- Rank within parent category
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY subcat_revenue DESC
    )                                                           AS rank_in_category
FROM subcat_revenue
ORDER BY category, rank_in_category;


-- ============================================================================
-- 4.4  Store Type Channel Mix (% of Total Orders and Revenue)
-- ============================================================================
-- Breaks down total business by sales channel.
-- Essential for channel strategy: is online growing relative to retail?
-- ============================================================================

WITH channel_metrics AS (
    SELECT
        ds.store_type,
        COUNT(DISTINCT ds.store_id)                             AS store_count,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        SUM(fs.quantity)                                        AS units_sold,
        ROUND(SUM(fs.sales_amount), 2)                          AS total_revenue,
        ROUND(AVG(fs.sales_amount), 2)                          AS avg_order_value,
        -- On-time rate per channel
        ROUND(
            SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                                    AS on_time_pct
    FROM gold.fact_sales fs
    JOIN gold.dim_store ds ON fs.store_id = ds.store_id
    GROUP BY ds.store_type
)
SELECT
    store_type,
    store_count,
    total_orders,
    units_sold,
    total_revenue,
    avg_order_value,
    on_time_pct,
    -- Channel revenue share
    ROUND(
        total_revenue * 100.0
        / NULLIF(SUM(total_revenue) OVER (), 0)
    , 2)                                                        AS pct_of_total_revenue,
    -- Channel order share
    ROUND(
        total_orders * 100.0
        / NULLIF(SUM(total_orders) OVER (), 0)
    , 2)                                                        AS pct_of_total_orders,
    -- Revenue per store (efficiency metric)
    ROUND(total_revenue / NULLIF(store_count, 0), 2)            AS revenue_per_store
FROM channel_metrics
ORDER BY total_revenue DESC;


-- ============================================================================
-- 4.5  Return Reason Distribution (% of Total Returns)
-- ============================================================================
-- Breaks down the 26,967 return transactions by reason.
-- Shows what proportion of returns are due to defects vs size vs other.
-- ============================================================================

WITH reason_totals AS (
    SELECT
        return_reason,
        COUNT(return_id)                                        AS return_count,
        ROUND(SUM(return_amount), 2)                            AS total_return_value,
        ROUND(AVG(return_amount), 2)                            AS avg_return_value
    FROM gold.fact_returns
    GROUP BY return_reason
)
SELECT
    return_reason,
    return_count,
    total_return_value,
    avg_return_value,
    -- Volume % of total returns
    ROUND(
        return_count * 100.0
        / NULLIF(SUM(return_count) OVER (), 0)
    , 2)                                                        AS pct_of_return_volume,
    -- Value % of total return value
    ROUND(
        total_return_value * 100.0
        / NULLIF(SUM(total_return_value) OVER (), 0)
    , 2)                                                        AS pct_of_return_value,
    -- Rank by volume
    DENSE_RANK() OVER (ORDER BY return_count DESC)              AS rank_by_volume,
    -- Rank by value (highest financial impact first)
    DENSE_RANK() OVER (ORDER BY total_return_value DESC)        AS rank_by_value
FROM reason_totals
ORDER BY return_count DESC;


-- ============================================================================
-- 4.6  Customer Demographic Revenue Contribution
-- ============================================================================
-- Shows what share of total revenue comes from each demographic segment.
-- Combined age + gender cross-tab reveals the most valuable customer profiles.
-- ============================================================================

-- By age group
WITH age_revenue AS (
    SELECT
        dc.age_group,
        COUNT(DISTINCT dc.customer_id)                          AS customer_count,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)                          AS total_revenue
    FROM gold.dim_customer dc
    LEFT JOIN gold.fact_sales fs ON dc.customer_id = fs.customer_id
    GROUP BY dc.age_group
)
SELECT
    age_group,
    customer_count,
    total_orders,
    total_revenue,
    -- Customer % of total
    ROUND(customer_count * 100.0 / NULLIF(SUM(customer_count) OVER (), 0), 2) AS pct_of_customers,
    -- Revenue % of total
    ROUND(total_revenue * 100.0 / NULLIF(SUM(total_revenue) OVER (), 0), 2)   AS pct_of_revenue,
    -- Revenue per customer (value density)
    ROUND(total_revenue / NULLIF(customer_count, 0), 2)         AS revenue_per_customer
FROM age_revenue
ORDER BY total_revenue DESC;

-- By gender
WITH gender_revenue AS (
    SELECT
        dc.gender,
        COUNT(DISTINCT dc.customer_id)                          AS customer_count,
        COUNT(DISTINCT fs.order_number)                         AS total_orders,
        ROUND(SUM(fs.sales_amount), 2)                          AS total_revenue
    FROM gold.dim_customer dc
    LEFT JOIN gold.fact_sales fs ON dc.customer_id = fs.customer_id
    GROUP BY dc.gender
)
SELECT
    gender,
    customer_count,
    total_orders,
    total_revenue,
    ROUND(customer_count * 100.0 / NULLIF(SUM(customer_count) OVER (), 0), 2) AS pct_of_customers,
    ROUND(total_revenue  * 100.0 / NULLIF(SUM(total_revenue)  OVER (), 0), 2) AS pct_of_revenue,
    ROUND(total_revenue / NULLIF(customer_count, 0), 2)         AS revenue_per_customer
FROM gender_revenue
ORDER BY total_revenue DESC;


-- ============================================================================
-- 4.7  Product Cost Tier Distribution (% of Catalog by Price Band)
-- ============================================================================
-- Classifies products into cost tiers and shows what % of the catalog
-- and total revenue each tier accounts for.
-- ============================================================================

WITH cost_tiers AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.cost,
        -- Assign cost tier based on product cost
        CASE
            WHEN p.cost < 50    THEN 'Budget (< $50)'
            WHEN p.cost < 200   THEN 'Mid-Range ($50-$199)'
            WHEN p.cost < 500   THEN 'Premium ($200-$499)'
            WHEN p.cost < 1000  THEN 'High-End ($500-$999)'
            ELSE                     'Luxury ($1000+)'
        END                                                     AS cost_tier,
        CASE
            WHEN p.cost < 50   THEN 1
            WHEN p.cost < 200  THEN 2
            WHEN p.cost < 500  THEN 3
            WHEN p.cost < 1000 THEN 4
            ELSE                    5
        END                                                     AS tier_sort
    FROM gold.dim_product p
),
tier_summary AS (
    SELECT
        ct.cost_tier,
        ct.tier_sort,
        COUNT(ct.product_id)                                    AS product_count,
        ROUND(SUM(COALESCE(fs.sales_amount, 0)), 2)             AS tier_revenue,
        ROUND(AVG(ct.cost), 2)                                  AS avg_cost_in_tier
    FROM cost_tiers ct
    LEFT JOIN gold.fact_sales fs ON ct.product_id = fs.product_id
    GROUP BY ct.cost_tier, ct.tier_sort
)
SELECT
    cost_tier,
    product_count,
    avg_cost_in_tier,
    tier_revenue,
    -- Products in tier as % of catalog
    ROUND(product_count * 100.0 / NULLIF(SUM(product_count) OVER (), 0), 2) AS pct_of_catalog,
    -- Revenue generated by tier as % of total
    ROUND(tier_revenue  * 100.0 / NULLIF(SUM(tier_revenue)  OVER (), 0), 2) AS pct_of_revenue,
    -- Revenue per product in tier
    ROUND(tier_revenue / NULLIF(product_count, 0), 2)           AS revenue_per_product
FROM tier_summary
ORDER BY tier_sort;


-- ============================================================================
-- 4.8  Delivery Status Composition by Store Type
-- ============================================================================
-- Shows On Time vs Late vs Unknown breakdown within each channel.
-- Uses conditional aggregation for an inline cross-tab.
-- ============================================================================

SELECT
    ds.store_type,
    COUNT(*)                                                    AS total_orders,
    -- On Time count and %
    SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1 ELSE 0 END) AS on_time_count,
    ROUND(
        SUM(CASE WHEN fs.delivery_status = 'On Time' THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                        AS on_time_pct,
    -- Late count and %
    SUM(CASE WHEN fs.delivery_status = 'Late'    THEN 1 ELSE 0 END) AS late_count,
    ROUND(
        SUM(CASE WHEN fs.delivery_status = 'Late'    THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                        AS late_pct,
    -- Unknown count and %
    SUM(CASE WHEN fs.delivery_status = 'Unknown' THEN 1 ELSE 0 END) AS unknown_count,
    ROUND(
        SUM(CASE WHEN fs.delivery_status = 'Unknown' THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                        AS unknown_pct
FROM gold.fact_sales fs
JOIN gold.dim_store ds ON fs.store_id = ds.store_id
GROUP BY ds.store_type
ORDER BY on_time_pct DESC;
