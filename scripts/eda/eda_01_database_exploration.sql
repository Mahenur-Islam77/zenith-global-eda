/*
================================================================================
  ZENITH GLOBAL DATA WAREHOUSE
  EDA Phase 1 -- Database Exploration
================================================================================
  Purpose : Discover the physical structure of the warehouse -- schemas,
            objects, columns, data types, and row counts -- before touching
            any data values.

  Database: Zenith-Global-Data-Warehouse
  Schema  : gold  (analytical / business-ready layer)

  Run Order:
    1.1  List all schemas
    1.2  Inventory all tables and views
    1.3  Column definitions for gold layer
    1.4  Row counts across all gold objects
================================================================================
*/

USE Zenith_Global_Data_Warehouse;
GO

-- ============================================================================
-- 1.1  List All Schemas in the Database
-- ============================================================================
-- Shows every schema present: bronze, silver, gold, dbo.
-- Confirms the medallion architecture is in place.
-- ============================================================================

SELECT
    name                            AS schema_name,
    schema_id,
    SUSER_SNAME(principal_id)       AS schema_owner
FROM sys.schemas
ORDER BY schema_id;


-- ============================================================================
-- 1.2  Inventory All Tables and Views per Schema
-- ============================================================================
-- TABLE_TYPE = 'BASE TABLE' for physical tables (bronze/silver)
--            = 'VIEW'       for gold layer objects
-- ============================================================================

SELECT
    TABLE_SCHEMA        AS schema_name,
    TABLE_NAME          AS object_name,
    TABLE_TYPE          AS object_type
FROM INFORMATION_SCHEMA.TABLES
ORDER BY
    TABLE_SCHEMA,
    TABLE_TYPE,
    TABLE_NAME;


-- ============================================================================
-- 1.3  Column Definitions for All Gold Layer Objects
-- ============================================================================
-- Reveals: column names, data types, max lengths, nullability.
-- Critical for understanding what each view exposes to analysts.
-- ============================================================================

SELECT
    TABLE_SCHEMA                    AS schema_name,
    TABLE_NAME                      AS object_name,
    ORDINAL_POSITION                AS col_position,
    COLUMN_NAME                     AS column_name,
    DATA_TYPE                       AS data_type,
    CHARACTER_MAXIMUM_LENGTH        AS max_length,
    IS_NULLABLE                     AS is_nullable,
    COLUMN_DEFAULT                  AS default_value
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold'
ORDER BY
    TABLE_NAME,
    ORDINAL_POSITION;


-- ============================================================================
-- 1.4  Row Count for Every Gold Object
-- ============================================================================
-- Quick sanity check: confirms data was loaded and gives immediate
-- scale context before any deeper analysis.
--
-- Expected:
--   gold.dim_customer   ~25,000
--   gold.dim_product     ~5,000
--   gold.dim_store          ~100
--   gold.fact_sales      ~30,000
--   gold.fact_returns    ~26,967
-- ============================================================================

SELECT 'gold.dim_customer'  AS gold_object, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'gold.dim_product',                  COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'gold.dim_store',                    COUNT(*) FROM gold.dim_store
UNION ALL
SELECT 'gold.fact_sales',                   COUNT(*) FROM gold.fact_sales
UNION ALL
SELECT 'gold.fact_returns',                 COUNT(*) FROM gold.fact_returns
ORDER BY row_count DESC;
