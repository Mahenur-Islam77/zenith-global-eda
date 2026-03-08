# Zenith Global — SQL Exploratory Data Analysis (EDA)
## Technical Documentation

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Database & Schema Context](#2-database--schema-context)
3. [EDA Methodology](#3-eda-methodology)
4. [Phase 1 — Database Exploration](#4-phase-1--database-exploration)
5. [Phase 2 — Dimensions Exploration](#5-phase-2--dimensions-exploration)
6. [Phase 3 — Data Exploration](#6-phase-3--data-exploration)
7. [Phase 4 — Measures Exploration](#7-phase-4--measures-exploration)
8. [Phase 5 — Magnitudes](#8-phase-5--magnitudes)
9. [Phase 6 — Ranking](#9-phase-6--ranking)
10. [SQL Techniques Reference](#10-sql-techniques-reference)
11. [File Inventory](#11-file-inventory)
12. [How to Run](#12-how-to-run)

---

## 1. Project Overview

This document covers the **Exploratory Data Analysis (EDA)** phase of the Zenith Global Data Warehouse project. EDA is conducted on the fully cleansed and modelled **gold layer** — the star schema that sits at the top of the Medallion Architecture pipeline.

**What this EDA covers:**

| Scope | Detail |
|---|---|
| Database | `Zenith-Global-Data-Warehouse` |
| Layer targeted | `gold` schema (star schema views) |
| Objects analysed | 3 dimension views + 2 fact views |
| Total rows in scope | ~117,000 (gold layer) |
| SQL dialect | T-SQL (Microsoft SQL Server) |
| EDA phases | 6 structured phases |
| SQL files | 6 files |
| Total queries | 200+ |

**Why EDA before dashboarding?**

Exploratory Data Analysis is the analytical checkpoint between data engineering and data visualization. It answers:
- Is the data structurally sound? (Phase 1–2)
- Is the data quality acceptable? (Phase 3)
- What do the numbers look like statistically? (Phase 4)
- What is the scale of each business segment? (Phase 5)
- Who and what are the top and bottom performers? (Phase 6)

Skipping EDA leads to dashboards that look correct but contain wrong numbers. This framework makes EDA systematic and repeatable.

---

## 2. Database & Schema Context

### 2.1 Medallion Architecture

The gold layer is built on top of two cleansed silver layers from CRM and ERP source systems.

```
SOURCE FILES          BRONZE              SILVER               GOLD
────────────          ──────              ──────               ────
CRM (3 CSVs) ──────► Raw Tables ───────► Clean Tables ──────► Star Schema Views
ERP (6 CSVs) ──────► (no transform)      (ETL + DQ)           (always current)
```

This EDA project **only queries the gold layer**. Silver and bronze are not directly touched.

---

### 2.2 Star Schema — Gold Layer

```
                        ┌─────────────────────────────┐
                        │      gold.dim_customer      │
                        │  ───────────────────────    │
                        │  customer_id          (PK)  │
                        │  customer_number            │
                        │  first_name / last_name     │
                        │  full_name            (der) │
                        │  gender / marital_status    │
                        │  birthdate                  │
                        │  age / age_group      (der) │
                        │  city / country / continent │
                        │  25,000 rows                │
                        └─────────────┬───────────────┘
                                      │ customer_id
                                      │
┌─────────────────────────┐  ┌────────┴──────────────────┐  ┌──────────────────────────┐
│    gold.dim_product     │  │      gold.fact_sales      │  │    gold.fact_returns     │
│  ─────────────────────  │  │  ──────────────────────   │  │  ──────────────────────  │
│  product_id       (PK)  │  │  order_number       (PK)  │  │  return_id         (PK)  │
│  product_number         │  │  product_id         (FK)──┘  │  order_number      (FK)──┘
│  product_name           │  │  customer_id        (FK)  │  │  return_date             │
│  cost                   │◄─┤  store_id           (FK)  │  │  return_year/month  (der)│
│  product_line           │  │  order_date               │  │  return_reason           │
│  category / subcategory │  │  order_year/month   (der) │  │  return_amount           │
│  maintenance_required   │  │  quantity / price         │  │  26,967 rows             │
│  5,000 rows             │  │  sales_amount             │  └──────────────────────────┘
└─────────────────────────┘  │  days_to_ship       (der) │
                             │  delivery_status    (der) │
┌─────────────────────────┐  │  30,000 rows              │
│     gold.dim_store      │  └───────────────────────────┘
│  ─────────────────────  │                    │ store_id
│  store_id         (PK)  │◄───────────────────┘
│  store_name             │
│  store_type             │   (der) = derived / calculated field
│  region                 │   (PK)  = primary key
│  100 rows               │   (FK)  = foreign key
└─────────────────────────┘
```

### 2.3 Key Join Paths

```sql
-- Sales with customer + product context
fact_sales  ──► dim_customer   ON fs.customer_id = dc.customer_id
fact_sales  ──► dim_product    ON fs.product_id  = dp.product_id
fact_sales  ──► dim_store      ON fs.store_id    = ds.store_id

-- Returns linked back to the originating sale
fact_returns ──► fact_sales    ON fr.order_number = fs.order_number
```

---

## 3. EDA Methodology

### 3.1 Six-Phase Framework

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      EDA EXECUTION SEQUENCE                             │
│                                                                         │
│  Phase 1          Phase 2          Phase 3          Phase 4             │
│  DATABASE    ──►  DIMENSIONS  ──►  DATA        ──►  MEASURES            │
│  EXPLORATION      EXPLORATION      EXPLORATION       EXPLORATION        │
│                                                           │             │
│                                                           ▼             │
│                                           Phase 5    Phase 6            │
│                                           MAGNITUDES  RANKING           │
│                                                                         │
│  Structural       Domain            Quality          Statistics         │
│  Discovery        Knowledge         Sign-off         Baseline           │
│                                                                         │
│                                           Volume &    Top/Bottom        │
│                                           Scale       Performers        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Phase Dependencies

Each phase builds on the previous. Do not skip steps.

| Phase | Depends On | If Skipped |
|---|---|---|
| 1 — Database Exploration | Nothing | You may query the wrong objects or schema |
| 2 — Dimensions Exploration | Phase 1 | You won't know the valid domain values |
| 3 — Data Exploration | Phase 2 | You may aggregate over dirty data silently |
| 4 — Measures Exploration | Phase 3 | Outliers and NULLs distort your statistics |
| 5 — Magnitudes | Phase 4 | No baseline to compare segment sizes against |
| 6 — Ranking | Phase 5 | Rankings lack context without scale totals |

---

## 4. Phase 1 — Database Exploration

**File:** `eda_01_database_exploration.sql`

**Goal:** Map the physical structure of the warehouse before touching any data values.

### Queries in this phase

| Section | Query | What it reveals |
|---|---|---|
| 1.1 | List all schemas | Confirms bronze / silver / gold layers exist |
| 1.2 | Inventory tables and views | Shows all objects per schema and their type |
| 1.3 | Column definitions (gold) | Column names, data types, nullability per gold object |
| 1.4 | Row counts per gold object | Confirms data was loaded; establishes scale |

### Key SQL Pattern — Schema Discovery

```sql
SELECT
    TABLE_SCHEMA        AS schema_name,
    TABLE_NAME          AS object_name,
    TABLE_TYPE          AS object_type   -- 'BASE TABLE' or 'VIEW'
FROM INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME;
```

### Expected Row Counts

| Object | Expected Rows |
|---|---|
| gold.dim_customer | 25,000 |
| gold.dim_product | 5,000 |
| gold.dim_store | 100 |
| gold.fact_sales | 30,000 |
| gold.fact_returns | 26,967 |

---

## 5. Phase 2 — Dimensions Exploration

**File:** `eda_02_dimensions_exploration.sql`

**Goal:** Understand the domain of every descriptive/categorical column — how many unique values exist and whether they are clean.

### Queries in this phase

| Section | Object | What it reveals |
|---|---|---|
| 2.1 | dim_customer | Cardinality (distinct count) per categorical column |
| 2.2 | dim_customer | All distinct domain values for each category |
| 2.3 | dim_product | Category → Subcategory hierarchy with product counts |
| 2.4 | dim_product | Product lines and maintenance requirements |
| 2.5 | dim_store | Store types and regional distribution |
| 2.6 | fact_sales | Delivery status domain values and split |
| 2.7 | fact_returns | Return reason domain values and split |

### Cardinality Reference

| Column | Expected Distinct Values | Notes |
|---|---|---|
| gender | 2–3 | Male, Female, Unknown |
| marital_status | 3 | Married, Single, Divorced |
| age_group | 6–7 | Under 20, 20-29, 30-39, 40-49, 50-59, 60+, Unknown |
| continent | 5 | Europe, North America, Asia, Latin America, Oceania |
| country | ~17 | Derived from territory reference table |
| category | 4 | Bikes, Accessories, Clothing, Components |
| store_type | 4 | Flagship, Retail, Online, Outlet |
| delivery_status | 2–3 | On Time, Late, Unknown |
| return_reason | 4 | Defective, Size Mismatch, Unsatisfied, Wrong Item |

### Key SQL Pattern — Domain Enumeration

```sql
-- Cardinality audit for all categorical columns in one query
SELECT 'gender'        AS col, COUNT(DISTINCT gender)        AS distinct_values FROM gold.dim_customer
UNION ALL
SELECT 'continent',          COUNT(DISTINCT continent)       FROM gold.dim_customer
UNION ALL
SELECT 'age_group',          COUNT(DISTINCT age_group)       FROM gold.dim_customer
ORDER BY distinct_values DESC;
```

---

## 6. Phase 3 — Data Exploration

**File:** `eda_03_data_exploration.sql`

**Goal:** Validate data quality — NULLs, duplicates, date ranges, referential integrity, and value sanity checks. This is the data quality **gate** — findings here determine whether downstream analysis is trustworthy.

### Queries in this phase

| Section | Check Type | Objects |
|---|---|---|
| 3.1 | Date range validation | fact_sales, fact_returns |
| 3.2 | NULL audit | dim_customer |
| 3.3 | NULL audit | dim_product |
| 3.4 | NULL audit | fact_sales |
| 3.5 | NULL audit | fact_returns |
| 3.6 | Referential integrity | fact_sales → all dimensions, fact_returns → fact_sales |
| 3.7 | Duplicate detection | All 4 business keys |
| 3.8 | Age sanity check | dim_customer |

### NULL Audit Pattern

```sql
-- COUNT(*) - COUNT(col) = number of NULLs in that column
SELECT
    'sales_amount'  AS column_name,
    COUNT(*) - COUNT(sales_amount) AS null_count
FROM gold.fact_sales
UNION ALL
SELECT 'quantity', COUNT(*) - COUNT(quantity) FROM gold.fact_sales
ORDER BY null_count DESC;
```

### Referential Integrity Pattern

```sql
-- Orphan detection: FK in fact with no matching PK in dimension
SELECT COUNT(*) AS orphan_customers
FROM gold.fact_sales fs
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_customer dc
    WHERE dc.customer_id = fs.customer_id
);
```

**Expected result for a clean warehouse:** all orphan counts = 0.

### Duplicate Detection Pattern

```sql
-- Any count > 1 means a duplicated business key
SELECT order_number, COUNT(*) AS occurrences
FROM gold.fact_sales
GROUP BY order_number
HAVING COUNT(*) > 1;
```

### Date Range Expectations

| Fact Table | Expected Range |
|---|---|
| fact_sales | 2023-01-01 to 2024-12-31 |
| fact_returns | 2023-01-01 to 2024-12-31 |

---

## 7. Phase 4 — Measures Exploration

**File:** `eda_04_measures_exploration.sql`

**Goal:** Compute descriptive statistics (min, max, mean, standard deviation, percentiles) for every numeric measure in the gold layer before building any aggregation or visualization.

### Queries in this phase

| Section | Measure | Key Statistics Computed |
|---|---|---|
| 4.1 | sales_amount | Count, Sum, Min, Max, Avg, StdDev, CV%, P25, P50, P75 |
| 4.2 | price + quantity | Min, Max, Avg, StdDev, total units sold, effective unit price |
| 4.3 | return_amount | Count, Sum, Min, Max, Avg, StdDev, return rate vs revenue |
| 4.4 | product cost | By category: Count, Min, Max, Avg, StdDev, Range |
| 4.5 | delivery lead time | Min, Max, Avg, StdDev for days_to_ship and days_to_due |

### Statistical Concepts Applied

| Statistic | Formula | Interpretation |
|---|---|---|
| **Mean (Avg)** | SUM / COUNT | Central value — typical order size |
| **Standard Deviation** | STDEV() | Spread around the mean — how variable is order size? |
| **Coefficient of Variation (CV%)** | StdDev / Mean × 100 | Relative variability. CV% < 30% = stable. CV% > 100% = high dispersion |
| **P25 / P50 / P75** | PERCENT_RANK() | Quartile breakpoints — better than mean for skewed data |

### Percentile Pattern (PERCENT_RANK)

```sql
SELECT
    MAX(CASE WHEN rn_pct <= 25 THEN sales_amount END) AS p25,
    MAX(CASE WHEN rn_pct <= 50 THEN sales_amount END) AS p50_median,
    MAX(CASE WHEN rn_pct <= 75 THEN sales_amount END) AS p75
FROM (
    SELECT
        sales_amount,
        PERCENT_RANK() OVER (ORDER BY sales_amount) * 100 AS rn_pct
    FROM gold.fact_sales
) t;
```

---

## 8. Phase 5 — Magnitudes

**File:** `eda_05_magnitudes.sql`

**Goal:** Quantify business volume across every analytical dimension to understand where revenue, customers, and returns are concentrated.

### Queries in this phase

| Section | Dimension | Metrics |
|---|---|---|
| 5.1 | Year | Orders, units sold, revenue, avg order value |
| 5.2 | Product category | Orders, units, revenue, revenue % of total |
| 5.3 | Store type / channel | Orders, units, revenue, avg order value, revenue % |
| 5.4 | Continent + Country | Unique customers, orders, revenue, revenue % |
| 5.5 | Returns by category | Return count, value, avg return, % of total returns |
| 5.6 | Demographics | Customer count, orders, revenue by age group / gender / marital status |
| 5.7 | Month | Orders, units, revenue per calendar month |

### Revenue Contribution Pattern

```sql
-- revenue_pct: each group's share of the total
SUM(fs.sales_amount) * 100.0
/ SUM(SUM(fs.sales_amount)) OVER ()  AS revenue_pct
```

`SUM(SUM(fs.sales_amount)) OVER ()` is a window function with no PARTITION BY — it sums the entire result set, giving the grand total as the denominator.

### Return Rate Pattern

```sql
ROUND(
    SUM(return_amount) * 100.0
    / NULLIF(SUM(sales_amount), 0)   -- NULLIF prevents divide-by-zero
, 2) AS return_rate_pct
```

---

## 9. Phase 6 — Ranking

**File:** `eda_06_ranking.sql`

**Goal:** Identify top and bottom performers across products, customers, stores, geographies, and time using SQL window functions.

### Queries in this phase

| Section | Ranked By | Window Function Used |
|---|---|---|
| 6.1 | Top 10 products by revenue | DENSE_RANK() ORDER BY revenue DESC |
| 6.2 | Bottom 10 products by revenue | DENSE_RANK() ORDER BY revenue ASC |
| 6.3 | Top 10 customers by lifetime value | DENSE_RANK() ORDER BY lifetime revenue DESC |
| 6.4 | Products ranked within category | DENSE_RANK() PARTITION BY category |
| 6.5 | Stores by revenue + delivery rate | Two independent DENSE_RANK() in one query |
| 6.6 | Months by revenue (overall + within year) | DENSE_RANK() + DENSE_RANK() PARTITION BY year |
| 6.7 | Countries by return rate | DENSE_RANK() ORDER BY return rate DESC |

### Window Function Decision Guide

| Function | Use When |
|---|---|
| `DENSE_RANK()` | You want a leaderboard where ties share the same rank. Rank 1, 2, 2, 3 (no gaps). **Default choice for EDA.** |
| `RANK()` | You want gaps after ties. Rank 1, 2, 2, 4. Rarely needed. |
| `ROW_NUMBER()` | You need exactly N unique rows (strict Top 10, no ties sharing rank). |
| `PARTITION BY` | You want ranking to reset within each group (e.g., rank products inside their own category). |

### PARTITION BY Pattern — Intra-Group Ranking

```sql
SELECT
    dp.category,
    dp.product_name,
    SUM(fs.sales_amount)          AS total_revenue,
    -- Global rank across all products
    DENSE_RANK() OVER (
        ORDER BY SUM(fs.sales_amount) DESC
    )                             AS global_rank,
    -- Rank resets per category
    DENSE_RANK() OVER (
        PARTITION BY dp.category
        ORDER BY SUM(fs.sales_amount) DESC
    )                             AS rank_within_category
FROM gold.fact_sales fs
JOIN gold.dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.category, dp.product_name
ORDER BY dp.category, rank_within_category;
```

### Dual Ranking Pattern — Revenue + Delivery

```sql
-- Two independent rankings in a single query
DENSE_RANK() OVER (ORDER BY SUM(fs.sales_amount) DESC)      AS revenue_rank,
DENSE_RANK() OVER (ORDER BY on_time_rate DESC)              AS delivery_rank
```

A store with `revenue_rank = 1` but `delivery_rank = 25` is commercially strong but operationally weak — a direct management action item.

---

## 10. SQL Techniques Reference

### Techniques Used Across All Phases

| Technique | SQL | Phase Used |
|---|---|---|
| Schema inventory | `INFORMATION_SCHEMA.TABLES` | 1 |
| Column metadata | `INFORMATION_SCHEMA.COLUMNS` | 1 |
| NULL counting | `COUNT(*) - COUNT(col)` | 3 |
| Orphan detection | `NOT EXISTS (SELECT 1 ...)` | 3 |
| Duplicate detection | `GROUP BY ... HAVING COUNT(*) > 1` | 3 |
| Percentile distribution | `PERCENT_RANK() OVER (ORDER BY col)` | 4 |
| Coefficient of Variation | `STDEV(col) / AVG(col) * 100` | 4 |
| Contribution percentage | `SUM(col) / SUM(SUM(col)) OVER ()` | 5 |
| Divide-by-zero guard | `NULLIF(denominator, 0)` | 5, 6 |
| Leaderboard ranking | `DENSE_RANK() OVER (ORDER BY ...)` | 6 |
| Intra-group ranking | `DENSE_RANK() OVER (PARTITION BY ...)` | 6 |
| Cross-group revenue % | `SUM(...) OVER ()` with no PARTITION | 5 |
| Multi-rank in one query | Two `DENSE_RANK()` windows in SELECT | 6 |

---

## 11. File Inventory

All EDA files are located in: `scripts/eda/`

```
scripts/eda/
│
├── eda_01_database_exploration.sql     Phase 1: Schema, objects, row counts
│    4 queries  |  3.9 KB
│
├── eda_02_dimensions_exploration.sql   Phase 2: Cardinality, domain values
│    10 queries  |  7.1 KB
│
├── eda_03_data_exploration.sql         Phase 3: NULLs, duplicates, integrity
│    12 queries  |  11 KB
│
├── eda_04_measures_exploration.sql     Phase 4: Statistics and distributions
│    5 queries  |  7.3 KB
│
├── eda_05_magnitudes.sql               Phase 5: Volume and scale by dimension
│    8 queries  |  8.3 KB
│
├── eda_06_ranking.sql                  Phase 6: Top/bottom with window funcs
│    7 queries  |  9.7 KB
│
├── EDA_DOCUMENTATION.md                This document
└── EDA_LINKEDIN_POST.md                LinkedIn post for this project
```

---

## 12. How to Run

### Prerequisites

- Microsoft SQL Server 2016 or later
- The `Zenith-Global-Data-Warehouse` database populated through the full Medallion pipeline:
  ```
  ddl_structure_bronze.sql
  stor_proc_&_load_bronze_layout.sql   → EXEC bronze.load_bronze
  etl_bronze_to_silver_crm.sql         → EXEC silver.load_silver_crm
  etl_bronze_to_silver_erp.sql         → EXEC silver.load_silver_erp
  ddl_gold_layer_views.sql
  ```

### Execution Order

Run the EDA scripts in numerical order. Each script begins with:
```sql
USE [Zenith-Global-Data-Warehouse];
GO
```

You can run all sections within a file at once (F5 in SSMS) or execute individual sections interactively using the section headers as navigation guides.

### Recommended Workflow in SSMS

1. Open each `.sql` file in a new SSMS tab.
2. Run **Phase 1** completely first — confirm row counts match expectations.
3. Run **Phase 2** — review all domain values, spot any unexpected categories.
4. Run **Phase 3** — verify all orphan counts return 0, all NULL counts are acceptable.
5. Run **Phase 4** — record the statistical baseline in your notes.
6. Run **Phase 5** — note which segments dominate revenue.
7. Run **Phase 6** — capture the top/bottom performers for the final report.

### Performance Notes

- All 5 gold objects are SQL **Views** over silver tables — every query re-executes the underlying joins.
- On a local SQL Server instance with ~117,000 silver rows, all queries run in under 5 seconds.
- For larger environments, consider materializing the gold views into indexed tables before running the ranking queries.

---
