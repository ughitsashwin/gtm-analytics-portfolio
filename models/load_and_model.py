# models/load_and_model.py
#
# WHY DUCKDB?
# DuckDB is an "in-process" analytical database. Unlike Postgres (which runs
# as a server), DuckDB is just a file on your disk. You query it with SQL
# but it's built for analytics — columnar storage, fast aggregations.
# This is functionally identical to Snowflake for our purposes.
#
# WHY SEPARATE LAYERS (staging → marts → metrics)?
# This is the "dbt pattern" — a standard way to organise SQL in analytics:
#   staging  = clean raw data, one table per source, minimal transformation
#   marts    = business-level joins and logic (the "mart" is the store of truth)
#   metrics  = pre-calculated numbers that power dashboards and reports

import duckdb
import os

# ── CONNECT TO DUCKDB ─────────────────────────────────────────────────────────
# This creates a file called gtm_warehouse.duckdb in the data/raw/ folder.
# Every table we create is stored in this file persistently.
con = duckdb.connect("data/raw/gtm_warehouse.duckdb")

print("Loading seed data into DuckDB...")

# ── STAGING LAYER: Load raw CSVs ──────────────────────────────────────────────
# We read the CSVs and store them as raw tables.
# The "staging" convention: keep column names close to the source, do minimal
# transformation — mainly casting types and renaming confusing columns.

con.execute("""
    CREATE OR REPLACE TABLE stg_accounts AS
    SELECT
        account_id,
        account_name,
        region,
        industry,
        CAST(created_at AS DATE) AS created_at   -- Ensure proper date type
    FROM read_csv_auto('data/seed/accounts.csv')
""")

con.execute("""
    CREATE OR REPLACE TABLE stg_opportunities AS
    SELECT
        opportunity_id,
        account_id,
        opportunity_name,
        stage,
        CAST(amount AS DOUBLE) AS amount,
        region,
        product,
        CAST(created_at AS DATE)   AS created_at,
        CAST(close_date AS DATE)   AS close_date,
        owner,
        -- Derived flag: is this deal in the active pipeline?
        -- "Closed Won" and "Closed Lost" are terminal states.
        CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN TRUE ELSE FALSE END AS is_open
    FROM read_csv_auto('data/seed/opportunities.csv')
""")

con.execute("""
    CREATE OR REPLACE TABLE stg_subscriptions AS
    SELECT
        subscription_id,
        account_id,
        product,
        CAST(mrr AS DOUBLE)            AS mrr,
        CAST(expansion_mrr AS DOUBLE)  AS expansion_mrr,
        CAST(total_mrr AS DOUBLE)      AS total_mrr,
        region,
        CAST(start_date AS DATE)       AS start_date,
        TRY_CAST(end_date AS DATE)     AS end_date,   -- TRY_CAST handles NULLs safely
        CAST(is_active AS BOOLEAN)     AS is_active
    FROM read_csv_auto('data/seed/subscriptions.csv')
""")

print("Staging tables created: stg_accounts, stg_opportunities, stg_subscriptions")

# ── MARTS LAYER: Business-level joins ─────────────────────────────────────────
# Now we join tables to create enriched business views.
# Think of a "mart" as a pre-joined table that a BI tool or analyst can query
# without needing to know how the raw tables relate.

con.execute("""
    CREATE OR REPLACE TABLE mart_pipeline AS
    SELECT
        o.opportunity_id,
        o.opportunity_name,
        o.stage,
        o.amount,
        o.region,
        o.product,
        o.close_date,
        o.owner,
        o.is_open,
        a.account_name,
        a.industry,
        -- Days until close: useful for urgency scoring and pipeline velocity
        DATEDIFF('day', CURRENT_DATE, o.close_date) AS days_to_close
    FROM stg_opportunities o
    LEFT JOIN stg_accounts a ON o.account_id = a.account_id
""")

con.execute("""
    CREATE OR REPLACE TABLE mart_revenue AS
    SELECT
        s.subscription_id,
        s.product,
        s.mrr,
        s.expansion_mrr,
        s.total_mrr,
        s.region,
        s.start_date,
        s.end_date,
        s.is_active,
        a.account_name,
        a.industry,
        -- ARR = MRR × 12 (Annual Recurring Revenue)
        s.total_mrr * 12 AS arr
    FROM stg_subscriptions s
    LEFT JOIN stg_accounts a ON s.account_id = a.account_id
""")

print("Mart tables created: mart_pipeline, mart_revenue")

# ── METRICS LAYER: Pre-calculated KPIs ────────────────────────────────────────
# This is the crown jewel of Phase 1 — defining your "source of truth" metrics.
# These are the numbers that go on dashboards and into board decks.

# METRIC: MRR Summary by region
# MRR = Monthly Recurring Revenue from active subscriptions only
con.execute("""
    CREATE OR REPLACE TABLE metric_mrr_by_region AS
    SELECT
        region,
        COUNT(*)                    AS active_customers,
        ROUND(SUM(mrr), 2)          AS base_mrr,
        ROUND(SUM(expansion_mrr), 2) AS expansion_mrr,
        ROUND(SUM(total_mrr), 2)    AS total_mrr,
        ROUND(SUM(arr), 2)          AS total_arr
    FROM mart_revenue
    WHERE is_active = TRUE          -- Only count active subscriptions!
    GROUP BY region
    ORDER BY total_mrr DESC
""")

# METRIC: NRR (Net Revenue Retention)
# NRR formula: (Active MRR + Expansion MRR) / Starting MRR
# A ratio > 1.0 means you're growing revenue from existing customers alone.
# This is THE key SaaS health metric.
con.execute("""
    CREATE OR REPLACE TABLE metric_nrr AS
    SELECT
        region,
        ROUND(SUM(CASE WHEN is_active THEN mrr ELSE 0 END), 2)           AS retained_mrr,
        ROUND(SUM(CASE WHEN is_active THEN expansion_mrr ELSE 0 END), 2) AS expansion_mrr,
        ROUND(SUM(mrr), 2)                                                AS starting_mrr,
        -- NRR = (retained + expansion) / total starting MRR
        ROUND(
            (SUM(CASE WHEN is_active THEN mrr + expansion_mrr ELSE 0 END) / 
             NULLIF(SUM(mrr), 0)) * 100,
        1) AS nrr_pct
    FROM stg_subscriptions
    GROUP BY region
""")

# METRIC: Pipeline health by stage
# Pipeline coverage = open pipeline value / quota (we'll mock quota here)
# Stage distribution tells you if deals are bunched at top or bottom of funnel
con.execute("""
    CREATE OR REPLACE TABLE metric_pipeline_health AS
    SELECT
        region,
        stage,
        COUNT(*)            AS deal_count,
        ROUND(SUM(amount), 2) AS total_value,
        ROUND(AVG(amount), 2) AS avg_deal_size,
        -- Win rate by stage (only meaningful for closed stages)
        ROUND(
            COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) * 100.0 /
            NULLIF(COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END), 0),
        1) AS win_rate_pct
    FROM mart_pipeline
    GROUP BY region, stage
    ORDER BY region, 
        CASE stage   -- Custom sort: funnel order, not alphabetical
            WHEN 'Prospecting'  THEN 1
            WHEN 'Qualification' THEN 2
            WHEN 'Demo'         THEN 3
            WHEN 'Proposal'     THEN 4
            WHEN 'Negotiation'  THEN 5
            WHEN 'Closed Won'   THEN 6
            WHEN 'Closed Lost'  THEN 7
        END
""")

print("Metric tables created: metric_mrr_by_region, metric_nrr, metric_pipeline_health")

# ── QUICK SANITY CHECK ────────────────────────────────────────────────────────
# Always verify your metrics look reasonable after building them.
print("\n── MRR by region ─────────────────────────────────────────────")
print(con.execute("SELECT * FROM metric_mrr_by_region").df().to_string(index=False))

print("\n── NRR by region ─────────────────────────────────────────────")
print(con.execute("SELECT * FROM metric_nrr").df().to_string(index=False))

print("\n── Pipeline health (open deals only) ────────────────────────")
print(con.execute("""
    SELECT region, stage, deal_count, total_value, avg_deal_size
    FROM metric_pipeline_health
    WHERE stage NOT IN ('Closed Won', 'Closed Lost')
    ORDER BY region, total_value DESC
""").df().to_string(index=False))

con.close()
print("\nPhase 1 complete. Warehouse saved to data/raw/gtm_warehouse.duckdb")