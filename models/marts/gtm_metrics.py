# models/marts/gtm_metrics.py
#
# PHASE 2 — GTM METRICS OWNERSHIP
#
# This file builds the four core analytical views that a GTM Analytics
# function owns. Each function connects to the warehouse, runs a query,
# and returns a clean pandas DataFrame — ready for reporting or AI summarisation.
#
# WHY FUNCTIONS INSTEAD OF JUST SQL FILES?
# In a real dbt project these would be .sql files run by the dbt CLI.
# Since we're keeping things simple and Python-native, we wrap each
# model in a function. Same logic, easier to call from other scripts
# (like our LLM insight generator in Phase 3).

import duckdb
import pandas as pd

# Always connect to the same warehouse file
DB_PATH = "data/raw/gtm_warehouse.duckdb"

def get_connection():
    """Return a DuckDB connection. Called at the start of each function."""
    return duckdb.connect(DB_PATH)


# ── 1. PIPELINE COVERAGE ──────────────────────────────────────────────────────
# Pipeline coverage = open pipeline value / quota
#
# WHY THIS MATTERS:
# If quota is $1M and you only have $1.5M in pipeline, you're at 1.5x coverage.
# Most sales orgs target 3-4x coverage because not all deals close.
# Low coverage = early warning that you'll miss the quarter.

def pipeline_coverage(quota_by_region: dict = None) -> pd.DataFrame:
    """
    Calculate pipeline coverage ratio per region.

    Args:
        quota_by_region: dict of {region: quarterly_quota}.
                         Defaults to mock quotas if not provided.
    Returns:
        DataFrame with coverage ratio and gap analysis.
    """
    # Mock quotas — in a real setup these come from Finance/RevOps
    if quota_by_region is None:
        quota_by_region = {
            "AMER": 2_000_000,
            "APAC": 2_500_000,
            "EMEA": 1_500_000,
        }

    con = get_connection()

    # Pull open pipeline value by region and stage
    pipeline_df = con.execute("""
        SELECT
            region,
            stage,
            COUNT(*)              AS deal_count,
            SUM(amount)           AS pipeline_value,
            -- Weight pipeline by stage probability
            -- Earlier stages are less likely to close, so we weight them down.
            -- This is called "weighted pipeline" — a more honest view of coverage.
            SUM(amount * CASE stage
                WHEN 'Prospecting'   THEN 0.10
                WHEN 'Qualification' THEN 0.20
                WHEN 'Demo'          THEN 0.35
                WHEN 'Proposal'      THEN 0.60
                WHEN 'Negotiation'   THEN 0.80
                ELSE 0
            END) AS weighted_pipeline_value
        FROM mart_pipeline
        WHERE is_open = TRUE
        GROUP BY region, stage
        ORDER BY region, stage
    """).df()

    con.close()

    # Aggregate to region level for coverage calculation
    region_summary = pipeline_df.groupby("region").agg(
        deal_count=("deal_count", "sum"),
        raw_pipeline=("pipeline_value", "sum"),
        weighted_pipeline=("weighted_pipeline_value", "sum"),
    ).reset_index()

    # Add quota and calculate coverage ratios
    region_summary["quota"] = region_summary["region"].map(quota_by_region)
    region_summary["raw_coverage_x"] = (
        region_summary["raw_pipeline"] / region_summary["quota"]
    ).round(2)
    region_summary["weighted_coverage_x"] = (
        region_summary["weighted_pipeline"] / region_summary["quota"]
    ).round(2)
    # Gap = how much MORE pipeline you need to feel safe (at 3x target)
    region_summary["pipeline_gap"] = (
        (region_summary["quota"] * 3) - region_summary["raw_pipeline"]
    ).clip(lower=0).round(0)  # clip at 0 — negative gap means you're covered

    return region_summary


# ── 2. MRR WATERFALL (Movement Analysis) ─────────────────────────────────────
# The MRR waterfall breaks revenue change into its components:
#   New MRR      = revenue from brand new customers
#   Expansion MRR = additional revenue from existing customers (upsells)
#   Churned MRR  = revenue lost from cancellations
#   Net MRR change = New + Expansion - Churned
#
# WHY THIS MATTERS:
# Total MRR going up doesn't tell you WHY. The waterfall shows whether growth
# is healthy (driven by expansion) or masking a churn problem.

def mrr_waterfall() -> pd.DataFrame:
    """
    Build an MRR movement waterfall by region.
    Simulates a monthly snapshot view using subscription start/end dates.
    """
    con = get_connection()

    waterfall = con.execute("""
        SELECT
            region,

            -- New MRR: subscriptions that started in the last 90 days
            ROUND(SUM(CASE
                WHEN start_date >= CURRENT_DATE - INTERVAL '90 days'
                AND is_active = TRUE
                THEN mrr ELSE 0
            END), 2) AS new_mrr,

            -- Expansion MRR: upsell revenue on active accounts
            ROUND(SUM(CASE
                WHEN is_active = TRUE
                THEN expansion_mrr ELSE 0
            END), 2) AS expansion_mrr,

            -- Churned MRR: revenue from subscriptions that ended in last 90 days
            ROUND(SUM(CASE
                WHEN is_active = FALSE
                AND end_date >= CURRENT_DATE - INTERVAL '90 days'
                THEN mrr ELSE 0
            END), 2) AS churned_mrr,

            -- Net = New + Expansion - Churn
            ROUND(
                SUM(CASE WHEN start_date >= CURRENT_DATE - INTERVAL '90 days'
                    AND is_active = TRUE THEN mrr ELSE 0 END)
                + SUM(CASE WHEN is_active = TRUE THEN expansion_mrr ELSE 0 END)
                - SUM(CASE WHEN is_active = FALSE
                    AND end_date >= CURRENT_DATE - INTERVAL '90 days'
                    THEN mrr ELSE 0 END),
            2) AS net_mrr_change

        FROM stg_subscriptions
        GROUP BY region
        ORDER BY net_mrr_change DESC
    """).df()

    con.close()
    return waterfall


# ── 3. CROSS-SELL / EXPANSION SIGNALS ────────────────────────────────────────
# Expansion signals = accounts that look ripe for an upsell conversation.
# This is "cross-sell coverage" — identifying which accounts the CS or
# Sales team should be calling this week.
#
# Signal logic (simple rule-based version — Phase 3 will make this smarter with LLM):
#   - Active customer on Starter or Growth (room to upgrade)
#   - No expansion MRR yet (hasn't been upsold)
#   - Account age > 90 days (established enough to have a conversation)

def expansion_signals() -> pd.DataFrame:
    """
    Identify accounts with high upsell potential.
    Returns a ranked list the CS/Sales team can act on.
    """
    con = get_connection()

    signals = con.execute("""
        SELECT
            a.account_name,
            a.region,
            a.industry,
            s.product                       AS current_product,
            ROUND(s.mrr, 2)                 AS current_mrr,
            s.expansion_mrr,

            -- Potential MRR if they upgraded to the next tier
            ROUND(CASE s.product
                WHEN 'Starter'  THEN 2000 - s.mrr   -- Starter → Growth delta
                WHEN 'Growth'   THEN 8000 - s.mrr   -- Growth → Enterprise delta
                ELSE 0
            END, 2) AS upsell_mrr_opportunity,

            -- How long have they been a customer? Longer = more trust = easier convo
            DATEDIFF('day', s.start_date, CURRENT_DATE) AS days_as_customer

        FROM stg_subscriptions s
        JOIN stg_accounts a ON s.account_id = a.account_id

        WHERE
            s.is_active = TRUE
            AND s.product != 'Enterprise'       -- Already at top tier, no upsell
            AND s.expansion_mrr = 0             -- Not yet expanded
            AND s.start_date <= CURRENT_DATE - INTERVAL '90 days'  -- Established customer

        ORDER BY upsell_mrr_opportunity DESC    -- Biggest opportunity first
        LIMIT 20                                -- Top 20 accounts to prioritise
    """).df()

    con.close()
    return signals


# ── 4. REGIONAL PERFORMANCE SUMMARY ──────────────────────────────────────────
# A single consolidated view combining pipeline, revenue, and retention
# per region. This is the "exec summary" table — what a CRO or VP Sales
# would look at in a Monday morning review.

def regional_performance() -> pd.DataFrame:
    """
    Consolidated regional scorecard combining pipeline + revenue + NRR.
    """
    con = get_connection()

    scorecard = con.execute("""
        SELECT
            r.region,

            -- Revenue metrics
            r.active_customers,
            r.total_mrr,
            r.total_arr,

            -- NRR (from our pre-built metric table)
            n.nrr_pct,

            -- Pipeline metrics (open deals only)
            COUNT(DISTINCT p.opportunity_id)    AS open_deals,
            ROUND(SUM(CASE WHEN p.is_open THEN p.amount ELSE 0 END), 2) AS open_pipeline,
            ROUND(AVG(CASE WHEN p.is_open THEN p.amount END), 2)        AS avg_deal_size,

            -- Win rate (closed deals only)
            ROUND(
                COUNT(CASE WHEN p.stage = 'Closed Won' THEN 1 END) * 100.0 /
                NULLIF(COUNT(CASE WHEN p.stage IN ('Closed Won','Closed Lost') THEN 1 END), 0),
            1) AS win_rate_pct

        FROM metric_mrr_by_region r
        LEFT JOIN metric_nrr n          ON r.region = n.region
        LEFT JOIN mart_pipeline p       ON r.region = p.region
        GROUP BY r.region, r.active_customers, r.total_mrr, r.total_arr, n.nrr_pct
        ORDER BY r.total_arr DESC
    """).df()

    con.close()
    return scorecard


# ── MAIN: Run all four models and print results ───────────────────────────────
if __name__ == "__main__":

    print("\n── 1. PIPELINE COVERAGE ──────────────────────────────────────")
    coverage = pipeline_coverage()
    print(coverage.to_string(index=False))

    print("\n── 2. MRR WATERFALL ──────────────────────────────────────────")
    waterfall = mrr_waterfall()
    print(waterfall.to_string(index=False))

    print("\n── 3. EXPANSION SIGNALS (top 10) ─────────────────────────────")
    signals = expansion_signals()
    print(signals.head(10).to_string(index=False))

    print("\n── 4. REGIONAL SCORECARD ─────────────────────────────────────")
    scorecard = regional_performance()
    print(scorecard.to_string(index=False))