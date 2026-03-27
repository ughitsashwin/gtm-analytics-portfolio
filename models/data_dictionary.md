# GTM Analytics — Data Dictionary

This file defines every metric in our warehouse.
In a real team, this is the "source of truth" for what numbers mean.

## Tables

### stg_accounts
| Column       | Type    | Description |
|-------------|---------|-------------|
| account_id  | string  | Unique ID for each company (e.g. ACC-0001) |
| account_name| string  | Company name |
| region      | string  | AMER, EMEA, or APAC |
| industry    | string  | Vertical (SaaS, FinTech, etc.) |
| created_at  | date    | When the account was created in CRM |

### stg_opportunities
| Column          | Type    | Description |
|----------------|---------|-------------|
| opportunity_id | string  | Unique deal ID |
| stage          | string  | Current pipeline stage |
| amount         | float   | Expected deal value in USD |
| is_open        | boolean | TRUE if deal is still in-progress |
| close_date     | date    | Expected or actual close date |

### stg_subscriptions
| Column         | Type    | Description |
|---------------|---------|-------------|
| mrr            | float   | Base monthly recurring revenue in USD |
| expansion_mrr  | float   | Additional MRR from upsells/expansions |
| total_mrr      | float   | mrr + expansion_mrr |
| is_active      | boolean | TRUE = paying customer, FALSE = churned |

## Key Metrics

### MRR (Monthly Recurring Revenue)
- **Definition**: Sum of `total_mrr` for all `is_active = TRUE` subscriptions
- **Why it matters**: Core health metric — shows revenue run rate
- **Pitfall**: Never include churned subscriptions in MRR

### NRR (Net Revenue Retention)
- **Definition**: `(retained_mrr + expansion_mrr) / starting_mrr × 100`
- **Target**: > 100% means existing customers are growing revenue
- **Why it matters**: NRR > 100% = the business grows even with zero new sales

### Pipeline Coverage
- **Definition**: Total open pipeline value / sales quota
- **Target**: 3–4× quota is healthy (not all deals close)
- **Pitfall**: Count by close date window, not all-time pipeline
