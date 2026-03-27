# data/seed/generate_data.py
#
# WHY THIS EXISTS:
# Real GTM analytics work requires CRM data (accounts, deals, revenue).
# Since we don't have a real CRM, we generate realistic fake data here.
# This is a very common practice when building or testing analytics pipelines.

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)  # Fixed seed = same data every run. Good for reproducibility.

# ── CONFIG ────────────────────────────────────────────────────────────────────
NUM_ACCOUNTS = 80
NUM_OPPS = 200
NUM_SUBSCRIPTIONS = 120

REGIONS = ["EMEA", "AMER", "APAC"]
STAGES = ["Prospecting", "Qualification", "Demo", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
PRODUCTS = ["Starter", "Growth", "Enterprise"]

# Product → monthly price mapping (MRR in $)
PRODUCT_PRICES = {
    "Starter":    500,
    "Growth":    2000,
    "Enterprise": 8000,
}

# ── ACCOUNTS TABLE ────────────────────────────────────────────────────────────
# Think of this as your Salesforce Accounts object.
# Each row = one customer or prospect company.
accounts = []
for i in range(NUM_ACCOUNTS):
    accounts.append({
        "account_id":   f"ACC-{i+1:04d}",          # e.g. ACC-0001
        "account_name": fake.company(),
        "region":       random.choice(REGIONS),
        "industry":     random.choice(["SaaS", "FinTech", "HealthTech", "eCommerce", "Logistics"]),
        "created_at":   fake.date_between(start_date="-3y", end_date="-6m"),
    })

accounts_df = pd.DataFrame(accounts)
accounts_df.to_csv("data/seed/accounts.csv", index=False)
print(f"Generated {len(accounts_df)} accounts")

# ── OPPORTUNITIES TABLE ───────────────────────────────────────────────────────
# This mirrors Salesforce Opportunities — every active or historical deal.
# Pipeline health metrics (coverage, velocity, win rate) all come from this table.
opps = []
for i in range(NUM_OPPS):
    account = random.choice(accounts)   # Link each opp to an existing account
    stage = random.choice(STAGES)
    created = fake.date_between(start_date="-18m", end_date="today")

    # Closed deals need a close date; open ones have an expected close date
    if "Closed" in stage:
        close_date = created + timedelta(days=random.randint(14, 120))
    else:
        close_date = datetime.today().date() + timedelta(days=random.randint(7, 90))

    opps.append({
        "opportunity_id":    f"OPP-{i+1:04d}",
        "account_id":        account["account_id"],
        "opportunity_name":  f"{account['account_name']} - {random.choice(PRODUCTS)}",
        "stage":             stage,
        "amount":            random.choice([5000, 10000, 25000, 50000, 100000, 200000]),
        "region":            account["region"],
        "product":           random.choice(PRODUCTS),
        "created_at":        created,
        "close_date":        close_date,
        "owner":             fake.name(),
    })

opps_df = pd.DataFrame(opps)
opps_df.to_csv("data/seed/opportunities.csv", index=False)
print(f"Generated {len(opps_df)} opportunities")

# ── SUBSCRIPTIONS TABLE ───────────────────────────────────────────────────────
# This is your revenue table — each row is one active or churned subscription.
# MRR (Monthly Recurring Revenue) and NRR (Net Revenue Retention) come from here.
#
# KEY CONCEPT — MRR vs ARR:
#   MRR = what a customer pays per month
#   ARR = MRR × 12 (annualised)
#   NRR = (starting MRR + expansion - contraction - churn) / starting MRR
#         A healthy SaaS company targets NRR > 100% (expansions > churn)
subscriptions = []
for i in range(NUM_SUBSCRIPTIONS):
    account = random.choice(accounts)
    product = random.choice(PRODUCTS)
    base_mrr = PRODUCT_PRICES[product]

    # Add some noise to MRR — real deals are negotiated, not list price
    mrr = base_mrr * random.uniform(0.8, 1.3)
    mrr = round(mrr / 100) * 100   # Round to nearest $100 for realism

    start_date = fake.date_between(start_date="-2y", end_date="-1m")

    # ~20% of subscriptions have churned — important for NRR calculation
    is_churned = random.random() < 0.20
    end_date = (start_date + timedelta(days=random.randint(60, 400))) if is_churned else None

    # ~30% of active subs have had an expansion (upsell)
    has_expansion = (not is_churned) and (random.random() < 0.30)
    expansion_mrr = round(mrr * random.uniform(0.2, 0.8)) if has_expansion else 0

    subscriptions.append({
        "subscription_id":  f"SUB-{i+1:04d}",
        "account_id":       account["account_id"],
        "product":          product,
        "mrr":              mrr,
        "expansion_mrr":    expansion_mrr,          # Additional MRR from upsells
        "total_mrr":        mrr + expansion_mrr,
        "region":           account["region"],
        "start_date":       start_date,
        "end_date":         end_date,               # NULL = still active
        "is_active":        not is_churned,
    })

subs_df = pd.DataFrame(subscriptions)
subs_df.to_csv("data/seed/subscriptions.csv", index=False)
print(f"Generated {len(subs_df)} subscriptions ({subs_df['is_active'].sum()} active)")