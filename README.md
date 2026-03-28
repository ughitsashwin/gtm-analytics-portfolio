# GTM Analytics Portfolio

An end-to-end GTM analytics system built to demonstrate five core capabilities modern revenue teams need: data infrastructure, metrics ownership, AI-powered insights, analytical automation, and self-serve tooling.

Built with Python, DuckDB, dbt-style SQL models, Google Gemini, and Flask.

---

## What this project does

Most GTM analytics work is manual — analysts pull data, write commentary, and send reports. This project automates that entire loop:

1. **A local data warehouse** stores mock CRM data (accounts, deals, subscriptions) modelled in clean SQL layers
2. **Metric models** calculate MRR, NRR, pipeline coverage, and expansion signals as reusable functions
3. **An LLM workflow** reads those metrics every week and writes an executive narrative + anomaly report automatically
4. **A scheduler** delivers that report to Slack every Monday morning without anyone triggering it
5. **A web app** lets any non-technical stakeholder ask questions in plain English and get answers backed by live data

---

## Live demo

> Ask the tool: *"Which accounts have the highest upsell potential?"*

The app generates SQL, queries the warehouse, and returns a plain-English answer with the underlying data table — no SQL knowledge required.

---

## Project structure

```
gtm-analytics-portfolio/
├── data/
│   ├── seed/                  # Mock CRM data generation (accounts, deals, subscriptions)
│   └── raw/                   # DuckDB warehouse file (git-ignored, generated locally)
├── models/
│   ├── staging/               # Clean source tables (stg_accounts, stg_opportunities, stg_subscriptions)
│   ├── marts/                 # Business-level joins (mart_pipeline, mart_revenue)
│   └── metrics/               # Pre-calculated KPIs (MRR, NRR, pipeline health)
├── insights/
│   ├── llm_flows/             # LLM-powered insight generation and anomaly detection
│   └── prompts/               # Reusable prompt templates
├── automation/
│   ├── workflows/             # End-to-end pipeline orchestrator + Slack notifier
│   └── alerts/                # Threshold-based alerting rules
└── app/
    ├── query_engine.py        # Text-to-SQL engine (NL → SQL → results → explanation)
    └── server.py              # Flask web server
```

---

## Phases

### Phase 1 — Data foundation

Generates a realistic mock GTM dataset (80 accounts, 200 opportunities, 120 subscriptions) and loads it into a DuckDB warehouse using a layered SQL architecture that mirrors how production dbt projects are structured.

- Staging layer: raw source tables with type casting and light cleaning
- Marts layer: pre-joined business views (pipeline + revenue)
- Metrics layer: pre-calculated KPIs ready for reporting

### Phase 2 — GTM metrics ownership

Defines the "source of truth" metrics a GTM Analytics function owns — built as reusable Python functions that any downstream script can call.

- **Pipeline coverage**: raw and weighted coverage ratio vs quota, with gap analysis
- **MRR waterfall**: new, expansion, and churned MRR broken down by region
- **Expansion signals**: ranked list of accounts with upsell potential based on tenure, product tier, and zero expansion to date
- **Regional scorecard**: consolidated view of ARR, NRR, open pipeline, avg deal size, and win rate per region

### Phase 3 — AI-powered insights

Connects the metric layer to an LLM (Google Gemini) to automate two analytical tasks that typically take hours of manual work each week.

- **Weekly narrative summary**: pulls all metric DataFrames, sends to Gemini, returns a structured executive summary with regional bullets and a recommended action
- **Anomaly detection**: scans metrics against defined thresholds (NRR < 90%, weighted coverage < 0.5x, churn > new MRR) and generates a flagged report with business risk explanations and investigation steps

### Phase 4 — Analytical automation

Wires the full pipeline together into a scheduled, autonomous workflow.

- **Orchestrator**: single function that runs metrics → LLM → Slack in sequence with error handling at each step
- **Slack delivery**: formats output using Slack Block Kit and posts to a configured channel via Incoming Webhook
- **Scheduler**: runs the full pipeline every Monday at 9am using Python's `schedule` library — no human trigger required

### Phase 5 — Self-serve analytics tool

A Flask web app that lets non-technical stakeholders query the warehouse in plain English.

- User types a question → Gemini writes SQL → DuckDB executes it → Gemini explains the results
- Shows the generated SQL (collapsible) so technical users can inspect and learn
- Handles SQL errors gracefully with user-friendly messages
- Ships with five example queries covering the most common GTM questions

---

## Key metrics defined

| Metric | Definition |
|--------|-----------|
| MRR | Sum of `total_mrr` for `is_active = TRUE` subscriptions |
| NRR | `(retained_mrr + expansion_mrr) / starting_mrr × 100` |
| Weighted pipeline | Pipeline value × stage probability (10%–80% by stage) |
| Pipeline coverage | Weighted pipeline / quarterly quota |
| Expansion signal | Active, non-Enterprise account with zero expansion MRR and 90+ days tenure |

---

## Tech stack

| Layer | Tool |
|-------|------|
| Warehouse | DuckDB (local), migratable to Snowflake |
| Transformation | dbt-style layered SQL models |
| AI / LLM | Google Gemini 2.5 Flash |
| Automation | Python `schedule` + Slack Incoming Webhooks |
| Web app | Flask + vanilla JS |
| Language | Python 3.13 |
| Version control | Git / GitHub |

---

## Setup

```bash
# Clone the repo
git clone https://github.com/ughitsashwin/gtm-analytics-portfolio.git
cd gtm-analytics-portfolio

# Install dependencies
pip install -r requirements.txt

# Add environment variables
cp .env.example .env
# Edit .env and add your GOOGLE_API_KEY and SLACK_WEBHOOK_URL

# Generate mock data and build the warehouse
python data/seed/generate_data.py
python models/load_and_model.py

# Run the self-serve query tool
python app/server.py
# Open http://localhost:5000

# Run the full automated pipeline manually
python automation/workflows/gtm_pipeline.py

# Start the weekly scheduler
python automation/workflows/scheduler.py
```

---

## Environment variables

Create a `.env` file in the project root (use `.env.example` as a template):

```
GOOGLE_API_KEY=your_google_ai_studio_key
SLACK_WEBHOOK_URL=your_slack_incoming_webhook_url
```

Get a free Google AI Studio key at [aistudio.google.com](https://aistudio.google.com).

Set up a Slack Incoming Webhook at [api.slack.com/apps](https://api.slack.com/apps).

---

## Sample outputs

**Weekly summary (generated by Gemini):**

> The most critical issue is the severely under-weighted sales pipeline across all regions, posing significant risk to quota attainment. AMER leads on ARR ($2.07M) but weighted pipeline coverage is a critical 0.38x. EMEA is the only region with healthy NRR at 102.9%. Recommended action: urgently address pipeline conversion blockers and implement targeted churn reduction in AMER and APAC.

**Anomaly report flags:**

- NRR APAC: 89.7% — critical (below 90% threshold)
- NRR AMER: 92.0% — warning
- Weighted pipeline AMER: 0.38x — critical
- Churn MRR exceeds new MRR in APAC and AMER

---

*Built as a learning project to demonstrate GTM analytics engineering capabilities.*
