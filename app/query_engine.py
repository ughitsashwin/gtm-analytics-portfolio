# app/query_engine.py
import os
import sys
import duckdb
import pandas as pd
from google import genai
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv()

client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])
DB_PATH = "data/raw/gtm_warehouse.duckdb"

SCHEMA_CONTEXT = """
You have access to a GTM analytics DuckDB warehouse with these tables:

TABLE: mart_revenue (pre-joined subscriptions + accounts — USE THIS for upsell/expansion queries)
- subscription_id, account_name, industry, region, product (Starter/Growth/Enterprise)
- mrr (float), expansion_mrr (float), total_mrr (float), arr (float)
- start_date (date), end_date (date), is_active (boolean)

TABLE: mart_pipeline (pre-joined opportunities + accounts — USE THIS for pipeline/deal queries)
- opportunity_id, account_name, industry, region, product, stage, amount (float)
- close_date (date), owner, is_open (boolean), days_to_close (integer)

TABLE: metric_mrr_by_region
- region, active_customers, base_mrr, expansion_mrr, total_mrr, total_arr

TABLE: metric_nrr
- region, retained_mrr, expansion_mrr, starting_mrr, nrr_pct

TABLE: metric_pipeline_health
- region, stage, deal_count, total_value, avg_deal_size, win_rate_pct

TABLE: stg_accounts
- account_id, account_name, region, industry, created_at (date)

TABLE: stg_opportunities
- opportunity_id, account_id, stage, amount, region, product, created_at, close_date, owner, is_open (boolean)

TABLE: stg_subscriptions
- subscription_id, account_id, product, mrr, expansion_mrr, total_mrr, region, start_date, end_date, is_active (boolean)

IMPORTANT RULES:
- For upsell/expansion queries: use mart_revenue, filter is_active = TRUE
- For pipeline queries: use mart_pipeline, filter is_open = TRUE for open deals
- For region summaries: use metric_mrr_by_region or metric_nrr
- Never SELECT a column that is not listed above for that table
- Never use account_id from mart_revenue or mart_pipeline (use account_name instead)
- Always LIMIT 100 unless doing aggregations
- No semicolons at end of query
"""


def generate_sql(question: str) -> str:
    """Convert a natural language question into SQL using Gemini."""
    prompt = f"""
{SCHEMA_CONTEXT}

User question: {question}

Write a DuckDB SQL query that answers this question.
Rules:
- Return ONLY the SQL query, nothing else
- No markdown code blocks, no explanation, no semicolons
- Use mart_ tables when possible as they are pre-joined
- Always include LIMIT 100 unless the question asks for aggregates
- Make column names readable using aliases like AS "Account Name"
"""
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    sql = response.text.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


def run_sql(sql: str):
    """Execute SQL against DuckDB. Returns (DataFrame, error_or_None)."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute(sql).df()
        con.close()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)


def explain_results(question: str, df: pd.DataFrame) -> str:
    """Ask Gemini to explain query results in plain English."""
    if df.empty:
        return "No data was returned for this query."

    data_preview = df.head(20).to_string(index=False)
    prompt = f"""
A business user asked: "{question}"

The data returned was:
{data_preview}

Write a 2-3 sentence plain English answer based on this data.
Be specific — use actual numbers. Do not mention SQL or technical details.
Highlight the most important insight.
"""
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    return response.text.strip()


def answer_question(question: str) -> dict:
    """
    Full text-to-SQL pipeline:
    1. Generate SQL from natural language
    2. Run SQL against DuckDB
    3. Explain results in plain English
    """
    print(f"\nQuestion: {question}")

    try:
        sql = generate_sql(question)
        print(f"Generated SQL:\n{sql}\n")
    except Exception as e:
        return {"error": f"Failed to generate SQL: {e}", "question": question}

    df, error = run_sql(sql)
    if error:
        return {
            "question": question,
            "sql": sql,
            "error": f"SQL error: {error}",
            "data": [],
            "columns": [],
            "explanation": "The generated SQL had an error. Try rephrasing your question.",
            "row_count": 0
        }

    explanation = explain_results(question, df)
    print(f"Explanation: {explanation}\n")

    return {
        "question": question,
        "sql": sql,
        "data": df.head(50).to_dict(orient="records"),
        "columns": list(df.columns),
        "explanation": explanation,
        "row_count": len(df),
        "error": None
    }


if __name__ == "__main__":
    test_questions = [
        "What is the MRR by region?",
        "Which accounts have the highest upsell potential?",
        "What is our win rate by region?",
    ]
    for q in test_questions:
        result = answer_question(q)
        print(f"Q: {result['question']}")
        print(f"Explanation: {result['explanation']}")
        print(f"Rows: {result['row_count']}")
        print("-" * 50)