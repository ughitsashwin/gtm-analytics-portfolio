import os
import sys
from google import genai
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from models.marts.gtm_metrics import (
    pipeline_coverage,
    mrr_waterfall,
    expansion_signals,
    regional_performance,
)

load_dotenv()

client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])


def load_prompt_template(filename: str) -> str:
    prompt_path = os.path.join(os.path.dirname(__file__), "../prompts", filename)
    with open(prompt_path, "r") as f:
        return f.read()


def dataframes_to_text(dataframes: dict) -> str:
    """
    Convert DataFrames into labelled text blocks for the prompt.
    The LLM reads this text and reasons over it like a spreadsheet.
    """
    sections = []
    for name, df in dataframes.items():
        sections.append(f"=== {name} ===")
        sections.append(df.to_string(index=False))
        sections.append("")
    return "\n".join(sections)


def generate_weekly_summary() -> str:
    """
    Pull GTM metrics and generate a narrative executive summary.
    Replaces 1-2 hours of manual analyst commentary writing.
    """
    print("Pulling metrics from warehouse...")
    data = {
        "Regional Scorecard":           regional_performance(),
        "Pipeline Coverage":            pipeline_coverage(),
        "MRR Waterfall (last 90 days)": mrr_waterfall(),
        "Top Expansion Signals":        expansion_signals().head(5),
    }
    data_text = dataframes_to_text(data)
    template = load_prompt_template("gtm_summary.txt")
    prompt = template.replace("{data}", data_text)

    print("Sending to Gemini for analysis...")
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    return response.text


def generate_anomaly_report() -> str:
    """
    Scan GTM metrics for values outside healthy thresholds.
    Gemini flags issues and explains the business risk in plain English.
    """
    print("Running anomaly detection...")
    data = {
        "Regional Scorecard (includes NRR and win rate)": regional_performance(),
        "Pipeline Coverage (includes weighted coverage)": pipeline_coverage(),
        "MRR Waterfall": mrr_waterfall(),
    }
    data_text = dataframes_to_text(data)
    template = load_prompt_template("anomaly_detection.txt")
    prompt = template.replace("{data}", data_text)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    return response.text


def save_output(content: str, filename: str):
    """Save generated insight to the output/ folder."""
    output_dir = os.path.join(os.path.dirname(__file__), "../../output")
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        f.write(content)
    print(f"Saved to {filepath}")


if __name__ == "__main__":

    print("\n" + "="*60)
    print("WEEKLY GTM SUMMARY")
    print("="*60)
    summary = generate_weekly_summary()
    print(summary)
    save_output(summary, "weekly_summary.txt")

    print("\n" + "="*60)
    print("ANOMALY DETECTION REPORT")
    print("="*60)
    anomalies = generate_anomaly_report()
    print(anomalies)
    save_output(anomalies, "anomaly_report.txt")