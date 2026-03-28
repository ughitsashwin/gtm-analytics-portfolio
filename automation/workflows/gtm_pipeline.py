# automation/workflows/gtm_pipeline.py
#
# PHASE 4 — ANALYTICAL AUTOMATION
#
# This is the orchestrator — it runs the full pipeline end to end:
#   1. Pull fresh metrics from the warehouse
#   2. Generate AI narrative and anomaly report
#   3. Post to Slack
#   4. Save to file as backup
#
# It can be triggered in two ways:
#   - Manually:   python automation/workflows/gtm_pipeline.py
#   - Scheduled:  python automation/workflows/scheduler.py (runs it every Monday)
#
# This pattern — a single orchestrator function that calls smaller focused
# functions — is how real data pipelines are structured. Each step is
# independently testable and the orchestrator just wires them together.

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add project root so we can import from other modules
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from insights.llm_flows.insight_generator import (
    generate_weekly_summary,
    generate_anomaly_report,
    save_output,
)
from automation.workflows.slack_notifier import post_to_slack

load_dotenv()


def run_gtm_pipeline():
    """
    Full end-to-end GTM analytics pipeline.
    This function is what the scheduler calls every Monday morning.
    """
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*60}")
    print(f"GTM PIPELINE STARTED — {run_time}")
    print(f"{'='*60}")

    # ── STEP 1: Generate weekly summary ───────────────────────────
    print("\n[1/3] Generating weekly GTM summary...")
    try:
        summary = generate_weekly_summary()
        save_output(summary, "weekly_summary.txt")
        print("Weekly summary generated")
    except Exception as e:
        print(f"ERROR generating summary: {e}")
        summary = None

    # ── STEP 2: Generate anomaly report ───────────────────────────
    print("\n[2/3] Running anomaly detection...")
    try:
        anomalies = generate_anomaly_report()
        save_output(anomalies, "anomaly_report.txt")
        print("Anomaly report generated")
    except Exception as e:
        print(f"ERROR in anomaly detection: {e}")
        anomalies = None

    # ── STEP 3: Post to Slack ──────────────────────────────────────
    print("\n[3/3] Posting to Slack...")

    if summary:
        post_to_slack(
            title=f"GTM Weekly Summary — {run_time}",
            text=summary
        )

    if anomalies:
        post_to_slack(
            title="Anomaly Detection Report",
            text=anomalies
        )

    print(f"\nPipeline complete — {run_time}")
    return True


if __name__ == "__main__":
    # Run immediately when called directly — useful for testing
    run_gtm_pipeline()