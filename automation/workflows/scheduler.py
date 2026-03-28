# automation/workflows/scheduler.py
#
# WHAT IS A SCHEDULER?
# A scheduler is a process that sits in the background and triggers
# functions at set times — like a cron job, but written in Python.
#
# We use the `schedule` library here. In production, you'd replace this
# with a proper tool like:
#   - Airflow (industry standard for data pipelines)
#   - GitHub Actions (free, runs in the cloud on a cron schedule)
#   - n8n (visual workflow builder — no code)
#
# For this portfolio, Python's `schedule` library demonstrates the concept
# clearly without requiring cloud infrastructure.

import schedule
import time
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from automation.workflows.gtm_pipeline import run_gtm_pipeline


def start_scheduler():
    """
    Set up the schedule and run indefinitely.

    HOW `schedule` WORKS:
    - You define jobs with schedule.every().monday.at("09:00").do(fn)
    - schedule.run_pending() checks if any scheduled jobs are due
    - The while True loop keeps the process alive and checks every 60 seconds
    - When Monday 9am hits, run_gtm_pipeline() is called automatically
    """

    # Run every Monday at 9:00am
    schedule.every().monday.at("09:00").do(run_gtm_pipeline)

    # Also run every day at 8:00am — useful for daily standups
    # Comment this out if you only want weekly reports
    # schedule.every().day.at("08:00").do(run_gtm_pipeline)

    print("Scheduler started.")
    print("Jobs scheduled:")
    for job in schedule.jobs:
        print(f"  - {job}")
    print("\nPress Ctrl+C to stop.\n")

    # Keep the process running and check for pending jobs every 60 seconds
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    # Run the pipeline once immediately so you can see it working
    # then start the schedule
    print("Running pipeline once immediately to verify setup...")
    run_gtm_pipeline()

    print("\nStarting weekly schedule...")
    start_scheduler()
    