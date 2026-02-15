"""
Data Quality Pipeline
======================
Runs GE validation, Pydantic validation, and Slack notification in sequence.

Usage:
    python dq_pipeline.py

Environment Variable:
    SLACK_WEBHOOK_URL  →  Slack Incoming Webhook address (optional)
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv

from src.ge_validation import run_ge_validation
from src.pydantic_validation import run_pydantic_validation
from src.slack_notifier import send_slack_notification

# ── Load .env file ───────────────────────────────────────────────────────────
load_dotenv() # loaded env

# ── Settings ─────────────────────────────────────────────────────────────────

CSV_PATH = os.path.join("data", "amazon_sales.csv")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "YOUR_SLACK_WEBHOOK_URL")


# ── Pipeline ─────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 60)
    print("   📦 DATA QUALITY PIPELINE")
    print("=" * 60)

    # 1️⃣  Load data
    print(f"\n📂 Loading data from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, low_memory=False)
    print(f"   Rows: {len(df):,}  |  Columns: {len(df.columns)}")

    # 2️⃣  Great Expectations Validation
    ge_summary = run_ge_validation(df)

    # 3️⃣  Pydantic Validation
    pydantic_summary = run_pydantic_validation(df)

    # 4️⃣  Slack Notification
    send_slack_notification(ge_summary, pydantic_summary, SLACK_WEBHOOK_URL)

    # 5️⃣  Overall Summary
    all_ok = ge_summary["overall_success"] and pydantic_summary["overall_success"]

    print("\n" + "=" * 60)
    print("   PIPELINE RESULT")
    print("=" * 60)
    print(f"   GE Validation      : {'✅' if ge_summary['overall_success'] else '❌'}")
    print(f"   Pydantic Validation : {'✅' if pydantic_summary['overall_success'] else '❌'}")
    print(f"   Overall             : {'✅ ALL PASSED' if all_ok else '❌ ISSUES FOUND'}")
    print("=" * 60)

    # CI/CD exit code
    if not all_ok:
        print("\n⚠️  Pipeline failed — exiting with code 1")
        sys.exit(1)
    else:
        print("\n🎉 All validations passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
