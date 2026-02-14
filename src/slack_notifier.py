"""
Slack Notifier Module
======================
Sends GE and Pydantic validation results to Slack as a Block Kit message.

Usage:
    send_slack_notification(ge_summary, pydantic_summary, webhook_url)
"""

import json
import requests
from datetime import datetime


def send_slack_notification(
    ge_summary: dict,
    pydantic_summary: dict,
    webhook_url: str,
) -> bool:
    """
    Send validation results to a Slack webhook.

    Args:
        ge_summary: Great Expectations validation summary
        pydantic_summary: Pydantic validation summary
        webhook_url: Slack Incoming Webhook URL

    Returns:
        True if successful, False otherwise
    """
    if not webhook_url or webhook_url == "YOUR_SLACK_WEBHOOK_URL":
        print("\n⚠️  Slack notification skipped (no webhook URL configured)")
        return False

    print("\n📤 Sending Slack notification...")

    # ── Status ───────────────────────────────────────────────────────────
    ge_ok = ge_summary.get("overall_success", False)
    py_ok = pydantic_summary.get("overall_success", False)
    all_ok = ge_ok and py_ok

    emoji = "✅" if all_ok else "❌"
    color = "#36a64f" if all_ok else "#dc3545"
    status_text = "ALL PASSED" if all_ok else "ISSUES FOUND"

    # ── GE Failed Details ────────────────────────────────────────────────
    ge_failed_text = ""
    if ge_summary.get("failed"):
        ge_failed_text = "\n".join(
            f"• *{f['expectation']}* (`{f['column']}`)"
            for f in ge_summary["failed"]
        )

    # ── Pydantic Error Details ───────────────────────────────────────────
    py_error_text = ""
    py_errors = pydantic_summary.get("errors", [])
    if py_errors:
        sample = py_errors[:5]
        py_error_text = "\n".join(
            f"• Row {e['row']}: *{e['field']}* — {e['message']}"
            for e in sample
        )
        if len(py_errors) > 5:
            py_error_text += f"\n_… and {len(py_errors) - 5} more errors_"

    # ── Slack Message (Block Kit) ────────────────────────────────────────
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Data Quality Report — {status_text}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Dataset:*\nAmazon Sales"},
                {"type": "mrkdwn", "text": f"*Timestamp:*\n{datetime.now().strftime('%Y-%m-%d %H:%M')}"},
            ],
        },
        {"type": "divider"},
        # GE Summary
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Great Expectations*"},
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{'✅' if ge_ok else '❌'} "
                        f"{ge_summary.get('passed_count', 0)} passed / "
                        f"{ge_summary.get('failed_count', 0)} failed"
                    ),
                },
            ],
        },
    ]

    if ge_failed_text:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*GE Failed Expectations:*\n{ge_failed_text}"},
            }
        )

    # Pydantic Summary
    blocks.append(
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Pydantic Validation*"},
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{'✅' if py_ok else '❌'} "
                        f"{pydantic_summary.get('valid_rows', 0)} valid / "
                        f"{pydantic_summary.get('invalid_rows', 0)} invalid rows"
                    ),
                },
            ],
        }
    )

    if py_error_text:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Pydantic Errors (sample):*\n{py_error_text}"},
            }
        )

    payload = {"attachments": [{"color": color, "blocks": blocks}]}

    # ── Send ─────────────────────────────────────────────────────────────
    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("   ✅ Slack notification sent successfully!")
            return True
        else:
            print(f"   ❌ Slack error: HTTP {resp.status_code}")
            return False
    except Exception as exc:
        print(f"   ❌ Slack connection error: {exc}")
        return False
