"""
Pydantic Validation Module
===========================
Validates each row of the Amazon Sales dataset using a Pydantic model.

Model: AmazonOrder
  - Order ID   → required (str)
  - Date       → MM-DD-YY format
  - Status     → allowed values only
  - Qty        → >= 0
  - Amount     → >= 0 (optional – may be None for cancelled orders)
  - currency   → INR
  - ship-country → IN
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Optional

import pandas as pd
from pydantic import BaseModel, field_validator, ValidationError


# ── Constants ────────────────────────────────────────────────────────────────

VALID_STATUSES = [
    "Cancelled",
    "Pending",
    "Pending - Waiting for Pick Up",
    "Shipped",
    "Shipped - Damaged",
    "Shipped - Delivered to Buyer",
    "Shipped - Lost in Transit",
    "Shipped - Out for Delivery",
    "Shipped - Picked Up",
    "Shipped - Rejected by Buyer",
    "Shipped - Returned to Seller",
    "Shipped - Returning to Seller",
    "Shipping",
]

VALID_FULFILMENT = ["Merchant", "Amazon"]
VALID_CURRENCIES = ["INR"]
VALID_COUNTRIES = ["IN"]

DATE_REGEX = re.compile(r"^\d{2}-\d{2}-\d{2}$")


# ── Pydantic Model ──────────────────────────────────────────────────────────

class AmazonOrder(BaseModel):
    """Validates a single Amazon order row."""

    order_id: str
    date: str
    status: str
    fulfilment: str
    currency: str
    qty: int
    amount: Optional[float] = None
    ship_country: str

    # ── Validators ───────────────────────────────────────────────────────

    @field_validator("order_id")
    @classmethod
    def order_id_not_empty(cls, v: str) -> str:
        if not v or v.strip() == "":
            raise ValueError("Order ID cannot be empty")
        return v

    @field_validator("date")
    @classmethod
    def date_format(cls, v: str) -> str:
        if not DATE_REGEX.match(v):
            raise ValueError(f"Invalid date format: '{v}' (expected: MM-DD-YY)")
        return v

    @field_validator("status")
    @classmethod
    def status_valid(cls, v: str) -> str:
        if v not in VALID_STATUSES:
            raise ValueError(f"Invalid status: '{v}'")
        return v

    @field_validator("fulfilment")
    @classmethod
    def fulfilment_valid(cls, v: str) -> str:
        if v not in VALID_FULFILMENT:
            raise ValueError(f"Invalid fulfilment: '{v}'")
        return v

    @field_validator("currency")
    @classmethod
    def currency_valid(cls, v: str) -> str:
        if v not in VALID_CURRENCIES:
            raise ValueError(f"Invalid currency: '{v}' (expected: INR)")
        return v

    @field_validator("qty")
    @classmethod
    def qty_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"Qty cannot be negative: {v}")
        return v

    @field_validator("amount")
    @classmethod
    def amount_non_negative(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError(f"Amount cannot be negative: {v}")
        return v

    @field_validator("ship_country")
    @classmethod
    def ship_country_valid(cls, v: str) -> str:
        if v not in VALID_COUNTRIES:
            raise ValueError(f"Invalid ship-country: '{v}' (expected: IN)")
        return v


# ── Helper: NaN → None ──────────────────────────────────────────────────────

def _safe(val):
    """Convert pandas NaN / None to Python None; everything else to str."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


# ── Main Function ────────────────────────────────────────────────────────────

def run_pydantic_validation(df: pd.DataFrame) -> dict:
    """
    Validate every row in the DataFrame against the AmazonOrder model.

    Returns:
        dict: {
            total_rows, valid_rows, invalid_rows,
            error_count, errors (list[dict]),
            overall_success (bool),
            timestamp
        }
    """
    print("\n🔍 Running Pydantic Validation...")

    errors: list[dict] = []

    for idx, row in df.iterrows():
        try:
            AmazonOrder(
                order_id=str(_safe(row.get("Order ID")) or ""),
                date=str(_safe(row.get("Date")) or ""),
                status=str(_safe(row.get("Status")) or ""),
                fulfilment=str(_safe(row.get("Fulfilment")) or ""),
                currency=str(_safe(row.get("currency")) or ""),
                qty=int(row.get("Qty", 0)),
                amount=_safe(row.get("Amount")),
                ship_country=str(_safe(row.get("ship-country")) or ""),
            )
        except ValidationError as e:
            for err in e.errors():
                errors.append(
                    {
                        "row": int(idx) + 2,  # +2: header row + 0-based index
                        "field": err["loc"][-1] if err["loc"] else "unknown",
                        "message": err["msg"],
                        "value": str(err.get("input", "")),
                    }
                )

    total = len(df)
    invalid_rows = len({e["row"] for e in errors})
    valid_rows = total - invalid_rows

    summary = {
        "total_rows": total,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "error_count": len(errors),
        "errors": errors,
        "overall_success": len(errors) == 0,
        "timestamp": datetime.now().isoformat(),
    }

    # Console output
    print(f"\n{'=' * 60}")
    print("   PYDANTIC VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"   Overall : {'✅ PASSED' if summary['overall_success'] else '❌ FAILED'}")
    print(f"   Total Rows   : {total}")
    print(f"   Valid Rows   : {valid_rows}")
    print(f"   Invalid Rows : {invalid_rows}")
    print(f"   Error Count  : {len(errors)}")
    print(f"{'=' * 60}")

    if errors:
        print("\n❌ PYDANTIC ERRORS (first 10):")
        print("-" * 60)
        for e in errors[:10]:
            print(f"   Row {e['row']:>6} | {e['field']:<15} | {e['message']}")

    return summary
