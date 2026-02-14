"""
Great Expectations Validation Module
=====================================
Runs data quality validation on the Amazon Sales dataset using Great Expectations.

Expectations:
  1. Order ID → not null, unique
  2. Qty → >= 0
  3. Amount → >= 0
  4. Status → valid set
  5. Fulfilment → Merchant | Amazon
  6. currency → INR
  7. ship-country → IN
  8. Date → MM-DD-YY regex
"""

import great_expectations as gx
import pandas as pd
from datetime import datetime


# ── Expected Values ──────────────────────────────────────────────────────────

VALID_STATUSES = [
    "Cancelled",
    "Shipped",
    "Shipped - Delivered to Buyer",
    "Pending",
    "Shipped - Returned to Seller",
    "Shipped - Rejected by Buyer",
    "Shipped - Returning to Seller",
    "Shipped - Out for Delivery",
    "Shipped - Picked Up",
]

VALID_FULFILMENT = ["Merchant", "Amazon"]
VALID_CURRENCIES = ["INR"]
VALID_COUNTRIES = ["IN"]


# ── Validation ───────────────────────────────────────────────────────────────

def run_ge_validation(df: pd.DataFrame) -> dict:
    """
    Run Great Expectations validation on the given DataFrame.

    Args:
        df: pandas DataFrame to validate

    Returns:
        dict: {
            overall_success, total_expectations,
            passed_count, failed_count,
            passed (list), failed (list),
            timestamp
        }
    """
    print("\n🔍 Running Great Expectations Validation...")

    # ── GX Context (Ephemeral – no file I/O) ─────────────────────────────
    context = gx.get_context()

    # Data Source → Asset → Batch
    data_source = context.data_sources.add_pandas("pandas_source")
    data_asset = data_source.add_dataframe_asset(name="amazon_sales")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_data")

    # ── Expectation Suite ────────────────────────────────────────────────
    suite = gx.ExpectationSuite(name="amazon_sales_suite")

    # 1. Order ID: must not be null & must be unique
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID")
    )

    # 2. Qty >= 0 (no negative quantities)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Qty", min_value=0, max_value=None
        )
    )

    # 3. Amount >= 0 (may be null for cancelled orders)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Amount", min_value=0, max_value=None
        )
    )

    # 4. Status must be in valid set
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Status", value_set=VALID_STATUSES
        )
    )

    # 5. Fulfilment: Merchant or Amazon
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Fulfilment", value_set=VALID_FULFILMENT
        )
    )

    # 6. currency must be INR
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="currency", value_set=VALID_CURRENCIES
        )
    )

    # 7. ship-country must be IN
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="ship-country", value_set=VALID_COUNTRIES
        )
    )

    # 8. Date format: MM-DD-YY
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="Date", regex=r"^\d{2}-\d{2}-\d{2}$"
        )
    )

    suite = context.suites.add(suite)

    # ── Run Validation ───────────────────────────────────────────────────
    validation_definition = gx.ValidationDefinition(
        name="amazon_sales_validation",
        data=batch_definition,
        suite=suite,
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    results = validation_definition.run(batch_parameters={"dataframe": df})

    # ── Process Results ──────────────────────────────────────────────────
    return _process_results(results)


# ── Helper ───────────────────────────────────────────────────────────────────

def _process_results(results) -> dict:
    """Convert raw GE results into a summary dictionary."""
    print("\n📊 Processing GE Results...")

    results_dict = results.to_json_dict()
    success = results_dict.get("success", False)
    expectation_results = results_dict.get("results", [])

    passed, failed = [], []

    for exp_result in expectation_results:
        exp_config = exp_result.get("expectation_config", {})
        exp_type = exp_config.get("type", "Unknown")
        column = exp_config.get("kwargs", {}).get("column", "N/A")
        success_flag = exp_result.get("success", False)
        result_detail = exp_result.get("result", {})

        info = {
            "expectation": exp_type,
            "column": column,
            "success": success_flag,
            "result": result_detail,
        }
        (passed if success_flag else failed).append(info)

    summary = {
        "overall_success": success,
        "total_expectations": len(expectation_results),
        "passed_count": len(passed),
        "failed_count": len(failed),
        "passed": passed,
        "failed": failed,
        "timestamp": datetime.now().isoformat(),
    }

    # Console output
    print(f"\n{'=' * 60}")
    print("   GE VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"   Overall : {'✅ PASSED' if success else '❌ FAILED'}")
    print(f"   Total   : {summary['total_expectations']}")
    print(f"   Passed  : {summary['passed_count']}")
    print(f"   Failed  : {summary['failed_count']}")
    print(f"{'=' * 60}")

    if failed:
        print("\n❌ FAILED EXPECTATIONS:")
        print("-" * 60)
        for f in failed:
            print(f"\n   📌 {f['expectation']}")
            print(f"      Column: {f['column']}")
            r = f.get("result", {})
            if r.get("unexpected_count"):
                print(f"      Unexpected Count  : {r['unexpected_count']}")
                print(f"      Unexpected Percent: {r.get('unexpected_percent', 0):.2f}%")
            vals = r.get("partial_unexpected_list", [])
            if vals:
                print(f"      Sample Values     : {vals[:5]}")

    if passed:
        print("\n✅ PASSED EXPECTATIONS:")
        print("-" * 60)
        for p in passed:
            print(f"   ✓ {p['expectation']} (Column: {p['column']})")

    return summary
