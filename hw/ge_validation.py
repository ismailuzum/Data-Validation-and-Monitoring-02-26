"""
📝 HOMEWORK 1 — Great Expectations (Amazon Sales Dataset)
=========================================================

Bu script, Amazon Sales dataseti üzerinde Great Expectations kullanarak
data quality validation yapar ve Slack'e bildirim gönderir.

Dataset'teki Kasıtlı Hatalar (Öğrenme Amaçlı):
----------------------------------------------
1. Satır 8:  Amount boş (NULL)
2. Satır 9:  ship-city = "Chennai" (küçük harf, tutarsızlık)
3. Satır 11: Order ID boş (NULL)
4. Satır 12: Qty = -1 (negatif değer)
5. Satır 13: currency = "USD" (INR olmalı), ship-country = "US" (IN olmalı)
6. Satır 14: Date = "invalid-date" (geçersiz tarih formatı)

Kurulum:
--------
pip install great_expectations pandas requests

Kullanım:
---------
python ge_validation.py
"""

import great_expectations as gx
import pandas as pd
import requests
import json
from datetime import datetime


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

CSV_PATH = "amazon_sales.csv"
SLACK_WEBHOOK_URL = "YOUR_SLACK_WEBHOOK_URL"  # Buraya kendi webhook URL'inizi yazın

# Beklenen değerler
VALID_STATUSES = [
    "Cancelled",
    "Shipped",
    "Shipped - Delivered to Buyer",
    "Pending",
    "Shipped - Returned to Seller",
    "Shipped - Rejected by Buyer",
    "Shipped - Returning to Seller",
    "Shipped - Out for Delivery",
    "Shipped - Picked Up"
]

VALID_FULFILMENT = ["Merchant", "Amazon"]
VALID_CURRENCIES = ["INR"]
VALID_COUNTRIES = ["IN"]


# =============================================================================
# 2. LOAD DATA
# =============================================================================

def load_data(path: str) -> pd.DataFrame:
    """CSV dosyasını yükle ve temel bilgileri göster."""
    print(f"\n📂 Loading data from: {path}")
    df = pd.read_csv(path)
    print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    print(f"   Columns: {list(df.columns)}")
    return df


# =============================================================================
# 3. GREAT EXPECTATIONS VALIDATION
# =============================================================================

def run_validation(df: pd.DataFrame) -> dict:
    """
    Great Expectations ile validation çalıştır.
    
    Returns:
        Validation sonuçlarını içeren dictionary
    """
    print("\n🔍 Running Great Expectations Validation...")
    
    # GX Context oluştur (Ephemeral - dosya yazmaz)
    context = gx.get_context()
    
    # Data Source ekle
    data_source = context.data_sources.add_pandas("pandas_source")
    
    # Data Asset ekle
    data_asset = data_source.add_dataframe_asset(name="amazon_sales")
    
    # Batch Definition oluştur
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_data")
    
    # Batch al
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    # Expectation Suite oluştur
    suite = gx.ExpectationSuite(name="amazon_sales_suite")
    
    # ==========================================================================
    # EXPECTATIONS TANIMLAMA
    # ==========================================================================
    
    # 1. Order ID: Not null ve unique olmalı
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID")
    )
    
    # 2. Qty: 0 veya daha büyük olmalı (negatif olamaz)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Qty",
            min_value=0,
            max_value=None
        )
    )
    
    # 3. Amount: 0 veya daha büyük olmalı (Cancelled siparişlerde null olabilir)
    # Not: Bu expectation null değerleri başarısız sayar
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Amount",
            min_value=0,
            max_value=None
        )
    )
    
    # 4. Status: Belirlenen değerler içinde olmalı
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Status",
            value_set=VALID_STATUSES
        )
    )
    
    # 5. Fulfilment: Merchant veya Amazon olmalı
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Fulfilment",
            value_set=VALID_FULFILMENT
        )
    )
    
    # 6. Currency: INR olmalı
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="currency",
            value_set=VALID_CURRENCIES
        )
    )
    
    # 7. Ship Country: IN olmalı
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="ship-country",
            value_set=VALID_COUNTRIES
        )
    )
    
    # 8. Date: Belirli formatta olmalı (regex ile kontrol)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="Date",
            regex=r"^\d{2}-\d{2}-\d{2}$"  # MM-DD-YY format
        )
    )
    
    # Suite'i context'e ekle
    suite = context.suites.add(suite)
    
    # ==========================================================================
    # VALIDATION ÇALIŞTIR
    # ==========================================================================
    
    validation_definition = gx.ValidationDefinition(
        name="amazon_sales_validation",
        data=batch_definition,
        suite=suite
    )
    
    validation_definition = context.validation_definitions.add(validation_definition)
    
    # Validation'ı çalıştır
    results = validation_definition.run(batch_parameters={"dataframe": df})
    
    return results


# =============================================================================
# 4. SONUÇLARI İŞLE
# =============================================================================

def process_results(results) -> dict:
    """
    Validation sonuçlarını işle ve özet oluştur.
    
    Returns:
        Özet bilgileri içeren dictionary
    """
    print("\n📊 Processing Results...")
    
    # Sonuçları çıkar
    results_dict = results.to_json_dict()
    
    # Özet bilgiler
    success = results_dict.get("success", False)
    
    # Expectation sonuçları
    expectation_results = results_dict.get("results", [])
    
    passed = []
    failed = []
    
    for exp_result in expectation_results:
        exp_config = exp_result.get("expectation_config", {})
        exp_type = exp_config.get("type", "Unknown")
        column = exp_config.get("kwargs", {}).get("column", "N/A")
        success_flag = exp_result.get("success", False)
        
        result_info = {
            "expectation": exp_type,
            "column": column,
            "success": success_flag,
            "result": exp_result.get("result", {})
        }
        
        if success_flag:
            passed.append(result_info)
        else:
            failed.append(result_info)
    
    summary = {
        "overall_success": success,
        "total_expectations": len(expectation_results),
        "passed_count": len(passed),
        "failed_count": len(failed),
        "passed": passed,
        "failed": failed,
        "timestamp": datetime.now().isoformat()
    }
    
    # Konsola yazdır
    print(f"\n{'='*60}")
    print(f"   VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"   Overall Success: {'✅ PASSED' if success else '❌ FAILED'}")
    print(f"   Total Expectations: {summary['total_expectations']}")
    print(f"   Passed: {summary['passed_count']}")
    print(f"   Failed: {summary['failed_count']}")
    print(f"{'='*60}")
    
    if failed:
        print("\n❌ FAILED EXPECTATIONS:")
        print("-" * 60)
        for f in failed:
            print(f"\n   📌 {f['expectation']}")
            print(f"      Column: {f['column']}")
            
            # Unexpected values göster
            result = f.get('result', {})
            unexpected_count = result.get('unexpected_count', 0)
            unexpected_percent = result.get('unexpected_percent', 0)
            unexpected_values = result.get('partial_unexpected_list', [])
            
            if unexpected_count:
                print(f"      Unexpected Count: {unexpected_count}")
                print(f"      Unexpected Percent: {unexpected_percent:.2f}%")
            if unexpected_values:
                print(f"      Sample Unexpected Values: {unexpected_values[:5]}")
    
    if passed:
        print("\n✅ PASSED EXPECTATIONS:")
        print("-" * 60)
        for p in passed:
            print(f"   ✓ {p['expectation']} (Column: {p['column']})")
    
    return summary


# =============================================================================
# 5. SLACK NOTIFICATION
# =============================================================================

def send_slack_notification(summary: dict, webhook_url: str) -> bool:
    """
    Slack'e validation sonuçlarını gönder.
    
    Args:
        summary: Validation özeti
        webhook_url: Slack Webhook URL
        
    Returns:
        True if successful, False otherwise
    """
    if webhook_url == "YOUR_SLACK_WEBHOOK_URL":
        print("\n⚠️  Slack notification skipped (no webhook URL configured)")
        return False
    
    print("\n📤 Sending Slack notification...")
    
    # Emoji ve renk belirle
    if summary["overall_success"]:
        emoji = "✅"
        color = "#36a64f"  # yeşil
        status_text = "PASSED"
    else:
        emoji = "❌"
        color = "#dc3545"  # kırmızı
        status_text = "FAILED"
    
    # Failed expectations detayı
    failed_details = ""
    if summary["failed"]:
        failed_details = "\n".join([
            f"• *{f['expectation']}* (Column: `{f['column']}`)"
            for f in summary["failed"]
        ])
    
    # Slack mesajı oluştur
    message = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emoji} Data Quality Validation {status_text}",
                            "emoji": True
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Dataset:*\nAmazon Sales"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Timestamp:*\n{summary['timestamp'][:19]}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Passed:*\n{summary['passed_count']} ✓"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Failed:*\n{summary['failed_count']} ✗"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Failed expectations varsa ekle
    if failed_details:
        message["attachments"][0]["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Failed Expectations:*\n{failed_details}"
            }
        })
    
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            print("   ✅ Slack notification sent successfully!")
            return True
        else:
            print(f"   ❌ Slack notification failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Slack notification error: {e}")
        return False


# =============================================================================
# 6. MAIN
# =============================================================================

def main():
    """Ana fonksiyon."""
    print("\n" + "="*60)
    print("   GREAT EXPECTATIONS - AMAZON SALES VALIDATION")
    print("="*60)
    
    # 1. Veriyi yükle
    df = load_data(CSV_PATH)
    
    # 2. Validation çalıştır
    results = run_validation(df)
    
    # 3. Sonuçları işle
    summary = process_results(results)
    
    # 4. Slack bildirimi gönder
    send_slack_notification(summary, SLACK_WEBHOOK_URL)
    
    # 5. Exit code (CI/CD için)
    if not summary["overall_success"]:
        print("\n⚠️  Validation failed! Exiting with code 1.")
        exit(1)
    else:
        print("\n🎉 All validations passed!")
        exit(0)


if __name__ == "__main__":
    main()
