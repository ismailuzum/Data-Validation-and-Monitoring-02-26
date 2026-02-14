# Data Validation & Monitoring

A data quality validation pipeline for the Amazon Sales dataset using **Great Expectations** and **Pydantic**.  
Sends **Slack** notifications on validation results and runs automatically via **GitHub Actions**.

---

## 📁 Project Structure

```
Data-Validation-and-Monitoring-02-26/
├── .github/workflows/
│   └── data_quality.yml         # CI/CD – GitHub Actions workflow
├── data/
│   └── amazon_sales.csv         # Amazon orders dataset
├── src/
│   ├── ge_validation.py         # Great Expectations validation module
│   ├── pydantic_validation.py   # Pydantic row-level validation
│   └── slack_notifier.py        # Slack notification module
├── dq_pipeline.py               # Pipeline orchestrator
├── requirements.txt             # Python dependencies
└── README.md
```

---

## 🚀 Setup & Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python dq_pipeline.py
```

### Slack Notification (optional)

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
python dq_pipeline.py
```

---

## 🔍 Validation Rules

### Great Expectations (GE)

| #  | Expectation                  | Column         | Description                 |
|----|------------------------------|----------------|-----------------------------|
| 1  | Not Null                     | Order ID       | Must not be empty           |
| 2  | Unique                       | Order ID       | Must not repeat             |
| 3  | Value Between (≥ 0)          | Qty            | Cannot be negative          |
| 4  | Value Between (≥ 0)          | Amount         | Cannot be negative          |
| 5  | Value In Set                 | Status         | Valid order statuses only   |
| 6  | Value In Set                 | Fulfilment     | Merchant or Amazon          |
| 7  | Value In Set                 | currency       | INR only                    |
| 8  | Value In Set                 | ship-country   | IN only                     |
| 9  | Match Regex (`MM-DD-YY`)     | Date           | Date format check           |

### Pydantic

Each row is validated individually using the `AmazonOrder` model with field validators:
- `order_id` → must not be empty  
- `date` → `MM-DD-YY` regex  
- `status`, `fulfilment`, `currency`, `ship_country` → set membership check  
- `qty` → ≥ 0  
- `amount` → ≥ 0 (optional, may be None for cancelled orders)

---

## ⚙️ GitHub Actions

Workflow file: `.github/workflows/data_quality.yml`

- **Triggers:** `push`, `pull_request` (main branch), `workflow_dispatch` (manual)
- **Environment:** Python 3.11
- **Steps:** Install dependencies → Run `dq_pipeline.py`
- **Slack Secret:** Add `SLACK_WEBHOOK_URL` under `Settings → Secrets and variables → Actions`

---

## 📤 Slack Notification

The pipeline sends a **Block Kit** formatted message to Slack containing:

- ✅ / ❌ Overall status
- GE passed/failed expectation counts
- Pydantic valid/invalid row counts
- Sample error details
