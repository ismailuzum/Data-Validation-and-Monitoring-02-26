# Data Validation & Monitoring

Amazon Sales verisi üzerinde **Great Expectations** ve **Pydantic** ile data quality doğrulama pipeline'ı.  
Hata tespitlerinde **Slack** bildirimi gönderir. **GitHub Actions** ile otomatik çalışır.

---

## 📁 Proje Yapısı

```
Data-Validation-and-Monitoring-02-26/
├── .github/workflows/
│   └── data_quality.yml         # CI/CD – GitHub Actions workflow
├── data/
│   └── amazon_sales.csv         # Amazon sipariş verisi
├── src/
│   ├── ge_validation.py         # Great Expectations validation modülü
│   ├── pydantic_validation.py   # Pydantic satır-bazlı doğrulama
│   └── slack_notifier.py        # Slack bildirim modülü
├── dq_pipeline.py               # Pipeline orkestrasyonu
├── requirements.txt             # Python bağımlılıkları
└── README.md
```

---

## 🚀 Kurulum & Çalıştırma

```bash
# Bağımlılıkları yükle
pip install -r requirements.txt

# Pipeline'ı çalıştır
python dq_pipeline.py
```

### Slack Bildirimi (opsiyonel)

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
python dq_pipeline.py
```

---

## 🔍 Doğrulama Kuralları

### Great Expectations (GE)

| #  | Expectation                  | Kolon          | Açıklama                    |
|----|------------------------------|----------------|-----------------------------|
| 1  | Not Null                     | Order ID       | Boş olamaz                  |
| 2  | Unique                       | Order ID       | Tekrar edemez               |
| 3  | Value Between (≥ 0)          | Qty            | Negatif olamaz              |
| 4  | Value Between (≥ 0)          | Amount         | Negatif olamaz              |
| 5  | Value In Set                 | Status         | Geçerli sipariş durumları   |
| 6  | Value In Set                 | Fulfilment     | Merchant veya Amazon        |
| 7  | Value In Set                 | currency       | Sadece INR                  |
| 8  | Value In Set                 | ship-country   | Sadece IN                   |
| 9  | Match Regex (`MM-DD-YY`)     | Date           | Tarih formatı kontrolü      |

### Pydantic

`AmazonOrder` modeli ile her satır ayrı doğrulanır. Field validatörleri:
- `order_id` → boş olamaz  
- `date` → `MM-DD-YY` regex  
- `status`, `fulfilment`, `currency`, `ship_country` → set kontrolü  
- `qty` → ≥ 0  
- `amount` → ≥ 0 (opsiyonel)

---

## ⚙️ GitHub Actions

Workflow (`.github/workflows/data_quality.yml`):

- **Tetikleyiciler:** `push`, `pull_request` (main), `workflow_dispatch`
- **Python 3.11** ortamı kurulur
- `dq_pipeline.py` çalıştırılır
- Slack webhook URL'si **GitHub Secret** olarak tanımlanmalıdır:  
  `Settings → Secrets → SLACK_WEBHOOK_URL`

---

## 📤 Slack Bildirimi

Pipeline sonucunda Slack'e **Block Kit** formatında mesaj gönderilir:

- ✅ / ❌ genel durum
- GE passed/failed sayıları
- Pydantic valid/invalid satır sayıları
- Hata detayları (sample)
