# Data Validation & Monitoring

A data quality validation pipeline for the Amazon Sales dataset using **Great Expectations** and **Pydantic**.
Sends **Slack** notifications on validation results and runs automatically via **GitHub Actions** and **Jenkins**.

---

## Project Structure

```
Data-Validation-and-Monitoring-02-26/
├── .github/workflows/
│   └── data_quality.yml            # CI/CD – GitHub Actions workflow
├── data/
│   ├── amazon_sales.csv            # Amazon orders dataset
│   └── bad_rows.csv                # Rows that failed validation
├── src/
│   ├── ge_validation.py            # Great Expectations validation module
│   ├── pydantic_validation.py      # Pydantic row-level validation
│   └── slack_notifier.py           # Slack notification module
├── dq_pipeline.py                  # Pipeline orchestrator
├── Jenkinsfile                     # CI/CD – Jenkins declarative pipeline
├── LICENSE
├── requirements.txt                # Python dependencies
└── README.md
```

---

## Setup & Usage

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

## Validation Rules

### Great Expectations (GE)

| #  | Expectation              | Column       | Description               |
|----|--------------------------|--------------|---------------------------|
| 1  | Not Null                 | Order ID     | Must not be empty         |
| 2  | Value Between (>= 0)    | Qty          | Cannot be negative        |
| 3  | Value Between (>= 0)    | Amount       | Cannot be negative        |
| 4  | Value In Set             | Status       | Valid order statuses only |
| 5  | Value In Set             | Fulfilment   | Merchant or Amazon        |
| 6  | Value In Set             | currency     | INR only                  |
| 7  | Value In Set             | ship-country | IN only                   |
| 8  | Match Regex (`MM-DD-YY`) | Date         | Date format check         |

### Pydantic

Each row is validated individually using the `AmazonOrder` model with field validators:
- `order_id` — must not be empty
- `date` — `MM-DD-YY` regex
- `status`, `fulfilment`, `currency`, `ship_country` — set membership check
- `qty` — >= 0
- `amount` — >= 0 (optional, may be None for cancelled orders)

---

## GitHub Actions

Workflow file: `.github/workflows/data_quality.yml`

- **Triggers:** `push`, `pull_request` (main branch), `schedule` (daily 08:00 UTC), `workflow_dispatch` (manual)
- **Environment:** Python 3.11
- **Steps:** Checkout → Setup Python → Cache pip → Install dependencies → Lint (Ruff) → Run pipeline → Upload report artifact → Job Summary
- **Slack Secret:** Add `SLACK_WEBHOOK_URL` under `Settings > Secrets and variables > Actions`

---

## Jenkins

Pipeline file: `Jenkinsfile`

- **Triggers:** `pollSCM` (every 15 min), `cron` (daily 08:00)
- **Credentials:** `slack-webhook-url` (Secret Text) — add via `Manage Jenkins > Credentials`
- **Stages:**
  1. **Setup Environment** — creates a Python virtual environment and installs dependencies
  2. **Lint Code** — runs Ruff linter (non-blocking)
  3. **Data Quality Validation** — executes `dq_pipeline.py`
- **Post Actions:** archive validation report, cleanup virtual environment

---

## Slack Notification

The pipeline sends a **Block Kit** formatted message to Slack containing:

- Overall status (passed / failed)
- GE passed/failed expectation counts
- Pydantic valid/invalid row counts
- Sample error details

---

## Docker Commands Reference

<!-- ──────────────────────────────────────────────────────────────────────
     Docker Cheat Sheet
     Useful commands for managing containers, images, volumes, and networks.
     ────────────────────────────────────────────────────────────────────── -->

### Container Management

```bash
# List running containers
docker ps

# List all containers (including stopped)
docker ps -a

# Stop a running container
docker stop <container_id>

# Stop all running containers
docker stop $(docker ps -q)

# Remove a stopped container
docker rm <container_id>

# Remove all stopped containers
docker rm $(docker ps -aq -f status=exited)

# Force-remove a running container
docker rm -f <container_id>
```

### Image Management

```bash
# List all images
docker images

# Remove an image
docker rmi <image_id>

# Remove all unused (dangling) images
docker image prune

# Remove ALL images (use with caution)
docker rmi $(docker images -q)
```

### Volume Management

```bash
# List all volumes
docker volume ls

# Remove a specific volume
docker volume rm <volume_name>

# Remove all unused volumes
docker volume prune
```

### Network Management

```bash
# List all networks
docker network ls

# Remove a specific network
docker network rm <network_name>

# Remove all unused networks
docker network prune
```

### Full Cleanup

```bash
# Remove all stopped containers, unused networks, dangling images, and build cache
docker system prune

# Remove EVERYTHING (containers, images, volumes, networks) — use with caution
docker system prune -a --volumes

# Show disk usage by Docker
docker system df
```

### Docker Compose

```bash
# Start services in detached mode
docker-compose up -d

# Stop and remove containers, networks, volumes created by 'up'
docker-compose down

# Stop and remove everything including images and volumes
docker-compose down --rmi all --volumes --remove-orphans

# View logs
docker-compose logs -f

# Rebuild images and restart
docker-compose up -d --build
```
