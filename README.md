# Financial Services Data Platform

**A full end-to-end data platform built with a Medallion Architecture, engineered for millions of historical rows with highly efficient change detection.**

---

## 🏛️ Repository Overview
This project is structured as a **Monorepo** to maintain a clean separation of concerns.

It is split into two primary packages:
1.  **`connectors/`**: A standalone, reusable Python library handling pure Database operations. Contains our abstract DB interfaces, Postgres logic, and the Mock Oracle extraction. **(See `connectors/README.md` for specific technical details)**.
2.  **`pipeline/`**: The Dagster project representing the data orchestration and transformation logic. It handles moving data through the layers, quality checks, and scheduling. **(See `pipeline/README.md` for specific orchestration details)**.

---

## ✨ System Architecture: The Medallion Approach

### 🥉 Bronze (Raw Ingestion & Change Detection)
The Bronze layer extracts data from the "Oracle" tables and lands it into Postgres.
**How we efficiently handle millions of rows (~50 changing per day):**
I implemented an **MD5- Hash Change Detection Strategy**. 
- The pipeline scans an incoming row and generates an MD5 cryptographic fingerprint of the data. 
- Using the `upsert` mechanism inside the `PostgresConnector` (`connectors/postgres.py`), it compares the new fingerprint to the existing fingerprint in the database.
- It only performs expensive I/O operations (`INSERT` or `UPDATE`) if the record is genuinely new or modified. Unmodified rows are instantly skipped in-memory.

### 🥈 Silver (Data Cleaning & Standardization)
This layer enforces data hygiene:
- Converts all schema/column names to `snake_case`.
- Lowercases and trims whitespace from string fields.
- Fills missing values safely (`pd.NA`, `"unknown"`, `0.0`).
- Drops exact row duplicates.

### 🥇 Gold (Business & Serving)
This layer distills the raw data into the two requested artifacts for the enterprise:
- `gold_risk_scores`: Customer-level aggregations (calculating `transaction_count`, `recency`, etc.) merged with the compliance flags. Designed for direct ingestion into a Risk Scoring ML Model.
- `gold_analytics_monthly`: A rolled-up dimensional view joining transactions and accounts at a monthly granularity for fast Dashboard rendering.

---

## 🛠️ Configuration & Multi-Tenant Deployment
The codebase contains **zero hardcoded connections, cadences, or active-pipeline flags.**
Everything is abstracted into YAML configuration.

To deploy this platform to a completely different client, you simply:
1. Create a new YAML config file (e.g., `config/client_acme.yaml`).
2. Point the environment variable to it: `export PIPELINE_CONFIG="pipeline/config/client_acme.yaml"`.
Dagster handles the rest via dynamic dependency injection.

---

## 🧪 Data Quality & Engineering Standards
1. **Asset Checks (DQ):** Embedded directly into Dagster, enforcing invariants like "Transaction Amounts must be > 0" and "IDs cannot be Null". If bad data enters Silver, the pipeline will flag it.
2. **Automated Testing:** 100% passing PyTest suite focusing strictly on business-critical logic:
   - `test_bronze_idempotency.py`: Proves the pipeline won't duplicate data.
   - `test_change_detection.py`: Proves the MD5 hashing logic correctly identifies Insert/Update/Skip behaviors.
   - `test_silver_nulls.py`: Proves the DQ cleaning rules work on dirty strings.

---

## 🚀 How to Run Locally

### Prerequisites
- Python 3.11+
- Docker & Docker Compose

### 1. Start the Infrastructure (Postgres)
```bash
cd pipeline
docker-compose up -d
cd ..
```

### 2. Install Packages
```bash
# Install the standalone database connector library
pip install -e connectors

# Install the Dagster pipeline
pip install -e pipeline
```

### 3. Run the Tests
```bash
python -m pytest pipeline/tests/
```

### 4. Launch the Orchestrator
```bash
cd pipeline
dagster dev
```
Open `http://localhost:3000` in your browser. From here, you can visualize the graph, acknowledge Asset Checks, and click **Materialize All** to run the pipeline!


