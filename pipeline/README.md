# Pipeline

Dagster-based data pipeline implementing a **Bronze → Silver → Gold** medallion architecture with hash-based change detection and per-client YAML configuration.

## Quick Start — Local Setup

```bash
# 1. Clone and enter the project
git clone <repo-url> && cd pipeline

# 2. Start Postgres (creates bronze, silver, gold schemas automatically)
docker-compose up -d

# 3. Install the connectors library (from the sibling directory)
pip install -e ../connectors

# 4. Install pipeline dependencies
pip install -e .

# 5. Launch Dagster
dagster dev
```

Open http://localhost:3000 → materialize all assets from the Dagster UI.

---

## Fictional Oracle SQL Queries

The CSVs in `data/` simulate these production Oracle queries:

### customers.csv
```sql
SELECT c.customer_id, c.full_name, c.date_of_birth, a.country,
       at.account_type_name AS account_type, k.kyc_status, c.created_at
FROM   core.customers c
JOIN   core.addresses a       ON c.customer_id = a.customer_id AND a.is_primary = 1
JOIN   core.account_types at  ON c.account_type_id = at.account_type_id
JOIN   compliance.kyc_records k ON c.customer_id = k.customer_id
WHERE  c.is_deleted = 0
```

### transactions.csv
```sql
SELECT t.transaction_id, t.customer_id, t.transaction_date, t.amount,
       cur.currency_code AS currency, tt.type_name AS transaction_type, t.status
FROM   ledger.transactions t
JOIN   reference.currencies cur    ON t.currency_id = cur.currency_id
JOIN   reference.transaction_types tt ON t.type_id = tt.type_id
WHERE  t.transaction_date >= ADD_MONTHS(SYSDATE, -24)
```

### accounts.csv
```sql
SELECT a.account_id, a.customer_id, a.account_number,
       a.current_balance AS balance, a.credit_limit,
       a.opened_date, a.is_active
FROM   core.accounts a
JOIN   core.customers c ON a.customer_id = c.customer_id
WHERE  a.closed_date IS NULL
```

### risk_flags.csv
```sql
SELECT rf.flag_id, rf.customer_id, ft.flag_type_name AS flag_type,
       rf.flagged_at, rf.resolved_at, sl.severity_label AS severity
FROM   compliance.risk_flags rf
JOIN   compliance.flag_types ft      ON rf.flag_type_id = ft.flag_type_id
JOIN   compliance.severity_levels sl ON rf.severity_id = sl.severity_id
WHERE  rf.flagged_at >= ADD_MONTHS(SYSDATE, -24)
```

---

## Key Design Decisions

### Bronze / Silver / Gold Rationale

| Layer | Purpose | Write Strategy |
|---|---|---|
| **Bronze** | Raw data landing zone — preserves source fidelity | Hash-based upsert (insert new, update changed, skip unchanged) |
| **Silver** | Cleaned, standardised, de-duplicated data | Full replace (idempotent snapshot of cleaned Bronze) |
| **Gold** | Business-ready aggregates and metrics | Full replace (recomputed from Silver each run) |

Bronze uses **change detection** (MD5 row hash) to avoid full table reloads and enable incremental ingestion. Silver and Gold use full-replace because they are derived views that should always reflect the latest upstream state.

### Change Detection Strategy

The `upsert()` method in `PostgresConnector`:
1. Fetches existing `{primary_key: row_hash}` pairs from the target table
2. For each incoming row, computes its key and compares hashes
3. Classifies as INSERT / UPDATE / SKIP accordingly

**Assumptions:**
- Primary keys are immutable (they identify the same entity across runs)
- MD5 collisions are negligible at this data scale
- The hash is computed over all source columns (not metadata columns)

### Dagster Assets vs Jobs

We chose **Software-Defined Assets (SDAs)** over jobs because:
- Assets encode the dependency graph declaratively — Dagster resolves execution order
- Asset materialisation is idempotent — safe to re-run
- The Dagster UI shows lineage, metadata, and freshness per asset
- Asset checks attach directly to the asset they validate

### Per-Client Configuration

All environment-specific values (Postgres connection, Oracle data directory, cron schedule) live in `config/client_default.yaml`. To onboard a new client:
1. Copy `client_default.yaml` → `client_acme.yaml`
2. Update connection strings and schedule
3. Set `PIPELINE_CONFIG=config/client_acme.yaml`
4. Run `dagster dev` — zero code changes needed

---

## What I Would Do Differently With More Time

1. **dbt for Silver/Gold** — Replace Silver and Gold Python assets with dbt models. SQL is more maintainable for transforms, dbt provides built-in testing and documentation, and `dagster-dbt` integrates cleanly.

2. **Real orchestration infrastructure** — Deploy to Dagster Cloud or a Kubernetes-based Dagster deployment (dagster-daemon + dagster-webserver) with proper health checks, alerting (PagerDuty/Slack), and auto-restart.

3. **Proper secrets management** — Move connection strings to HashiCorp Vault, AWS Secrets Manager, or environment variables injected by CI. Never store passwords in YAML, even for local dev.

4. **Data contracts** — Add Pandera or Great Expectations schemas at Silver layer boundaries to enforce column types, value ranges, and cross-table referential integrity.

5. **Partitioned assets** — Partition Bronze and Silver assets by date so that backfills don't reload the entire history and Dagster can track per-partition freshness.

6. **Observability** — Add OpenTelemetry tracing, ship Dagster logs to Datadog/Grafana, and build dashboards for pipeline health (rows processed, latency, failure rates).

7. **CI/CD** — GitHub Actions pipeline: lint (ruff), type-check (mypy), test (pytest), Docker build, deploy to staging, run integration tests, promote to production.

---

## Running Tests

```bash
cd pipeline
pytest tests/ -v
```
