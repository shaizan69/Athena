# Connectors

Standalone Python package providing abstract database connector interfaces and concrete implementations for data platform pipelines.

## Installation

```bash
# From the connectors/ directory
pip install -e .
```

Or with Poetry:

```bash
poetry install
```

## Why a Separate Package?

The connectors library lives in its own repository for three strategic reasons:

1. **Reusability across clients** — Every client deployment uses the same connector interfaces. A new banking client just needs a new config file, not new connector code.
2. **Independent versioning** — Connector bug-fixes and improvements ship without redeploying every pipeline. Pipelines pin to a known-good connector version.
3. **Swappable Oracle layer** — Development and CI use `MockOracleConnector` (reads CSVs). Production simply swaps in `OracleConnector` via config — zero code changes in assets.

## Available Connectors

| Class | Module | Purpose |
|---|---|---|
| `AbstractConnector` | `connectors.base` | Read / extract interface |
| `AbstractWriter` | `connectors.base` | Write / upsert interface |
| `MockOracleConnector` | `connectors.mock_oracle` | CSV-backed mock extractor |
| `OracleConnector` | `connectors.oracle` | Production Oracle extractor (requires `cx_Oracle`) |
| `PostgresConnector` | `connectors.postgres` | SQLAlchemy-based Postgres reader/writer with change-detection upsert |

## Swapping Mock Oracle for Real Oracle

1. Install the Oracle client library:
   ```bash
   pip install cx-Oracle
   ```
2. Uncomment `import cx_Oracle` in `connectors/oracle.py`.
3. In your Dagster resource config (`pipeline/pipeline/resources.py`), change:
   ```python
   # FROM:
   MockOracleConnector(data_dir=config["oracle"]["data_dir"])
   # TO:
   OracleConnector(
       host="oracle-host.example.com",
       port=1521,
       service_name="ORCLPDB1",
       user="etl_user",
       password="s3cret",
   )
   ```
4. Replace logical query names (`"customers"`, `"transactions"`, etc.) with actual SQL strings in each Bronze asset.

No other pipeline code changes — both connectors return a `pd.DataFrame` and implement the same `connect() / extract() / close()` contract.

## Usage Example

```python
from connectors import MockOracleConnector, PostgresConnector

# Extract from CSV mock
oracle = MockOracleConnector(data_dir="data/")
oracle.connect()
df = oracle.extract("customers")
oracle.close()

# Write to Postgres
pg = PostgresConnector("postgresql://user:password@localhost:5432/dataplatform")
pg.connect()
pg.write(df, "customers", "bronze")
pg.close()
```
