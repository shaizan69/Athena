"""
Bronze layer assets.

Each asset extracts data from the mock Oracle source, adds ingestion
metadata (``ingested_at``, ``row_hash``), and upserts into the
``bronze`` schema in Postgres.  Only rows whose hash has changed
are inserted or updated — unchanged rows are skipped.
"""

import hashlib
import logging
from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)


def _compute_row_hash(row: pd.Series, columns: list[str]) -> str:
    """Compute an MD5 hash of all original columns concatenated.

    Args:
        row: A single row from the DataFrame.
        columns: List of column names to include in the hash.

    Returns:
        Hex-encoded MD5 digest string.
    """
    concat = "|".join(str(row[c]) for c in columns)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def _bronze_extract_and_load(
    context: AssetExecutionContext,
    query_name: str,
    table_name: str,
    key_col: str,
) -> MaterializeResult:
    """Shared logic for all Bronze assets.

    1. Extract CSV via MockOracle resource.
    2. Add ``ingested_at`` and ``row_hash``.
    3. Upsert into ``bronze.<table_name>``.

    Args:
        context: Dagster execution context (provides resources).
        query_name: Logical query name for the MockOracle connector.
        table_name: Target Postgres table name in the ``bronze`` schema.
        key_col: Primary key column name for change detection.

    Returns:
        MaterializeResult with row-count metadata.
    """
    oracle = context.resources.oracle
    postgres = context.resources.postgres

    # 1. Extract
    df = oracle.extract(query_name)
    original_cols = list(df.columns)
    logger.info("Bronze %s: extracted %d rows.", table_name, len(df))

    # 2. Add metadata columns
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["row_hash"] = df.apply(lambda r: _compute_row_hash(r, original_cols), axis=1)

    # 3. Upsert
    counts = postgres.upsert(
        df=df,
        table=table_name,
        key_cols=[key_col],
        hash_col="row_hash",
        schema="bronze",
    )

    logger.info(
        "Bronze %s complete — inserted: %d, updated: %d, skipped: %d",
        table_name,
        counts["inserted"],
        counts["updated"],
        counts["skipped"],
    )

    return MaterializeResult(
        metadata={
            "rows_extracted": MetadataValue.int(len(df)),
            "rows_inserted": MetadataValue.int(counts["inserted"]),
            "rows_updated": MetadataValue.int(counts["updated"]),
            "rows_skipped": MetadataValue.int(counts["skipped"]),
            "ingested_at": MetadataValue.text(datetime.now(timezone.utc).isoformat()),
        }
    )


# ──────────────────────────────────────────────────────────────────────
# Individual Bronze assets — one per source table
# ──────────────────────────────────────────────────────────────────────


@asset(
    name="bronze_customers",
    group_name="bronze",
    required_resource_keys={"oracle", "postgres"},
)
def bronze_customers(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest customers from Oracle source into bronze.customers."""
    return _bronze_extract_and_load(context, "customers", "customers", "customer_id")


@asset(
    name="bronze_transactions",
    group_name="bronze",
    required_resource_keys={"oracle", "postgres"},
)
def bronze_transactions(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest transactions from Oracle source into bronze.transactions."""
    return _bronze_extract_and_load(context, "transactions", "transactions", "transaction_id")


@asset(
    name="bronze_accounts",
    group_name="bronze",
    required_resource_keys={"oracle", "postgres"},
)
def bronze_accounts(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest accounts from Oracle source into bronze.accounts."""
    return _bronze_extract_and_load(context, "accounts", "accounts", "account_id")


@asset(
    name="bronze_risk_flags",
    group_name="bronze",
    required_resource_keys={"oracle", "postgres"},
)
def bronze_risk_flags(context: AssetExecutionContext) -> MaterializeResult:
    """Ingest risk flags from Oracle source into bronze.risk_flags."""
    return _bronze_extract_and_load(context, "risk_flags", "risk_flags", "flag_id")
