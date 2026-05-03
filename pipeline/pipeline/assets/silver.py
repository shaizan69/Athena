"""
Silver layer assets.

Each asset reads from the corresponding Bronze table, applies cleaning
transforms (normalise column names, standardise casing, fill nulls,
de-duplicate), adds pipeline metadata, and writes to the ``silver`` schema.
"""

import logging
import re
import uuid
from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, AssetIn, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────


def _to_snake_case(name: str) -> str:
    """Convert a column name to snake_case.

    Args:
        name: Original column name.

    Returns:
        snake_case version of the name.
    """
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s2 = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s1)
    return s2.replace(" ", "_").replace("-", "_").lower()


def _clean_dataframe(df: pd.DataFrame, numeric_cols: list[str] | None = None) -> pd.DataFrame:
    """Apply standard Silver cleaning transforms to a DataFrame.

    Steps:
        1. Normalise column names to snake_case.
        2. Strip whitespace and lowercase all string columns.
        3. Fill nulls — numeric → 0, string → "unknown".
        4. Drop exact duplicate rows.

    Args:
        df: Raw DataFrame from Bronze.
        numeric_cols: Explicit list of numeric column names (after snake_case
            normalisation).  If ``None``, auto-detected via dtype.

    Returns:
        Cleaned DataFrame.
    """
    # 1. Snake-case column names
    df.columns = [_to_snake_case(c) for c in df.columns]

    # 2. Strip and lowercase string columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
        # Replace literal "nan" strings introduced by astype(str)
        df[col] = df[col].replace("nan", pd.NA)

    # 3. Fill nulls
    if numeric_cols:
        for nc in numeric_cols:
            if nc in df.columns:
                df[nc] = pd.to_numeric(df[nc], errors="coerce").fillna(0)
    else:
        for col in df.select_dtypes(include=["number"]).columns:
            df[col] = df[col].fillna(0)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("unknown")

    # Also handle any remaining NA in any column type
    df = df.fillna("unknown")

    # 4. Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d exact duplicate rows.", dropped)

    return df


def _silver_transform_and_write(
    context: AssetExecutionContext,
    bronze_table: str,
    silver_table: str,
    numeric_cols: list[str] | None = None,
) -> MaterializeResult:
    """Shared logic for all Silver assets.

    Args:
        context: Dagster execution context.
        bronze_table: Source table name in the ``bronze`` schema.
        silver_table: Target table name in the ``silver`` schema.
        numeric_cols: Columns to treat as numeric during null-filling.

    Returns:
        MaterializeResult with row counts and metadata.
    """
    postgres = context.resources.postgres

    # Read from Bronze
    df = postgres.read_table(bronze_table, "bronze")
    raw_count = len(df)
    logger.info("Silver %s: read %d rows from bronze.%s", silver_table, raw_count, bronze_table)

    # Clean
    df = _clean_dataframe(df, numeric_cols=numeric_cols)

    # Add pipeline metadata
    df["pipeline_run_id"] = str(uuid.uuid4())
    df["processed_at"] = datetime.now(timezone.utc).isoformat()

    # Write to Silver (full replace — Silver is an idempotent snapshot)
    row_count = postgres.write(df, silver_table, "silver")

    logger.info("Silver %s: wrote %d rows to silver.%s", silver_table, row_count, silver_table)

    return MaterializeResult(
        metadata={
            "rows_from_bronze": MetadataValue.int(raw_count),
            "rows_after_cleaning": MetadataValue.int(row_count),
            "duplicates_dropped": MetadataValue.int(raw_count - row_count),
            "processed_at": MetadataValue.text(datetime.now(timezone.utc).isoformat()),
        }
    )


# ──────────────────────────────────────────────────────────────────────
# Individual Silver assets
# ──────────────────────────────────────────────────────────────────────


@asset(
    name="silver_customers",
    group_name="silver",
    deps=["bronze_customers"],
    required_resource_keys={"postgres"},
)
def silver_customers(context: AssetExecutionContext) -> MaterializeResult:
    """Clean and standardise customers into silver.customers."""
    return _silver_transform_and_write(context, "customers", "customers")


@asset(
    name="silver_transactions",
    group_name="silver",
    deps=["bronze_transactions"],
    required_resource_keys={"postgres"},
)
def silver_transactions(context: AssetExecutionContext) -> MaterializeResult:
    """Clean and standardise transactions into silver.transactions."""
    return _silver_transform_and_write(
        context, "transactions", "transactions",
        numeric_cols=["amount"],
    )


@asset(
    name="silver_accounts",
    group_name="silver",
    deps=["bronze_accounts"],
    required_resource_keys={"postgres"},
)
def silver_accounts(context: AssetExecutionContext) -> MaterializeResult:
    """Clean and standardise accounts into silver.accounts."""
    return _silver_transform_and_write(
        context, "accounts", "accounts",
        numeric_cols=["balance", "credit_limit"],
    )


@asset(
    name="silver_risk_flags",
    group_name="silver",
    deps=["bronze_risk_flags"],
    required_resource_keys={"postgres"},
)
def silver_risk_flags(context: AssetExecutionContext) -> MaterializeResult:
    """Clean and standardise risk flags into silver.risk_flags."""
    return _silver_transform_and_write(context, "risk_flags", "risk_flags")
