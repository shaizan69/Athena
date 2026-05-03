"""
Dagster asset check definitions.

Each check validates a critical data-quality invariant on a Silver or Gold
table that must hold before downstream consumers can trust the data.
"""

import logging

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

logger = logging.getLogger(__name__)


@asset_check(asset=AssetKey("silver_customers"), required_resource_keys={"postgres"})
def check_silver_customers_no_null_id(context) -> AssetCheckResult:
    """Assert that ``customer_id`` has no null values in silver.customers.

    A null customer_id would break all downstream joins and aggregations.
    """
    postgres = context.resources.postgres
    df = postgres.read_table("customers", "silver")
    null_count = int(df["customer_id"].isna().sum())

    passed = null_count == 0
    logger.info(
        "check_silver_customers_no_null_id: %s (null_count=%d)",
        "PASSED" if passed else "FAILED",
        null_count,
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_customer_id_count": null_count},
        description="customer_id must not contain null values.",
    )


@asset_check(asset=AssetKey("silver_transactions"), required_resource_keys={"postgres"})
def check_silver_transactions_valid(context) -> AssetCheckResult:
    """Assert no null transaction_id and all amounts > 0 in silver.transactions.

    A null transaction_id breaks deduplication and a non-positive amount
    indicates data corruption or incomplete extraction.
    """
    postgres = context.resources.postgres
    df = postgres.read_table("transactions", "silver")

    null_txn_id = int(df["transaction_id"].isna().sum())
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    non_positive_amount = int((df["amount"] <= 0).sum())

    passed = null_txn_id == 0 and non_positive_amount == 0
    logger.info(
        "check_silver_transactions_valid: %s (null_txn=%d, bad_amount=%d)",
        "PASSED" if passed else "FAILED",
        null_txn_id,
        non_positive_amount,
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "null_transaction_id_count": null_txn_id,
            "non_positive_amount_count": non_positive_amount,
        },
        description="transaction_id must not be null and amount must be > 0.",
    )


@asset_check(asset=AssetKey("gold_risk_scores"), required_resource_keys={"postgres"})
def check_gold_risk_scores_row_count(context) -> AssetCheckResult:
    """Assert gold.risk_scores row count matches distinct customers in Silver.

    There should be exactly one risk-score row per distinct customer.
    A mismatch indicates a join fanout or dropped customers.
    """
    postgres = context.resources.postgres
    risk_scores = postgres.read_table("risk_scores", "gold")
    customers = postgres.read_table("customers", "silver")

    gold_count = len(risk_scores)
    silver_distinct = int(customers["customer_id"].nunique())

    passed = gold_count == silver_distinct
    logger.info(
        "check_gold_risk_scores_row_count: %s (gold=%d, silver_distinct=%d)",
        "PASSED" if passed else "FAILED",
        gold_count,
        silver_distinct,
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "gold_risk_scores_rows": gold_count,
            "silver_distinct_customers": silver_distinct,
        },
        description="gold.risk_scores row count must equal distinct customer_id count in silver.customers.",
    )
