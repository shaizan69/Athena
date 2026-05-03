"""
Gold layer assets.

Produce business-ready analytical tables by joining Silver layer data
and computing aggregated metrics.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, AssetIn, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)

# Severity ranking for max_severity computation
SEVERITY_RANK = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


@asset(
    name="gold_risk_scores",
    group_name="gold",
    deps=["silver_transactions", "silver_customers", "silver_risk_flags"],
    required_resource_keys={"postgres"},
)
def gold_risk_scores(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Compute per-customer risk scoring metrics.

    Joins silver.transactions, silver.customers, and silver.risk_flags to
    produce:

    - ``recency_days`` — days since most recent transaction
    - ``frequency`` — total transaction count
    - ``monetary_total`` — sum of amounts
    - ``monetary_avg`` — average amount
    - ``active_flags`` — count of unresolved risk flags
    - ``max_severity`` — highest severity among that customer's flags

    Writes to ``gold.risk_scores``.
    """
    postgres = context.resources.postgres

    txns = postgres.read_table("transactions", "silver")
    customers = postgres.read_table("customers", "silver")
    flags = postgres.read_table("risk_flags", "silver")

    now = datetime.now(timezone.utc)

    # --- Transaction aggregates ---
    txns["amount"] = pd.to_numeric(txns["amount"], errors="coerce").fillna(0)
    txns["transaction_date"] = pd.to_datetime(txns["transaction_date"], errors="coerce")

    txn_agg = txns.groupby("customer_id").agg(
        recency_date=("transaction_date", "max"),
        frequency=("transaction_id", "count"),
        monetary_total=("amount", "sum"),
        monetary_avg=("amount", "mean"),
    ).reset_index()

    txn_agg["recency_days"] = txn_agg["recency_date"].apply(
        lambda d: (now - d.to_pydatetime().replace(tzinfo=timezone.utc)).days if pd.notna(d) else None
    )
    txn_agg = txn_agg.drop(columns=["recency_date"])

    # --- Flag aggregates ---
    # Unresolved flags: resolved_at is empty / "unknown" / NaT
    flags["is_unresolved"] = flags["resolved_at"].isin(["", "unknown", None]) | flags["resolved_at"].isna()
    flags["severity_rank"] = flags["severity"].str.lower().map(SEVERITY_RANK).fillna(0).astype(int)

    flag_agg = flags.groupby("customer_id").agg(
        active_flags=("is_unresolved", "sum"),
        max_severity_rank=("severity_rank", "max"),
    ).reset_index()

    # Map rank back to label
    rank_to_label = {v: k for k, v in SEVERITY_RANK.items()}
    flag_agg["max_severity"] = flag_agg["max_severity_rank"].map(rank_to_label).fillna("none")
    flag_agg["active_flags"] = flag_agg["active_flags"].astype(int)
    flag_agg = flag_agg.drop(columns=["max_severity_rank"])

    # --- Join all ---
    result = customers[["customer_id"]].drop_duplicates()
    result = result.merge(txn_agg, on="customer_id", how="left")
    result = result.merge(flag_agg, on="customer_id", how="left")

    # Fill missing values for customers with no transactions / flags
    result["frequency"] = result["frequency"].fillna(0).astype(int)
    result["monetary_total"] = result["monetary_total"].fillna(0)
    result["monetary_avg"] = result["monetary_avg"].fillna(0)
    result["recency_days"] = result["recency_days"].fillna(-1).astype(int)
    result["active_flags"] = result["active_flags"].fillna(0).astype(int)
    result["max_severity"] = result["max_severity"].fillna("none")

    result["computed_at"] = now.isoformat()

    row_count = postgres.write(result, "risk_scores", "gold")
    logger.info("Gold risk_scores: wrote %d rows.", row_count)

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "distinct_customers": MetadataValue.int(result["customer_id"].nunique()),
            "computed_at": MetadataValue.text(now.isoformat()),
        }
    )


@asset(
    name="gold_analytics_monthly",
    group_name="gold",
    deps=["silver_transactions", "silver_customers", "silver_accounts", "silver_risk_flags"],
    required_resource_keys={"postgres"},
)
def gold_analytics_monthly(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Compute monthly per-customer analytics.

    Joins all 4 Silver tables to produce per ``customer_id`` per
    ``year_month`` (YYYY-MM):

    - ``transaction_count``
    - ``transaction_volume`` — sum of amounts
    - ``unique_currencies`` — distinct currency count
    - ``account_balance_snapshot`` — latest account balance that month
    - ``flags_raised`` — count of flags raised that month

    Writes to ``gold.analytics_monthly``.
    """
    postgres = context.resources.postgres

    txns = postgres.read_table("transactions", "silver")
    customers = postgres.read_table("customers", "silver")
    accounts = postgres.read_table("accounts", "silver")
    flags = postgres.read_table("risk_flags", "silver")

    # Parse dates and amounts
    txns["amount"] = pd.to_numeric(txns["amount"], errors="coerce").fillna(0)
    txns["transaction_date"] = pd.to_datetime(txns["transaction_date"], errors="coerce")
    txns["year_month"] = txns["transaction_date"].dt.to_period("M").astype(str)

    # --- Transaction aggregates per customer per month ---
    txn_monthly = txns.groupby(["customer_id", "year_month"]).agg(
        transaction_count=("transaction_id", "count"),
        transaction_volume=("amount", "sum"),
        unique_currencies=("currency", "nunique"),
    ).reset_index()

    # --- Account balance snapshot: latest opened_date balance per month ---
    accounts["opened_date"] = pd.to_datetime(accounts["opened_date"], errors="coerce")
    accounts["balance"] = pd.to_numeric(accounts["balance"], errors="coerce").fillna(0)
    accounts["year_month"] = accounts["opened_date"].dt.to_period("M").astype(str)
    acct_snap = (
        accounts.sort_values("opened_date")
        .groupby(["customer_id", "year_month"])
        .agg(account_balance_snapshot=("balance", "last"))
        .reset_index()
    )

    # --- Flags raised per month ---
    flags["flagged_at"] = pd.to_datetime(flags["flagged_at"], errors="coerce")
    flags["year_month"] = flags["flagged_at"].dt.to_period("M").astype(str)
    flag_monthly = (
        flags.groupby(["customer_id", "year_month"])
        .agg(flags_raised=("flag_id", "count"))
        .reset_index()
    )

    # --- Merge everything ---
    result = txn_monthly.merge(acct_snap, on=["customer_id", "year_month"], how="left")
    result = result.merge(flag_monthly, on=["customer_id", "year_month"], how="left")

    result["account_balance_snapshot"] = result["account_balance_snapshot"].fillna(0)
    result["flags_raised"] = result["flags_raised"].fillna(0).astype(int)

    now = datetime.now(timezone.utc)
    result["computed_at"] = now.isoformat()

    row_count = postgres.write(result, "analytics_monthly", "gold")
    logger.info("Gold analytics_monthly: wrote %d rows.", row_count)

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "distinct_customers": MetadataValue.int(result["customer_id"].nunique()),
            "distinct_months": MetadataValue.int(result["year_month"].nunique()),
            "computed_at": MetadataValue.text(now.isoformat()),
        }
    )
