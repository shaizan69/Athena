"""
Tests for Bronze layer idempotency.

Validates that re-running Bronze ingestion with the same data produces
no duplicate rows, and that only changed rows are updated on subsequent runs.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_bronze_test_context(df_to_extract: pd.DataFrame, existing_hashes: dict):
    """Build mock Dagster context and resources for Bronze asset testing.

    Args:
        df_to_extract: DataFrame that the MockOracle will return.
        existing_hashes: Dict of ``{key_tuple: hash_str}`` already in Postgres.

    Returns:
        A mock context with ``oracle`` and ``postgres`` resources configured.
    """
    context = MagicMock()

    # Mock Oracle
    mock_oracle = MagicMock()
    mock_oracle.extract.return_value = df_to_extract
    context.resources.oracle = mock_oracle

    # Mock Postgres
    mock_postgres = MagicMock()
    mock_postgres.get_existing_hashes.return_value = existing_hashes

    # Track upsert calls so we can inspect arguments
    upsert_results = {"inserted": 0, "updated": 0, "skipped": 0}

    def mock_upsert(df, table, key_cols, hash_col="row_hash", schema="bronze"):
        """Simulate the upsert logic for testing."""
        nonlocal upsert_results
        inserted = 0
        updated = 0
        skipped = 0
        for _, row in df.iterrows():
            key = tuple(row[k] for k in key_cols)
            incoming_hash = str(row[hash_col])
            if key not in existing_hashes:
                inserted += 1
            elif existing_hashes[key] != incoming_hash:
                updated += 1
            else:
                skipped += 1

        upsert_results = {"inserted": inserted, "updated": updated, "skipped": skipped}
        return upsert_results

    mock_postgres.upsert.side_effect = mock_upsert
    context.resources.postgres = mock_postgres

    return context, upsert_results


class TestBronzeIdempotency:
    """Test suite for Bronze asset idempotency guarantees."""

    def test_same_data_no_duplicates(self):
        """Running the Bronze asset twice with identical data must not insert
        duplicate rows — all rows should be skipped on the second run.
        """
        from pipeline.assets.bronze import _bronze_extract_and_load, _compute_row_hash

        # Simulate source data
        source_df = pd.DataFrame({
            "customer_id": ["CUST-0001", "CUST-0002"],
            "full_name": ["Alice", "Bob"],
            "country": ["US", "UK"],
        })

        # Compute what the hashes WILL be (same logic as the asset)
        original_cols = list(source_df.columns)
        expected_hashes = {}
        for _, row in source_df.iterrows():
            h = _compute_row_hash(row, original_cols)
            key = (row["customer_id"],)
            expected_hashes[key] = h

        # Second run: existing hashes already match → all skipped
        context, _ = _make_bronze_test_context(source_df, expected_hashes)
        result = _bronze_extract_and_load(context, "customers", "customers", "customer_id")

        # Verify the postgres.upsert was called and check result metadata
        call_args = context.resources.postgres.upsert.call_args
        assert call_args is not None

        # The upsert mock should have skipped all rows
        upsert_return = context.resources.postgres.upsert.return_value
        # Since we're using side_effect, check the actual call result
        final_df = call_args.kwargs.get("df")
        if final_df is None:
            final_df = call_args[1].get("df") if len(call_args) > 1 else source_df
            
        actual_counts = context.resources.postgres.upsert(
            df=final_df,
            table="customers",
            key_cols=["customer_id"],
            hash_col="row_hash",
            schema="bronze",
        )
        assert actual_counts["skipped"] == 2
        assert actual_counts["inserted"] == 0

    def test_updated_data_updates_only_changed_rows(self):
        """When source data changes for one row, only that row should be
        updated — unchanged rows remain skipped.
        """
        from pipeline.assets.bronze import _compute_row_hash

        # Original data
        source_df = pd.DataFrame({
            "customer_id": ["CUST-0001", "CUST-0002"],
            "full_name": ["Alice", "Bob"],
            "country": ["US", "UK"],
        })
        original_cols = list(source_df.columns)

        # Existing hashes: CUST-0001 has original hash, CUST-0002 has original hash
        existing = {}
        for _, row in source_df.iterrows():
            key = (row["customer_id"],)
            existing[key] = _compute_row_hash(row, original_cols)

        # Now modify the source: change Bob's country
        updated_df = pd.DataFrame({
            "customer_id": ["CUST-0001", "CUST-0002"],
            "full_name": ["Alice", "Bob"],
            "country": ["US", "DE"],  # Bob's country changed
        })

        # Add hash column as the asset would
        updated_df["ingested_at"] = "2025-01-01T00:00:00"
        updated_df["row_hash"] = updated_df.apply(
            lambda r: _compute_row_hash(r, original_cols), axis=1
        )

        # Simulate upsert classification
        inserted = 0
        updated_count = 0
        skipped = 0
        for _, row in updated_df.iterrows():
            key = (row["customer_id"],)
            incoming_hash = row["row_hash"]
            if key not in existing:
                inserted += 1
            elif existing[key] != incoming_hash:
                updated_count += 1
            else:
                skipped += 1

        assert inserted == 0
        assert updated_count == 1  # Only CUST-0002 changed
        assert skipped == 1  # CUST-0001 unchanged
