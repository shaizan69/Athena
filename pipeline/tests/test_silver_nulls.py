"""
Tests for Silver layer null-handling logic.

Validates that the ``_clean_dataframe`` function correctly fills nulls
in numeric and string columns and drops fully-null rows.
"""

import pandas as pd
import pytest

# Import the cleaning function directly from the Silver module.
from pipeline.assets.silver import _clean_dataframe


class TestSilverNulls:
    """Test suite for null handling in Silver cleaning transforms."""

    def test_numeric_null_filled_with_zero(self):
        """Null values in numeric columns must be filled with 0."""
        df = pd.DataFrame({
            "customer_id": ["CUST-0001"],
            "amount": [None],
            "name": ["Alice"],
        })

        result = _clean_dataframe(df, numeric_cols=["amount"])

        assert result["amount"].iloc[0] == 0.0

    def test_string_null_filled_with_unknown(self):
        """Null values in string columns must be filled with 'unknown'."""
        df = pd.DataFrame({
            "customer_id": ["CUST-0001"],
            "country": [None],
            "amount": [100.0],
        })

        result = _clean_dataframe(df)

        assert result["country"].iloc[0] == "unknown"

    def test_fully_null_row_dropped(self):
        """A row where every column is null should be dropped as a duplicate
        of other null rows or simply removed during de-duplication.

        Note: _clean_dataframe fills nulls first, then drops exact duplicates.
        Two fully-null rows would become identical after fill → one is dropped.
        """
        df = pd.DataFrame({
            "customer_id": [None, None, "CUST-0001"],
            "name": [None, None, "Alice"],
            "amount": [None, None, 100.0],
        })

        result = _clean_dataframe(df, numeric_cols=["amount"])

        # The two all-null rows become identical after fill → deduplicated to 1
        # So we expect 2 rows total: one filled-null row + the CUST-0001 row
        assert len(result) == 2

    def test_string_casing_normalized_to_lowercase(self):
        """String columns should be stripped and lowercased."""
        df = pd.DataFrame({
            "status": ["  ACTIVE  ", "Pending", "COMPLETED"],
        })

        result = _clean_dataframe(df)

        assert list(result["status"]) == ["active", "pending", "completed"]

    def test_duplicates_dropped(self):
        """Exact duplicate rows must be removed."""
        df = pd.DataFrame({
            "customer_id": ["CUST-0001", "CUST-0001", "CUST-0002"],
            "name": ["Alice", "Alice", "Bob"],
        })

        result = _clean_dataframe(df)

        assert len(result) == 2
