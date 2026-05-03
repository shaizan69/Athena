"""
Tests for the change-detection logic in PostgresConnector.upsert().

Uses pytest with mocked Postgres connections to verify that the upsert
correctly classifies rows as insert, update, or skip based on hash
comparison.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import Column, MetaData, String, Table


def _make_upsert_testable():
    """Build a PostgresConnector with mocked internals for testing.

    Returns a tuple of (connector, mock_engine) so tests can configure
    the mock as needed.
    """
    from connectors.postgres import PostgresConnector

    pg = PostgresConnector.__new__(PostgresConnector)
    pg._connection_string = "postgresql://test:test@localhost/test"
    pg._engine = MagicMock()
    return pg


class TestChangeDetection:
    """Test suite for the hash-based change detection in upsert()."""

    def test_changed_hash_detected_as_update(self):
        """A row whose hash differs from the existing hash must be updated."""
        pg = _make_upsert_testable()

        # Existing row in Postgres: key=("CUST-0001",), hash="aaa"
        pg.get_existing_hashes = MagicMock(return_value={("CUST-0001",): "aaa"})

        # Incoming row has key "CUST-0001" but a DIFFERENT hash
        df = pd.DataFrame([{
            "customer_id": "CUST-0001",
            "full_name": "Updated Name",
            "row_hash": "bbb",  # different from "aaa"
        }])

        # Mock the table-existence check and the table reflection
        with patch("connectors.postgres.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.return_value = True

            # Use a real Table object but mock the Metadata reflection
            meta = MetaData(schema="bronze")
            real_table = Table(
                "customers", meta,
                Column("customer_id", String, primary_key=True),
                Column("full_name", String),
                Column("row_hash", String),
            )

            with patch("connectors.postgres.MetaData") as mock_meta_class:
                mock_meta_instance = mock_meta_class.return_value
                mock_meta_instance.tables = {"bronze.customers": real_table}
                
                mock_conn = MagicMock()
                pg._engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
                pg._engine.begin.return_value.__exit__ = MagicMock(return_value=False)

                counts = pg.upsert(
                    df=df,
                    table="customers",
                    key_cols=["customer_id"],
                    hash_col="row_hash",
                    schema="bronze",
                )

        assert counts["updated"] == 1
        assert counts["inserted"] == 0
        assert counts["skipped"] == 0

    def test_unchanged_hash_detected_as_skip(self):
        """A row whose hash matches the existing hash must be skipped."""
        pg = _make_upsert_testable()

        pg.get_existing_hashes = MagicMock(return_value={("CUST-0001",): "same_hash"})

        df = pd.DataFrame([{
            "customer_id": "CUST-0001",
            "full_name": "Same Name",
            "row_hash": "same_hash",  # same as existing
        }])

        with patch("connectors.postgres.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.return_value = True
            counts = pg.upsert(
                df=df,
                table="customers",
                key_cols=["customer_id"],
                hash_col="row_hash",
                schema="bronze",
            )

        assert counts["skipped"] == 1
        assert counts["inserted"] == 0
        assert counts["updated"] == 0

    def test_new_row_detected_as_insert(self):
        """A row with a key not present in the existing table must be inserted."""
        pg = _make_upsert_testable()

        # Empty table — no existing hashes
        pg.get_existing_hashes = MagicMock(return_value={})

        df = pd.DataFrame([{
            "customer_id": "CUST-9999",
            "full_name": "Brand New Customer",
            "row_hash": "new_hash",
        }])

        with patch("connectors.postgres.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.return_value = True

            # Mock to_sql for the insert path
            with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
                counts = pg.upsert(
                    df=df,
                    table="customers",
                    key_cols=["customer_id"],
                    hash_col="row_hash",
                    schema="bronze",
                )

        assert counts["inserted"] == 1
        assert counts["updated"] == 0
        assert counts["skipped"] == 0
