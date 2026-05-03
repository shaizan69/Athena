"""
PostgreSQL connector using SQLAlchemy Core.

Provides full read/write capabilities including an efficient change-detection
based ``upsert`` that compares row hashes to minimise writes.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    insert,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.engine import Engine

from connectors.base import AbstractWriter

logger = logging.getLogger(__name__)


class PostgresConnector(AbstractWriter):
    """SQLAlchemy-Core-based Postgres reader/writer with change detection.

    Args:
        connection_string: Full SQLAlchemy connection URL, e.g.
            ``postgresql://user:password@localhost:5432/dataplatform``.
    """

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self._engine: Optional[Engine] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Create the SQLAlchemy engine and verify connectivity.

        Raises:
            ConnectionError: If the database is unreachable.
        """
        try:
            self._engine = create_engine(self._connection_string, pool_pre_ping=True)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("PostgresConnector connected to %s", self._connection_string)
        except Exception as exc:
            self._engine = None
            raise ConnectionError(f"Postgres connection failed: {exc}") from exc

    def close(self) -> None:
        """Dispose of the engine and release all pooled connections."""
        if self._engine is not None:
            self._engine.dispose()
            logger.info("PostgresConnector connection pool disposed.")
            self._engine = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def engine(self) -> Engine:
        """Return the active engine, raising if not connected."""
        if self._engine is None:
            raise RuntimeError("PostgresConnector is not connected. Call connect() first.")
        return self._engine

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def read_table(self, table: str, schema: str) -> pd.DataFrame:
        """Read an entire table into a DataFrame.

        Args:
            table: Table name.
            schema: Schema name.

        Returns:
            pd.DataFrame with all rows from the table.
        """
        qualified = f"{schema}.{table}"
        try:
            df = pd.read_sql_table(table, self.engine, schema=schema)
            logger.info("Read %d rows from %s", len(df), qualified)
            return df
        except Exception as exc:
            raise RuntimeError(f"Failed to read {qualified}: {exc}") from exc

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, df: pd.DataFrame, table: str, schema: str) -> int:
        """Write a DataFrame to a table, replacing all existing rows.

        Args:
            df: Data to write.
            table: Target table name.
            schema: Target schema name.

        Returns:
            Number of rows written.
        """
        try:
            row_count = len(df)
            df.to_sql(
                table,
                self.engine,
                schema=schema,
                if_exists="replace",
                index=False,
            )
            logger.info("Wrote %d rows to %s.%s", row_count, schema, table)
            return row_count
        except Exception as exc:
            raise RuntimeError(f"Failed to write to {schema}.{table}: {exc}") from exc

    # ------------------------------------------------------------------
    # Change detection helpers
    # ------------------------------------------------------------------

    def get_existing_hashes(
        self,
        table: str,
        schema: str,
        key_cols: List[str],
        hash_col: str = "row_hash",
    ) -> Dict[Tuple, str]:
        """Fetch ``{primary_key_tuple: row_hash}`` for every row in the table.

        Args:
            table: Table name.
            schema: Schema name.
            key_cols: Column(s) forming the natural key.
            hash_col: Name of the hash column.

        Returns:
            Dictionary mapping key tuples to their current hash strings.
        """
        qualified = f"{schema}.{table}"
        try:
            if not inspect(self.engine).has_table(table, schema=schema):
                logger.info("Table %s does not exist yet; returning empty hash map.", qualified)
                return {}

            cols = ", ".join(key_cols + [hash_col])
            query = text(f'SELECT {cols} FROM {schema}."{table}"')
            with self.engine.connect() as conn:
                rows = conn.execute(query).fetchall()

            hash_map: Dict[Tuple, str] = {}
            for row in rows:
                key = tuple(row[:-1])
                hash_val = row[-1]
                hash_map[key] = hash_val

            logger.info("Fetched %d existing hashes from %s", len(hash_map), qualified)
            return hash_map
        except Exception as exc:
            raise RuntimeError(f"Failed to get hashes from {qualified}: {exc}") from exc

    # ------------------------------------------------------------------
    # Upsert with change detection
    # ------------------------------------------------------------------

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        key_cols: List[str],
        hash_col: str = "row_hash",
        schema: str = "bronze",
    ) -> Dict[str, int]:
        """Insert new rows, update changed rows, skip unchanged rows.

        Algorithm:
            1. Fetch existing ``{key: hash}`` from the target table.
            2. For each incoming row compute its key tuple.
            3. If key not in existing  → INSERT
            4. If key exists & hash differs → UPDATE
            5. If key exists & hash matches → SKIP

        Uses SQLAlchemy Core for efficiency — no ORM overhead.

        Args:
            df: Incoming data (must include ``hash_col``).
            table: Target table name.
            key_cols: Natural key columns.
            hash_col: Column containing the pre-computed row hash.
            schema: Target schema (default ``"bronze"``).

        Returns:
            Dict with counts: ``{"inserted": N, "updated": N, "skipped": N}``.
        """
        if df.empty:
            logger.warning("upsert called with empty DataFrame for %s.%s", schema, table)
            return {"inserted": 0, "updated": 0, "skipped": 0}

        existing_hashes = self.get_existing_hashes(table, schema, key_cols, hash_col)

        to_insert: List[Dict[str, Any]] = []
        to_update: List[Dict[str, Any]] = []
        skipped = 0

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            key = tuple(row_dict[k] for k in key_cols)
            incoming_hash = str(row_dict[hash_col])

            if key not in existing_hashes:
                to_insert.append(row_dict)
            elif existing_hashes[key] != incoming_hash:
                to_update.append(row_dict)
            else:
                skipped += 1

        # Ensure the table exists (create on first run).
        if not inspect(self.engine).has_table(table, schema=schema):
            logger.info("Table %s.%s does not exist — creating via initial write.", schema, table)
            df.head(0).to_sql(table, self.engine, schema=schema, if_exists="replace", index=False)

        # ------ INSERTS ------
        if to_insert:
            insert_df = pd.DataFrame(to_insert)
            insert_df.to_sql(table, self.engine, schema=schema, if_exists="append", index=False)
            logger.info("Inserted %d rows into %s.%s", len(to_insert), schema, table)

        # ------ UPDATES ------
        if to_update:
            meta = MetaData(schema=schema)
            meta.reflect(bind=self.engine, only=[table], schema=schema)
            tbl = meta.tables[f"{schema}.{table}"]

            with self.engine.begin() as conn:
                for row_dict in to_update:
                    where_clause = None
                    for kc in key_cols:
                        condition = tbl.c[kc] == row_dict[kc]
                        where_clause = condition if where_clause is None else where_clause & condition

                    values = {k: v for k, v in row_dict.items() if k not in key_cols}
                    conn.execute(update(tbl).where(where_clause).values(**values))

            logger.info("Updated %d rows in %s.%s", len(to_update), schema, table)

        counts = {
            "inserted": len(to_insert),
            "updated": len(to_update),
            "skipped": skipped,
        }
        logger.info(
            "Upsert summary for %s.%s — inserted: %d, updated: %d, skipped: %d",
            schema,
            table,
            counts["inserted"],
            counts["updated"],
            counts["skipped"],
        )
        return counts
