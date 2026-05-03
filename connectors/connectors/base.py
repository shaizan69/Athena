"""
Abstract base classes for all database connectors.

Defines the contract that every reader (extractor) and writer connector
must satisfy, enabling seamless swapping between real and mock
implementations without changing pipeline code.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd


class AbstractConnector(ABC):
    """Abstract base class for database read / extract connectors.

    Every source connector (Oracle, CSV mock, etc.) must implement
    ``connect``, ``extract``, and ``close``.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the data source.

        Raises:
            ConnectionError: If the connection cannot be established.
        """

    @abstractmethod
    def extract(self, query: str) -> pd.DataFrame:
        """Execute a query (or query-name) and return the result as a DataFrame.

        Args:
            query: A SQL query string or a logical query name, depending on
                   the concrete implementation.

        Returns:
            pd.DataFrame containing the extracted rows.

        Raises:
            RuntimeError: If extraction fails.
        """

    @abstractmethod
    def close(self) -> None:
        """Release all resources held by the connector."""


class AbstractWriter(ABC):
    """Abstract base class for database write connectors.

    Every target connector (Postgres, etc.) must implement
    ``connect``, ``write``, ``upsert``, and ``close``.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the data target.

        Raises:
            ConnectionError: If the connection cannot be established.
        """

    @abstractmethod
    def write(self, df: pd.DataFrame, table: str, schema: str) -> int:
        """Write a DataFrame to the specified table, replacing existing data.

        Args:
            df: Data to write.
            table: Target table name.
            schema: Target schema name.

        Returns:
            Number of rows written.
        """

    @abstractmethod
    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        key_cols: List[str],
        hash_col: str = "row_hash",
    ) -> Dict[str, int]:
        """Insert new rows and update changed rows based on a hash column.

        Args:
            df: Incoming data with a pre-computed hash column.
            table: Fully-qualified target table (e.g. ``"bronze.customers"``).
            key_cols: Column(s) forming the natural / primary key.
            hash_col: Name of the column containing the row hash.

        Returns:
            Dict with keys ``inserted``, ``updated``, ``skipped`` and their counts.
        """

    @abstractmethod
    def close(self) -> None:
        """Release all resources held by the writer."""
