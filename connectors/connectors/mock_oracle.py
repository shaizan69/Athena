"""
CSV-based mock Oracle connector.

Satisfies the :class:`AbstractConnector` interface so it can drop in anywhere
the real Oracle connector is expected.  Reads CSV files from a configured
``data_dir`` and maps logical query names to file paths.

Mapping convention:
    query name  →  CSV file
    "customers"      →  customers.csv
    "transactions"   →  transactions.csv
    "accounts"       →  accounts.csv
    "risk_flags"     →  risk_flags.csv
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from connectors.base import AbstractConnector

logger = logging.getLogger(__name__)

# Default mapping from logical query names to CSV filenames.
DEFAULT_QUERY_MAP: Dict[str, str] = {
    "customers": "customers.csv",
    "transactions": "transactions.csv",
    "accounts": "accounts.csv",
    "risk_flags": "risk_flags.csv",
}


class MockOracleConnector(AbstractConnector):
    """CSV-backed mock that emulates an Oracle extractor.

    Args:
        data_dir: Path to the directory containing CSV files.
        query_map: Optional override mapping ``{query_name: filename}``.
    """

    def __init__(
        self,
        data_dir: str,
        query_map: Optional[Dict[str, str]] = None,
    ) -> None:
        self._data_dir = Path(data_dir)
        self._query_map = query_map or DEFAULT_QUERY_MAP
        self._connected = False

    def connect(self) -> None:
        """Validate that the data directory exists.

        Raises:
            ConnectionError: If ``data_dir`` is not a valid directory.
        """
        if not self._data_dir.is_dir():
            raise ConnectionError(
                f"Mock Oracle data directory does not exist: {self._data_dir}"
            )
        self._connected = True
        logger.info("MockOracleConnector connected to %s", self._data_dir)

    def extract(self, query: str) -> pd.DataFrame:
        """Return a DataFrame for the given logical query name.

        Args:
            query: A key in the query map (e.g. ``"customers"``).

        Returns:
            pd.DataFrame read from the corresponding CSV.

        Raises:
            RuntimeError: If not connected or query name is unknown.
        """
        if not self._connected:
            raise RuntimeError("MockOracleConnector is not connected. Call connect() first.")

        if query not in self._query_map:
            raise RuntimeError(
                f"Unknown query '{query}'. Available queries: {list(self._query_map.keys())}"
            )

        csv_path = self._data_dir / self._query_map[query]
        if not csv_path.exists():
            raise RuntimeError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(csv_path)
        logger.info(
            "MockOracleConnector extracted %d rows for query '%s' from %s",
            len(df),
            query,
            csv_path,
        )
        return df

    def close(self) -> None:
        """Mark the connector as disconnected."""
        self._connected = False
        logger.info("MockOracleConnector closed.")
