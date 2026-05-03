"""
Oracle database connector (production skeleton).

This module satisfies the same :class:`AbstractConnector` interface as
:class:`MockOracleConnector`.  It is provided as a drop-in replacement for
when real Oracle connectivity is required.

How to swap the mock for real Oracle
-------------------------------------
1. Install cx_Oracle:  ``pip install cx-Oracle``
2. Uncomment the ``import cx_Oracle`` line below.
3. In your Dagster resource configuration (``resources.py``), change::

       MockOracleConnector(data_dir=config["oracle"]["data_dir"])

   to::

       OracleConnector(
           host="oracle-host.example.com",
           port=1521,
           service_name="ORCLPDB1",
           user="etl_user",
           password="s3cret",
       )

4. Replace logical query names with actual SQL strings in each Bronze asset.
   The ``extract(query)`` method on this class runs the SQL directly.

No other pipeline code needs to change — both connectors return a
``pd.DataFrame`` and implement ``connect() / extract() / close()``.
"""

import logging
from typing import Optional

import pandas as pd

from connectors.base import AbstractConnector

# Uncomment when cx_Oracle is installed:
# import cx_Oracle

logger = logging.getLogger(__name__)


class OracleConnector(AbstractConnector):
    """Production Oracle connector using cx_Oracle.

    Args:
        host: Database hostname.
        port: Listener port (default 1521).
        service_name: Oracle service name.
        user: Database username.
        password: Database password.
    """

    def __init__(
        self,
        host: str,
        port: int = 1521,
        service_name: str = "ORCLPDB1",
        user: str = "etl_user",
        password: str = "password",
    ) -> None:
        self._host = host
        self._port = port
        self._service_name = service_name
        self._user = user
        self._password = password
        self._connection = None

    def connect(self) -> None:
        """Establish a connection to the Oracle database.

        Raises:
            ConnectionError: If the connection cannot be established.
        """
        try:
            # Uncomment the following block when cx_Oracle is installed:
            # dsn = cx_Oracle.makedsn(self._host, self._port, service_name=self._service_name)
            # self._connection = cx_Oracle.connect(
            #     user=self._user,
            #     password=self._password,
            #     dsn=dsn,
            # )
            # logger.info("OracleConnector connected to %s:%s/%s", self._host, self._port, self._service_name)
            raise NotImplementedError(
                "OracleConnector requires cx_Oracle. "
                "Install it and uncomment the connection block above."
            )
        except NotImplementedError:
            raise
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to Oracle: {exc}") from exc

    def extract(self, query: str) -> pd.DataFrame:
        """Execute a SQL query against Oracle and return a DataFrame.

        Args:
            query: A raw SQL query string.

        Returns:
            pd.DataFrame containing the query results.

        Raises:
            RuntimeError: If extraction fails or connector is not connected.
        """
        if self._connection is None:
            raise RuntimeError("OracleConnector is not connected. Call connect() first.")

        try:
            df = pd.read_sql(query, self._connection)
            logger.info("OracleConnector extracted %d rows.", len(df))
            return df
        except Exception as exc:
            raise RuntimeError(f"Oracle extraction failed: {exc}") from exc

    def close(self) -> None:
        """Close the Oracle connection and release resources."""
        if self._connection is not None:
            try:
                self._connection.close()
                logger.info("OracleConnector connection closed.")
            except Exception as exc:
                logger.warning("Error closing Oracle connection: %s", exc)
            finally:
                self._connection = None
