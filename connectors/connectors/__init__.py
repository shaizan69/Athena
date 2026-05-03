"""
Connectors Library
==================
Standalone Python package providing abstract database connector interfaces
and concrete implementations for Oracle, Postgres, and CSV-based mock sources.

Usage:
    from connectors.mock_oracle import MockOracleConnector
    from connectors.postgres import PostgresConnector
"""

from connectors.base import AbstractConnector, AbstractWriter
from connectors.mock_oracle import MockOracleConnector
from connectors.postgres import PostgresConnector

__all__ = [
    "AbstractConnector",
    "AbstractWriter",
    "MockOracleConnector",
    "PostgresConnector",
]
