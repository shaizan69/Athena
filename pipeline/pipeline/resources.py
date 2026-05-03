"""
Dagster resources for Postgres and Oracle connections.

Both resources read their configuration from ``config/client_default.yaml``
via :func:`pipeline.load_config`, ensuring zero hardcoded values.
"""

import logging
from typing import Any, Dict

from dagster import InitResourceContext, resource

from connectors.mock_oracle import MockOracleConnector
from connectors.postgres import PostgresConnector
from pipeline import load_config

logger = logging.getLogger(__name__)


@resource
def postgres_resource(context: InitResourceContext) -> PostgresConnector:
    """Dagster resource that yields a connected PostgresConnector.

    Reads ``postgres.connection_string`` from the client config YAML.

    Returns:
        A connected :class:`PostgresConnector` instance.
    """
    config = load_config()
    conn_str = config["postgres"]["connection_string"]
    pg = PostgresConnector(connection_string=conn_str)
    pg.connect()
    logger.info("postgres_resource: connected.")
    return pg


@resource
def oracle_resource(context: InitResourceContext) -> MockOracleConnector:
    """Dagster resource that yields a connected MockOracleConnector.

    Reads ``oracle.data_dir`` from the client config YAML.

    To use a real Oracle connector, swap ``MockOracleConnector`` for
    ``OracleConnector`` and update the config keys accordingly.

    Returns:
        A connected :class:`MockOracleConnector` instance.
    """
    config = load_config()
    data_dir = config["oracle"]["data_dir"]
    oracle = MockOracleConnector(data_dir=data_dir)
    oracle.connect()
    logger.info("oracle_resource: connected to mock oracle (data_dir=%s).", data_dir)
    return oracle
