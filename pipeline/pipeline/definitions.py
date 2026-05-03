"""
Top-level Dagster Definitions object.

Combines all assets, resources, schedules, sensors, and asset checks
into a single :class:`Definitions` instance that Dagster discovers
automatically via the ``[tool.dagster]`` config in ``pyproject.toml``.
"""

from dagster import Definitions

from pipeline.assets.bronze import (
    bronze_accounts,
    bronze_customers,
    bronze_risk_flags,
    bronze_transactions,
)
from pipeline.assets.gold import gold_analytics_monthly, gold_risk_scores
from pipeline.assets.silver import (
    silver_accounts,
    silver_customers,
    silver_risk_flags,
    silver_transactions,
)
from pipeline.checks import (
    check_gold_risk_scores_row_count,
    check_silver_customers_no_null_id,
    check_silver_transactions_valid,
)
from pipeline.resources import oracle_resource, postgres_resource
from pipeline.schedules import daily_full_pipeline_schedule
from pipeline.sensors import csv_file_change_sensor

defs = Definitions(
    assets=[
        # Bronze
        bronze_customers,
        bronze_transactions,
        bronze_accounts,
        bronze_risk_flags,
        # Silver
        silver_customers,
        silver_transactions,
        silver_accounts,
        silver_risk_flags,
        # Gold
        gold_risk_scores,
        gold_analytics_monthly,
    ],
    resources={
        "postgres": postgres_resource,
        "oracle": oracle_resource,
    },
    schedules=[daily_full_pipeline_schedule],
    sensors=[csv_file_change_sensor],
    asset_checks=[
        check_silver_customers_no_null_id,
        check_silver_transactions_valid,
        check_gold_risk_scores_row_count,
    ],
)
"""Single Definitions object for the entire pipeline.

Dagster loads this module via ``[tool.dagster] module_name = "pipeline.definitions"``
in ``pyproject.toml``.
"""
