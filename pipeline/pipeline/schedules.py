"""
Dagster schedule definitions.

Reads the cron expression and timezone from the client YAML config
so nothing is hardcoded.
"""

import logging

from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition

from pipeline import load_config

logger = logging.getLogger(__name__)

config = load_config()
cron_schedule = config["schedule"]["cron"]
timezone = config["schedule"]["timezone"]

daily_full_pipeline_schedule = ScheduleDefinition(
    name="daily_full_pipeline",
    cron_schedule=cron_schedule,
    target=AssetSelection.groups("bronze", "silver", "gold"),
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone=timezone,
)
"""Runs the entire Bronze → Silver → Gold pipeline daily.

The cron expression and timezone are read from ``config/client_default.yaml``
under ``schedule.cron`` and ``schedule.timezone``.
"""

logger.info(
    "Schedule 'daily_full_pipeline' configured: cron=%s, tz=%s",
    cron_schedule,
    timezone,
)
