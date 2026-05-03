"""
Dagster sensor definitions.

Polls the ``data/`` directory for CSV modifications and triggers only
the Bronze assets whose source files have changed.
"""

import logging
import os
from pathlib import Path

from dagster import (
    AssetKey,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    sensor,
)

from pipeline import PROJECT_ROOT, load_config

logger = logging.getLogger(__name__)

# Map CSV filenames to their corresponding Bronze asset keys.
CSV_TO_ASSET = {
    "customers.csv": AssetKey("bronze_customers"),
    "transactions.csv": AssetKey("bronze_transactions"),
    "accounts.csv": AssetKey("bronze_accounts"),
    "risk_flags.csv": AssetKey("bronze_risk_flags"),
}


@sensor(
    name="csv_file_change_sensor",
    minimum_interval_seconds=30,
)
def csv_file_change_sensor(context: SensorEvaluationContext):
    """Polls the data/ directory every 30 seconds for modified CSV files.

    If any CSV has been modified since the last sensor evaluation, a
    RunRequest is emitted targeting only the Bronze asset(s) for the
    changed file(s).

    The sensor uses the cursor to persist the last-known modification
    timestamps.
    """
    config = load_config()
    data_dir = Path(config["oracle"]["data_dir"])
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir

    # Restore previous timestamps from cursor (format: "file1:ts1,file2:ts2")
    prev_timestamps: dict[str, float] = {}
    if context.cursor:
        for entry in context.cursor.split(","):
            if ":" in entry:
                fname, ts = entry.rsplit(":", 1)
                try:
                    prev_timestamps[fname] = float(ts)
                except ValueError:
                    pass

    changed_assets: list[AssetKey] = []
    new_timestamps: dict[str, float] = {}

    for csv_name, asset_key in CSV_TO_ASSET.items():
        csv_path = data_dir / csv_name
        if not csv_path.exists():
            continue

        mtime = os.path.getmtime(csv_path)
        new_timestamps[csv_name] = mtime

        prev_mtime = prev_timestamps.get(csv_name)
        if prev_mtime is None or mtime > prev_mtime:
            changed_assets.append(asset_key)
            logger.info("Sensor detected change in %s", csv_name)

    # Persist new timestamps
    cursor_str = ",".join(f"{k}:{v}" for k, v in new_timestamps.items())
    context.update_cursor(cursor_str)

    if changed_assets:
        yield RunRequest(
            run_key=f"csv_change_{cursor_str}",
            asset_selection=changed_assets,
        )
    else:
        logger.debug("No CSV changes detected.")
