"""
Pipeline package initializer.

Configures logging and exposes the config-loading utility used
throughout the pipeline modules.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# Resolve the project root (parent of the pipeline/ package directory).
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path: str | None = None) -> Dict[str, Any]:
    """Load the client YAML configuration file.

    Args:
        config_path: Explicit path to the YAML file.  Falls back to
            ``config/client_default.yaml`` relative to the project root.

    Returns:
        Parsed configuration dictionary.
    """
    if config_path is None:
        config_path = os.environ.get(
            "PIPELINE_CONFIG",
            str(PROJECT_ROOT / "config" / "client_default.yaml"),
        )
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r", encoding="utf-8") as fh:
        cfg: Dict[str, Any] = yaml.safe_load(fh)

    logging.getLogger(__name__).info("Loaded config from %s", path)
    return cfg
