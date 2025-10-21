"""Configure logging for pipeline runs."""
from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    level_name = os.getenv("PIPELINE_LOG_LEVEL", "INFO")
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
