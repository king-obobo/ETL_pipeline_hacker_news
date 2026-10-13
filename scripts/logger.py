"""Logging setup. Call setup_logging() once, from main(); modules just use
logging.getLogger(__name__)."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def setup_logging(level: int = logging.INFO, log_dir: Path | None = None) -> None:
    """Configure the root logger: console at `level`, logs/pipeline.log at WARNING and above.

    Safe to call more than once (handlers are added only the first time).
    """
    root = logging.getLogger()
    if any(getattr(h, "_hn_etl", False) for h in root.handlers):
        return

    log_dir = Path(log_dir) if log_dir else LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = RotatingFileHandler(
        log_dir / "pipeline.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    for handler in (file_handler, stream_handler):
        handler._hn_etl = True #type: ignore
        root.addHandler(handler)
    root.setLevel(level)
