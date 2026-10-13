from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import requests

from . import etl
from .client import USER_AGENT
from .downloader import download_window
from .helpers import parse_date
from .logger import setup_logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def plan_downloads(checkpoint_end: datetime | None, start_date: datetime | None,
                until: datetime | None, now: datetime, window_hours: int = 6,
                trailing: int = 2, force_all: bool = False) -> list[tuple[datetime, datetime, bool]]:
    """(window_start, window_end, force) for every window to download, oldest first.

    Only windows that ended before `now` (and before `until`) are included. Windows that end
    at or before the checkpoint are the trailing re-pull and are forced.
    """
    if window_hours <= 0 or 24 % window_hours != 0:
        raise ValueError("window_hours must be a positive divisor of 24 (1, 2, 3, 4, 6, 8, 12 or 24)")
    step = timedelta(hours=window_hours)
    if checkpoint_end is None:
        if start_date is None:
            raise ValueError("first run: no checkpoint yet, pass --start-date YYYY-MM-DD")
        first = start_date
    else:
        first = checkpoint_end - trailing * step
    midnight = first.replace(hour=0, minute=0, second=0, microsecond=0)
    if (first - midnight) % step:
        raise ValueError(f"{first.isoformat()} is not aligned to {window_hours}-hour windows; "
                        "use the same --window-hours as the earlier runs")
    limit = min(now, until) if until else now
    out, s = [], first
    while s + step <= limit:
        out.append((s, s + step, force_all or (checkpoint_end is not None and s + step <= checkpoint_end)))
        s += step
    return out


# database
def connect_with_retry(dsn: str, attempts: int = 15, delay: float = 2.0) -> psycopg.Connection:
    """Connect (autocommit), retrying while the database container is still starting."""
    for i in range(1, attempts + 1):
        try:
            return psycopg.connect(dsn, autocommit=True)
        except psycopg.OperationalError as exc:
            if i == attempts:
                raise
            logger.warning("database not ready (%s); retry %d/%d in %ss", str(exc).strip().splitlines()[0],
                        i, attempts - 1, delay)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def apply_schema(conn: psycopg.Connection, sql_dir: Path | None = None) -> Path:
    """Run sql/01_create_schema*_tables.sql (safe to repeat)."""
    sql_dir = Path(sql_dir) if sql_dir else PROJECT_ROOT / "sql"
    found = sorted(sql_dir.glob("01_create_schema*_tables.sql"))
    if not found:
        raise FileNotFoundError(f"no 01_create_schema*_tables.sql in {sql_dir}")
    conn.execute(found[0].read_text(encoding="utf-8")) # type: ignore
    return found[0]


# the pipeline
def run_pipeline(conn: psycopg.Connection, data_dir: Path, start_date: datetime | None = None,
                until: datetime | None = None, window_hours: int = 6, trailing: int = 2,
                force_download: bool = False, skip_download: bool = False, delay: float = 0.3,
                now: datetime | None = None, session: requests.Session | None = None,
                sql_dir: Path | None = None) -> int:
    """Schema -> download -> ETL. Returns the process exit code (0 ok, 1 something failed)."""
    now = now or datetime.now(timezone.utc)
    data_dir = Path(data_dir)
    schema_file = apply_schema(conn, sql_dir)
    logger.info("schema ready (%s)", schema_file.name)

    row = conn.execute("SELECT last_completed_window_end FROM etl.checkpoint WHERE pipeline_name = %s",
                    (etl.PIPELINE_NAME,)).fetchone()
    checkpoint_end = row[0] if row else None
    if checkpoint_end is not None and start_date is not None:
        logger.warning("checkpoint exists (%s): --start-date is ignored", checkpoint_end.isoformat())

    etl_until, failed_download = until, False
    if not skip_download:
        plan = plan_downloads(checkpoint_end, start_date, until, now, window_hours, trailing, force_download)
        if not plan:
            logger.info("no new complete windows to download")
        session = session or requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        counts = {"downloaded": 0, "skipped": 0, "re-pulled": 0}
        for start, end, force in plan:
            try:
                result = download_window(session, start, end, data_dir, force, delay)
            except Exception as exc:                  # stop here: later windows must not be loaded
                logger.error("download FAILED %s -> %s: %s", start.isoformat(), end.isoformat(), exc)
                failed_download, etl_until = True, start
                break
            counts["re-pulled" if (result == "downloaded" and force) else result] += 1
        logger.info("download: %d new, %d re-pulled, %d already on disk%s", counts["downloaded"],
                    counts["re-pulled"], counts["skipped"], " (stopped at a failure)" if failed_download else "")

    summary = etl.run_etl(conn, data_dir, "auto", trailing, etl_until)
    if summary.run_id is not None:
        logger.info("done: run %d (%s) %s, loaded %d, rejected %d", summary.run_id, summary.run_type,
                    summary.status, summary.loaded, summary.rejected)
    return 0 if (summary.status == "success" and not failed_download) else 1


# CLI
def _date(text: str) -> datetime:
    try:
        return parse_date(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a YYYY-MM-DD date")


def main(argv=None) -> int:
    setup_logging()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start-date", type=_date, help="first run only: YYYY-MM-DD (00:00 UTC) to start from")
    ap.add_argument("--until", type=_date, help="stop before this date, YYYY-MM-DD 00:00 UTC (default: now)")
    ap.add_argument("--data-dir", default=PROJECT_ROOT / "data", help="folder for the JSON files (default: ./data)")
    ap.add_argument("--dsn", default=None, help="libpq connection string (default: $HN_ETL_DSN or compose defaults)")
    ap.add_argument("--window-hours", type=int, default=6)
    ap.add_argument("--trailing-windows", type=int, default=2,
                    help="already-loaded windows to re-download and re-read (default: 2)")
    ap.add_argument("--force-download", action="store_true", help="re-download windows that are already on disk")
    ap.add_argument("--skip-download", action="store_true", help="only load the files already in --data-dir")
    ap.add_argument("--delay", type=float, default=0.3, help="seconds between page requests")
    args = ap.parse_args(argv)
    if args.trailing_windows < 0:
        ap.error("--trailing-windows must be >= 0")

    try:
        with connect_with_retry(args.dsn or etl.default_dsn()) as conn:
            return run_pipeline(conn, Path(args.data_dir), args.start_date, args.until, args.window_hours,
                                args.trailing_windows, args.force_download, args.skip_download, args.delay)
    except (psycopg.OperationalError, FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())