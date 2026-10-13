from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from .client import ENDPOINT, USER_AGENT, fetch_range
from .helpers import build_windows, parse_date, to_epoch
from .logger import setup_logging

logger = logging.getLogger(__name__)


def window_path(out_dir: Path, start: datetime, end: datetime) -> Path:
    return out_dir / f"hn_{start:%Y%m%dT%H%MZ}_{end:%Y%m%dT%H%MZ}.json"


def download_window(session: requests.Session, start: datetime, end: datetime, out_dir: Path,
                    force: bool = False, delay: float = 0.3) -> str:
    """Fetch one window and write it atomically. Returns 'downloaded' or 'skipped'."""
    path = window_path(out_dir, start, end)
    if path.exists() and not force:
        logger.info("skip   %s (exists)", path.name)
        return "skipped"

    s, e = to_epoch(start), to_epoch(end)
    hits, info = fetch_range(session, s, e, delay)

    ids = [h.get("objectID") for h in hits]
    dupes = len(ids) - len(set(ids))
    if dupes:
        logger.warning("%s: %d duplicate objectIDs inside the window (kept as received)",
                    path.name, dupes)

    payload = {
        "meta": {
            "source": ENDPOINT,
            "query": {"tags": "story", "filter": "created_at_i >= start AND created_at_i < end"},
            "window_start_utc": start.isoformat(),
            "window_end_utc": end.isoformat(),
            "start_epoch": s,
            "end_epoch": e,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "nbHits": info["nbHits"],
            "hits_retrieved": len(hits),
            "pages": info["pages"],
            "splits": info["splits"],
            "duplicate_object_ids": dupes,
        },
        "hits": hits,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    logger.info("saved  %s  hits=%d pages=%d splits=%d",
                path.name, len(hits), info["pages"], info["splits"])
    return "downloaded"


def main(argv=None) -> int:
    setup_logging()
    project_root = Path(__file__).resolve().parents[1]

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD, 00:00 UTC")
    ap.add_argument("--days", type=int, default=1, help="number of days to download (default: 1)")
    ap.add_argument("--window-hours", type=int, default=6)
    ap.add_argument("--out-dir", default=project_root / "data",
                    help="directory for the JSON files (default: <project root>/data)")
    ap.add_argument("--delay", type=float, default=0.3, help="seconds between page requests")
    ap.add_argument("--force", action="store_true", help="re-download existing windows")
    args = ap.parse_args(argv)

    try:
        windows = build_windows(parse_date(args.start_date), args.days, args.window_hours)
    except ValueError as exc:
        ap.error(str(exc))

    now = datetime.now(timezone.utc)
    complete = [w for w in windows if w[1] <= now]
    if len(complete) < len(windows):
        logger.warning("skipping %d window(s) that have not finished yet", len(windows) - len(complete))
    if not complete:
        logger.error("no complete windows to download")
        return 1

    out_dir = Path(args.out_dir)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    counts = {"downloaded": 0, "skipped": 0, "failed": 0}
    for start, end in complete:
        try:
            counts[download_window(session, start, end, out_dir, args.force, args.delay)] += 1
        except Exception as exc:
            counts["failed"] += 1
            logger.error("FAILED %s -> %s: %s", start.isoformat(), end.isoformat(), exc)

    logger.info("done: %(downloaded)d downloaded, %(skipped)d skipped, %(failed)d failed", counts)
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
