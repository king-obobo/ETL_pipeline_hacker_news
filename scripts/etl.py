from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from . import transform as T
from .logger import setup_logging

logger = logging.getLogger(__name__)

PIPELINE_NAME = "hn_stories"
LOCK_KEY = 727_001                      # pg_advisory_lock key: one ETL run at a time
_FILE_RE = re.compile(r"hn_(\d{8}T\d{4}Z)_(\d{8}T\d{4}Z)\.json")


# planning
@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    path: Path

    @property
    def label(self) -> str:
        return self.path.name


def _parse_stamp(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%dT%H%MZ").replace(tzinfo=timezone.utc)


def discover_windows(data_dir: Path) -> list[Window]:
    """Window files in data_dir, oldest first (the window comes from the file name)."""
    wins = []
    for p in Path(data_dir).glob("hn_*.json"):
        m = _FILE_RE.fullmatch(p.name)
        if m:
            wins.append(Window(_parse_stamp(m.group(1)), _parse_stamp(m.group(2)), p))
    return sorted(wins, key=lambda w: w.start)


def plan_windows(windows: list[Window], checkpoint_end: datetime | None,
                mode: str = "auto", trailing: int = 2) -> tuple[str, list[Window]]:
    """Return (run_type, windows to process)."""
    if mode not in ("auto", "full", "incremental"):
        raise ValueError(f"unknown mode {mode!r}")
    if mode == "incremental" and checkpoint_end is None:
        raise ValueError("--mode incremental needs a checkpoint: run a full load first")
    if mode == "full" or checkpoint_end is None:
        return "full", list(windows)
    # a window is processed if it ends after the checkpoint, or within `trailing` windows of it
    todo = [w for w in windows if w.end > checkpoint_end - trailing * (w.end - w.start)]
    return "incremental", todo


def check_no_gaps(todo: list[Window], checkpoint_end: datetime | None) -> None:
    """Refuse to load across a hole. The checkpoint only moves forward, so a missing window
    between the checkpoint and the new data would silently never be loaded."""
    if not todo:
        return
    if checkpoint_end is not None and todo[0].start > checkpoint_end:
        raise ValueError(f"missing window file(s) between {checkpoint_end.isoformat()} and "
                        f"{todo[0].start.isoformat()}: download them first")
    for a, b in zip(todo, todo[1:]):
        if b.start > a.end:
            raise ValueError(f"missing window file(s) between {a.end.isoformat()} and {b.start.isoformat()}: "
                            "download them first")
        if b.start < a.end:
            raise ValueError(f"overlapping window files {a.label} and {b.label}: "
                            "use one window size per data directory")


# file loading
def load_window_file(win: Window) -> list:
    """Read one window file and check it is complete and is the window its name says."""
    doc = json.loads(win.path.read_text(encoding="utf-8"))
    meta, hits = doc["meta"], doc["hits"]
    counts = (meta["nbHits"], meta["hits_retrieved"], len(hits))
    if len(set(counts)) != 1:
        raise ValueError(f"{win.label}: count mismatch (nbHits={counts[0]}, "
                        f"hits_retrieved={counts[1]}, array={counts[2]})")
    span = (datetime.fromisoformat(meta["window_start_utc"]), datetime.fromisoformat(meta["window_end_utc"]))
    if span != (win.start, win.end):
        raise ValueError(f"{win.label}: meta window {span[0]}..{span[1]} does not match the file name")
    return hits


def _as_object(hit: Any) -> dict:
    return hit if isinstance(hit, dict) else {"_non_object_hit": hit}


# --------------------------------------------------------------------------- SQL
SQL_CLEAN_UPSERT = """
INSERT INTO clean.hn_stories AS c
    (object_id, title, url, author, points, num_comments, created_at, updated_at,
    story_type, row_hash, first_run_id, last_run_id)
SELECT t.object_id, t.title, t.url, t.author, t.points, t.num_comments, t.created_at, t.updated_at,
    t.story_type, t.row_hash, %(run)s, %(run)s
FROM unnest(%(ids)s::bigint[], %(titles)s::text[], %(urls)s::text[], %(authors)s::text[],
            %(points)s::int[], %(comments)s::int[], %(created)s::timestamptz[],
            %(updated)s::timestamptz[], %(types)s::text[], %(hashes)s::text[])
    AS t(object_id, title, url, author, points, num_comments, created_at, updated_at,
        story_type, row_hash)
ON CONFLICT (object_id) DO UPDATE
    SET title = EXCLUDED.title, url = EXCLUDED.url, author = EXCLUDED.author,
        points = EXCLUDED.points, num_comments = EXCLUDED.num_comments,
        updated_at = EXCLUDED.updated_at, story_type = EXCLUDED.story_type,
        row_hash = EXCLUDED.row_hash, last_run_id = EXCLUDED.last_run_id, modified_at = now()
WHERE c.row_hash IS DISTINCT FROM EXCLUDED.row_hash
    AND COALESCE(EXCLUDED.updated_at, '-infinity') >= COALESCE(c.updated_at, '-infinity')
RETURNING (xmax = 0) AS inserted
"""

SQL_FACT_UPSERT = """
INSERT INTO dw.fact_story AS f
    (story_id, author_key, domain_key, created_at, hour_of_day, story_type,
    points, num_comments, has_discussion)
SELECT s.id, a.author_key, d.domain_key, s.created_at, s.hour, s.story_type,
    s.points, s.comments, s.disc
FROM unnest(%(ids)s::bigint[], %(authors)s::text[], %(domains)s::text[], %(created)s::timestamptz[],
            %(hours)s::smallint[], %(types)s::text[], %(points)s::int[], %(comments)s::int[],
            %(disc)s::boolean[])
    AS s(id, author, domain, created_at, hour, story_type, points, comments, disc)
LEFT JOIN dw.dim_author a ON a.author_name = s.author
LEFT JOIN dw.dim_domain d ON d.domain = s.domain
ON CONFLICT (story_id) DO UPDATE
    SET author_key = EXCLUDED.author_key, domain_key = EXCLUDED.domain_key,
        created_at = EXCLUDED.created_at, hour_of_day = EXCLUDED.hour_of_day,
        story_type = EXCLUDED.story_type, points = EXCLUDED.points,
        num_comments = EXCLUDED.num_comments, has_discussion = EXCLUDED.has_discussion
WHERE (f.author_key, f.domain_key, f.created_at, f.hour_of_day, f.story_type,
    f.points, f.num_comments, f.has_discussion)
    IS DISTINCT FROM
    (EXCLUDED.author_key, EXCLUDED.domain_key, EXCLUDED.created_at, EXCLUDED.hour_of_day,
    EXCLUDED.story_type, EXCLUDED.points, EXCLUDED.num_comments, EXCLUDED.has_discussion)
RETURNING (xmax = 0) AS inserted
"""

SQL_CHECKPOINT = """
INSERT INTO etl.checkpoint AS k (pipeline_name, last_completed_window_end, max_updated_at, last_run_id)
VALUES (%(name)s, %(win_end)s, (SELECT max(updated_at) FROM clean.hn_stories), %(run)s)
ON CONFLICT (pipeline_name) DO UPDATE
    SET last_completed_window_end = GREATEST(k.last_completed_window_end, EXCLUDED.last_completed_window_end),
        max_updated_at            = GREATEST(k.max_updated_at, EXCLUDED.max_updated_at),
        last_run_id               = EXCLUDED.last_run_id,
        updated_at                = now()
"""


# window
@dataclass
class WindowResult:
    extracted: int = 0
    rejected: int = 0
    valid: int = 0
    distinct: int = 0
    inserted: int = 0
    updated: int = 0
    loaded: int = 0 

    @property
    def unchanged(self) -> int:
        return self.distinct - self.inserted - self.updated


def _fact_values(rows: list[tuple]) -> dict[str, list]:
    """rows = (object_id, author, url, created_at, story_type, points, num_comments) from clean."""
    return {
        "ids": [r[0] for r in rows], "authors": [r[1] for r in rows],
        "domains": [T.extract_domain(r[2]) for r in rows], "created": [r[3] for r in rows],
        "hours": [r[3].astimezone(timezone.utc).hour for r in rows], "types": [r[4] for r in rows],
        "points": [r[5] for r in rows], "comments": [r[6] for r in rows],
        "disc": [(r[6] or 0) > 0 for r in rows],
    }


def process_window(conn: psycopg.Connection, run_id: int, win: Window, hits: list) -> WindowResult:
    """Load one window. Must be called inside a transaction; the caller commits or rolls back."""
    res = WindowResult(extracted=len(hits))
    objs = [_as_object(h) for h in hits]
    outcomes = [T.process(o) for o in objs]
    stagings = [T.stg_row(o) for o in objs]

    with conn.cursor() as cur:
        batch_id = cur.execute(
            "INSERT INTO etl.batch_log (run_id, window_start, window_end, records_extracted) "
            "VALUES (%s, %s, %s, %s) RETURNING batch_id",
            (run_id, win.start, win.end, len(hits))).fetchone()[0] # type: ignore

        cur.executemany(
            "INSERT INTO stg.hn_stories_raw (run_id, batch_id, object_id, title, url, author, points, "
            "num_comments, created_at, created_at_i, updated_at, tags, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [(run_id, batch_id, s["object_id"], s["title"], s["url"], s["author"], s["points"],
            s["num_comments"], s["created_at"], s["created_at_i"], s["updated_at"], s["tags"],
            Jsonb(s["payload"])) for s in stagings])

        rejects = [(run_id, batch_id, stg["object_id"], Jsonb(stg["payload"]), reason)
                for (story, reason), stg in zip(outcomes, stagings) if reason]
        if rejects:
            cur.executemany(
                "INSERT INTO etl.rejected_records (run_id, batch_id, object_id, raw_payload, reason) "
                "VALUES (%s, %s, %s, %s, %s)", rejects)
        res.rejected = len(rejects)

        valid = [story for story, _ in outcomes if story]
        stories = T.dedupe(valid)
        res.valid, res.distinct = len(valid), len(stories)

        if stories:
            cur.execute(SQL_CLEAN_UPSERT, {
                "run": run_id, "ids": [s.object_id for s in stories],
                "titles": [s.title for s in stories], "urls": [s.url for s in stories],
                "authors": [s.author for s in stories], "points": [s.points for s in stories],
                "comments": [s.num_comments for s in stories], "created": [s.created_at for s in stories],
                "updated": [s.updated_at for s in stories], "types": [s.story_type for s in stories],
                "hashes": [s.row_hash for s in stories]})
            flags = [r[0] for r in cur.fetchall()]
            res.inserted, res.updated = sum(flags), len(flags) - sum(flags)

            # fact is reconciled from what clean now holds (an older/unchanged incoming row
            # must not overwrite newer clean data), like 04_transform does
            cur.execute("SELECT object_id, author, url, created_at, story_type, points, num_comments "
                        "FROM clean.hn_stories WHERE object_id = ANY(%s)", ([s.object_id for s in stories],))
            fv = _fact_values(cur.fetchall())
            cur.execute("INSERT INTO dw.dim_author (author_name) SELECT DISTINCT a "
                        "FROM unnest(%s::text[]) a WHERE a IS NOT NULL ORDER BY a "
                        "ON CONFLICT (author_name) DO NOTHING", (fv["authors"],))
            cur.execute("INSERT INTO dw.dim_domain (domain) SELECT DISTINCT d "
                        "FROM unnest(%s::text[]) d WHERE d IS NOT NULL ORDER BY d "
                        "ON CONFLICT (domain) DO NOTHING", (fv["domains"],))
            cur.execute(SQL_FACT_UPSERT, fv)
            res.loaded = len(cur.fetchall())

        cur.execute("UPDATE etl.batch_log SET status = 'success', records_loaded = %s, "
                    "records_rejected = %s, ended_at = clock_timestamp() WHERE batch_id = %s",
                    (res.loaded, res.rejected, batch_id))
        cur.execute(SQL_CHECKPOINT, {"name": PIPELINE_NAME, "win_end": win.end, "run": run_id})
    return res


# run
@dataclass
class RunSummary:
    run_id: int | None
    run_type: str
    status: str
    windows_done: int = 0
    windows_planned: int = 0
    extracted: int = 0
    rejected: int = 0
    loaded: int = 0
    error: str | None = None


def _checkpoint_end(conn) -> datetime | None:
    row = conn.execute("SELECT last_completed_window_end FROM etl.checkpoint WHERE pipeline_name = %s",
                    (PIPELINE_NAME,)).fetchone()
    return row[0] if row else None


def _close_stale_runs(conn) -> None:
    """Runs left 'running' by a killed process are marked failed so history stays truthful."""
    with conn.transaction():
        n = conn.execute(
            "UPDATE etl.pipeline_runs SET status = 'failed', ended_at = now(), "
            "error_message = 'interrupted: process ended before the run finished' "
            "WHERE status = 'running'").rowcount
        conn.execute("UPDATE etl.batch_log SET status = 'failed', ended_at = now(), "
                    "error_message = 'interrupted' WHERE status = 'running'")
    if n:
        logger.warning("marked %d earlier run(s) left 'running' as failed", n)


def _finish_run(conn, run_id: int, status: str, error: str | None) -> tuple[int, int, int]:
    with conn.transaction():
        return conn.execute(
            "UPDATE etl.pipeline_runs r SET status = %s, ended_at = now(), error_message = %s, "
            "records_extracted = COALESCE(b.e, 0), records_loaded = COALESCE(b.l, 0), "
            "records_rejected = COALESCE(b.r, 0) "
            "FROM (SELECT sum(records_extracted) e, sum(records_loaded) l, sum(records_rejected) r "
            "      FROM etl.batch_log WHERE run_id = %s AND status = 'success') b "
            "WHERE r.run_id = %s RETURNING r.records_extracted, r.records_loaded, r.records_rejected",
            (status, error, run_id, run_id)).fetchone()


def run_etl(conn: psycopg.Connection, data_dir: Path, mode: str = "auto", trailing: int = 2,
            until: datetime | None = None) -> RunSummary:
    """Plan and execute one run. `conn` must be in autocommit mode (transactions are explicit).
    `until` ignores window files that end after it."""
    if not conn.autocommit:
        raise ValueError("run_etl needs an autocommit connection")
    if not conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,)).fetchone()[0]: # type: ignore
        raise RuntimeError("another ETL run holds the lock; not starting")
    try:
        return _run_locked(conn, data_dir, mode, trailing, until)
    finally:
        try:                       # explicit release: a closed session frees it only a moment later
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
        except psycopg.Error:
            pass


def _run_locked(conn: psycopg.Connection, data_dir: Path, mode: str, trailing: int,
                until: datetime | None) -> RunSummary:
    _close_stale_runs(conn)

    windows = discover_windows(data_dir)
    if not windows:
        raise FileNotFoundError(f"no hn_*.json files in {data_dir}")
    if until is not None:
        windows = [w for w in windows if w.end <= until]
    checkpoint_end = _checkpoint_end(conn)
    run_type, todo = plan_windows(windows, checkpoint_end, mode, trailing)
    check_no_gaps(todo, checkpoint_end)
    if not todo:
        logger.info("nothing to do")
        return RunSummary(None, run_type, "success")

    run_id = conn.execute(
        "INSERT INTO etl.pipeline_runs (run_type, range_start, range_end) VALUES (%s, %s, %s) RETURNING run_id",
        (run_type, todo[0].start, todo[-1].end)).fetchone()[0] # type: ignore
    logger.info("run %d (%s): %d window(s), %s .. %s", run_id, run_type, len(todo),
                todo[0].start.isoformat(), todo[-1].end.isoformat())

    summary = RunSummary(run_id, run_type, "running", windows_planned=len(todo))
    for win in todo:
        n_hits = 0
        try:
            hits = load_window_file(win)
            n_hits = len(hits)
            with conn.transaction():
                res = process_window(conn, run_id, win, hits)
        except Exception as exc:                      # this window rolled back; stop the run
            summary.status, summary.error = "failed", f"{win.label}: {type(exc).__name__}: {exc}"
            logger.error("FAILED %s", summary.error)
            try:
                with conn.transaction():
                    conn.execute(
                        "INSERT INTO etl.batch_log (run_id, window_start, window_end, status, "
                        "records_extracted, ended_at, error_message) "
                        "VALUES (%s, %s, %s, 'failed', %s, now(), %s)",
                        (run_id, win.start, win.end, n_hits, summary.error[:2000]))
            except Exception:
                logger.exception("could not record the failed batch")
            break
        summary.windows_done += 1
        logger.info("window %s: extracted=%d rejected=%d valid=%d distinct=%d clean +%d ~%d =%d fact written=%d",
                    win.label, res.extracted, res.rejected, res.valid, res.distinct,
                    res.inserted, res.updated, res.unchanged, res.loaded)
    else:
        summary.status = "success"

    summary.extracted, summary.loaded, summary.rejected = _finish_run(conn, run_id, summary.status, summary.error)
    logger.info("run %d %s: %d/%d windows, extracted=%d loaded=%d rejected=%d",
                run_id, summary.status, summary.windows_done, summary.windows_planned,
                summary.extracted, summary.loaded, summary.rejected)
    return summary


# CLI
def default_dsn() -> str:
    """HN_ETL_DSN, else the docker-compose defaults (override with POSTGRES_* variables)."""
    return os.environ.get("HN_ETL_DSN") or (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'hn_etl')} user={os.environ.get('POSTGRES_USER', 'etl_user')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'etl_pass')}")


def main(argv=None) -> int:
    setup_logging()
    project_root = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default=project_root / "data", help="folder with hn_*.json (default: ./data)")
    ap.add_argument("--dsn", default=None, help="libpq connection string (default: $HN_ETL_DSN or compose defaults)")
    ap.add_argument("--mode", choices=["auto", "full", "incremental"], default="auto")
    ap.add_argument("--trailing-windows", type=int, default=2,
                    help="already-loaded windows to re-read in incremental mode (default: 2)")
    args = ap.parse_args(argv)
    if args.trailing_windows < 0:
        ap.error("--trailing-windows must be >= 0")

    try:
        with psycopg.connect(args.dsn or default_dsn(), autocommit=True) as conn:
            summary = run_etl(conn, Path(args.data_dir), args.mode, args.trailing_windows)
    except (psycopg.OperationalError, FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("%s", exc)
        return 1
    return 0 if summary.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())