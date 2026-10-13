"""Algolia APi client and range fetcher with automatic splitting of large ranges into smaller windows."""
from __future__ import annotations
import time
import requests

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_URL = "https://hn.algolia.com/api/v1"
ENDPOINT = f"{BASE_URL}/search_by_date"
PAGE_SIZE = 1000
RESULT_CAP = 1000
MAX_RETRIES = 5
BACKOFF_BASE = 2
TIMEOUT = 30
USER_AGENT = "hn-etl-download/1.0"


def _get_json(session: requests.Session, params: dict) -> dict:
    """GET the search endpoint and return the parsed JSON body.

    Retries (exponential backoff: 2, 4, 8, 16 s) on connection errors, timeouts,
    HTTP 429, HTTP 5xx and invalid JSON. Any other 4xx is a client error that retrying
    cannot fix, so it is raised immediately.

    Args:
        session (requests.Session): _description_
        params (dict): _description_

    Raises:
        requests.HTTPError: non-retryable 4xx, or a retryable HTTP error that kept failing.
        requests.RequestException / ValueError: the last error after MAX_RETRIES attempts.      

    Returns:
        dict: _description_
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(ENDPOINT, params=dict(params), timeout=TIMEOUT)
        except requests.RequestException as exc:
            error: Exception = exc
        else:
            if resp.status_code == 429 or resp.status_code >= 500:
                error = requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            else:
                resp.raise_for_status()
                
                try:
                    return resp.json()
                except ValueError as exc:
                    error = exc
        
        if attempt == MAX_RETRIES:
            raise error
        wait = BACKOFF_BASE ** attempt
        logger.warning("request failed (%s); retry %d/%d in %ss",
                    error, attempt, MAX_RETRIES - 1, wait)
        time.sleep(wait)
    raise RuntimeError("unreachable")


def fetch_range(session: requests.Session, start: int, end: int, delay: float = 0.3):
    """All stories with start <= created_at_i < end, as (hits, info).

    If the range holds RESULT_CAP or more hits, Algolia would silently truncate,
    so the range is split in half and each half fetched separately.
    """
    params = {
        "tags": "story",
        "numericFilters": f"created_at_i>={start},created_at_i<{end}",
        "hitsPerPage": PAGE_SIZE,
        "page": 0,
    }
    first = _get_json(session, params)
    nb_hits = first["nbHits"]

    if nb_hits >= RESULT_CAP:
        if end - start <= 1:
            raise RuntimeError(f"{nb_hits} hits in a single second; cannot split further")
        mid = (start + end) // 2
        logger.warning("%d hits >= cap %d for [%d, %d): splitting", nb_hits, RESULT_CAP, start, end)
        left, li = fetch_range(session, start, mid, delay)
        right, ri = fetch_range(session, mid, end, delay)
        return left + right, {
            "nbHits": li["nbHits"] + ri["nbHits"],
            "pages": li["pages"] + ri["pages"],
            "splits": 1 + li["splits"] + ri["splits"],
        }

    hits = list(first["hits"])
    pages = 1
    while params["page"] + 1 < first["nbPages"]:
        params["page"] += 1
        time.sleep(delay)
        hits.extend(_get_json(session, params)["hits"])
        pages += 1

    if len(hits) != nb_hits:
        raise RuntimeError(f"retrieved {len(hits)} hits but API reported nbHits={nb_hits}")
    return hits, {"nbHits": nb_hits, "pages": pages, "splits": 0}