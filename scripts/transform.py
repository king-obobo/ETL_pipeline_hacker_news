from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

# Reject reasons: the same strings as 03_clean.sql, in the same order of precedence.
MISSING_ID = "missing objectID"
BAD_ID = "non-integer objectID"
BAD_CREATED_AT = "unparseable created_at"
NEG_POINTS = "negative points"
BAD_POINTS = "non-integer points"
NEG_COMMENTS = "negative num_comments"
BAD_COMMENTS = "non-integer num_comments"
BAD_TAGS = "unknown story_type: _tags missing or not an array"


# text helpers
def field_text(value: Any) -> str | None:
    """A JSON value as text, like jsonb ->> 'key' does (NULL for missing / null)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return format(Decimal(repr(value)), "f")
    return json.dumps(value, ensure_ascii=False, separators=(", ", ": "))


def _btrim(s: str | None) -> str | None:
    """Postgres btrim(): trims spaces only (not tabs or newlines)."""
    return None if s is None else s.strip(" ")


def _nullif_blank(s: str | None) -> str | None:
    return s if s else None


_ENTITY_RE = re.compile(r"&(#[xX][0-9a-fA-F]{1,6}|#[0-9]{1,7}|[A-Za-z]+);")
_NAMED_ENTITIES = {"amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'", "nbsp": "\xa0"}


def _decode_entity(m: re.Match) -> str:
    body = m.group(1)
    if body[0] == "#":
        cp = int(body[2:], 16) if body[1] in "xX" else int(body[1:])
        if 1 <= cp <= 1114111 and not 55296 <= cp <= 57343:
            return chr(cp)
        return m.group(0)
    return _NAMED_ENTITIES.get(body, m.group(0)) # type: ignore


def html_unescape(text: str | None) -> str | None:
    """Single left-to-right pass; anything it does not recognise is left as is."""
    if text is None or "&" not in text:
        return text
    return _ENTITY_RE.sub(_decode_entity, text)


_HOST_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://([^/?#]*)")


def _simple_lower(text: str) -> str:
    """Per-character lowercase, like Postgres lower() (libc): no final-sigma rule, and U+0130
    becomes plain 'i' (Python's str.lower() would give two characters)."""
    return "".join("i" if ch == "\u0130" else ch.lower() for ch in text)


def extract_domain(url: str | None) -> str | None:
    """Lowercase host without userinfo, port and leading 'www.'; None if there is no host."""
    if url is None:
        return None
    m = _HOST_RE.match(url.strip(" "))
    if m is None:
        return None
    host = re.sub(r"^.*@", "", m.group(1), count=1, flags=re.DOTALL)
    host = re.sub(r":[0-9]*\Z", "", host, count=1)
    host = re.sub(r"^www\.", "", _simple_lower(host), count=1)
    return host or None


_TS_RE = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?"
    r"(Z|[+-][0-9]{2}:?[0-9]{2})"
)


def parse_timestamp(text: str | None) -> datetime | None:
    """ISO-8601 timestamp with offset -> aware UTC datetime, or None if invalid."""
    if text is None:
        return None
    m = _TS_RE.fullmatch(text)
    if m is None:
        return None
    year, month, day, hour, minute, second = (int(g) for g in m.group(1, 2, 3, 4, 5, 6))
    frac, tz = m.group(7) or "", m.group(8)
    if minute > 59 or second > 60 or hour > 24:
        return None
    # Postgres parses the fraction as a C double and rint()s it to microseconds (half to
    # even); Python's float() and round() do the same arithmetic. Range checks come after.
    micro = round(float("0." + frac) * 1_000_000) if frac else 0
    if (hour == 24 and (minute or second or micro)) or (second == 60 and micro):
        return None
    if tz == "Z":
        offset = timedelta(0)
    else:
        sign = -1 if tz[0] == "-" else 1
        digits = tz[1:].replace(":", "")
        off_h, off_m = int(digits[:2]), int(digits[2:])
        if off_h > 15 or off_m > 59:
            return None
        offset = sign * timedelta(hours=off_h, minutes=off_m)
    try:
        local = datetime(year, month, day, tzinfo=timezone.utc) + timedelta(
            hours=hour, minutes=minute, seconds=second, microseconds=micro)
        return local - offset
    except (ValueError, OverflowError):
        return None


def story_type(tags: Iterable[Any]) -> str:
    """show_hn wins over ask_hn; every other tag (front_page, author_x, ...) is ignored."""
    tags = list(tags)
    if "show_hn" in tags:
        return "show_hn"
    if "ask_hn" in tags:
        return "ask_hn"
    return "regular"


def row_hash(title, url, author, points, num_comments, story_type_) -> str:
    """md5 of the JSON array text Postgres builds with jsonb_build_array(...)::text."""
    text = json.dumps([title, url, author, points, num_comments, story_type_],
                    ensure_ascii=False, separators=(", ", ": "))
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# records
_TEXT_FIELDS = ("objectID", "title", "url", "author", "points", "num_comments",
                "created_at", "updated_at")


def _texts(hit: Any) -> dict[str, str | None]:
    if not isinstance(hit, dict):
        hit = {}
    return {k: field_text(hit.get(k)) for k in _TEXT_FIELDS}


def validate(hit: Any) -> str | None:
    """Return the reject reason for a raw API hit, or None if it is valid."""
    t = _texts(hit)
    oid = t["objectID"]
    if oid is None or _btrim(oid) == "":
        return MISSING_ID
    if not re.fullmatch(r"[0-9]{1,18}", oid):
        return BAD_ID
    if parse_timestamp(t["created_at"]) is None:
        return BAD_CREATED_AT
    for key, neg, bad in (("points", NEG_POINTS, BAD_POINTS),
                        ("num_comments", NEG_COMMENTS, BAD_COMMENTS)):
        v = t[key]
        if v is not None and re.fullmatch(r"-[0-9]+", v):
            return neg
        if v is not None and not re.fullmatch(r"[0-9]{1,9}", v):
            return bad
    if not (isinstance(hit, dict) and isinstance(hit.get("_tags"), list)):
        return BAD_TAGS
    return None


@dataclass(frozen=True)
class CleanStory:
    """One validated, cleaned story: the columns of clean.hn_stories (minus run ids)."""
    object_id: int
    title: str | None
    url: str | None
    author: str | None
    points: int | None
    num_comments: int | None
    created_at: datetime
    updated_at: datetime | None
    story_type: str
    row_hash: str

    @property
    def domain(self) -> str | None:
        return extract_domain(self.url)

    @property
    def hour_of_day(self) -> int:
        return self.created_at.astimezone(timezone.utc).hour

    @property
    def has_discussion(self) -> bool:
        return (self.num_comments or 0) > 0


def clean(hit: dict) -> CleanStory:
    """Clean a valid hit. Raises ValueError (with the reject reason) if it is not valid."""
    reason = validate(hit)
    if reason:
        raise ValueError(reason)
    t = _texts(hit)
    title = _nullif_blank(_btrim(html_unescape(t["title"])))
    url = _nullif_blank(_btrim(t["url"]))
    author = _nullif_blank(_btrim(t["author"]))
    points = int(t["points"]) if t["points"] is not None else None
    comments = int(t["num_comments"]) if t["num_comments"] is not None else None
    stype = story_type(hit["_tags"])
    return CleanStory(
        object_id=int(t["objectID"]), # type: ignore
        title=title, url=url, author=author, points=points, num_comments=comments,
        created_at=parse_timestamp(t["created_at"]), # type: ignore
        updated_at=parse_timestamp(t["updated_at"]), # type: ignore       # bad value -> None, not a reject 
        story_type=stype,
        row_hash=row_hash(title, url, author, points, comments, stype),
    )


def process(hit: Any) -> tuple[CleanStory | None, str | None]:
    """(story, None) for a valid hit, (None, reason) for a rejected one."""
    reason = validate(hit)
    return (None, reason) if reason else (clean(hit), None)


def _recency(s: CleanStory) -> tuple:
    return (0,) if s.updated_at is None else (1, s.updated_at)


def dedupe(stories: Iterable[CleanStory]) -> list[CleanStory]:
    """One story per object_id: latest updated_at wins (None is oldest), then the later row."""
    best: dict[int, CleanStory] = {}
    for s in stories:
        cur = best.get(s.object_id)
        if cur is None or _recency(s) >= _recency(cur):
            best[s.object_id] = s
    return list(best.values())


def stg_row(hit: dict) -> dict[str, Any]:
    """Values for stg.hn_stories_raw: raw fields as text, tags as JSON text, payload minus
    _highlightResult (the SQL loader does the same)."""
    return {
        "object_id": field_text(hit.get("objectID")),
        "title": field_text(hit.get("title")),
        "url": field_text(hit.get("url")),
        "author": field_text(hit.get("author")),
        "points": field_text(hit.get("points")),
        "num_comments": field_text(hit.get("num_comments")),
        "created_at": field_text(hit.get("created_at")),
        "created_at_i": field_text(hit.get("created_at_i")),
        "updated_at": field_text(hit.get("updated_at")),
        "tags": field_text(hit.get("_tags")),
        "payload": {k: v for k, v in hit.items() if k != "_highlightResult"},
    }
