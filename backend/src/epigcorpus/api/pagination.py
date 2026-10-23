"""Opaque cursors: an offset bound to one query and one corpus snapshot.

A cursor is base64url JSON {"o": offset, "f": fingerprint}. The fingerprint
hashes the query's parameters (everything but cursor and limit) together with
the corpus snapshot, so a cursor reused with other filters, or after the corpus
was rebuilt, is refused with 400 instead of silently paging the wrong list.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass

from fastapi import Request, Response

from epigcorpus.api.problems import Problem

DEFAULT_LIMIT = 50
MAX_LIMIT = 500


def fingerprint(request: Request, snapshot: str) -> str:
    params = sorted(
        (key, value)
        for key, value in request.query_params.multi_items()
        if key not in {"cursor", "limit"}
    )
    material = json.dumps([request.url.path, params, snapshot], ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


def encode(offset: int, query_fingerprint: str) -> str:
    raw = json.dumps({"o": offset, "f": query_fingerprint}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode(cursor: str, query_fingerprint: str) -> int:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        offset = int(data["o"])
        issued_for = str(data["f"])
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        raise Problem(
            400, "bad-cursor", "Malformed cursor", "The cursor could not be read."
        ) from None
    if issued_for != query_fingerprint or offset < 0:
        raise Problem(
            400,
            "cursor-mismatch",
            "Cursor does not belong to this query",
            "The cursor was issued for different filters or an earlier corpus snapshot. "
            "Restart from the first page.",
        )
    return offset


@dataclass(frozen=True)
class Page:
    start: int
    stop: int
    total: int
    next_cursor: str | None

    def info(self, limit: int) -> dict:
        return {"limit": limit, "total": self.total, "next_cursor": self.next_cursor}


def paginate(
    request: Request, response: Response, total: int, limit: int, cursor: str | None, snapshot: str
) -> Page:
    """Work out the slice for this page and set the Link header for the next one."""
    query_fingerprint = fingerprint(request, snapshot)
    start = decode(cursor, query_fingerprint) if cursor else 0
    stop = min(start + limit, total)
    next_cursor = encode(stop, query_fingerprint) if stop < total else None
    if next_cursor is not None:
        next_url = request.url.include_query_params(cursor=next_cursor, limit=limit)
        response.headers["Link"] = f'<{next_url.path}?{next_url.query}>; rel="next"'
    return Page(start, stop, total, next_cursor)
