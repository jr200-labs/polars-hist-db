from __future__ import annotations

import base64
import binascii
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Callable, Generic, Iterable, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None


def paginate(
    items: Iterable[T],
    key: Callable[[T], tuple[datetime, str]],
    *,
    cursor: str | None,
    limit: int,
) -> Page[T]:
    if isinstance(limit, bool) or limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")

    ordered = sorted(items, key=key)
    keys = [key(item) for item in ordered]
    start = bisect_right(keys, decode_cursor(cursor)) if cursor else 0
    selected = ordered[start : start + limit + 1]
    page = selected[:limit]
    next_cursor = encode_cursor(key(page[-1])) if len(selected) > len(page) else None
    return Page(tuple(page), next_cursor)


def encode_cursor(value: tuple[datetime, str]) -> str:
    payload = json.dumps([value[0].isoformat(), value[1]], separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode()


def decode_cursor(value: str) -> tuple[datetime, str]:
    try:
        decoded = json.loads(base64.urlsafe_b64decode(value.encode()).decode())
        if (
            not isinstance(decoded, list)
            or len(decoded) != 2
            or not isinstance(decoded[0], str)
            or not isinstance(decoded[1], str)
        ):
            raise ValueError
        timestamp = datetime.fromisoformat(decoded[0])
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError
        return timestamp, decoded[1]
    except (ValueError, TypeError, binascii.Error, json.JSONDecodeError) as exc:
        raise ValueError("invalid cursor") from exc
