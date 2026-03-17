from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any


@dataclass
class CursorToken:
    pit_id: str
    search_after: list[Any]
    size: int


def encode_cursor(token: CursorToken) -> str:
    payload = {
        "pit_id": token.pit_id,
        "search_after": token.search_after,
        "size": token.size,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(value: str) -> CursorToken:
    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("invalid cursor") from exc

    pit_id = data.get("pit_id")
    search_after = data.get("search_after")
    size = data.get("size")
    if not isinstance(pit_id, str) or not pit_id:
        raise ValueError("invalid cursor")
    if not isinstance(search_after, list) or not search_after:
        raise ValueError("invalid cursor")
    if not isinstance(size, int) or size <= 0:
        raise ValueError("invalid cursor")
    return CursorToken(pit_id=pit_id, search_after=search_after, size=size)
