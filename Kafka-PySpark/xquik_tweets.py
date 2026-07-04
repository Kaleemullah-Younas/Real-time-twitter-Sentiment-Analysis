"""Helpers for sending Xquik tweet exports through the Kafka pipeline."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


TEXT_FIELDS = ("text", "content", "tweet", "full_text", "body")
ID_FIELDS = ("id", "tweet_id", "tweetId", "rest_id")
AUTHOR_FIELDS = ("username", "author", "screen_name", "handle")


def _first_string(record: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return ""


def normalize_xquik_record(record: dict[str, Any]) -> list[str]:
    """Return the same row shape as the validation CSV producer."""
    text = _first_string(record, TEXT_FIELDS)
    if not text:
        raise ValueError("Xquik tweet record is missing text")

    tweet_id = _first_string(record, ID_FIELDS) or "xquik"
    author = _first_string(record, AUTHOR_FIELDS) or "Xquik"
    return [tweet_id, author, "Unknown", text]


def iter_xquik_jsonl(path: str | Path, limit: int | None = None) -> Iterator[list[str]]:
    """Yield validated Kafka rows from a Xquik JSON Lines export."""
    emitted = 0
    with Path(path).open(encoding="utf-8") as file_obj:
        for line_number, line in enumerate(file_obj, 1):
            raw = line.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc.msg}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Line {line_number} must be a JSON object")
            yield normalize_xquik_record(record)
            emitted += 1
            if limit is not None and emitted >= limit:
                return
