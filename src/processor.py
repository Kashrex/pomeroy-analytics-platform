from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

from .models import NormalizedEvent, RejectedRecord
from .validation import normalize_event, reject_from_exception


def process_jsonl_files(paths: Iterable[Path]) -> tuple[list[NormalizedEvent], list[NormalizedEvent], list[RejectedRecord], int]:
    """Validate records independently and retain only the newest version of each event."""
    latest_by_event: dict[str, NormalizedEvent] = {}
    accepted_events: list[NormalizedEvent] = []
    rejected: list[RejectedRecord] = []
    duplicate_or_superseded = 0

    for path in sorted(paths):
        with path.open(encoding="utf-8") as source:
            for row_number, line in enumerate(source, start=1):
                raw_line = line.rstrip("\n")
                if not raw_line.strip():
                    rejected.append(RejectedRecord(path.name, row_number, "blank JSONL line", raw_line))
                    continue
                try:
                    event = normalize_event(json.loads(raw_line), path.name, row_number)
                except (ValueError, json.JSONDecodeError) as exc:
                    rejected.append(reject_from_exception(path.name, row_number, raw_line, exc))
                    continue

                existing = latest_by_event.get(event.event_id)
                accepted_events.append(event)
                if existing is None or event.updated_at_utc > existing.updated_at_utc:
                    if existing is not None:
                        duplicate_or_superseded += 1
                    latest_by_event[event.event_id] = event
                else:
                    duplicate_or_superseded += 1

    return accepted_events, list(latest_by_event.values()), rejected, duplicate_or_superseded
