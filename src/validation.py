from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .models import NormalizedEvent, RejectedRecord

VALID_PRIORITIES = {"P1", "P2", "P3", "P4"}


def parse_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    candidate = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a valid ISO timestamp") from exc
    # Assumption: timestamps with no offset are UTC. Keep this explicit in the README.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def required_string(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} is required")
    return value.strip()


def normalize_event(
    record: dict[str, Any], source_file: str, source_row_number: int
) -> NormalizedEvent:
    if not isinstance(record, dict):
        raise ValueError("JSON document must be an object")

    priority = record.get("priority")
    if priority is not None and priority not in VALID_PRIORITIES:
        raise ValueError("priority must be one of P1, P2, P3, P4 when present")

    technician = record.get("technician") or {}
    location = record.get("location") or {}
    labor = record.get("labor")
    if not isinstance(technician, dict) or not isinstance(location, dict):
        raise ValueError("technician and location must be objects when present")
    if labor is not None and not isinstance(labor, dict):
        raise ValueError("labor must be an object or null")

    labor_minutes = None if labor is None else labor.get("minutes")
    if labor_minutes is not None:
        if isinstance(labor_minutes, bool) or not isinstance(labor_minutes, int) or labor_minutes < 0:
            raise ValueError("labor.minutes must be a non-negative integer when present")

    canonical_payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return NormalizedEvent(
        event_id=required_string(record, "event_id"),
        work_order_id=required_string(record, "work_order_id"),
        client_id=required_string(record, "client_id"),
        event_type=required_string(record, "event_type").upper(),
        event_timestamp_utc=parse_timestamp(record.get("event_timestamp"), "event_timestamp"),
        updated_at_utc=parse_timestamp(record.get("updated_at"), "updated_at"),
        priority=priority,
        technician_id=technician.get("id"),
        store_id=location.get("store_id"),
        region=location.get("region"),
        labor_minutes=labor_minutes,
        source_system=record.get("source"),
        source_file=source_file,
        source_row_number=source_row_number,
        payload=record,
        payload_hash=hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
    )


def reject_from_exception(source_file: str, row: int, payload: str, exc: Exception) -> RejectedRecord:
    return RejectedRecord(source_file, row, str(exc), payload)
