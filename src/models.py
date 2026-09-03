from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    work_order_id: str
    client_id: str
    event_type: str
    event_timestamp_utc: datetime
    updated_at_utc: datetime
    priority: str | None
    technician_id: str | None
    store_id: str | None
    region: str | None
    labor_minutes: int | None
    source_system: str | None
    source_file: str
    source_row_number: int
    payload: dict[str, Any]
    payload_hash: str


@dataclass(frozen=True)
class RejectedRecord:
    source_file: str
    source_row_number: int
    reason: str
    payload: str
