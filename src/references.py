from __future__ import annotations

import csv
from pathlib import Path

from .models import RejectedRecord


def read_reference_file(path: Path, required_columns: set[str]) -> tuple[list[dict[str, str]], list[RejectedRecord]]:
    records: list[dict[str, str]] = []
    rejects: list[RejectedRecord] = []
    with path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames or not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"{path.name} is missing required columns: {sorted(required_columns)}")
        for row_number, row in enumerate(reader, start=2):
            if any(not (row.get(key) or "").strip() for key in required_columns):
                rejects.append(RejectedRecord(path.name, row_number, "missing required reference value", str(row)))
            elif (row.get("active_flag") or "").upper() not in {"TRUE", "FALSE"}:
                rejects.append(RejectedRecord(path.name, row_number, "active_flag must be TRUE or FALSE", str(row)))
            else:
                records.append({key: (value or "").strip() for key, value in row.items()})
    return records, rejects
