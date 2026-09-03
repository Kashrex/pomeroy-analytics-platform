from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VALID_EVENT_TYPES = {"OPENED", "ASSIGNED", "WORK_STARTED", "WORK_COMPLETED", "REOPENED", "CLOSED"}
VALID_PRIORITIES = {"P1", "P2", "P3", "P4"}
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Event:
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
class Reject:
    source_file: str
    source_row_number: int
    reason: str
    payload: str


def parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def required_string(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} is required")
    return value.strip()


def normalize_event(record: dict[str, Any], source_file: str, row: int) -> Event:
    if not isinstance(record, dict):
        raise ValueError("JSON document must be an object")

    event_type = required_string(record, "event_type").upper()
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type}")

    priority = record.get("priority")
    if priority is not None:
        if not isinstance(priority, str) or priority.upper() not in VALID_PRIORITIES:
            raise ValueError("priority must be P1, P2, P3 or P4 when present")
        priority = priority.upper()

    technician = record.get("technician") or {}
    location = record.get("location") or {}
    labor = record.get("labor")
    if not isinstance(technician, dict) or not isinstance(location, dict):
        raise ValueError("technician and location must be objects when present")
    if labor is not None and not isinstance(labor, dict):
        raise ValueError("labor must be an object or null")

    technician_id = technician.get("id")
    if technician_id is not None:
        technician_id = required_string(technician, "id")

    store_id = location.get("store_id")
    if store_id is not None:
        store_id = required_string(location, "store_id")

    region = location.get("region")
    if region is not None:
        region = required_string(location, "region")

    labor_minutes = None if labor is None else labor.get("minutes")
    if labor_minutes is not None and (
        isinstance(labor_minutes, bool) or not isinstance(labor_minutes, int) or labor_minutes < 0
    ):
        raise ValueError("labor.minutes must be a non-negative integer when present")

    source_system = record.get("source")
    if source_system is not None:
        source_system = required_string(record, "source")

    payload_hash = hashlib.sha256(
        json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()

    return Event(
        event_id=required_string(record, "event_id"),
        work_order_id=required_string(record, "work_order_id"),
        client_id=required_string(record, "client_id"),
        event_type=event_type,
        event_timestamp_utc=parse_timestamp(record.get("event_timestamp"), "event_timestamp"),
        updated_at_utc=parse_timestamp(record.get("updated_at"), "updated_at"),
        priority=priority,
        technician_id=technician_id,
        store_id=store_id,
        region=region,
        labor_minutes=labor_minutes,
        source_system=source_system,
        source_file=source_file,
        source_row_number=row,
        payload=record,
        payload_hash=payload_hash,
    )


def read_references(
    source_dir: Path,
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], list[Reject]]:
    rejects: list[Reject] = []
    stores: dict[str, dict[str, str]] = {}
    technicians: dict[str, dict[str, str]] = {}

    for filename, key, target in [
        ("stores.csv", "store_id", stores),
        ("technicians.csv", "technician_id", technicians),
    ]:
        path = source_dir / filename
        with path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames or key not in reader.fieldnames:
                raise ValueError(f"{filename} is missing required column {key}")
            for row_no, row in enumerate(reader, start=2):
                if not (row.get(key) or "").strip():
                    rejects.append(Reject(filename, row_no, f"missing {key}", str(row)))
                    continue
                if (row.get("active_flag") or "").upper() not in {"TRUE", "FALSE"}:
                    rejects.append(Reject(filename, row_no, "active_flag must be TRUE or FALSE", str(row)))
                    continue
                target[(row[key] or "").strip()] = {k: (v or "").strip() for k, v in row.items()}

    return stores, technicians, rejects


def process_files(
    paths: list[Path],
    stores: dict[str, dict[str, str]],
    technicians: dict[str, dict[str, str]],
):
    latest: dict[str, Event] = {}
    accepted_versions: list[Event] = []
    rejects: list[Reject] = []
    superseded = 0

    for path in sorted(paths):
        with path.open(encoding="utf-8") as fh:
            for row_no, line in enumerate(fh, start=1):
                raw = line.rstrip("\n\r")
                if not raw.strip():
                    rejects.append(Reject(path.name, row_no, "blank JSONL line", raw))
                    continue

                try:
                    record = json.loads(raw)
                    event = normalize_event(record, path.name, row_no)
                    if event.store_id and event.store_id not in stores:
                        raise ValueError(f"unknown store_id: {event.store_id}")
                    if event.technician_id and event.technician_id not in technicians:
                        raise ValueError(f"unknown technician_id: {event.technician_id}")
                except (json.JSONDecodeError, ValueError, TypeError) as exc:
                    rejects.append(Reject(path.name, row_no, str(exc), raw))
                    continue

                accepted_versions.append(event)
                previous = latest.get(event.event_id)
                if previous is None:
                    latest[event.event_id] = event
                elif (event.updated_at_utc, event.payload_hash) > (
                    previous.updated_at_utc,
                    previous.payload_hash,
                ):
                    latest[event.event_id] = event
                    superseded += 1
                else:
                    superseded += 1

    return accepted_versions, list(latest.values()), rejects, superseded


def source_checksum(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def snowflake_connection():
    from snowflake.connector import connect

    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("Missing Snowflake environment variables: " + ", ".join(missing))

    return connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "WORK"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        autocommit=False,
    )


def load_to_snowflake(
    events: list[Event],
    rejects: list[Reject],
    stores: dict[str, dict[str, str]],
    technicians: dict[str, dict[str, str]],
    checksum: str,
) -> str:
    run_id = str(uuid.uuid4())
    conn = snowflake_connection()
    cur = conn.cursor()

    try:
        # Load reference CSVs first. MERGE keeps these loads idempotent.
        cur.execute("CREATE TEMPORARY TABLE STAGE_STORES (STORE_ID STRING, CLIENT_ID STRING, STORE_NUMBER STRING, REGION STRING, ACTIVE_FLAG STRING)")
        cur.execute("CREATE TEMPORARY TABLE STAGE_TECHNICIANS (TECHNICIAN_ID STRING, TECHNICIAN_NAME STRING, HOME_REGION STRING, ACTIVE_FLAG STRING)")

        if stores:
            cur.executemany(
                "INSERT INTO STAGE_STORES (STORE_ID, CLIENT_ID, STORE_NUMBER, REGION, ACTIVE_FLAG) VALUES (%s, %s, %s, %s, %s)",
                [(r.get("store_id"), r.get("client_id"), r.get("store_number"), r.get("region"), r.get("active_flag")) for r in stores.values()],
            )
        if technicians:
            cur.executemany(
                "INSERT INTO STAGE_TECHNICIANS (TECHNICIAN_ID, TECHNICIAN_NAME, HOME_REGION, ACTIVE_FLAG) VALUES (%s, %s, %s, %s)",
                [(r.get("technician_id"), r.get("technician_name"), r.get("home_region"), r.get("active_flag")) for r in technicians.values()],
            )

        cur.execute("MERGE INTO STORES target USING STAGE_STORES source ON target.STORE_ID = source.STORE_ID WHEN MATCHED THEN UPDATE SET CLIENT_ID=source.CLIENT_ID, STORE_NUMBER=source.STORE_NUMBER, REGION=source.REGION, ACTIVE_FLAG=source.ACTIVE_FLAG WHEN NOT MATCHED THEN INSERT (STORE_ID, CLIENT_ID, STORE_NUMBER, REGION, ACTIVE_FLAG) VALUES (source.STORE_ID, source.CLIENT_ID, source.STORE_NUMBER, source.REGION, source.ACTIVE_FLAG)")
        cur.execute("MERGE INTO TECHNICIANS target USING STAGE_TECHNICIANS source ON target.TECHNICIAN_ID = source.TECHNICIAN_ID WHEN MATCHED THEN UPDATE SET TECHNICIAN_NAME=source.TECHNICIAN_NAME, HOME_REGION=source.HOME_REGION, ACTIVE_FLAG=source.ACTIVE_FLAG WHEN NOT MATCHED THEN INSERT (TECHNICIAN_ID, TECHNICIAN_NAME, HOME_REGION, ACTIVE_FLAG) VALUES (source.TECHNICIAN_ID, source.TECHNICIAN_NAME, source.HOME_REGION, source.ACTIVE_FLAG)")

        cur.execute(
            "SELECT RUN_ID FROM INGESTION_RUNS WHERE SOURCE_CHECKSUM=%s",
            (checksum,),
        )
        existing_run = cur.fetchone()
        if existing_run:
            conn.commit()
            LOGGER.info("source checksum already loaded; reference tables reconciled; skipping event load")
            return existing_run[0]

        cur.execute(
            """
            INSERT INTO INGESTION_RUNS (
                RUN_ID, SOURCE_CHECKSUM, FILE_COUNT, RECORD_COUNT,
                UNIQUE_EVENT_COUNT, REJECTED_COUNT
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                checksum,
                len({event.source_file for event in events} | {reject.source_file for reject in rejects}),
                len(events) + len(rejects),
                len(events),
                len(rejects),
            ),
        )

        cur.execute(
            """
            CREATE TEMPORARY TABLE STAGE_WORK_ORDER_EVENTS (
                EVENT_ID STRING,
                WORK_ORDER_ID STRING,
                CLIENT_ID STRING,
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP_UTC TIMESTAMP_TZ,
                UPDATED_AT_UTC TIMESTAMP_TZ,
                PRIORITY STRING,
                TECHNICIAN_ID STRING,
                STORE_ID STRING,
                REGION STRING,
                LABOR_MINUTES NUMBER(10,0),
                SOURCE_SYSTEM STRING,
                SOURCE_FILE STRING,
                SOURCE_ROW_NUMBER NUMBER,
                PAYLOAD_HASH STRING,
                RAW_PAYLOAD_STRING STRING,
                RAW_PAYLOAD VARIANT,
                RUN_ID STRING
            )
            """
        )

        event_rows = [
            (
                event.event_id,
                event.work_order_id,
                event.client_id,
                event.event_type,
                event.event_timestamp_utc,
                event.updated_at_utc,
                event.priority,
                event.technician_id,
                event.store_id,
                event.region,
                event.labor_minutes,
                event.source_system,
                event.source_file,
                event.source_row_number,
                event.payload_hash,
                json.dumps(event.payload, separators=(",", ":"), ensure_ascii=False),
                run_id,
            )
            for event in events
        ]

        if event_rows:
            cur.executemany(
                """
                INSERT INTO STAGE_WORK_ORDER_EVENTS (
                    EVENT_ID, WORK_ORDER_ID, CLIENT_ID, EVENT_TYPE,
                    EVENT_TIMESTAMP_UTC, UPDATED_AT_UTC, PRIORITY,
                    TECHNICIAN_ID, STORE_ID, REGION, LABOR_MINUTES,
                    SOURCE_SYSTEM, SOURCE_FILE, SOURCE_ROW_NUMBER,
                    PAYLOAD_HASH, RAW_PAYLOAD_STRING, RUN_ID
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                event_rows,
            )

        # Parse JSON in a separate statement. Snowflake MERGE VALUES does not
        # reliably accept PARSE_JSON(...) as an inline VALUES expression.
        cur.execute(
            """
            UPDATE STAGE_WORK_ORDER_EVENTS
            SET RAW_PAYLOAD = PARSE_JSON(RAW_PAYLOAD_STRING)
            """
        )

        cur.execute(
            """
            MERGE INTO WORK_ORDER_EVENTS AS target
            USING STAGE_WORK_ORDER_EVENTS AS source
              ON target.EVENT_ID = source.EVENT_ID

            WHEN MATCHED AND (
                source.UPDATED_AT_UTC > target.UPDATED_AT_UTC
                OR (
                    source.UPDATED_AT_UTC = target.UPDATED_AT_UTC
                    AND source.PAYLOAD_HASH <> target.PAYLOAD_HASH
                )
            )
            THEN UPDATE SET
                WORK_ORDER_ID = source.WORK_ORDER_ID,
                CLIENT_ID = source.CLIENT_ID,
                EVENT_TYPE = source.EVENT_TYPE,
                EVENT_TIMESTAMP_UTC = source.EVENT_TIMESTAMP_UTC,
                UPDATED_AT_UTC = source.UPDATED_AT_UTC,
                PRIORITY = source.PRIORITY,
                TECHNICIAN_ID = source.TECHNICIAN_ID,
                STORE_ID = source.STORE_ID,
                REGION = source.REGION,
                LABOR_MINUTES = source.LABOR_MINUTES,
                SOURCE_SYSTEM = source.SOURCE_SYSTEM,
                SOURCE_FILE = source.SOURCE_FILE,
                SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER,
                PAYLOAD_HASH = source.PAYLOAD_HASH,
                RAW_PAYLOAD = source.RAW_PAYLOAD,
                LAST_SEEN_AT = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                EVENT_ID, WORK_ORDER_ID, CLIENT_ID, EVENT_TYPE,
                EVENT_TIMESTAMP_UTC, UPDATED_AT_UTC, PRIORITY,
                TECHNICIAN_ID, STORE_ID, REGION, LABOR_MINUTES,
                SOURCE_SYSTEM, SOURCE_FILE, SOURCE_ROW_NUMBER,
                PAYLOAD_HASH, RAW_PAYLOAD, FIRST_SEEN_AT, LAST_SEEN_AT
            )
            VALUES (
                source.EVENT_ID, source.WORK_ORDER_ID, source.CLIENT_ID,
                source.EVENT_TYPE, source.EVENT_TIMESTAMP_UTC,
                source.UPDATED_AT_UTC, source.PRIORITY, source.TECHNICIAN_ID,
                source.STORE_ID, source.REGION, source.LABOR_MINUTES,
                source.SOURCE_SYSTEM, source.SOURCE_FILE,
                source.SOURCE_ROW_NUMBER, source.PAYLOAD_HASH,
                source.RAW_PAYLOAD,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """
        )

        reject_rows = [
            (run_id, reject.source_file, reject.source_row_number, reject.reason, reject.payload)
            for reject in rejects
        ]
        if reject_rows:
            cur.executemany(
                """
                INSERT INTO REJECTED_RECORDS
                    (RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, REASON, RAW_PAYLOAD)
                VALUES (%s, %s, %s, %s, %s)
                """,
                reject_rows,
            )

        cur.execute(
            """
            UPDATE INGESTION_RUNS
            SET COMPLETED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = %s
            """,
            (run_id,),
        )

        conn.commit()
        return run_id

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate, normalize, deduplicate and load work-order events."
    )
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process and validate source data without loading Snowflake.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    event_files = sorted(args.source_dir.glob("work_order_events_*.jsonl"))
    if not event_files:
        raise FileNotFoundError("No work_order_events_*.jsonl files found")

    stores, technicians, reference_rejects = read_references(args.source_dir)
    accepted, current, rejects, superseded = process_files(
        event_files, stores, technicians
    )
    rejects.extend(reference_rejects)

    checksum = source_checksum(
        event_files
        + [args.source_dir / "stores.csv", args.source_dir / "technicians.csv"]
    )

    stats = {
        "files_read": len(event_files) + 2,
        "records_read": len(accepted) + len(rejects) - len(reference_rejects),
        "accepted_event_versions": len(accepted),
        "unique_valid_events": len(current),
        "duplicate_or_superseded_records": superseded,
        "rejected_records": len(rejects),
        "unique_work_orders": len({event.work_order_id for event in current}),
        "stores": len(stores),
        "technicians": len(technicians),
        "source_checksum": checksum,
    }

    LOGGER.info("processing statistics: %s", json.dumps(stats, sort_keys=True))

    if args.dry_run:
        LOGGER.info("dry-run completed; Snowflake load skipped")
        return

    run_id = load_to_snowflake(current, rejects, stores, technicians, checksum)
    LOGGER.info("Snowflake load result: %s", run_id)


if __name__ == "__main__":
    main()
