from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VALID_EVENT_TYPES = {"OPENED", "ASSIGNED", "WORK_STARTED", "WORK_COMPLETED", "REOPENED", "CLOSED"}
VALID_PRIORITIES = {"P1", "P2", "P3", "P4"}


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
        if not isinstance(priority, str) or priority not in VALID_PRIORITIES:
            raise ValueError("priority must be P1, P2, P3 or P4 when present")

    technician = record.get("technician") or {}
    location = record.get("location") or {}
    labor = record.get("labor")
    if not isinstance(technician, dict) or not isinstance(location, dict):
        raise ValueError("technician and location must be objects when present")
    if labor is not None and not isinstance(labor, dict):
        raise ValueError("labor must be an object or null")

    labor_minutes = None if labor is None else labor.get("minutes")
    if labor_minutes is not None and (
        isinstance(labor_minutes, bool) or not isinstance(labor_minutes, int) or labor_minutes < 0
    ):
        raise ValueError("labor.minutes must be a non-negative integer when present")

    payload_hash = hashlib.sha256(
        json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return Event(
        event_id=required_string(record, "event_id"),
        work_order_id=required_string(record, "work_order_id"),
        client_id=required_string(record, "client_id"),
        event_type=event_type,
        event_timestamp_utc=parse_timestamp(record.get("event_timestamp"), "event_timestamp"),
        updated_at_utc=parse_timestamp(record.get("updated_at"), "updated_at"),
        priority=priority,
        technician_id=technician.get("id"),
        store_id=location.get("store_id"),
        region=location.get("region"),
        labor_minutes=labor_minutes,
        source_system=record.get("source"),
        source_file=source_file,
        source_row_number=row,
        payload=record,
        payload_hash=payload_hash,
    )


def read_references(source_dir: Path) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], list[Reject]]:
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


def process_files(paths: list[Path], stores: dict[str, dict[str, str]], technicians: dict[str, dict[str, str]]):
    latest: dict[str, Event] = {}
    accepted_versions: list[Event] = []
    rejects: list[Reject] = []
    superseded = 0

    for path in sorted(paths):
        with path.open(encoding="utf-8") as fh:
            for row_no, line in enumerate(fh, start=1):
                raw = line.rstrip("\n")
                if not raw.strip():
                    rejects.append(Reject(path.name, row_no, "blank JSONL line", raw))
                    continue
                try:
                    event = normalize_event(json.loads(raw), path.name, row_no)
                    if event.store_id and event.store_id not in stores:
                        raise ValueError(f"unknown store_id: {event.store_id}")
                    if event.technician_id and event.technician_id not in technicians:
                        raise ValueError(f"unknown technician_id: {event.technician_id}")
                except (json.JSONDecodeError, ValueError) as exc:
                    rejects.append(Reject(path.name, row_no, str(exc), raw))
                    continue

                accepted_versions.append(event)
                previous = latest.get(event.event_id)
                if previous is None:
                    latest[event.event_id] = event
                elif (event.updated_at_utc, event.payload_hash) > (previous.updated_at_utc, previous.payload_hash):
                    latest[event.event_id] = event
                    superseded += 1
                else:
                    superseded += 1

    return accepted_versions, list(latest.values()), rejects, superseded


def source_checksum(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def write_outputs(output_dir: Path, events: list[Event], rejects: list[Reject], stats: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    fields = [
        "event_id", "work_order_id", "client_id", "event_type", "event_timestamp_utc",
        "updated_at_utc", "priority", "technician_id", "store_id", "region", "labor_minutes",
        "source_system", "source_file", "source_row_number", "payload_hash",
    ]
    with (output_dir / "normalized_events.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for event in sorted(events, key=lambda x: x.event_id):
            row = asdict(event)
            row.pop("payload")
            row["event_timestamp_utc"] = event.event_timestamp_utc.isoformat()
            row["updated_at_utc"] = event.updated_at_utc.isoformat()
            writer.writerow(row)

    with (output_dir / "rejected_records.jsonl").open("w", encoding="utf-8") as fh:
        for reject in rejects:
            fh.write(json.dumps(asdict(reject), ensure_ascii=False) + "\n")

    (output_dir / "processing_stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")


def snowflake_connection():
    from snowflake.connector import connect

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE"]
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


def load_to_snowflake(events: list[Event], rejects: list[Reject], checksum: str) -> str:
    run_id = str(uuid.uuid4())
    with snowflake_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM INGESTION_RUNS WHERE SOURCE_CHECKSUM=%s AND STATUS='SUCCEEDED'", (checksum,))
            if cur.fetchone()[0]:
                conn.rollback()
                return "SKIPPED_ALREADY_LOADED"

            cur.execute(
                "INSERT INTO INGESTION_RUNS (RUN_ID,SOURCE_CHECKSUM,STARTED_AT,STATUS) VALUES (%s,%s,CURRENT_TIMESTAMP(),'RUNNING')",
                (run_id, checksum),
            )
            event_rows = [
                (
                    e.event_id, e.work_order_id, e.client_id, e.event_type, e.event_timestamp_utc,
                    e.updated_at_utc, e.priority, e.technician_id, e.store_id, e.region,
                    e.labor_minutes, e.source_system, e.source_file, e.source_row_number,
                    e.payload_hash, json.dumps(e.payload, separators=(",", ":")), run_id,
                ) for e in events
            ]
            cur.execute("""CREATE TEMPORARY TABLE STAGE_WORK_ORDER_EVENTS (
                EVENT_ID STRING, WORK_ORDER_ID STRING, CLIENT_ID STRING, EVENT_TYPE STRING,
                EVENT_TIMESTAMP_UTC TIMESTAMP_TZ, UPDATED_AT_UTC TIMESTAMP_TZ, PRIORITY STRING,
                TECHNICIAN_ID STRING, STORE_ID STRING, REGION STRING, LABOR_MINUTES NUMBER(10,0),
                SOURCE_SYSTEM STRING, SOURCE_FILE STRING, SOURCE_ROW_NUMBER NUMBER, PAYLOAD_HASH STRING,
                RAW_PAYLOAD VARIANT, RUN_ID STRING
            )""")
            if event_rows:
                cur.executemany(
                    """INSERT INTO STAGE_WORK_ORDER_EVENTS VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,PARSE_JSON(%s),%s)""",
                    event_rows,
                )
            cur.execute("""MERGE INTO WORK_ORDER_EVENTS t USING STAGE_WORK_ORDER_EVENTS s
                ON t.EVENT_ID=s.EVENT_ID
                WHEN MATCHED AND (s.UPDATED_AT_UTC>t.UPDATED_AT_UTC OR
                    (s.UPDATED_AT_UTC=t.UPDATED_AT_UTC AND s.PAYLOAD_HASH<>t.PAYLOAD_HASH)) THEN
                  UPDATE SET WORK_ORDER_ID=s.WORK_ORDER_ID,CLIENT_ID=s.CLIENT_ID,EVENT_TYPE=s.EVENT_TYPE,
                    EVENT_TIMESTAMP_UTC=s.EVENT_TIMESTAMP_UTC,UPDATED_AT_UTC=s.UPDATED_AT_UTC,PRIORITY=s.PRIORITY,
                    TECHNICIAN_ID=s.TECHNICIAN_ID,STORE_ID=s.STORE_ID,REGION=s.REGION,LABOR_MINUTES=s.LABOR_MINUTES,
                    SOURCE_SYSTEM=s.SOURCE_SYSTEM,SOURCE_FILE=s.SOURCE_FILE,SOURCE_ROW_NUMBER=s.SOURCE_ROW_NUMBER,
                    PAYLOAD_HASH=s.PAYLOAD_HASH,RAW_PAYLOAD=s.RAW_PAYLOAD,LAST_SEEN_AT=CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                  (EVENT_ID,WORK_ORDER_ID,CLIENT_ID,EVENT_TYPE,EVENT_TIMESTAMP_UTC,UPDATED_AT_UTC,PRIORITY,
                   TECHNICIAN_ID,STORE_ID,REGION,LABOR_MINUTES,SOURCE_SYSTEM,SOURCE_FILE,SOURCE_ROW_NUMBER,
                   PAYLOAD_HASH,RAW_PAYLOAD,FIRST_SEEN_AT,LAST_SEEN_AT)
                  VALUES (s.EVENT_ID,s.WORK_ORDER_ID,s.CLIENT_ID,s.EVENT_TYPE,s.EVENT_TIMESTAMP_UTC,s.UPDATED_AT_UTC,
                   s.PRIORITY,s.TECHNICIAN_ID,s.STORE_ID,s.REGION,s.LABOR_MINUTES,s.SOURCE_SYSTEM,s.SOURCE_FILE,
                   s.SOURCE_ROW_NUMBER,s.PAYLOAD_HASH,s.RAW_PAYLOAD,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())""")
            reject_rows = [(run_id, r.source_file, r.source_row_number, r.reason, r.payload) for r in rejects]
            if reject_rows:
                cur.executemany(
                    "INSERT INTO REJECTED_RECORDS (RUN_ID,SOURCE_FILE,SOURCE_ROW_NUMBER,REASON,RAW_PAYLOAD) VALUES (%s,%s,%s,%s,%s)",
                    reject_rows,
                )
            cur.execute(
                "UPDATE INGESTION_RUNS SET COMPLETED_AT=CURRENT_TIMESTAMP(),VALID_EVENT_COUNT=%s,REJECTED_EVENT_COUNT=%s,STATUS='SUCCEEDED' WHERE RUN_ID=%s",
                (len(events), len(rejects), run_id),
            )
            conn.commit()
            return run_id
        except Exception as exc:
            conn.rollback()
            try:
                cur.execute(
                    "UPDATE INGESTION_RUNS SET COMPLETED_AT=CURRENT_TIMESTAMP(),STATUS='FAILED',ERROR_MESSAGE=%s WHERE RUN_ID=%s",
                    (str(exc)[:5000], run_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
            raise
        finally:
            cur.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate, normalize and load work-order events.")
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--dry-run", action="store_true", help="Process and write outputs without loading Snowflake")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    event_files = sorted(args.source_dir.glob("work_order_events_*.jsonl"))
    if not event_files:
        raise FileNotFoundError("No work_order_events_*.jsonl files found")

    stores, technicians, reference_rejects = read_references(args.source_dir)
    accepted, current, rejects, superseded = process_files(event_files, stores, technicians)
    rejects.extend(reference_rejects)
    checksum = source_checksum(event_files + [args.source_dir / "stores.csv", args.source_dir / "technicians.csv"])

    stats = {
        "files_read": len(event_files) + 2,
        "records_read": len(accepted) + len(rejects) - len(reference_rejects),
        "accepted_event_versions": len(accepted),
        "unique_valid_events": len(current),
        "duplicate_or_superseded_records": superseded,
        "rejected_records": len(rejects),
        "unique_work_orders": len({e.work_order_id for e in current}),
        "stores": len(stores),
        "technicians": len(technicians),
        "source_checksum": checksum,
    }
    write_outputs(args.output_dir, current, rejects, stats)
    logging.info("processing statistics: %s", json.dumps(stats, sort_keys=True))

    if not args.dry_run:
        run_id = load_to_snowflake(current, rejects, checksum)
        logging.info("Snowflake load result: %s", run_id)


if __name__ == "__main__":
    main()
