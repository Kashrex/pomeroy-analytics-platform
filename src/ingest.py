"""Ingest work-order JSONL events into Snowflake."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import snowflake.connector


LOGGER = logging.getLogger(__name__)

VALID_EVENT_TYPES = {
    "OPENED",
    "ASSIGNED",
    "WORK_STARTED",
    "WORK_COMPLETED",
    "REOPENED",
    "CLOSED",
}

VALID_PRIORITIES = {"P1", "P2", "P3", "P4"}


@dataclass(frozen=True)
class Event:
    event_id: str
    work_order_id: str
    client_id: str
    event_type: str
    event_timestamp_utc: datetime
    updated_at_utc: datetime
    priority: str
    technician_id: str
    store_id: str
    region: str
    labor_minutes: int
    source_system: str
    source_file: str
    source_row_number: int
    payload_hash: str
    raw_payload: str


@dataclass(frozen=True)
class Reject:
    source_file: str
    source_row_number: int
    event_id: str | None
    work_order_id: str | None
    reason: str
    raw_record: str


def parse_timestamp(value: Any, field_name: str) -> datetime:
    """Parse timestamps and normalize them to UTC."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")

    text = value.strip().replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        try:
            parsed = datetime.strptime(
                text,
                "%Y-%m-%d %H:%M:%S",
            )
        except ValueError:
            raise ValueError(
                f"invalid {field_name}: {value}"
            ) from exc

    # Assessment assumption:
    # timestamps without timezone are treated as UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def required_string(value: Any, field_name: str) -> str:
    """Validate and return a non-empty string."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")

    return value.strip()


def normalize_event(
    payload: dict[str, Any],
    source_file: str,
    source_row_number: int,
    stores: dict[str, dict[str, str]],
    technicians: dict[str, dict[str, str]],
) -> Event:
    """Validate and normalize one raw event."""

    event_id = required_string(
        payload.get("event_id"),
        "event_id",
    )

    work_order_id = required_string(
        payload.get("work_order_id"),
        "work_order_id",
    )

    client_id = required_string(
        payload.get("client_id"),
        "client_id",
    )

    event_type = required_string(
        payload.get("event_type"),
        "event_type",
    ).upper()

    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(
            f"invalid event_type: {event_type}"
        )

    priority = required_string(
        payload.get("priority"),
        "priority",
    ).upper()

    if priority not in VALID_PRIORITIES:
        raise ValueError(
            f"invalid priority: {priority}"
        )

    event_timestamp_utc = parse_timestamp(
        payload.get("event_timestamp"),
        "event_timestamp",
    )

    updated_at_utc = parse_timestamp(
        payload.get("updated_at"),
        "updated_at",
    )

    technician = payload.get("technician")

    if not isinstance(technician, dict):
        raise ValueError("technician is required")

    technician_id = required_string(
        technician.get("id"),
        "technician.id",
    )

    if technician_id not in technicians:
        raise ValueError(
            f"unknown technician_id: {technician_id}"
        )

    location = payload.get("location")

    if not isinstance(location, dict):
        raise ValueError("location is required")

    store_id = required_string(
        location.get("store_id"),
        "location.store_id",
    )

    region = required_string(
        location.get("region"),
        "location.region",
    )

    if store_id not in stores:
        raise ValueError(
            f"unknown store_id: {store_id}"
        )

    labor = payload.get("labor")

    if labor is None:
        labor_minutes = 0

    elif isinstance(labor, dict):
        labor_value = labor.get("minutes")

        if (
            not isinstance(labor_value, int)
            or isinstance(labor_value, bool)
        ):
            raise ValueError(
                "labor.minutes must be an integer or null"
            )

        if labor_value < 0:
            raise ValueError(
                "labor.minutes cannot be negative"
            )

        labor_minutes = labor_value

    else:
        raise ValueError(
            "labor must be an object or null"
        )

    source_system = required_string(
        payload.get("source"),
        "source",
    )

    # Canonical JSON provides a deterministic hash for
    # duplicate/correction resolution.
    canonical_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    payload_hash = hashlib.sha256(
        canonical_payload.encode("utf-8")
    ).hexdigest()

    return Event(
        event_id=event_id,
        work_order_id=work_order_id,
        client_id=client_id,
        event_type=event_type,
        event_timestamp_utc=event_timestamp_utc,
        updated_at_utc=updated_at_utc,
        priority=priority,
        technician_id=technician_id,
        store_id=store_id,
        region=region,
        labor_minutes=labor_minutes,
        source_system=source_system,
        source_file=source_file,
        source_row_number=source_row_number,
        payload_hash=payload_hash,
        raw_payload=canonical_payload,
    )


def load_references(
    source_dir: Path,
) -> tuple[
    dict[str, dict[str, str]],
    dict[str, dict[str, str]],
]:
    """Load store and technician reference data."""

    stores: dict[str, dict[str, str]] = {}
    technicians: dict[str, dict[str, str]] = {}

    with (
        source_dir / "stores.csv"
    ).open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        for row in csv.DictReader(file):
            store_id = required_string(
                row.get("store_id"),
                "store_id",
            )
            stores[store_id] = row

    with (
        source_dir / "technicians.csv"
    ).open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        for row in csv.DictReader(file):
            technician_id = required_string(
                row.get("technician_id"),
                "technician_id",
            )
            technicians[technician_id] = row

    return stores, technicians


def source_checksum(files: list[Path]) -> str:
    """Generate a checksum for the complete source batch."""

    digest = hashlib.sha256()

    for path in files:
        digest.update(
            path.name.encode("utf-8")
        )
        digest.update(b"\0")

        with path.open("rb") as file:
            for chunk in iter(
                lambda: file.read(1024 * 1024),
                b"",
            ):
                digest.update(chunk)

    return digest.hexdigest()


def read_events(
    source_dir: Path,
    stores: dict[str, dict[str, str]],
    technicians: dict[str, dict[str, str]],
) -> tuple[
    list[Event],
    list[Reject],
    dict[str, Any],
]:
    """Read, validate and deduplicate all event files."""

    event_files = sorted(
        source_dir.glob(
            "work_order_events_*.jsonl"
        )
    )

    if not event_files:
        raise FileNotFoundError(
            "No work_order_events_*.jsonl files "
            f"found in {source_dir}"
        )

    checksum = source_checksum(
        event_files
        + [
            source_dir / "stores.csv",
            source_dir / "technicians.csv",
        ]
    )

    latest_by_event_id: dict[str, Event] = {}
    rejects: list[Reject] = []

    records_read = 0
    accepted_versions = 0

    for path in event_files:
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:

            for row_number, raw_line in enumerate(
                file,
                start=1,
            ):
                records_read += 1

                raw_record = raw_line.rstrip(
                    "\n\r"
                )

                payload: Any = None

                try:
                    payload = json.loads(
                        raw_record
                    )

                    if not isinstance(
                        payload,
                        dict,
                    ):
                        raise ValueError(
                            "record must be a JSON object"
                        )

                    event = normalize_event(
                        payload,
                        path.name,
                        row_number,
                        stores,
                        technicians,
                    )

                    accepted_versions += 1

                except (
                    json.JSONDecodeError,
                    ValueError,
                    TypeError,
                ) as exc:

                    event_id = (
                        payload.get("event_id")
                        if isinstance(
                            payload,
                            dict,
                        )
                        else None
                    )

                    work_order_id = (
                        payload.get(
                            "work_order_id"
                        )
                        if isinstance(
                            payload,
                            dict,
                        )
                        else None
                    )

                    rejects.append(
                        Reject(
                            source_file=path.name,
                            source_row_number=row_number,
                            event_id=event_id,
                            work_order_id=work_order_id,
                            reason=str(exc),
                            raw_record=raw_record,
                        )
                    )

                    continue

                existing = latest_by_event_id.get(
                    event.event_id
                )

                if existing is None:
                    latest_by_event_id[
                        event.event_id
                    ] = event
                    continue

                # Newer updated_at wins.
                #
                # If updated_at is identical, payload_hash
                # provides deterministic correction resolution.
                if (
                    event.updated_at_utc
                    > existing.updated_at_utc
                    or (
                        event.updated_at_utc
                        == existing.updated_at_utc
                        and event.payload_hash
                        > existing.payload_hash
                    )
                ):
                    latest_by_event_id[
                        event.event_id
                    ] = event

    duplicate_or_superseded = (
        accepted_versions
        - len(latest_by_event_id)
    )

    stats = {
        "files_read": len(event_files),
        "records_read": records_read,
        "accepted_event_versions": accepted_versions,
        "unique_valid_events": len(
            latest_by_event_id
        ),
        "duplicate_or_superseded_records": (
            duplicate_or_superseded
        ),
        "rejected_records": len(rejects),
        "source_checksum": checksum,
        "stores": len(stores),
        "technicians": len(technicians),
        "unique_work_orders": len(
            {
                event.work_order_id
                for event in latest_by_event_id.values()
            }
        ),
    }

    return (
        list(latest_by_event_id.values()),
        rejects,
        stats,
    )


def snowflake_connection():
    """Create a Snowflake connection from environment variables."""

    connection_args = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
    }

    optional_settings = {
        "SNOWFLAKE_WAREHOUSE": "warehouse",
        "SNOWFLAKE_ROLE": "role",
    }

    for env_name, connection_name in (
        optional_settings.items()
    ):
        value = os.getenv(env_name)

        if value:
            connection_args[
                connection_name
            ] = value

    return snowflake.connector.connect(
        **connection_args
    )


def load_to_snowflake(
    events: list[Event],
    rejects: list[Reject],
    checksum: str,
) -> str:
    """Load validated events into Snowflake transactionally."""

    run_id = checksum[:16]

    connection = snowflake_connection()

    try:
        with connection.cursor() as cur:

            # Batch-level idempotency.
            cur.execute(
                """
                SELECT COUNT(*)
                FROM INGESTION_RUNS
                WHERE SOURCE_CHECKSUM = %s
                """,
                (checksum,),
            )

            if cur.fetchone()[0] > 0:
                LOGGER.info(
                    "Source checksum %s was already "
                    "processed; skipping load.",
                    checksum,
                )

                connection.commit()
                return run_id

            cur.execute(
                """
                CREATE OR REPLACE TEMPORARY TABLE
                STAGE_WORK_ORDER_EVENTS (
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
                    SOURCE_ROW_NUMBER NUMBER(38,0),
                    PAYLOAD_HASH STRING,
                    RAW_PAYLOAD_STRING STRING,
                    RUN_ID STRING
                )
                """
            )

            rows = [
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
                    event.raw_payload,
                    run_id,
                )
                for event in events
            ]

            if rows:
                cur.executemany(
                    """
                    INSERT INTO STAGE_WORK_ORDER_EVENTS (
                        EVENT_ID,
                        WORK_ORDER_ID,
                        CLIENT_ID,
                        EVENT_TYPE,
                        EVENT_TIMESTAMP_UTC,
                        UPDATED_AT_UTC,
                        PRIORITY,
                        TECHNICIAN_ID,
                        STORE_ID,
                        REGION,
                        LABOR_MINUTES,
                        SOURCE_SYSTEM,
                        SOURCE_FILE,
                        SOURCE_ROW_NUMBER,
                        PAYLOAD_HASH,
                        RAW_PAYLOAD_STRING,
                        RUN_ID
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    rows,
                )

            # MERGE provides event-level idempotency and
            # applies newer correction versions.
            cur.execute(
                """
                MERGE INTO WORK_ORDER_EVENTS AS target
                USING STAGE_WORK_ORDER_EVENTS AS source
                  ON target.EVENT_ID = source.EVENT_ID

                WHEN MATCHED AND (
                    source.UPDATED_AT_UTC
                        > target.UPDATED_AT_UTC
                    OR (
                        source.UPDATED_AT_UTC
                            = target.UPDATED_AT_UTC
                        AND source.PAYLOAD_HASH
                            <> target.PAYLOAD_HASH
                    )
                )
                THEN UPDATE SET
                    WORK_ORDER_ID =
                        source.WORK_ORDER_ID,
                    CLIENT_ID =
                        source.CLIENT_ID,
                    EVENT_TYPE =
                        source.EVENT_TYPE,
                    EVENT_TIMESTAMP_UTC =
                        source.EVENT_TIMESTAMP_UTC,
                    UPDATED_AT_UTC =
                        source.UPDATED_AT_UTC,
                    PRIORITY =
                        source.PRIORITY,
                    TECHNICIAN_ID =
                        source.TECHNICIAN_ID,
                    STORE_ID =
                        source.STORE_ID,
                    REGION =
                        source.REGION,
                    LABOR_MINUTES =
                        source.LABOR_MINUTES,
                    SOURCE_SYSTEM =
                        source.SOURCE_SYSTEM,
                    SOURCE_FILE =
                        source.SOURCE_FILE,
                    SOURCE_ROW_NUMBER =
                        source.SOURCE_ROW_NUMBER,
                    PAYLOAD_HASH =
                        source.PAYLOAD_HASH,
                    RAW_PAYLOAD =
                        PARSE_JSON(
                            source.RAW_PAYLOAD_STRING
                        ),
                    LAST_SEEN_AT =
                        CURRENT_TIMESTAMP()

                WHEN NOT MATCHED THEN
                    INSERT (
                        EVENT_ID,
                        WORK_ORDER_ID,
                        CLIENT_ID,
                        EVENT_TYPE,
                        EVENT_TIMESTAMP_UTC,
                        UPDATED_AT_UTC,
                        PRIORITY,
                        TECHNICIAN_ID,
                        STORE_ID,
                        REGION,
                        LABOR_MINUTES,
                        SOURCE_SYSTEM,
                        SOURCE_FILE,
                        SOURCE_ROW_NUMBER,
                        PAYLOAD_HASH,
                        RAW_PAYLOAD,
                        LAST_SEEN_AT
                    )
                    VALUES (
                        source.EVENT_ID,
                        source.WORK_ORDER_ID,
                        source.CLIENT_ID,
                        source.EVENT_TYPE,
                        source.EVENT_TIMESTAMP_UTC,
                        source.UPDATED_AT_UTC,
                        source.PRIORITY,
                        source.TECHNICIAN_ID,
                        source.STORE_ID,
                        source.REGION,
                        source.LABOR_MINUTES,
                        source.SOURCE_SYSTEM,
                        source.SOURCE_FILE,
                        source.SOURCE_ROW_NUMBER,
                        source.PAYLOAD_HASH,
                        PARSE_JSON(
                            source.RAW_PAYLOAD_STRING
                        ),
                        CURRENT_TIMESTAMP()
                    )
                """
            )

            reject_rows = [
                (
                    reject.source_file,
                    reject.source_row_number,
                    reject.event_id,
                    reject.work_order_id,
                    reject.reason,
                    reject.raw_record,
                    run_id,
                )
                for reject in rejects
            ]

            if reject_rows:
                cur.executemany(
                    """
                    INSERT INTO REJECTED_RECORDS (
                        SOURCE_FILE,
                        SOURCE_ROW_NUMBER,
                        EVENT_ID,
                        WORK_ORDER_ID,
                        REASON,
                        RAW_RECORD,
                        RUN_ID
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    reject_rows,
                )

            cur.execute(
                """
                INSERT INTO INGESTION_RUNS (
                    RUN_ID,
                    SOURCE_CHECKSUM,
                    FILE_COUNT,
                    RECORD_COUNT,
                    UNIQUE_EVENT_COUNT,
                    REJECTED_COUNT,
                    COMPLETED_AT
                )
                SELECT
                    %s,
                    %s,
                    COUNT(DISTINCT SOURCE_FILE),
                    COUNT(*),
                    COUNT(DISTINCT EVENT_ID),
                    %s,
                    CURRENT_TIMESTAMP()
                FROM STAGE_WORK_ORDER_EVENTS
                """,
                (
                    run_id,
                    checksum,
                    len(rejects),
                ),
            )

        connection.commit()

        LOGGER.info(
            "Snowflake load completed. run_id=%s",
            run_id,
        )

        return run_id

    except Exception:
        connection.rollback()

        LOGGER.exception(
            "Snowflake load failed; "
            "transaction rolled back."
        )

        raise

    finally:
        connection.close()


def configure_logging() -> None:
    """Configure application logging."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate, normalize, deduplicate "
            "and load work-order events."
        )
    )

    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/source"),
        help=(
            "Directory containing source CSV "
            "and JSONL files."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Process and validate source data "
            "without loading Snowflake."
        ),
    )

    args = parser.parse_args()

    configure_logging()

    stores, technicians = load_references(
        args.source_dir
    )

    events, rejects, stats = read_events(
        args.source_dir,
        stores,
        technicians,
    )

    LOGGER.info(
        "processing statistics: %s",
        json.dumps(
            stats,
            sort_keys=True,
        ),
    )

    if args.dry_run:
        LOGGER.info(
            "dry-run completed; "
            "Snowflake load skipped."
        )
        return

    run_id = load_to_snowflake(
        events,
        rejects,
        stats["source_checksum"],
    )

    LOGGER.info(
        "ingestion completed successfully: "
        "run_id=%s",
        run_id,
    )


if __name__ == "__main__":
    main()