"""
Alternative Snowpark implementation of the Pomeroy ingestion flow.

This file is an alternative to src/ingest.py. It is intentionally kept
separate so the assessment can demonstrate two viable approaches:

    1. src/ingest.py           -> external Python + Snowflake Connector
    2. src/ingest_snowpark.py  -> Snowpark Python running transformations
                                   inside Snowflake

Assumptions for this alternative:
- Snowpark Session is created from environment variables.
- The supplied files are available locally under --source-dir.
- The script uploads them to an internal Snowflake stage before processing.
- The target reference tables already exist:
      STORES(STORE_ID, CLIENT_ID, STORE_NUMBER, REGION, ACTIVE_FLAG)
      TECHNICIANS(TECHNICIAN_ID, TECHNICIAN_NAME, HOME_REGION, ACTIVE_FLAG)
- The event/audit tables from the Flyway migrations already exist.

This approach uses Snowflake/Snowpark for the transformation work. The
external Python process is primarily responsible for establishing the
session and putting source files into the Snowflake stage.

Note:
Snowflake's COPY INTO ... ON_ERROR=CONTINUE behavior can continue past
malformed source records, but exact per-record malformed JSON payload capture
is less straightforward than the external-Python implementation. For that
reason, src/ingest.py remains the preferred assessment implementation when
the requirement is to retain every rejected raw record and rejection reason.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import uuid
from pathlib import Path

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    count,
    current_timestamp,
    lit,
    row_number,
    sha2,
    to_timestamp_tz,
    upper,
    when,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark.window import Window


VALID_EVENT_TYPES = (
    "OPENED",
    "ASSIGNED",
    "WORK_STARTED",
    "WORK_COMPLETED",
    "REOPENED",
    "CLOSED",
)

VALID_PRIORITIES = ("P1", "P2", "P3", "P4")

# Explicit Snowpark schemas for the reference CSV files. DataFrameReader.schema()
# requires a StructType (built from StructField objects), not a plain list of
# column-name strings -- passing a list causes:
#   AttributeError: 'list' object has no attribute 'fields'
STORES_SCHEMA = StructType(
    [
        StructField("STORE_ID", StringType()),
        StructField("CLIENT_ID", StringType()),
        StructField("STORE_NUMBER", StringType()),
        StructField("REGION", StringType()),
        StructField("ACTIVE_FLAG", StringType()),
    ]
)

TECHNICIANS_SCHEMA = StructType(
    [
        StructField("TECHNICIAN_ID", StringType()),
        StructField("TECHNICIAN_NAME", StringType()),
        StructField("HOME_REGION", StringType()),
        StructField("ACTIVE_FLAG", StringType()),
    ]
)


def create_session() -> Session:
    """Create a Snowpark session from environment variables."""
    required = {
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
    }

    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing Snowflake environment variables: " + ", ".join(missing)
        )

    connection_parameters = {
        "account": required["SNOWFLAKE_ACCOUNT"],
        "user": required["SNOWFLAKE_USER"],
        "password": required["SNOWFLAKE_PASSWORD"],
        "database": required["SNOWFLAKE_DATABASE"],
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "WORK"),
    }

    if os.getenv("SNOWFLAKE_WAREHOUSE"):
        connection_parameters["warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]

    if os.getenv("SNOWFLAKE_ROLE"):
        connection_parameters["role"] = os.environ["SNOWFLAKE_ROLE"]

    return Session.builder.configs(connection_parameters).create()


def source_checksum(source_files: list[Path]) -> str:
    """Create a deterministic checksum for the complete source batch."""
    digest = hashlib.sha256()

    for path in sorted(source_files):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")

    return digest.hexdigest()


def put_source_files(session: Session, source_dir: Path, stage: str) -> None:
    """Upload all assessment source files to a Snowflake internal stage."""
    session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()

    for path in sorted(source_dir.glob("*.jsonl")):
        session.file.put(
            str(path),
            f"@{stage}/events",
            auto_compress=True,
            overwrite=True,
        )

    for filename in ("stores.csv", "technicians.csv"):
        path = source_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")

        session.file.put(
            str(path),
            f"@{stage}/reference",
            auto_compress=True,
            overwrite=True,
        )


def load_reference_tables(session: Session, stage: str) -> None:
    """
    Load STORES and TECHNICIANS using Snowpark DataFrames.

    CSV files are staged first, then read as Snowpark DataFrames and merged
    into the target reference tables.
    """
    csv_format = "POMEROY_CSV_FORMAT"

    session.sql(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {csv_format}
        TYPE = CSV
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        """
    ).collect()

    stores = (
        session.read.option("FIELD_DELIMITER", ",")
        .option("SKIP_HEADER", 1)
        .schema(STORES_SCHEMA)
        .csv(f"@{stage}/reference/stores.csv.gz")
    )

    technicians = (
        session.read.option("FIELD_DELIMITER", ",")
        .option("SKIP_HEADER", 1)
        .schema(TECHNICIANS_SCHEMA)
        .csv(f"@{stage}/reference/technicians.csv.gz")
    )

    stores.create_or_replace_temp_view("STG_STORES")
    technicians.create_or_replace_temp_view("STG_TECHNICIANS")

    session.sql(
        """
        MERGE INTO STORES target
        USING STG_STORES source
          ON target.STORE_ID = source.STORE_ID
        WHEN MATCHED THEN UPDATE SET
            CLIENT_ID = source.CLIENT_ID,
            STORE_NUMBER = source.STORE_NUMBER,
            REGION = source.REGION,
            ACTIVE_FLAG = source.ACTIVE_FLAG
        WHEN NOT MATCHED THEN INSERT
            (STORE_ID, CLIENT_ID, STORE_NUMBER, REGION, ACTIVE_FLAG)
        VALUES
            (source.STORE_ID, source.CLIENT_ID, source.STORE_NUMBER,
             source.REGION, source.ACTIVE_FLAG)
        """
    ).collect()

    session.sql(
        """
        MERGE INTO TECHNICIANS target
        USING STG_TECHNICIANS source
          ON target.TECHNICIAN_ID = source.TECHNICIAN_ID
        WHEN MATCHED THEN UPDATE SET
            TECHNICIAN_NAME = source.TECHNICIAN_NAME,
            HOME_REGION = source.HOME_REGION,
            ACTIVE_FLAG = source.ACTIVE_FLAG
        WHEN NOT MATCHED THEN INSERT
            (TECHNICIAN_ID, TECHNICIAN_NAME, HOME_REGION, ACTIVE_FLAG)
        VALUES
            (source.TECHNICIAN_ID, source.TECHNICIAN_NAME,
             source.HOME_REGION, source.ACTIVE_FLAG)
        """
    ).collect()


def read_events(session: Session, stage: str):
    """
    Load staged JSONL files into a staging table as VARIANT rows.

    NOTE: session.read.json(...) compiles to a plain SELECT against the
    staged files, and Snowflake's JSON parser has no per-record error
    tolerance in a SELECT -- a single malformed JSON line anywhere in a
    file (e.g. a missing colon) raises SnowparkSQLException and aborts the
    entire read.

    COPY INTO, by contrast, is a load operation and does support
    ON_ERROR = 'CONTINUE', which skips individual malformed records
    instead of failing the whole batch. We therefore stage the raw
    payloads into a temporary table via COPY INTO rather than querying
    the files directly.
    """
    json_format = "POMEROY_JSON_FORMAT"
    staging_table = "STG_RAW_EVENTS"

    session.sql(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {json_format}
        TYPE = JSON
        STRIP_OUTER_ARRAY = FALSE
        """
    ).collect()

    session.sql(
        f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS {staging_table} (
            PAYLOAD VARIANT
        )
        """
    ).collect()

    # Guard against leftover rows if the session/table is reused.
    session.sql(f"TRUNCATE TABLE {staging_table}").collect()

    copy_results = session.sql(
        f"""
        COPY INTO {staging_table} (PAYLOAD)
        FROM (SELECT $1 FROM @{stage}/events/)
        FILE_FORMAT = (FORMAT_NAME = {json_format})
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
        """
    ).collect()

    total_rows_loaded = 0
    total_rows_parsed = 0
    for row in copy_results:
        row_dict = row.as_dict()
        print(f"COPY INTO status: {row_dict}")
        total_rows_loaded += row_dict.get("rows_loaded") or 0
        total_rows_parsed += row_dict.get("rows_parsed") or 0

    skipped = total_rows_parsed - total_rows_loaded
    if skipped:
        print(
            f"Warning: {skipped} malformed JSON record(s) were skipped "
            f"during load (rows_parsed={total_rows_parsed}, "
            f"rows_loaded={total_rows_loaded}). Individual rejection "
            f"reasons are not captured per-record in this Snowpark path; "
            f"use src/ingest.py if that level of detail is required."
        )

    return session.table(staging_table)


def normalize_events(session: Session, raw_df):
    """
    Flatten the nested event structure using Snowpark DataFrame expressions.

    The source JSON object is expected to contain:
      event_id, work_order_id, client_id, event_type,
      event_timestamp, updated_at, priority, source,
      technician.id, location.store_id, location.region,
      labor.minutes
    """
    df = raw_df.select(col("PAYLOAD"))

    normalized = df.select(
        col("PAYLOAD")["event_id"].cast("string").alias("EVENT_ID"),
        col("PAYLOAD")["work_order_id"].cast("string").alias("WORK_ORDER_ID"),
        col("PAYLOAD")["client_id"].cast("string").alias("CLIENT_ID"),
        upper(col("PAYLOAD")["event_type"].cast("string")).alias("EVENT_TYPE"),
        col("PAYLOAD")["event_timestamp"]
        .cast("string")
        .alias("EVENT_TIMESTAMP_RAW"),
        col("PAYLOAD")["updated_at"]
        .cast("string")
        .alias("UPDATED_AT_RAW"),
        upper(col("PAYLOAD")["priority"].cast("string")).alias("PRIORITY"),
        col("PAYLOAD")["technician"]["id"]
        .cast("string")
        .alias("TECHNICIAN_ID"),
        col("PAYLOAD")["location"]["store_id"]
        .cast("string")
        .alias("STORE_ID"),
        col("PAYLOAD")["location"]["region"]
        .cast("string")
        .alias("REGION"),
        col("PAYLOAD")["labor"]["minutes"]
        .cast("integer")
        .alias("LABOR_MINUTES"),
        col("PAYLOAD")["source"].cast("string").alias("SOURCE_SYSTEM"),
        col("PAYLOAD").cast("variant").alias("RAW_PAYLOAD"),
    )

    normalized = normalized.select(
        "*",
        to_timestamp_tz(col("EVENT_TIMESTAMP_RAW")).alias("EVENT_TIMESTAMP_UTC"),
        to_timestamp_tz(col("UPDATED_AT_RAW")).alias("UPDATED_AT_UTC"),
    )

    # Keep only records that satisfy the assessment's core validation rules.
    validated = normalized.filter(
        col("EVENT_ID").is_not_null()
        & col("WORK_ORDER_ID").is_not_null()
        & col("CLIENT_ID").is_not_null()
        & col("EVENT_TIMESTAMP_UTC").is_not_null()
        & col("UPDATED_AT_UTC").is_not_null()
        & col("EVENT_TYPE").isin(list(VALID_EVENT_TYPES))
        & (
            col("PRIORITY").is_null()
            | col("PRIORITY").isin(list(VALID_PRIORITIES))
        )
        & (
            col("LABOR_MINUTES").is_null()
            | (col("LABOR_MINUTES") >= lit(0))
        )
    )

    return validated


def deduplicate_events(df):
    """Keep the latest correction for each EVENT_ID."""
    payload_hash = sha2(
        df["RAW_PAYLOAD"].cast("string"),
        256,
    )

    window = (
        Window.partition_by("EVENT_ID")
        .order_by(
            col("UPDATED_AT_UTC").desc(),
            payload_hash.desc(),
        )
    )

    return (
        df.with_column("PAYLOAD_HASH", payload_hash)
        .with_column("_RN", row_number().over(window))
        .filter(col("_RN") == 1)
        .drop("_RN")
    )


def load_events(session: Session, events_df, run_id: str) -> None:
    """Merge the current event versions into WORK_ORDER_EVENTS."""
    events_df = events_df.with_column("RUN_ID", lit(run_id))
    events_df.create_or_replace_temp_view("STG_WORK_ORDER_EVENTS")

    session.sql(
        """
        MERGE INTO WORK_ORDER_EVENTS target
        USING STG_WORK_ORDER_EVENTS source
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
            SOURCE_FILE = 'snowpark_stage',
            SOURCE_ROW_NUMBER = NULL,
            PAYLOAD_HASH = source.PAYLOAD_HASH,
            RAW_PAYLOAD = source.RAW_PAYLOAD,
            LAST_SEEN_AT = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
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
            FIRST_SEEN_AT,
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
            'snowpark_stage',
            NULL,
            source.PAYLOAD_HASH,
            source.RAW_PAYLOAD,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """
    ).collect()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alternative Snowpark ingestion implementation."
    )
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--stage",
        default="POMEROY_INGEST_STAGE",
        help="Snowflake internal stage used for source files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/transform through Snowpark without merging events.",
    )
    args = parser.parse_args()

    source_dir = args.source_dir
    event_files = sorted(source_dir.glob("work_order_events_*.jsonl"))
    source_files = event_files + [
        source_dir / "stores.csv",
        source_dir / "technicians.csv",
    ]

    if not event_files:
        raise FileNotFoundError("No work_order_events_*.jsonl files found")

    for path in source_files:
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")

    checksum = source_checksum(source_files)
    run_id = str(uuid.uuid4())

    session = create_session()

    try:
        put_source_files(session, source_dir, args.stage)
        load_reference_tables(session, args.stage)

        raw_events = read_events(session, args.stage)
        normalized = normalize_events(session, raw_events)
        current = deduplicate_events(normalized)

        stats = {
            "files_read": len(source_files),
            "event_files": len(event_files),
            "unique_valid_events": current.count(),
            "source_checksum": checksum,
        }

        print(f"Snowpark processing statistics: {stats}")

        if not args.dry_run:
            load_events(session, current, run_id)
            print(f"Snowpark load completed: {run_id}")
        else:
            print("Snowpark dry-run completed; event merge skipped.")

    finally:
        session.close()


if __name__ == "__main__":
    main()
