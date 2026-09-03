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
import logging
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
        .schema(
            [
                "STORE_ID",
                "CLIENT_ID",
                "STORE_NUMBER",
                "REGION",
                "ACTIVE_FLAG",
            ]
        )
        .csv(f"@{stage}/reference/stores.csv.gz")
    )

    technicians = (
        session.read.option("FIELD_DELIMITER", ",")
        .option("SKIP_HEADER", 1)
        .schema(
            [
                "TECHNICIAN_ID",
                "TECHNICIAN_NAME",
                "HOME_REGION",
                "ACTIVE_FLAG",
            ]
        )
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
    """Read JSONL as raw physical lines and preserve Snowflake file metadata."""
    line_format = "POMEROY_JSONL_LINE_FORMAT"
    session.sql(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {line_format}
        TYPE = CSV
        FIELD_DELIMITER = NONE
        RECORD_DELIMITER = '\\n'
        FIELD_OPTIONALLY_ENCLOSED_BY = NONE
        EMPTY_FIELD_AS_NULL = FALSE
        """
    ).collect()

    return session.sql(
        f"""
        SELECT
            $1::STRING AS RAW_LINE,
            METADATA$FILENAME::STRING AS SOURCE_FILE,
            METADATA$FILE_ROW_NUMBER::NUMBER AS SOURCE_ROW_NUMBER
        FROM @{stage}/events/
        (FILE_FORMAT => '{line_format}')
        """
    )


def normalize_events(raw_df, stores_df, technicians_df):
    """Parse, flatten, validate and retain invalid rows for the DLQ."""
    from snowflake.snowpark.functions import when

    payload = call_function("TRY_PARSE_JSON", col("RAW_LINE"))
    df = raw_df.with_column("PAYLOAD", payload)

    normalized = df.select(
        "RAW_LINE", "SOURCE_FILE", "SOURCE_ROW_NUMBER", "PAYLOAD",
        col("PAYLOAD")["event_id"].cast("string").alias("EVENT_ID"),
        col("PAYLOAD")["work_order_id"].cast("string").alias("WORK_ORDER_ID"),
        col("PAYLOAD")["client_id"].cast("string").alias("CLIENT_ID"),
        upper(col("PAYLOAD")["event_type"].cast("string")).alias("EVENT_TYPE"),
        col("PAYLOAD")["event_timestamp"].cast("string").alias("EVENT_TIMESTAMP_RAW"),
        col("PAYLOAD")["updated_at"].cast("string").alias("UPDATED_AT_RAW"),
        upper(col("PAYLOAD")["priority"].cast("string")).alias("PRIORITY"),
        col("PAYLOAD")["technician"]["id"].cast("string").alias("TECHNICIAN_ID"),
        col("PAYLOAD")["location"]["store_id"].cast("string").alias("STORE_ID"),
        col("PAYLOAD")["location"]["region"].cast("string").alias("REGION"),
        col("PAYLOAD")["labor"]["minutes"].cast("integer").alias("LABOR_MINUTES"),
        col("PAYLOAD")["source"].cast("string").alias("SOURCE_SYSTEM"),
    )
    normalized = normalized.select(
        "*",
        call_function("TRY_TO_TIMESTAMP_TZ", col("EVENT_TIMESTAMP_RAW")).alias("EVENT_TIMESTAMP_UTC"),
        call_function("TRY_TO_TIMESTAMP_TZ", col("UPDATED_AT_RAW")).alias("UPDATED_AT_UTC"),
        sha2(col("RAW_LINE"), 256).alias("PAYLOAD_HASH"),
    )

    store_keys = stores_df.select(col("STORE_ID").alias("_REF_STORE_ID")).drop_duplicates()
    tech_keys = technicians_df.select(col("TECHNICIAN_ID").alias("_REF_TECHNICIAN_ID")).drop_duplicates()
    enriched = (
        normalized
        .join(store_keys, normalized["STORE_ID"] == store_keys["_REF_STORE_ID"], "left")
        .join(tech_keys, normalized["TECHNICIAN_ID"] == tech_keys["_REF_TECHNICIAN_ID"], "left")
    )

    reason = (
        when(col("PAYLOAD").is_null(), lit("malformed JSON"))
        .when(col("EVENT_ID").is_null() | (col("EVENT_ID") == lit("")), lit("event_id is required"))
        .when(col("WORK_ORDER_ID").is_null() | (col("WORK_ORDER_ID") == lit("")), lit("work_order_id is required"))
        .when(col("CLIENT_ID").is_null() | (col("CLIENT_ID") == lit("")), lit("client_id is required"))
        .when(~col("EVENT_TYPE").isin(list(VALID_EVENT_TYPES)), lit("unsupported event_type"))
        .when(col("EVENT_TIMESTAMP_UTC").is_null(), lit("event_timestamp is not a valid ISO timestamp"))
        .when(col("UPDATED_AT_UTC").is_null(), lit("updated_at is not a valid ISO timestamp"))
        .when(col("PRIORITY").is_not_null() & ~col("PRIORITY").isin(list(VALID_PRIORITIES)), lit("priority must be P1, P2, P3 or P4 when present"))
        .when(col("LABOR_MINUTES").is_not_null() & (col("LABOR_MINUTES") < lit(0)), lit("labor.minutes must be a non-negative integer when present"))
        .when(col("STORE_ID").is_not_null() & (col("STORE_ID") != lit("")) & col("_REF_STORE_ID").is_null(), lit("unknown store_id"))
        .when(col("TECHNICIAN_ID").is_not_null() & (col("TECHNICIAN_ID") != lit("")) & col("_REF_TECHNICIAN_ID").is_null(), lit("unknown technician_id"))
    )
    return enriched.select("*", reason.alias("REASON"), when(reason.is_null(), lit(True)).otherwise(lit(False)).alias("VALID_FLAG"))


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
            SOURCE_FILE = source.SOURCE_FILE,
            SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER,
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
            source.SOURCE_FILE,
            source.SOURCE_ROW_NUMBER,
            source.PAYLOAD_HASH,
            source.RAW_PAYLOAD,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """
    ).collect()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate, normalize, deduplicate and load work-order events with Snowpark."
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
        help="Process and validate with Snowpark without writing target tables.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    event_files, reference_files = validate_source_files(args.source_dir)
    source_files = event_files + reference_files
    checksum = source_checksum(source_files)

    session = create_session()
    try:
        put_source_files(session, args.source_dir, args.stage)

        csv_format = "POMEROY_INGEST_CSV_FORMAT"
        session.sql(
            f"""
            CREATE FILE FORMAT IF NOT EXISTS {csv_format}
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            EMPTY_FIELD_AS_NULL = TRUE
            """
        ).collect()

        stores_df = (
            session.read
            .option("SKIP_HEADER", 1)
            .schema(["STORE_ID", "CLIENT_ID", "STORE_NUMBER", "REGION", "ACTIVE_FLAG"])
            .csv(f"@{args.stage}/reference/stores.csv.gz")
            .select(
                col("STORE_ID").cast("string").alias("STORE_ID"),
                col("CLIENT_ID").cast("string").alias("CLIENT_ID"),
                col("STORE_NUMBER").cast("string").alias("STORE_NUMBER"),
                col("REGION").cast("string").alias("REGION"),
                upper(col("ACTIVE_FLAG").cast("string")).alias("ACTIVE_FLAG"),
            )
        )
        technicians_df = (
            session.read
            .option("SKIP_HEADER", 1)
            .schema(["TECHNICIAN_ID", "TECHNICIAN_NAME", "HOME_REGION", "ACTIVE_FLAG"])
            .csv(f"@{args.stage}/reference/technicians.csv.gz")
            .select(
                col("TECHNICIAN_ID").cast("string").alias("TECHNICIAN_ID"),
                col("TECHNICIAN_NAME").cast("string").alias("TECHNICIAN_NAME"),
                col("HOME_REGION").cast("string").alias("HOME_REGION"),
                upper(col("ACTIVE_FLAG").cast("string")).alias("ACTIVE_FLAG"),
            )
        )

        # Reference validation. Bad reference rows are not used for event
        # validation; valid IDs are loaded idempotently into the target tables.
        stores_valid = stores_df.filter(
            col("STORE_ID").is_not_null()
            & (col("STORE_ID") != lit(""))
            & col("ACTIVE_FLAG").isin(["TRUE", "FALSE"])
        ).drop_duplicates(["STORE_ID"])
        technicians_valid = technicians_df.filter(
            col("TECHNICIAN_ID").is_not_null()
            & (col("TECHNICIAN_ID") != lit(""))
            & col("ACTIVE_FLAG").isin(["TRUE", "FALSE"])
        ).drop_duplicates(["TECHNICIAN_ID"])

        stores_count = stores_valid.count()
        technicians_count = technicians_valid.count()

        if not args.dry_run:
            session.sql("BEGIN").collect()
            try:
                stores_valid.create_or_replace_temp_view("STG_STORES")
                technicians_valid.create_or_replace_temp_view("STG_TECHNICIANS")
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
                session.sql("COMMIT").collect()
            except Exception:
                session.sql("ROLLBACK").collect()
                raise

        raw_events = read_events(session, args.stage)
        processed = normalize_events(raw_events, stores_valid, technicians_valid)
        ranked = deduplicate_events(processed)

        records_read = processed.count()
        accepted_versions = ranked.filter(col("VALID_FLAG")).count()
        unique_valid_events = ranked.filter(
            col("VALID_FLAG") & col("_IS_CURRENT")
        ).count()
        rejected_count = processed.filter(~col("VALID_FLAG")).count()
        duplicate_or_superseded = accepted_versions - unique_valid_events
        unique_work_orders = ranked.filter(
            col("VALID_FLAG") & col("_IS_CURRENT")
        ).select("WORK_ORDER_ID").distinct().count()

        stats = {
            "files_read": len(source_files),
            "records_read": records_read,
            "accepted_event_versions": accepted_versions,
            "unique_valid_events": unique_valid_events,
            "duplicate_or_superseded_records": duplicate_or_superseded,
            "rejected_records": rejected_count,
            "unique_work_orders": unique_work_orders,
            "stores": stores_count,
            "technicians": technicians_count,
            "source_checksum": checksum,
        }
        LOGGER.info("processing statistics: %s", stats)

        if args.dry_run:
            LOGGER.info("dry-run completed; Snowflake target writes skipped")
            return

        run_id = str(uuid.uuid4())
        current_events = (
            ranked
            .filter(col("VALID_FLAG") & col("_IS_CURRENT"))
            .select(
                col("EVENT_ID"), col("WORK_ORDER_ID"), col("CLIENT_ID"),
                col("EVENT_TYPE"), col("EVENT_TIMESTAMP_UTC"),
                col("UPDATED_AT_UTC"), col("PRIORITY"), col("TECHNICIAN_ID"),
                col("STORE_ID"), col("REGION"), col("LABOR_MINUTES"),
                col("SOURCE_SYSTEM"), col("SOURCE_FILE"),
                col("SOURCE_ROW_NUMBER"), col("PAYLOAD_HASH"),
                col("PAYLOAD").alias("RAW_PAYLOAD"),
            )
            .with_column("RUN_ID", lit(run_id))
        )
        rejects = (
            processed
            .filter(~col("VALID_FLAG"))
            .select(
                lit(run_id).alias("RUN_ID"),
                col("SOURCE_FILE"), col("SOURCE_ROW_NUMBER"),
                col("REASON"), col("RAW_LINE").alias("RAW_PAYLOAD"),
            )
        )

        # Event load, DLQ write and audit completion are one transaction.
        session.sql("BEGIN").collect()
        try:
            existing = session.sql(
                f"""
                SELECT RUN_ID FROM INGESTION_RUNS
                WHERE SOURCE_CHECKSUM = '{checksum}'
                LIMIT 1
                """
            ).collect()
            if existing:
                session.sql("ROLLBACK").collect()
                LOGGER.info(
                    "source checksum already loaded; event load skipped: %s",
                    existing[0]["RUN_ID"],
                )
                return

            session.sql(
                f"""
                INSERT INTO INGESTION_RUNS
                    (RUN_ID, SOURCE_CHECKSUM, FILE_COUNT, RECORD_COUNT,
                     UNIQUE_EVENT_COUNT, REJECTED_COUNT)
                VALUES
                    ('{run_id}', '{checksum}', {len(source_files)},
                     {records_read}, {unique_valid_events}, {rejected_count})
                """
            ).collect()

            current_events.create_or_replace_temp_view("STG_WORK_ORDER_EVENTS")
            rejects.create_or_replace_temp_view("STG_REJECTED_RECORDS")

            session.sql(
                """
                MERGE INTO WORK_ORDER_EVENTS target
                USING STG_WORK_ORDER_EVENTS source
                  ON target.EVENT_ID = source.EVENT_ID
                WHEN MATCHED AND (
                    source.UPDATED_AT_UTC > target.UPDATED_AT_UTC
                    OR (source.UPDATED_AT_UTC = target.UPDATED_AT_UTC
                        AND source.PAYLOAD_HASH <> target.PAYLOAD_HASH)
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
                ) VALUES (
                    source.EVENT_ID, source.WORK_ORDER_ID, source.CLIENT_ID,
                    source.EVENT_TYPE, source.EVENT_TIMESTAMP_UTC,
                    source.UPDATED_AT_UTC, source.PRIORITY, source.TECHNICIAN_ID,
                    source.STORE_ID, source.REGION, source.LABOR_MINUTES,
                    source.SOURCE_SYSTEM, source.SOURCE_FILE, source.SOURCE_ROW_NUMBER,
                    source.PAYLOAD_HASH, source.RAW_PAYLOAD,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """
            ).collect()

            session.sql(
                """
                INSERT INTO REJECTED_RECORDS
                    (RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, REASON, RAW_PAYLOAD)
                SELECT RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, REASON, RAW_PAYLOAD
                FROM STG_REJECTED_RECORDS
                """
            ).collect()

            session.sql(
                f"""
                UPDATE INGESTION_RUNS
                SET COMPLETED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = '{run_id}'
                """
            ).collect()

            session.sql("COMMIT").collect()
            LOGGER.info("Snowpark load completed: %s", run_id)
        except Exception:
            session.sql("ROLLBACK").collect()
            raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
