"""
Snowpark implementation of the Pomeroy ingestion flow with full functional parity.

This version implements:
- Batch Idempotency via INGESTION_RUNS.
- Source tracking via Snowflake metadata columns (using a Temp View bypass).
- Referential integrity checks against loaded reference data.
- Split-stream routing to write failed records to REJECTED_RECORDS.
- Caching to prevent lazy-evaluation recomputation.
- Safe JSON parsing, timestamp 'Z' handling, and deterministic hashing.
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
    is_null,
    lit,
    replace,
    row_number,
    sha2,
    to_json,
    to_timestamp_tz,
    try_parse_json,
    upper,
    when,
)
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from snowflake.snowpark.window import Window

VALID_EVENT_TYPES = [
    "OPENED", "ASSIGNED", "WORK_STARTED", 
    "WORK_COMPLETED", "REOPENED", "CLOSED"
]
VALID_PRIORITIES = ["P1", "P2", "P3", "P4"]

LOGGER = logging.getLogger(__name__)


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
        raise RuntimeError("Missing Snowflake environment variables: " + ", ".join(missing))

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
        session.file.put(str(path), f"@{stage}/events", auto_compress=True, overwrite=True)

    for filename in ("stores.csv", "technicians.csv"):
        path = source_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")
        session.file.put(str(path), f"@{stage}/reference", auto_compress=True, overwrite=True)


def load_reference_tables(session: Session, stage: str) -> None:
    """Load STORES and TECHNICIANS using Snowpark DataFrames."""
    csv_format = "POMEROY_CSV_FORMAT"
    session.sql(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {csv_format}
        TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        """
    ).collect()

    store_schema = StructType([
        StructField("STORE_ID", StringType()), StructField("CLIENT_ID", StringType()),
        StructField("STORE_NUMBER", StringType()), StructField("REGION", StringType()),
        StructField("ACTIVE_FLAG", StringType())
    ])
    
    stores = session.read.option("FIELD_DELIMITER", ",").option("SKIP_HEADER", 1)\
        .schema(store_schema).csv(f"@{stage}/reference/stores.csv.gz")
    stores.create_or_replace_temp_view("STG_STORES")

    technician_schema = StructType([
        StructField("TECHNICIAN_ID", StringType()), StructField("TECHNICIAN_NAME", StringType()),
        StructField("HOME_REGION", StringType()), StructField("ACTIVE_FLAG", StringType())
    ])
    
    technicians = session.read.option("FIELD_DELIMITER", ",").option("SKIP_HEADER", 1)\
        .schema(technician_schema).csv(f"@{stage}/reference/technicians.csv.gz")
    technicians.create_or_replace_temp_view("STG_TECHNICIANS")

    session.sql("""
        MERGE INTO STORES target USING STG_STORES source ON target.STORE_ID = source.STORE_ID
        WHEN MATCHED THEN UPDATE SET CLIENT_ID = source.CLIENT_ID, STORE_NUMBER = source.STORE_NUMBER, REGION = source.REGION, ACTIVE_FLAG = source.ACTIVE_FLAG
        WHEN NOT MATCHED THEN INSERT (STORE_ID, CLIENT_ID, STORE_NUMBER, REGION, ACTIVE_FLAG) VALUES (source.STORE_ID, source.CLIENT_ID, source.STORE_NUMBER, source.REGION, source.ACTIVE_FLAG)
    """).collect()

    session.sql("""
        MERGE INTO TECHNICIANS target USING STG_TECHNICIANS source ON target.TECHNICIAN_ID = source.TECHNICIAN_ID
        WHEN MATCHED THEN UPDATE SET TECHNICIAN_NAME = source.TECHNICIAN_NAME, HOME_REGION = source.HOME_REGION, ACTIVE_FLAG = source.ACTIVE_FLAG
        WHEN NOT MATCHED THEN INSERT (TECHNICIAN_ID, TECHNICIAN_NAME, HOME_REGION, ACTIVE_FLAG) VALUES (source.TECHNICIAN_ID, source.TECHNICIAN_NAME, source.HOME_REGION, source.ACTIVE_FLAG)
    """).collect()


def process_events(session: Session, stage: str, run_id: str) -> dict:
    """Process files, validate, isolate rejects, and deduplicate."""
    raw_format = "POMEROY_RAW_LINE_FORMAT"
    session.sql(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS {raw_format}
        TYPE = CSV FIELD_DELIMITER = NONE RECORD_DELIMITER = '\\n' ESCAPE_UNENCLOSED_FIELD = NONE
        """
    ).collect()

    # 1. Define the table explicitly, then insert to bypass stage CTAS limitations
    session.sql(
        """
        CREATE OR REPLACE TEMPORARY TABLE STG_RAW_EVENTS (
            SOURCE_FILE STRING,
            SOURCE_ROW_NUMBER NUMBER,
            RAW_PAYLOAD_STRING STRING
        )
        """
    ).collect()

    session.sql(
        f"""
        INSERT INTO STG_RAW_EVENTS (SOURCE_FILE, SOURCE_ROW_NUMBER, RAW_PAYLOAD_STRING)
        SELECT 
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            $1
        FROM @{stage}/events/ (FORMAT_NAME => '{raw_format}')
        WHERE $1 IS NOT NULL
        """
    ).collect()

    # df = session.table("STG_RAW_EVENTS")

    df = session.table("STG_RAW_EVENTS")

    # 2. Parse JSON safely
    df = df.with_column("PAYLOAD", try_parse_json(col("RAW_PAYLOAD_STRING")))

    # 3. Extract Fields with explicit Types to strip Variant quotes and fix Z suffix
    df = df.select(
        col("SOURCE_FILE"),
        col("SOURCE_ROW_NUMBER"),
        col("RAW_PAYLOAD_STRING"),
        col("PAYLOAD"),
        col("PAYLOAD")["event_id"].cast(StringType()).alias("EVENT_ID"),
        col("PAYLOAD")["work_order_id"].cast(StringType()).alias("WORK_ORDER_ID"),
        col("PAYLOAD")["client_id"].cast(StringType()).alias("CLIENT_ID"),
        upper(col("PAYLOAD")["event_type"].cast(StringType())).alias("EVENT_TYPE"),
        replace(col("PAYLOAD")["event_timestamp"].cast(StringType()), lit("Z"), lit("+00:00")).alias("EVENT_TIMESTAMP_RAW"),
        replace(col("PAYLOAD")["updated_at"].cast(StringType()), lit("Z"), lit("+00:00")).alias("UPDATED_AT_RAW"),
        upper(col("PAYLOAD")["priority"].cast(StringType())).alias("PRIORITY"),
        col("PAYLOAD")["technician"]["id"].cast(StringType()).alias("TECHNICIAN_ID"),
        col("PAYLOAD")["location"]["store_id"].cast(StringType()).alias("STORE_ID"),
        col("PAYLOAD")["location"]["region"].cast(StringType()).alias("REGION"),
        col("PAYLOAD")["labor"]["minutes"].cast(IntegerType()).alias("LABOR_MINUTES"),
        col("PAYLOAD")["source"].cast(StringType()).alias("SOURCE_SYSTEM")
    )

    # 4. Reference Lookups
    stores_stg = session.table("STORES").select(col("STORE_ID").alias("REF_STORE_ID"))
    techs_stg = session.table("TECHNICIANS").select(col("TECHNICIAN_ID").alias("REF_TECH_ID"))
    
    df = df.join(stores_stg, df["STORE_ID"] == stores_stg["REF_STORE_ID"], "left")
    df = df.join(techs_stg, df["TECHNICIAN_ID"] == techs_stg["REF_TECH_ID"], "left")

    # 5. Validation Routing
    df = df.with_column(
        "REJECT_REASON",
        when(is_null(col("PAYLOAD")), lit("Invalid JSON"))
        .when(is_null(col("EVENT_ID")), lit("Missing event_id"))
        .when(is_null(col("WORK_ORDER_ID")), lit("Missing work_order_id"))
        .when(is_null(col("CLIENT_ID")), lit("Missing client_id"))
        .when(is_null(to_timestamp_tz(col("EVENT_TIMESTAMP_RAW"))), lit("Invalid event_timestamp"))
        .when(is_null(to_timestamp_tz(col("UPDATED_AT_RAW"))), lit("Invalid updated_at"))
        .when(~col("EVENT_TYPE").isin(VALID_EVENT_TYPES), lit("Invalid event_type"))
        .when(col("PRIORITY").is_not_null() & ~col("PRIORITY").isin(VALID_PRIORITIES), lit("Invalid priority"))
        .when(col("LABOR_MINUTES").is_not_null() & (col("LABOR_MINUTES") < 0), lit("Labor minutes must be non-negative"))
        .when(col("STORE_ID").is_not_null() & col("REF_STORE_ID").is_null(), lit("Unknown store_id"))
        .when(col("TECHNICIAN_ID").is_not_null() & col("REF_TECH_ID").is_null(), lit("Unknown technician_id"))
        .otherwise(None)
    )

    # 6. Cache Result to prevent 4x re-execution of the joins and JSON parsing
    cached_df = df.cache_result()

    rejected_df = cached_df.filter(col("REJECT_REASON").is_not_null())
    accepted_df = cached_df.filter(col("REJECT_REASON").is_null())

    # Write Rejects
    rejects_to_write = rejected_df.select(
        lit(run_id).alias("RUN_ID"),
        col("SOURCE_FILE"),
        col("SOURCE_ROW_NUMBER"),
        col("REJECT_REASON").alias("REASON"),
        col("RAW_PAYLOAD_STRING").alias("RAW_PAYLOAD")
    )
    rejects_to_write.write.mode("append").save_as_table("REJECTED_RECORDS")
    reject_count = rejects_to_write.count()

    # 7. Deduplicate Accepted
    accepted_df = accepted_df.with_column("EVENT_TIMESTAMP_UTC", to_timestamp_tz(col("EVENT_TIMESTAMP_RAW")))
    accepted_df = accepted_df.with_column("UPDATED_AT_UTC", to_timestamp_tz(col("UPDATED_AT_RAW")))
    
    # Hash predictably using to_json instead of the raw whitespace-sensitive string
    accepted_df = accepted_df.with_column("PAYLOAD_HASH", sha2(to_json(col("PAYLOAD")), 256))

    window = Window.partition_by("EVENT_ID").order_by(col("UPDATED_AT_UTC").desc(), col("PAYLOAD_HASH").desc())
    deduped_df = accepted_df.with_column("_RN", row_number().over(window)).filter(col("_RN") == 1).drop("_RN")
    
    return {
        "deduped_df": deduped_df, 
        "accepted_count": accepted_df.count(), 
        "unique_count": deduped_df.count(), 
        "reject_count": reject_count
    }


def load_events(session: Session, events_df, run_id: str) -> None:
    """Merge the current event versions into WORK_ORDER_EVENTS."""
    events_df = events_df.with_column("RUN_ID", lit(run_id))
    events_df.create_or_replace_temp_view("STG_WORK_ORDER_EVENTS")

    session.sql("""
        MERGE INTO WORK_ORDER_EVENTS target USING STG_WORK_ORDER_EVENTS source ON target.EVENT_ID = source.EVENT_ID
        WHEN MATCHED AND (source.UPDATED_AT_UTC > target.UPDATED_AT_UTC OR (source.UPDATED_AT_UTC = target.UPDATED_AT_UTC AND source.PAYLOAD_HASH <> target.PAYLOAD_HASH))
        THEN UPDATE SET
            WORK_ORDER_ID = source.WORK_ORDER_ID, CLIENT_ID = source.CLIENT_ID, EVENT_TYPE = source.EVENT_TYPE,
            EVENT_TIMESTAMP_UTC = source.EVENT_TIMESTAMP_UTC, UPDATED_AT_UTC = source.UPDATED_AT_UTC,
            PRIORITY = source.PRIORITY, TECHNICIAN_ID = source.TECHNICIAN_ID, STORE_ID = source.STORE_ID,
            REGION = source.REGION, LABOR_MINUTES = source.LABOR_MINUTES, SOURCE_SYSTEM = source.SOURCE_SYSTEM,
            SOURCE_FILE = source.SOURCE_FILE, SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER,
            PAYLOAD_HASH = source.PAYLOAD_HASH, RAW_PAYLOAD = source.PAYLOAD, LAST_SEEN_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            EVENT_ID, WORK_ORDER_ID, CLIENT_ID, EVENT_TYPE, EVENT_TIMESTAMP_UTC, UPDATED_AT_UTC,
            PRIORITY, TECHNICIAN_ID, STORE_ID, REGION, LABOR_MINUTES, SOURCE_SYSTEM,
            SOURCE_FILE, SOURCE_ROW_NUMBER, PAYLOAD_HASH, RAW_PAYLOAD, FIRST_SEEN_AT, LAST_SEEN_AT
        ) VALUES (
            source.EVENT_ID, source.WORK_ORDER_ID, source.CLIENT_ID, source.EVENT_TYPE, source.EVENT_TIMESTAMP_UTC, source.UPDATED_AT_UTC,
            source.PRIORITY, source.TECHNICIAN_ID, source.STORE_ID, source.REGION, source.LABOR_MINUTES, source.SOURCE_SYSTEM,
            source.SOURCE_FILE, source.SOURCE_ROW_NUMBER, source.PAYLOAD_HASH, source.PAYLOAD, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
    """).collect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--stage", default="POMEROY_INGEST_STAGE")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    source_dir = args.source_dir
    event_files = sorted(source_dir.glob("work_order_events_*.jsonl"))
    source_files = event_files + [source_dir / "stores.csv", source_dir / "technicians.csv"]

    if not event_files:
        raise FileNotFoundError("No work_order_events_*.jsonl files found")

    checksum = source_checksum(source_files)
    run_id = str(uuid.uuid4())
    session = create_session()

    try:
        # Idempotency Check
        existing_run = session.table("INGESTION_RUNS").filter(col("SOURCE_CHECKSUM") == checksum).limit(1).collect()
        if existing_run:
            LOGGER.info("source checksum already loaded; skipping event load")
            return

        put_source_files(session, source_dir, args.stage)
        load_reference_tables(session, args.stage)

        # Process and track
        results = process_events(session, args.stage, run_id)
        
        # Log Run metadata
        session.sql(f"""
            INSERT INTO INGESTION_RUNS (RUN_ID, SOURCE_CHECKSUM, FILE_COUNT, RECORD_COUNT, UNIQUE_EVENT_COUNT, REJECTED_COUNT, COMPLETED_AT)
            VALUES ('{run_id}', '{checksum}', {len(source_files)}, {results['accepted_count'] + results['reject_count']}, {results['unique_count']}, {results['reject_count']}, CURRENT_TIMESTAMP())
        """).collect()

        if not args.dry_run:
            load_events(session, results["deduped_df"], run_id)
            LOGGER.info("Snowpark load result: %s", run_id)
        else:
            LOGGER.info("Snowpark dry-run completed; event merge skipped.")

    finally:
        session.close()

if __name__ == "__main__":
    main()