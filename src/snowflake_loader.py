from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Iterable

from snowflake.connector import connect

from .models import NormalizedEvent, RejectedRecord


def connection_from_environment():
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    return connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"), schema=os.environ.get("SNOWFLAKE_SCHEMA", "WORK"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def load_events(events: Iterable[NormalizedEvent], rejects: Iterable[RejectedRecord], source_checksum: str,
                stores: list[dict[str, str]] | None = None, technicians: list[dict[str, str]] | None = None) -> str:
    """Load a run atomically enough to be safely re-run; SQL MERGE applies source corrections."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    events, rejects, stores, technicians = list(events), list(rejects), stores or [], technicians or []
    with connection_from_environment() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT run_id FROM INGESTION_RUNS WHERE source_checksum=%s AND status='SUCCEEDED' LIMIT 1", (source_checksum,))
            completed_run = cur.fetchone()
            if completed_run:
                return completed_run[0]
            cur.execute(
                "INSERT INTO INGESTION_RUNS (run_id, source_checksum, started_at, status) VALUES (%s,%s,%s,'RUNNING')",
                (run_id, source_checksum, started_at),
            )
            # The raw payload is retained even though columns are normalized for easy review.
            cur.executemany(
                """INSERT INTO BRONZE_EVENT_LANDING
                (run_id,event_id,work_order_id,client_id,event_type,event_timestamp_utc,updated_at_utc,priority,
                 technician_id,store_id,region,labor_minutes,source_system,source_file,source_row_number,payload_hash,raw_payload)
                SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,PARSE_JSON(%s)""",
                [(run_id, e.event_id, e.work_order_id, e.client_id, e.event_type, e.event_timestamp_utc,
                  e.updated_at_utc, e.priority, e.technician_id, e.store_id, e.region, e.labor_minutes,
                  e.source_system, e.source_file, e.source_row_number, e.payload_hash, json.dumps(e.payload)) for e in events],
            )
            cur.executemany(
                "INSERT INTO BRONZE_REJECTED_EVENTS (run_id,source_file,source_row_number,rejection_reason,raw_payload) SELECT %s,%s,%s,%s,TRY_PARSE_JSON(%s)",
                [(run_id, r.source_file, r.source_row_number, r.reason, r.payload) for r in rejects],
            )
            cur.executemany(
                "INSERT INTO BRONZE_STORE_LANDING (run_id,store_id,client_id,store_number,region,active_flag,source_file,source_row_number) VALUES (%s,%s,%s,%s,%s,%s,'stores.csv',%s)",
                [(run_id, x['store_id'], x['client_id'], x['store_number'], x['region'], x['active_flag'] == 'TRUE', i) for i, x in enumerate(stores, 2)],
            )
            cur.executemany(
                "INSERT INTO BRONZE_TECHNICIAN_LANDING (run_id,technician_id,technician_name,home_region,active_flag,source_file,source_row_number) VALUES (%s,%s,%s,%s,%s,'technicians.csv',%s)",
                [(run_id, x['technician_id'], x['technician_name'], x['home_region'], x['active_flag'] == 'TRUE', i) for i, x in enumerate(technicians, 2)],
            )
            cur.execute("CALL APPLY_SILVER_EVENT_CHANGES(%s)", (run_id,))
            cur.execute("""MERGE INTO SILVER_STORES t USING (SELECT * FROM BRONZE_STORE_LANDING WHERE run_id=%s) s ON t.store_id=s.store_id
                WHEN MATCHED THEN UPDATE SET client_id=s.client_id,store_number=s.store_number,region=s.region,active_flag=s.active_flag,last_seen_at=CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (store_id,client_id,store_number,region,active_flag) VALUES (s.store_id,s.client_id,s.store_number,s.region,s.active_flag)""", (run_id,))
            cur.execute("""MERGE INTO SILVER_TECHNICIANS t USING (SELECT * FROM BRONZE_TECHNICIAN_LANDING WHERE run_id=%s) s ON t.technician_id=s.technician_id
                WHEN MATCHED THEN UPDATE SET technician_name=s.technician_name,home_region=s.home_region,active_flag=s.active_flag,last_seen_at=CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (technician_id,technician_name,home_region,active_flag) VALUES (s.technician_id,s.technician_name,s.home_region,s.active_flag)""", (run_id,))
            cur.execute(
                "UPDATE INGESTION_RUNS SET completed_at=CURRENT_TIMESTAMP(), valid_event_count=%s, rejected_event_count=%s, status='SUCCEEDED' WHERE run_id=%s",
                (len(events), len(rejects), run_id),
            )
    return run_id
