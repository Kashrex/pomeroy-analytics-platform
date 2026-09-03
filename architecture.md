# Medallion architecture

```text
Source JSONL / CSV today; paginated REST API tomorrow
                 |
                 v
Python ingestion - validation, UTC normalization, retries, batch checkpoint
        |                                   |
        v                                   v
WORK.BRONZE_* landing tables         WORK.BRONZE_REJECTED_EVENTS
        |
        v
WORK.SILVER_* canonical tables - latest version of each EVENT_ID
        |
        v
WORK.WORK_ORDER_SUMMARY and analytical views
```

The Python job owns source interaction and record-level fault isolation. Flyway owns Snowflake DDL and the SQL transformations. A source checksum and `INGESTION_RUNS` provide file-batch checkpoints; `EVENT_ID` plus `UPDATED_AT_UTC` provides event-level idempotency and correction handling.

For an hourly API deployment, the orchestrator persists an API watermark plus pagination cursor only after the corresponding Snowflake run succeeds. It retries bounded transient failures with exponential backoff and honors `Retry-After`; permanent bad records are quarantined without stopping the batch. API credentials are held in a secret manager and injected at runtime, never committed. Alert on failed runs, stale checkpoints, rejection-rate spikes, and unexpected duplicate/correction volumes.
