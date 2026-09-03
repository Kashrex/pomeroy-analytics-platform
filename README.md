# Pomeroy Data Analytics & Engineering Technical Assessment

## Overview

This repository implements the Pomeroy work-order event ingestion and analytics flow using **Python + Snowpark for Snowflake**.

The ingestion process:

1. Discovers the three `work_order_events_*.jsonl` files plus `stores.csv` and `technicians.csv`.
2. Calculates a deterministic SHA-256 checksum for the complete source batch.
3. Uses `INGESTION_RUNS` for batch-level idempotency.
4. Uploads source files to a Snowflake internal stage.
5. Loads the reference CSVs into `STORES` and `TECHNICIANS` using Snowpark DataFrames and `MERGE`.
6. Reads raw JSONL lines from the Snowflake stage into a temporary staging table using `COPY INTO`.
7. Parses JSON safely and extracts the nested work-order fields.
8. Validates required fields, timestamps, event types, priority, labor minutes, and store/technician references.
9. Routes invalid records to `REJECTED_RECORDS`.
10. Deduplicates valid events by `EVENT_ID`, retaining the newest `UPDATED_AT` version.
11. Merges the canonical event version into `WORK_ORDER_EVENTS`.
12. Uses SQL migrations to create the ingestion, curated, reference, and analysis objects.

The implementation is intentionally assessment-sized: Snowflake is the system of record, while Snowpark performs the ingestion, validation, and deduplication logic.

---

## Repository structure

```text
.
├── data/
│   └── source/
│       ├── stores.csv
│       ├── technicians.csv
│       ├── work_order_events_01.jsonl
│       ├── work_order_events_02.jsonl
│       └── work_order_events_03.jsonl
├── src/
│   └── ingest.py
├── sql/
│   └── flyway/
│       ├── V001__create_ingestion_runs.sql
│       ├── V002__create_WORK_ORDER_EVENTS.sql
│       ├── V003__create_REJECTED_RECORDS.sql
│       ├── V004__create_work_order_summary.sql
│       ├── V005__create_analysis_technician_close_rank.sql
│       ├── V006__create_analysis_reopen_rate.sql
│       ├── V007__create_analysis_weekly_completed.sql
│       ├── V008__create_analysis_abnormal_sequences.sql
│       ├── V009__create_stores.sql
│       └── V010__create_technicians.sql
├── architecture.md
├── flyway.toml
├── requirements.txt
└── .github/
    └── workflows/
        └── ci.yml
```

There are no repeatable `R__*.sql` migrations. The repository uses versioned `V001`–`V010` migrations.

---

## How to run

### 1. Install dependencies

```bash
python -m pip install -r requirements.txt
```

The ingestion requires the Snowflake Python connector/Snowpark dependencies specified in `requirements.txt`.

### 2. Configure Snowflake connection variables

The script reads:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA       # optional; defaults to WORK
SNOWFLAKE_WAREHOUSE    # optional
SNOWFLAKE_ROLE         # optional
```

For a production deployment, credentials should not be stored in the repository. They should be supplied through the orchestration platform's secret/connection mechanism.

### 3. Apply Snowflake migrations

Apply the Flyway migrations in order:

```text
V001 → V010
```

These create the ingestion audit table, event/reject tables, curated summary, analysis outputs, and reference tables.

### 4. Run the ingestion

From the repository root:

```bash
python -m src.ingest --source-dir data/source
```

The default Snowflake internal stage is:

```text
POMEROY_INGEST_STAGE
```

A different stage can be supplied with:

```bash
python -m src.ingest \
  --source-dir data/source \
  --stage MY_INGEST_STAGE
```

### 5. Dry run

```bash
python -m src.ingest \
  --source-dir data/source \
  --dry-run
```

The dry run performs the source upload, reference loading, parsing, validation, rejection routing, and deduplication, but skips the final merge into `WORK_ORDER_EVENTS`.

---

## Main design decisions

### Snowpark rather than local Python JSON processing

The working implementation uses Snowpark to keep ingestion processing close to Snowflake.

Raw JSONL files are uploaded to a Snowflake internal stage and read into Snowflake through `COPY INTO`. Snowpark then performs JSON parsing, nested-field extraction, validation, reference joins, caching, and deduplication.

This avoids requiring a separate local dataframe engine and keeps the loaded data inside Snowflake.

### Raw-line staging

The source JSONL contains malformed records that must not stop the entire batch.

The implementation therefore stages the raw line together with:

- source filename
- source row number
- raw payload

This preserves enough source context to route invalid records to `REJECTED_RECORDS`.

### Safe JSON parsing

`TRY_PARSE_JSON` is used instead of a strict JSON parser inside Snowflake. A malformed JSON line therefore becomes an invalid payload that can be routed to the reject stream instead of terminating the complete batch.

### Nested JSON normalization

The implementation extracts fields from nested structures such as:

```text
technician.id
location.store_id
location.region
labor.minutes
```

and produces the relational columns required by `WORK_ORDER_EVENTS`.

### Referential validation

The reference tables are loaded before event processing.

The event stream is left-joined to `STORES` and `TECHNICIANS`. An unknown store or technician is rejected rather than silently loaded as a valid event.

### Duplicate/correction handling

Valid records are partitioned by `EVENT_ID` and ordered by:

```text
UPDATED_AT_UTC DESC
PAYLOAD_HASH DESC
```

The first record is retained.

The final load uses a Snowflake `MERGE` against `WORK_ORDER_EVENTS`. If an existing event has an older `UPDATED_AT_UTC`, the newer version replaces it.

The payload hash provides a deterministic tie-breaker when timestamps are equal.

### Deterministic payload hashing

The hash is calculated from the parsed JSON representation rather than the raw JSON text. This prevents insignificant JSON whitespace/formatting differences from being treated as different payloads.

### Batch idempotency

The complete source batch is hashed using:

- sorted source filenames
- file contents

The resulting checksum is stored in `INGESTION_RUNS`.

If the same source batch is encountered again, the process detects the existing checksum and skips the event load.

---

## Validation and data-quality handling

The ingestion validates:

- JSON syntax
- `event_id`
- `work_order_id`
- `client_id`
- `event_timestamp`
- `updated_at`
- event type
- priority
- non-negative labor minutes
- referenced store existence
- referenced technician existence

Invalid records are written to:

```text
REJECTED_RECORDS
```

with:

```text
RUN_ID
SOURCE_FILE
SOURCE_ROW_NUMBER
REASON
RAW_PAYLOAD
```

This allows the source problem to be investigated without losing the rest of the batch.

### Source observations

The supplied source data contains examples of:

- malformed JSON
- missing `event_id`
- negative labor minutes
- an invalid event type
- an unknown store/technician reference

These are intentionally treated as record-level data-quality failures rather than batch-fatal failures.

---

## Snowflake objects

### `INGESTION_RUNS`

Tracks each processed source batch:

```text
RUN_ID
SOURCE_CHECKSUM
FILE_COUNT
RECORD_COUNT
UNIQUE_EVENT_COUNT
REJECTED_COUNT
COMPLETED_AT
```

### `WORK_ORDER_EVENTS`

Contains the canonical/latest version of each accepted event.

Important metadata includes:

```text
EVENT_ID
SOURCE_FILE
SOURCE_ROW_NUMBER
PAYLOAD_HASH
RAW_PAYLOAD
FIRST_SEEN_AT
LAST_SEEN_AT
```

### `REJECTED_RECORDS`

Stores invalid source records and the validation reason.

### `STORES` and `TECHNICIANS`

Reference data used during referential validation.

### `WORK_ORDER_SUMMARY`

The curated work-order-level model is built from the canonical event table.

The model supports:

- one row per work order
- client/store/technician attributes
- priority
- open/close timestamps
- current status
- labor totals
- event count
- reopen count
- resolution time
- last update

The analysis migrations answer the assessment's required analytical questions.

---

## Production architecture

The current submission is a batch-oriented assessment implementation. In production, the same logical flow would be scheduled hourly and the source would be replaced by a paginated REST API.

```text
                 ┌─────────────────────────────┐
                 │ Airflow / MWAA               │
                 │ Hourly schedule              │
                 └──────────────┬──────────────┘
                                │
                                v
                 ┌─────────────────────────────┐
                 │ REST API                     │
                 │ Pagination + checkpoint      │
                 └──────────────┬──────────────┘
                                │
                         retry / backoff
                                │
                                v
                 ┌─────────────────────────────┐
                 │ Python / Snowpark ingestion │
                 │                             │
                 │ Parse → Validate → Dedup    │
                 │ Reject invalid records      │
                 └──────────────┬──────────────┘
                                │
                                v
                 ┌─────────────────────────────┐
                 │ Snowflake                    │
                 │                             │
                 │ WORK_ORDER_EVENTS            │
                 │ REJECTED_RECORDS             │
                 │ INGESTION_RUNS               │
                 │ STORES / TECHNICIANS         │
                 │ WORK_ORDER_SUMMARY            │
                 └──────────────┬──────────────┘
                                │
                                v
                    BI / downstream analytics
```

### 1. Incremental processing and checkpoints

For an API source, the production version should persist the last successfully processed API cursor/token or source watermark.

A checkpoint should advance only after the corresponding page/batch has been successfully persisted.

### 2. Pagination

The ingestion client should repeatedly request pages until the API indicates that there is no next page.

A checkpoint prevents the process from starting over from the beginning after every run.

### 3. Retries and rate limits

Transient HTTP failures should use bounded retries with exponential backoff.

HTTP `429` responses should honor the API's rate-limit/`Retry-After` information where available.

Permanent client errors should fail the run and raise an alert rather than being retried indefinitely.

### 4. Authentication and secrets

Production API credentials and Snowflake credentials should be managed through the orchestration/cloud secret mechanism rather than environment files committed to Git.

Examples include Airflow connections/variables backed by a cloud secrets manager.

### 5. Duplicate prevention and idempotency

The current implementation uses a source-batch checksum plus `EVENT_ID`-based canonicalization.

For an API, the same principle should be retained:

```text
API page/checkpoint
       +
EVENT_ID
       +
UPDATED_AT
       +
MERGE
```

A replayed page should not create duplicate canonical events.

### 6. Failure recovery

Failures should be isolated by page/batch where practical.

A failed run should retain its checkpoint and resume from the last successfully committed position.

Rejected records should remain queryable for remediation and replay.

### 7. Logging, monitoring, and alerting

Production monitoring should capture:

- run ID
- start/end time
- pages processed
- records received
- records accepted
- records rejected
- duplicates/corrections
- API response failures
- processing duration
- checkpoint position

Alerts should be generated for failed runs and persistent data-quality or API failures.

### 8. Orchestration

Airflow/MWAA is a suitable production scheduler because it can:

- run the process hourly
- manage retries
- maintain task dependencies
- expose run status
- integrate with secrets/connections
- provide operational visibility

### 9. Deployment and version control

Source code and SQL migrations should be version-controlled in Git.

CI should validate:

- Python compilation
- dependency installation
- required files
- Flyway migration presence/order
- ingestion dry-run

Deployment should promote a known Git revision rather than manually edited production code.

### 10. Where each technology belongs

| Concern | Recommended implementation |
|---|---|
| API client | Python |
| Pagination/checkpoint logic | Python |
| HTTP retry/backoff | Python |
| JSON normalization | Python/Snowpark |
| Record validation | Snowpark |
| Reference-data validation | Snowpark/Snowflake |
| Canonical event merge | Snowflake SQL |
| Curated work-order model | Snowflake SQL |
| Analytics | Snowflake SQL |
| Scheduling/orchestration | Airflow / MWAA |
| Secrets | Cloud secrets manager / Airflow connections |
| CI/CD | GitHub Actions + Git |

The important separation is that orchestration and API concerns remain outside Snowflake, while data-intensive relational transformations and the system of record remain in Snowflake.

---

## Known limitations

This is an assessment-sized implementation rather than a full production platform.

1. **The current source is file-based.** Production would replace the file discovery/staging step with a paginated API client.
2. **The current checkpoint is batch-level.** Production API ingestion should persist page/cursor checkpoints.
3. **The current Snowflake connection uses environment variables.** Production should use managed secrets/connections.
4. **The current implementation does not provide a complete automated unit/integration test suite.** CI currently performs compilation, dependency, file/migration, and dry-run validation.
5. **Operational alerting is described in the architecture but is not implemented in this assessment repository.**
6. **The ingestion audit row is written before the final event merge.** A production implementation should make run status explicit (for example `STARTED`, `SUCCEEDED`, `FAILED`) so an interrupted run cannot be mistaken for a successfully completed load.
7. **The assessment's batch checksum treats the complete source set as one batch.** An hourly API implementation should use durable source checkpoints/watermarks so newly arrived data can be processed incrementally.
8. **Snowflake temporary staging objects are execution-scoped.** Production orchestration should keep stage/file-format naming and permissions controlled through deployment configuration.

---

## Engineering judgment

The solution deliberately favors a clear division of responsibility:

- **Snowpark** handles ingestion-side processing close to Snowflake.
- **Snowflake SQL** handles the curated work-order model and analytical transformations.
- **Airflow/MWAA** handles scheduling and production orchestration.
- **Git + CI/CD** handles versioning and controlled deployment.

This keeps the assessment implementation reasonably small while providing a direct path to an hourly production pipeline.
