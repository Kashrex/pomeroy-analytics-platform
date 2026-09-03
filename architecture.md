# Production Architecture

## Pomeroy Work-Order Ingestion

### 1. Current assessment implementation

The submitted implementation is a **Python + Snowpark** batch ingestion process.

```text
 data/source/
 ├── work_order_events_01.jsonl
 ├── work_order_events_02.jsonl
 ├── work_order_events_03.jsonl
 ├── stores.csv
 └── technicians.csv
          |
          v
 ┌───────────────────────────────┐
 │ Python / Snowpark             │
 │                               │
 │ 1. Discover source files      │
 │ 2. SHA-256 batch checksum     │
 │ 3. Idempotency check          │
 │ 4. Upload to Snowflake stage  │
 │ 5. Load reference data        │
 │ 6. COPY raw JSONL lines       │
 │ 7. TRY_PARSE_JSON             │
 │ 8. Normalize nested fields    │
 │ 9. Validate records           │
 │ 10. Route rejects             │
 │ 11. Deduplicate EVENT_ID      │
 │ 12. MERGE canonical events    │
 └───────────────┬───────────────┘
                 |
                 v
 ┌─────────────────────────────────────────┐
 │ Snowflake                               │
 │                                         │
 │ INGESTION_RUNS                           │
 │ REJECTED_RECORDS                         │
 │ WORK_ORDER_EVENTS                        │
 │ STORES                                   │
 │ TECHNICIANS                              │
 │ WORK_ORDER_SUMMARY                       │
 │ ANALYSIS_*                               │
 └─────────────────────────────────────────┘
```

### 2. Why the implementation uses Snowpark

The working ingestion implementation keeps source processing close to Snowflake.

The JSONL files are uploaded to an internal Snowflake stage. Snowflake's native `COPY INTO` is used to stage the raw line together with source filename and row number. Snowpark then performs parsing, normalization, validation, reference joins, caching, and deduplication.

This gives the process:

- source traceability
- safe malformed-record handling
- Snowflake-side reference validation
- deterministic deduplication
- a direct path into the canonical event table

### 3. Data-quality and reject flow

```text
                    Raw JSONL
                       |
                       v
                TRY_PARSE_JSON
                       |
             ┌─────────┴─────────┐
             |                   |
          invalid               valid
             |                   |
             v                   v
    REJECTED_RECORDS       Field extraction
                                 |
                                 v
                         Reference validation
                                 |
                         ┌───────┴────────┐
                         |                |
                      rejected          accepted
                         |                |
                         v                v
                REJECTED_RECORDS    Dedup by EVENT_ID
                                          |
                                          v
                                   WORK_ORDER_EVENTS
```

Validation covers:

- JSON validity
- required identifiers
- timestamp validity
- event type
- priority
- non-negative labor
- store existence
- technician existence

Every rejected record retains the source filename, source row number, raw payload, run ID, and rejection reason.

### 4. Duplicate and correction handling

Accepted records are partitioned by `EVENT_ID`.

```text
EVENT_ID
   |
   +-- version 1: UPDATED_AT = 10:00
   |
   +-- version 2: UPDATED_AT = 12:00  <-- retained
   |
   +-- version 3: UPDATED_AT = 11:00
```

The ordering is:

```text
UPDATED_AT_UTC DESC
PAYLOAD_HASH DESC
```

The selected canonical version is merged into `WORK_ORDER_EVENTS`.

An existing event is updated only when the incoming version is newer, or when the timestamp is equal and the deterministic payload hash differs.

### 5. Batch idempotency

Before loading, the implementation creates a SHA-256 checksum from the sorted source filenames and their complete contents.

```text
source files
     |
     v
SHA-256
     |
     v
SOURCE_CHECKSUM
     |
     v
INGESTION_RUNS
     |
  already exists?
    /       \
  yes       no
   |         |
 skip      process
```

This prevents replaying the exact same source batch.

---

# Productionized hourly architecture

The assessment asks us to assume that production runs hourly and that the source becomes a paginated REST API.

```text
                         ┌───────────────────────┐
                         │ Git / CI/CD           │
                         │ versioned code + SQL  │
                         └───────────┬───────────┘
                                     |
                                     v
┌─────────────────────────────────────────────────────────┐
│ Airflow / MWAA                                          │
│                                                         │
│ Hourly DAG                                              │
│  ├── load credentials/connections                       │
│  ├── read checkpoint                                    │
│  ├── call API                                            │
│  ├── retry / backoff / rate-limit handling              │
│  ├── persist page                                        │
│  ├── advance checkpoint after success                   │
│  └── publish run metrics / alert on failure             │
└──────────────────────────┬──────────────────────────────┘
                           |
                           v
                ┌─────────────────────┐
                │ Paginated REST API  │
                └──────────┬──────────┘
                           |
                  next-page / cursor
                           |
                           v
                ┌─────────────────────┐
                │ Python + Snowpark   │
                │                     │
                │ Parse               │
                │ Normalize           │
                │ Validate            │
                │ Reference checks    │
                │ Deduplicate         │
                │ Reject invalid rows │
                └──────────┬──────────┘
                           |
                           v
                ┌─────────────────────┐
                │ Snowflake           │
                │                     │
                │ INGESTION_RUNS      │
                │ REJECTED_RECORDS    │
                │ WORK_ORDER_EVENTS   │
                │ STORES              │
                │ TECHNICIANS         │
                └──────────┬──────────┘
                           |
                           v
                ┌─────────────────────┐
                │ Snowflake SQL       │
                │                     │
                │ WORK_ORDER_SUMMARY  │
                │ ANALYSIS_*          │
                └──────────┬──────────┘
                           |
                           v
                    BI / consumers
```

## Production design by requirement

### 1. Incremental processing / checkpoints

Persist the API cursor, page token, or source watermark in durable control metadata.

A checkpoint advances **only after the corresponding page/batch has been successfully persisted**.

This makes a failed run restartable without silently skipping data.

### 2. Pagination

The Python API client loops over pages until no next-page token remains.

For example:

```text
GET page 1
   |
persist successfully
   |
save checkpoint
   |
GET page 2
   |
persist successfully
   |
save checkpoint
   |
...
```

### 3. Retries / rate limits

Use bounded exponential backoff for transient failures.

Handle:

- connection failures
- HTTP 5xx
- HTTP 429
- `Retry-After`

Do not retry permanent 4xx validation/authentication failures indefinitely.

### 4. Authentication / secrets

API and Snowflake credentials should be supplied through managed secrets/connections.

No credentials should be committed to Git.

A production Airflow/MWAA deployment can use an appropriate cloud secrets manager or Airflow connection backed by one.

### 5. Duplicate prevention / idempotency

Use multiple layers:

```text
checkpoint
    +
EVENT_ID
    +
UPDATED_AT
    +
PAYLOAD_HASH
    +
Snowflake MERGE
```

A replayed API page therefore does not create duplicate canonical events.

### 6. Failure recovery

Use page/batch-level restartability.

The sequence should be:

```text
read checkpoint
      |
fetch page
      |
validate/load
      |
successful commit
      |
advance checkpoint
```

Never advance the checkpoint before the corresponding data is safely persisted.

### 7. Logging / monitoring / alerting

Capture at minimum:

```text
run_id
start/end time
checkpoint
pages requested
records received
records accepted
records rejected
duplicates/corrections
API failures
processing duration
```

Alert on failed DAG runs and sustained source/data-quality problems.

### 8. Orchestration / scheduling

Use Airflow/MWAA for the hourly workflow.

Benefits include:

- scheduling
- retry policies
- task dependencies
- run history
- operational visibility
- connection/secret integration

### 9. Deployment / version control

Git is the source of truth.

CI should validate:

- Python compilation
- dependencies
- required source/migration files
- Flyway migration set
- ingestion dry run

Production should deploy a specific Git revision.

### 10. Python vs Snowflake SQL vs cloud services

| Requirement | Technology |
|---|---|
| REST API client | Python |
| Pagination | Python |
| Retry/backoff | Python |
| API checkpoint handling | Python + Snowflake control metadata |
| JSON normalization | Snowpark |
| Record validation | Snowpark |
| Reference validation | Snowpark / Snowflake |
| Canonical merge | Snowflake SQL |
| Work-order summary | Snowflake SQL |
| Analytics | Snowflake SQL |
| Hourly scheduling | Airflow / MWAA |
| Secrets | Cloud secrets manager / Airflow connections |
| CI/CD | GitHub Actions |

## Production improvements beyond the assessment

The current implementation is intentionally compact. Before production, I would additionally introduce:

1. Explicit ingestion run states such as `STARTED`, `SUCCEEDED`, and `FAILED`.
2. Durable API cursor/watermark checkpoints.
3. Automated unit tests for parsing and validation.
4. Integration tests against a controlled Snowflake environment.
5. Centralized structured logging and metrics.
6. Alerting integrated with the production incident channel.
7. Least-privilege Snowflake roles and stage permissions.
8. Secret rotation through the cloud secret manager.
9. Controlled deployment/promotion of Python and Flyway migrations.
10. Replay tooling for rejected records after data-quality correction.

The assessment implementation therefore provides the core ingestion/data-quality model while leaving the operational platform concerns to the production orchestration layer.
