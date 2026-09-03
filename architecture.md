# Production Architecture

## 1. Architecture objective

The assessment implementation is intentionally small, but the production design should preserve the same separation of responsibilities:

- **Python** owns external ingestion, validation, normalization, reference checks, and batch control.
- **Snowflake** owns durable storage, transformation into the curated work-order model, and analytics.
- **An orchestrator** controls scheduling, retries, dependencies, and operational state.
- **GitHub Actions / CI/CD** controls code and database deployment.

The design avoids introducing stored procedures or unnecessary processing layers when the same requirement can be handled more simply by Python or Snowflake SQL.

## 2. High-level architecture

```text
                         +----------------------+
                         |   Work Order REST    |
                         |        API           |
                         +----------+-----------+
                                    |
                              Hourly ingestion
                                    |
                                    v
                         +----------------------+
                         | Airflow / MWAA        |
                         | Orchestration         |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Python ingestion      |
                         |----------------------|
                         | Pagination            |
                         | Checkpoint handling   |
                         | Validation            |
                         | UTC normalization     |
                         | Reference checks      |
                         | Correction handling   |
                         | Reject capture        |
                         | Idempotency           |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |      Snowflake       |
                         |----------------------|
                         | STORES               |
                         | TECHNICIANS          |
                         | WORK_ORDER_EVENTS    |
                         | REJECTED_RECORDS     |
                         | INGESTION_RUNS       |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | WORK_ORDER_SUMMARY   |
                         +----------+-----------+
                                    |
             +----------------------+----------------------+
             |                      |                      |
             v                      v                      v
      Technician close       Reopen-rate analysis    Weekly completed
          ranking                                      analysis
             |
             v
      Abnormal sequences
```

## 3. Current assessment implementation

For the assessment, the external API is represented by local source files:

```text
data/source/
  work_order_events_01.jsonl
  work_order_events_02.jsonl
  work_order_events_03.jsonl
  stores.csv
  technicians.csv
```

Python processes the source data and loads it into Snowflake.

The flow is:

```text
JSONL + CSV
    |
    v
Python validation / normalization
    |
    +--> rejected records
    |
    +--> reference tables
    |
    +--> normalized events
    |
    v
Snowflake
    |
    v
WORK_ORDER_SUMMARY
    |
    v
Analytical views
```

## 4. Production hourly ingestion

An Airflow/MWAA DAG runs the ingestion approximately once per hour.

A production run should:

1. Read the persisted API checkpoint.
2. Request the next API page.
3. Validate and normalize the response.
4. Load the valid records into Snowflake.
5. Capture invalid records without failing the entire batch.
6. Commit the Snowflake transaction.
7. Advance the checkpoint only after the load succeeds.
8. Continue pagination until the required page/window is exhausted.
9. Record run statistics and operational metrics.

The checkpoint must never advance before the corresponding data is successfully committed.

## 5. Pagination and checkpoints

The API may expose page numbers, cursors, timestamps, or another continuation token.

The ingestion state should persist at least:

```text
source/system
checkpoint or watermark
pagination cursor
last successful run
run identifier
```

For timestamp-based APIs, use a small overlap window when necessary to protect against late-arriving records. Event-level idempotency prevents duplicate delivery from creating duplicate target rows.

For cursor-based APIs, persist the cursor only after the page has been successfully loaded.

## 6. Retry and rate-limit handling

Transient API failures should use bounded retries with exponential backoff.

Typical retry candidates include:

- HTTP 429
- HTTP 500
- HTTP 502
- HTTP 503
- HTTP 504
- network timeouts

If the API supplies `Retry-After`, respect it.

Retries should be bounded so a permanently unavailable source does not hold the orchestration slot indefinitely.

Authentication failures and malformed requests should generally be treated as non-transient and surfaced immediately.

## 7. Idempotency and correction handling

The solution uses two complementary controls.

### Batch-level idempotency

A deterministic checksum identifies the source batch:

```text
SOURCE_CHECKSUM
```

`INGESTION_RUNS` records the processed batch and prevents the same completed source batch from being processed repeatedly.

### Event-level idempotency

`EVENT_ID` is the event business key.

When the same event arrives again with a newer `UPDATED_AT`, the newer version wins.

When update timestamps are equal, the payload hash provides deterministic tie-breaking.

This handles:

- retries
- duplicate API delivery
- corrected event payloads
- overlapping extraction windows

## 8. Reference data

Stores and technicians are treated as first-class Snowflake reference tables:

```text
STORES
TECHNICIANS
```

The Python ingestion process uses these datasets to validate event references.

The production API version should either:

- ingest reference data through the same controlled pipeline, or
- source it from an authoritative master-data system.

Reference loads should remain idempotent through `MERGE` operations.

## 9. Error handling and dead-letter path

A bad record should not cause an otherwise valid batch to fail.

Examples:

```text
Malformed JSON
Missing event_id
Missing work_order_id
Invalid timestamp
Unsupported event_type
Invalid priority
Negative labor
Unknown store
Unknown technician
```

The rejected-record path should retain:

```text
run_id
source
source row / record identifier
rejection reason
raw payload
rejected timestamp
```

This provides traceability without silently discarding bad data.

## 10. Snowflake responsibilities

Snowflake is responsible for:

### Raw/normalized event storage

`WORK_ORDER_EVENTS`

One row represents the current valid version of an event at `EVENT_ID` grain.

### Reference data

```text
STORES
TECHNICIANS
```

### Ingestion audit

`INGESTION_RUNS`

Stores batch-level statistics and source identity.

### Rejections

`REJECTED_RECORDS`

Stores records that could not be normalized or validated.

### Curated work-order model

`WORK_ORDER_SUMMARY`

Provides one row per work order for downstream analytics.

### Analytics

Separate analytical views answer the assessment questions without embedding those calculations into the ingestion process.

## 11. Observability

Production monitoring should capture at least:

### Pipeline metrics

- run success/failure
- run duration
- records received
- records accepted
- records rejected
- duplicate count
- correction count
- records loaded
- API request count

### Data-quality metrics

- rejection percentage
- unknown-reference count
- unsupported-event count
- invalid timestamp count
- negative-labor count
- unexpected event-sequence count

### Operational metrics

- API latency
- API rate-limit responses
- retry count
- checkpoint age
- Snowflake load duration
- Snowflake failures

### Alerts

Alert on:

- failed ingestion runs
- stale checkpoints
- sustained API failures
- rejection spikes
- unexpected correction spikes
- significant volume drops
- Snowflake load failures

## 12. Security

Production credentials must not be stored in the repository.

Use a secret-management mechanism such as:

```text
AWS Secrets Manager
```

or the organization's approved equivalent.

Credentials should be injected into the runtime environment.

Access should follow least privilege:

- ingestion identity can write required Snowflake tables
- analytical consumers receive read access to curated objects
- administrative Flyway permissions are separated from normal ingestion permissions where practical

Sensitive columns should use the organization's Snowflake security controls where required.

## 13. CI/CD

The repository should use separate concerns for validation and deployment.

### CI

```text
Pull request
    |
    +--> Python compilation / static validation
    |
    +--> Flyway migration file validation
    |
    +--> repository checks
```

### Deployment

```text
Approved merge
    |
    v
Deployment workflow
    |
    +--> authenticate to Snowflake
    |
    +--> run Flyway migrations
    |
    +--> validate migration state
    |
    +--> execute ingestion
    |
    v
Operational monitoring
```

Flyway migrations remain versioned and ordered. The current repository uses V001 through V010; repeatable `R__*.sql` migrations are intentionally not required.

## 14. Failure recovery

The design supports recovery without manually reconstructing data.

### API failure

Retry transient failures. If the run ultimately fails, retain the previous successful checkpoint and retry on the next scheduled run.

### Snowflake failure

Do not advance the source checkpoint until the transaction succeeds.

The same source window can therefore be replayed safely because event-level idempotency prevents duplicate target records.

### Bad records

Continue processing valid records and retain invalid records in the rejection path.

### Partial orchestration failure

Use the persisted checkpoint and event-level `MERGE` logic to safely resume.

## 15. Python vs. Snowflake responsibilities

| Responsibility | Python | Snowflake |
|---|---|---|
| REST API calls | Yes | No |
| Pagination | Yes | No |
| API retries/rate limits | Yes | No |
| Source parsing | Yes | No |
| Record validation | Yes | Some downstream validation |
| Timestamp normalization | Yes | Possible |
| Reference checks | Yes | Possible |
| Rejection capture | Yes | Storage |
| Event correction handling | Coordinates | `MERGE` target |
| Durable event storage | No | Yes |
| Curated work-order model | No | Yes |
| Analytical aggregations | No | Yes |
| BI/query consumption | No | Yes |

This boundary keeps external-system concerns out of Snowflake while allowing Snowflake to perform set-based transformations and analytics efficiently.
