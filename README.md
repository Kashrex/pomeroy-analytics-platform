# Pomeroy Analytics Platform

Assessment-focused data engineering solution for ingesting work-order event data, validating and deduplicating the source records, loading them into Snowflake, and producing the required work-order analytics.

## Solution overview

The implementation keeps the assessment deliberately simple:

- **Python** handles file ingestion, validation, normalization, reference-data validation, correction handling, rejection capture, statistics, and Snowflake loading.
- **Snowflake** stores the normalized event and reference data and provides the curated work-order model and analytical views.
- **Flyway** manages Snowflake schema/model changes through ordered versioned migrations.
- **GitHub Actions** performs lightweight CI validation and supports deployment.

No stored procedures or unnecessary platform layers are introduced.

## Repository structure

```text
src/
  ingest.py                         # Python ingestion and Snowflake loading

sql/flyway/
  V001__create_ingestion_runs.sql
  V002__create_WORK_ORDER_EVENTS.sql
  V003__create_REJECTED_RECORDS.sql
  V004__create_work_order_summary.sql
  V005__create_analysis_technician_close_rank.sql
  V006__create_analysis_reopen_rate.sql
  V007__create_analysis_weekly_completed.sql
  V008__create_analysis_abnormal_sequences.sql
  V009__create_stores.sql
  V010__create_technicians.sql

data/source/
  stores.csv
  technicians.csv
  work_order_events_01.jsonl
  work_order_events_02.jsonl
  work_order_events_03.jsonl

architecture.md                    # Production architecture and operating model
flyway.toml                         # Flyway configuration
requirements.txt                    # Python dependencies
.github/workflows/                   # CI/CD workflows
```

All Flyway migrations are versioned (`V###`). Repeatable `R__*.sql` migrations are not used.

## Source data

The assessment source consists of:

- 3 JSONL work-order event files
- 1 store reference CSV
- 1 technician reference CSV

The reference CSV schemas are:

### `stores.csv`

```text
store_id
client_id
store_number
region
active_flag
```

### `technicians.csv`

```text
technician_id
technician_name
home_region
active_flag
```

The CSV reference data is not only used for validation; it is also loaded into the Snowflake `STORES` and `TECHNICIANS` tables.

## Python ingestion

Run the ingestion locally with:

```bash
python -m pip install -r requirements.txt
```

### Dry run

```bash
python -m src.ingest --source-dir data/source --dry-run
```

The dry run performs processing and validation without connecting to Snowflake.

### Snowflake load

Set the required environment variables:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_DATABASE
```

Optional:

```text
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_ROLE
SNOWFLAKE_SCHEMA
```

`SNOWFLAKE_SCHEMA` defaults to `WORK`.

Then run:

```bash
python -m src.ingest --source-dir data/source
```

## Ingestion behavior

The ingestion process:

1. Discovers and processes every `work_order_events_*.jsonl` file.
2. Reads `stores.csv` and `technicians.csv`.
3. Validates required fields and supported event types.
4. Validates priorities, timestamps, nested objects, and non-negative labor.
5. Validates event store and technician references against the CSV reference data.
6. Normalizes timestamps to UTC.
7. Continues processing after malformed or invalid records.
8. Captures rejected records with source file, row number, reason, and raw payload.
9. Resolves duplicate/correction records by `EVENT_ID`.
10. Keeps the newest version using `UPDATED_AT` with a payload-hash tie breaker.
11. Calculates ingestion statistics and a deterministic source checksum.
12. Loads reference data to `STORES` and `TECHNICIANS`.
13. Loads normalized events to `WORK_ORDER_EVENTS`.
14. Uses Snowflake `MERGE` operations to make event and reference loading idempotent.
15. Records ingestion-run metadata in `INGESTION_RUNS`.

### Source statistics for the supplied dataset

The supplied source produced:

| Metric | Result |
|---|---:|
| Source files | 5 |
| Event records read | 11,822 |
| Accepted event versions | 11,817 |
| Unique valid events | 11,507 |
| Duplicate/superseded records | 310 |
| Rejected records | 5 |
| Unique work orders | 2,209 |
| Stores | 96 |
| Technicians | 40 |

These statistics are produced by the Python ingestion process and logged at runtime.

## Snowflake model

Flyway applies the migrations in order:

| Version | Purpose |
|---|---|
| V001 | Creates `INGESTION_RUNS` |
| V002 | Creates `WORK_ORDER_EVENTS` |
| V003 | Creates `REJECTED_RECORDS` |
| V004 | Creates `WORK_ORDER_SUMMARY` |
| V005 | Technician work-order closing analysis |
| V006 | Reopen-rate analysis |
| V007 | Weekly completed work-order analysis |
| V008 | Abnormal event-sequence analysis |
| V009 | Creates `STORES` |
| V010 | Creates `TECHNICIANS` |

The reference tables are included as versioned migrations so the repository remains reproducible from an empty Snowflake schema.

## Curated model

`WORK_ORDER_SUMMARY` provides one row per `WORK_ORDER_ID` and contains the work-order-level attributes and measures needed for analysis, including:

- client
- store
- technician
- priority
- open timestamp
- close timestamp
- current status
- total labor
- event count
- reopen count
- resolution hours

The analytical views are maintained as separate Flyway migrations rather than being combined into a single analysis script.

## Analytical outputs

The solution provides separate views for:

1. **Technician close ranking** — identifies technicians closing the most work orders.
2. **Reopen rate** — calculates the percentage of completed/currently closed work orders that experienced a reopen event.
3. **Weekly completed work orders** — reports completed volume and average resolution time by week.
4. **Abnormal event sequences** — identifies unexpected transitions such as a work order moving from a completed/closed state back into an active state.

## Idempotency and correction handling

Two levels of idempotency are used:

- **Batch level:** `INGESTION_RUNS.SOURCE_CHECKSUM` identifies an already processed source batch.
- **Event level:** `EVENT_ID` is the business key for corrections. A newer `UPDATED_AT` version replaces an older version through `MERGE`.

Reference tables are also loaded using `MERGE`, allowing the CSVs to be safely reconciled on repeated executions.

## Error handling

Invalid records do not stop the entire ingestion batch.

Examples include:

- malformed JSON
- missing required identifiers
- invalid timestamps
- unsupported event types
- invalid priorities
- negative labor
- unknown store IDs
- unknown technician IDs
- invalid reference CSV rows

Rejected records are written to `REJECTED_RECORDS` for investigation.

## CI/CD

CI performs lightweight repository validation, including:

- checking that `flyway.toml` exists
- compiling the Python source
- verifying all expected V001–V010 Flyway migrations exist

The project does not add a test suite solely for the assessment because tests were not a stated deliverable.

Deployment applies Flyway migrations before executing the ingestion process.

## Production considerations

The supplied assessment uses local JSONL/CSV files. The production architecture extends this design to an hourly REST ingestion process with:

- API pagination
- persisted checkpoints/watermarks
- bounded retries and exponential backoff
- rate-limit handling
- secret-manager authentication
- idempotent ingestion
- dead-letter/rejection handling
- structured logging and metrics
- alerting
- orchestration
- CI/CD

See `architecture.md` for the production design.
