# Pomeroy Analytics Platform

An assessment-sized Snowflake medallion implementation for work-order events. It processes all three JSONL event files plus the store and technician reference CSVs, keeps rejected records reviewable, and exposes a one-row-per-work-order curated view.

## Run locally

1. Create a virtual environment and run `pip install -r requirements.txt`.
2. Apply the migration scripts with Flyway from the repository root: `flyway migrate -url="$SNOWFLAKE_JDBC_URL" -user="$SNOWFLAKE_USER" -password="$SNOWFLAKE_PASSWORD"`.
3. Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, and `SNOWFLAKE_DATABASE` (optionally warehouse, role, and schema).
4. Validate without loading: `python -m src.ingest --source-dir data/source --dry-run`.
5. Load: `python -m src.ingest --source-dir data/source`.

Confirm access without changing data by running `python -m src.preflight`. The expected schema is `WORK`.

Run tests with `python -m unittest discover --start-directory tests --top-level-directory . --verbose`. No third-party test runner is required.

## Design decisions

- Bronze retains received valid payloads and provenance; rejects retain source file, row, reason, and payload where JSON is parseable.
- Silver stores one current version per `EVENT_ID`. A later `UPDATED_AT` replaces an older event; an equal timestamp with a changed payload is also treated as a correction.
- Gold is the `WORK_ORDER_SUMMARY` view. Its grain is exactly one `WORK_ORDER_ID`.
- `INGESTION_RUNS` records each batch. A completed identical source checksum is skipped; the Silver merge is also deterministic, so correction replays cannot create duplicate current events.

## Flyway script layout

Every physical table has its own versioned `V###__*.sql` migration. Flyway records each one once in `WORK.FLYWAY_SCHEMA_HISTORY`. Procedures and views use numerically ordered `R__###_*.sql` repeatable migrations: Flyway re-applies them whenever their file checksum changes. The ordering creates `WORK_ORDER_SUMMARY` before the analysis views that depend on it. Deployment migrates first, validates the resulting state, then ingests source data. Do not rename or edit a versioned migration after it has run in a shared environment; add a new versioned migration instead.

## Validation and assumptions

- Required event fields: event ID, work-order ID, client ID, event type, event timestamp, and update timestamp.
- Nested `technician`, `location`, and `labor` values must be objects when present. Labor minutes must be a non-negative integer.
- Accept priorities P1-P4 when present. Bad JSON, blank JSONL lines, invalid timestamps, and invalid values are quarantined while other rows continue.
- Offset-aware timestamps are converted to UTC. Naive timestamps are assumed UTC; this must be confirmed with the source owner before production.
- `OPENED`, `CLOSED`, `REOPENED`, and `WORK_COMPLETED` are the event semantics used by the curated model. Confirm the source event-type contract before production.

## Data-quality checks and limitations

`ANALYSIS_ABNORMAL_SEQUENCES` flags missing opens, close-before-open, a reopen history ending in closed, and single-event work orders. The current file loader intentionally reads each input batch in memory; for large production volumes replace it with streaming staging uploads. Reference CSV history is type-1 only. Production should use key-pair/OAuth authentication, a secret manager, API cursor checkpoints, dead-letter retention policy, observability dashboards, and alert thresholds.

See [architecture.md](architecture.md) for the production design.

## GitHub deployment prerequisites

The supplied source files are versioned under `data/source`, so the workflow runs on a GitHub-hosted Ubuntu runner and needs no manual path input. The runner needs outbound HTTPS access to GitHub, Snowflake, and the Flyway download endpoint. Add the Snowflake secrets listed in the deployment documentation to the `Development` GitHub environment. The workflow installs Python dependencies and Flyway, checks the Snowflake connection with `flyway info`, applies migrations, and only then starts ingestion.
