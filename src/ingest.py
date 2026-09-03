from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path

from .processor import process_jsonl_files
from .references import read_reference_file


def file_set_checksum(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and load work-order JSONL events.")
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Validate only; do not connect to Snowflake.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    paths = list(args.source_dir.glob("work_order_events_*.jsonl"))
    if not paths:
        raise FileNotFoundError("No work_order_events_*.jsonl files found")
    accepted_events, current_events, rejects, superseded = process_jsonl_files(paths)
    stores, store_rejects = read_reference_file(args.source_dir / 'stores.csv', {'store_id', 'client_id', 'store_number', 'region', 'active_flag'})
    technicians, technician_rejects = read_reference_file(args.source_dir / 'technicians.csv', {'technician_id', 'technician_name', 'home_region', 'active_flag'})
    rejects.extend(store_rejects + technician_rejects)
    stats = {"files": len(paths) + 2, "accepted_event_versions": len(accepted_events), "valid_current_events": len(current_events), "stores": len(stores), "technicians": len(technicians), "rejected_records": len(rejects), "duplicate_or_superseded_records": superseded}
    logging.info("processing statistics: %s", json.dumps(stats, sort_keys=True))
    if args.dry_run:
        return
    from .snowflake_loader import load_events
    run_id = load_events(
        accepted_events, rejects,
        file_set_checksum(paths + [args.source_dir / 'stores.csv', args.source_dir / 'technicians.csv']),
        stores, technicians,
    )
    logging.info("Snowflake load succeeded: run_id=%s", run_id)


if __name__ == "__main__":
    main()
