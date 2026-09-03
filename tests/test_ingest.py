from pathlib import Path
import tempfile
import unittest

from src.ingest import normalize_event, process_files


class IngestTests(unittest.TestCase):
    def test_normalizes_timestamp_and_nested_fields(self):
        event = normalize_event({
            "event_id": "E1", "work_order_id": "W1", "client_id": "C1",
            "event_type": "opened", "event_timestamp": "2026-01-01T05:00:00+05:00",
            "updated_at": "2026-01-01T05:00:00+05:00", "technician": {"id": "T1"},
            "location": {"store_id": "S1", "region": "R1"}, "labor": {"minutes": 10}
        }, "events.jsonl", 1)
        self.assertEqual("OPENED", event.event_type)
        self.assertEqual("2026-01-01T00:00:00+00:00", event.event_timestamp_utc.isoformat())
        self.assertEqual("T1", event.technician_id)

    def test_keeps_latest_correction_and_rejects_bad_record(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "work_order_events_01.jsonl"
            p.write_text(
                '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"OPENED","event_timestamp":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}\n'
                '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"CLOSED","event_timestamp":"2026-01-02T00:00:00Z","updated_at":"2026-01-02T00:00:00Z"}\n'
                'not-json\n', encoding="utf-8")
            accepted, current, rejects, superseded = process_files([p], {}, {})
        self.assertEqual(2, len(accepted))
        self.assertEqual(1, len(current))
        self.assertEqual("CLOSED", current[0].event_type)
        self.assertEqual(1, len(rejects))
        self.assertEqual(1, superseded)

    def test_reference_ids_are_validated(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "work_order_events_01.jsonl"
            p.write_text(
                '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"OPENED","event_timestamp":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","location":{"store_id":"BAD"}}\n',
                encoding="utf-8")
            _, current, rejects, _ = process_files([p], {"S1": {}}, {"T1": {}})
        self.assertEqual([], current)
        self.assertIn("unknown store_id", rejects[0].reason)
