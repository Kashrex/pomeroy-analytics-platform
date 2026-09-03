from pathlib import Path
import tempfile
import unittest

from src.processor import process_jsonl_files


class ProcessorTests(unittest.TestCase):
    def test_keeps_newest_correction_and_rejects_bad_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / 'work_order_events_01.jsonl'
            source.write_text(
                '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"OPENED","event_timestamp":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}\n'
                '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"CLOSED","event_timestamp":"2026-01-02T00:00:00Z","updated_at":"2026-01-02T00:00:00Z"}\nnot-json\n',
                encoding='utf-8'
            )
            accepted, events, rejects, superseded = process_jsonl_files([source])
        self.assertEqual(2, len(accepted))
        self.assertEqual(1, len(events))
        self.assertEqual('CLOSED', events[0].event_type)
        self.assertEqual(1, len(rejects))
        self.assertEqual(1, superseded)
