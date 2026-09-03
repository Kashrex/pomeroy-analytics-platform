from pathlib import Path

from src.processor import process_jsonl_files


def test_keeps_newest_correction_and_rejects_bad_json(tmp_path: Path):
    source = tmp_path / 'work_order_events_01.jsonl'
    source.write_text(
        '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"OPENED","event_timestamp":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}\n'
        '{"event_id":"E1","work_order_id":"W1","client_id":"C1","event_type":"CLOSED","event_timestamp":"2026-01-02T00:00:00Z","updated_at":"2026-01-02T00:00:00Z"}\nnot-json\n',
        encoding='utf-8'
    )
    accepted, events, rejects, superseded = process_jsonl_files([source])
    assert len(accepted) == 2
    assert len(events) == 1
    assert events[0].event_type == 'CLOSED'
    assert len(rejects) == 1
    assert superseded == 1
