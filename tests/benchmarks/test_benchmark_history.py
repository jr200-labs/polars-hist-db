import json

from benchmarks.record_history import record_history


def test_record_history_replaces_rerun_for_same_commit(tmp_path):
    results_path = tmp_path / "results.json"
    history_path = tmp_path / "history.json"
    results_path.write_text(
        json.dumps([{"name": "partitioning", "unit": "seconds", "value": 1.2}])
    )

    first = record_history(
        results_path,
        history_path,
        commit="abc123",
        repository="jr200-labs/polars-hist-db",
        runner="Linux/X64",
        timestamp="2026-07-18T00:00:00+00:00",
    )
    results_path.write_text(
        json.dumps([{"name": "partitioning", "unit": "seconds", "value": 1.1}])
    )
    second = record_history(
        results_path,
        history_path,
        commit="abc123",
        repository="jr200-labs/polars-hist-db",
        runner="Linux/X64",
        timestamp="2026-07-18T00:01:00+00:00",
    )

    assert len(first["entries"]) == 1
    assert len(second["entries"]) == 1
    assert second["entries"][0]["benchmarks"][0]["value"] == 1.1
    assert json.loads(history_path.read_text()) == second
