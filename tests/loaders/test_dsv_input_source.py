from datetime import datetime, timezone

import pytest

from polars_hist_db.loaders.dsv_input_source import _make_dsv_file_commit_fn


@pytest.mark.asyncio
async def test_dsv_file_commit_succeeds_when_no_tables_changed(monkeypatch):
    def fail_audit_ops(schema):
        raise AssertionError("no audit entry should be written without changes")

    monkeypatch.setattr(
        "polars_hist_db.loaders.dsv_input_source.AuditOps", fail_audit_ops
    )

    commit_fn = _make_dsv_file_commit_fn(
        "/tmp/source.csv",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert await commit_fn(object(), []) is True
