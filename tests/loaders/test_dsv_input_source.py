from datetime import datetime, timezone

import pytest

from polars_hist_db.loaders.dsv_input_source import _make_dsv_file_finalizer


@pytest.mark.asyncio
async def test_dsv_file_commit_succeeds_when_no_tables_changed(monkeypatch):
    def fail_audit_ops(schema):
        raise AssertionError("no audit entry should be written without changes")

    monkeypatch.setattr(
        "polars_hist_db.loaders.dsv_input_source.AuditOps", fail_audit_ops
    )

    finalizer = _make_dsv_file_finalizer(
        "/tmp/source.csv",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert finalizer.write_audit_before_commit(object(), []) is True
