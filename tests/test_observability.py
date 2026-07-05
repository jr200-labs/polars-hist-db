"""Tests for polars_hist_db.observability.record_uploader_batch."""

from types import SimpleNamespace

import pytest

from polars_hist_db import observability


@pytest.fixture(autouse=True)
def _reset_counter_cache():
    observability.clear_counters()
    yield
    observability.clear_counters()


class _FakeCounter:
    def __init__(self):
        self.calls = []

    def add(self, value, attributes=None):
        self.calls.append((value, dict(attributes or {})))


class _FakeMeter:
    def __init__(self):
        self.counters: dict[str, _FakeCounter] = {}

    def create_counter(self, name, description=""):
        counter = self.counters.setdefault(name, _FakeCounter())
        return counter


def _install_fake_otel(monkeypatch):
    meter = _FakeMeter()

    class _MetricsShim:
        @staticmethod
        def get_meter(_name):
            return meter

    fake_otel = SimpleNamespace(metrics=_MetricsShim())
    monkeypatch.setitem(__import__("sys").modules, "opentelemetry", fake_otel)
    return meter


def test_record_uploader_batch_emits_three_counters(monkeypatch):
    meter = _install_fake_otel(monkeypatch)

    observability.record_uploader_batch(
        table="fakedata.records",
        subject="uploader.records",
        received=50,
        written=12,
    )

    assert set(meter.counters) == {
        "uploader_rows_received_total",
        "uploader_rows_written_total",
        "uploader_rows_dropped_total",
    }
    assert meter.counters["uploader_rows_received_total"].calls == [
        (50, {"table": "fakedata.records", "subject": "uploader.records"})
    ]
    assert meter.counters["uploader_rows_written_total"].calls == [
        (12, {"table": "fakedata.records", "subject": "uploader.records"})
    ]
    assert meter.counters["uploader_rows_dropped_total"].calls == [
        (38, {"table": "fakedata.records", "subject": "uploader.records"})
    ]


def test_record_uploader_batch_all_written(monkeypatch):
    meter = _install_fake_otel(monkeypatch)

    observability.record_uploader_batch(
        table="fakedata.records",
        subject="uploader.records",
        received=10,
        written=10,
    )

    assert meter.counters["uploader_rows_dropped_total"].calls == [
        (0, {"table": "fakedata.records", "subject": "uploader.records"})
    ]


def test_record_uploader_batch_stuck_scenario(monkeypatch):
    """The exact shape the 'uploader stuck' alert keys on: received > 0,
    written == 0, dropped == received."""
    meter = _install_fake_otel(monkeypatch)

    observability.record_uploader_batch(
        table="fakedata.records",
        subject="uploader.records",
        received=50,
        written=0,
    )

    assert meter.counters["uploader_rows_written_total"].calls == [
        (0, {"table": "fakedata.records", "subject": "uploader.records"})
    ]
    assert meter.counters["uploader_rows_dropped_total"].calls == [
        (50, {"table": "fakedata.records", "subject": "uploader.records"})
    ]


def test_record_uploader_batch_noop_without_otel(monkeypatch):
    """When opentelemetry is not importable, the recorder silently does
    nothing — the library remains usable without the optional dep."""
    import sys

    monkeypatch.setitem(sys.modules, "opentelemetry", None)

    observability.record_uploader_batch(
        table="fakedata.records",
        subject="uploader.records",
        received=50,
        written=12,
    )


def test_record_uploader_batch_ignores_negative_counts(monkeypatch):
    meter = _install_fake_otel(monkeypatch)

    observability.record_uploader_batch(
        table="fakedata.records", subject="uploader.records", received=-1, written=0
    )

    assert meter.counters == {}
