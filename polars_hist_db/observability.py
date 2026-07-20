"""Uploader-side metrics for polars_hist_db loaders.

Exposed as OpenTelemetry counters that a host service can export to
Prometheus via any OTLP-compatible exporter. If the `opentelemetry`
package is not installed, all recorders are no-ops — the library stays
usable without the optional observability dependency.

The concrete signal wired today is `record_uploader_batch`, called from
`JetStreamInputSource.next_df` after the delta filter runs. It emits:

* `uploader_rows_received_total{table, subject}` — messages arriving on
  the wire (before any filtering).
* `uploader_rows_written_total{table, subject}` — rows surviving the
  delta / audit / dedupe filter, i.e. rows that WILL be written to the
  target table if commit succeeds.
* `uploader_rows_dropped_total{table, subject}` — derived: received minus
  written. Emitted separately so an alert can key on it directly.

Deployment-specific alerting belongs outside this reusable library. A canonical
"uploader stuck" signal is `rate(received) > 0 AND rate(written) == 0`.
"""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Any

LOGGER = logging.getLogger(__name__)

_COUNTERS: dict[str, Any] = {}
_HISTOGRAMS: dict[str, Any] = {}


def _get_counter(name: str, description: str) -> Any | None:
    """Return an OTel Counter, or None if OTel isn't installed. Cached
    per-name so callers don't pay the get_meter/create_counter cost each
    batch."""
    if name in _COUNTERS:
        return _COUNTERS[name]
    try:
        from opentelemetry import metrics
    except ImportError:
        _COUNTERS[name] = None
        return None
    try:
        meter = metrics.get_meter("polars_hist_db.uploader")
        counter = meter.create_counter(name, description=description)
    except Exception:  # noqa: BLE001 - metrics are best-effort
        _COUNTERS[name] = None
        return None
    _COUNTERS[name] = counter
    return counter


def clear_counters() -> None:
    """Reset the counter cache (used by tests)."""
    _COUNTERS.clear()
    _HISTOGRAMS.clear()


def _get_histogram(name: str, description: str, unit: str) -> Any | None:
    if name in _HISTOGRAMS:
        return _HISTOGRAMS[name]
    try:
        from opentelemetry import metrics

        histogram = metrics.get_meter("polars_hist_db.overrides").create_histogram(
            name, description=description, unit=unit
        )
    except Exception:  # noqa: BLE001 - telemetry is best-effort
        histogram = None
    _HISTOGRAMS[name] = histogram
    return histogram


def arrow_override_sync_span(
    *, backend: str, pending_rows: int
) -> AbstractContextManager[Any]:
    try:
        from opentelemetry import trace

        return trace.get_tracer("polars_hist_db.overrides").start_as_current_span(
            "override.arrow.sync",
            attributes={
                "override.backend": backend,
                "override.pending_rows": pending_rows,
            },
        )
    except Exception:  # noqa: BLE001 - telemetry is best-effort
        return nullcontext(None)


def record_arrow_override_sync(
    *,
    backend: str,
    outcome: str,
    duration_seconds: float,
    pending: int,
    accepted: int,
    duplicates: int,
    projection_rows: int,
    conflicts: int,
) -> None:
    attrs = {"backend": backend, "outcome": outcome}
    measurements = (
        (
            "override_arrow_sync_duration_seconds",
            duration_seconds,
            "Arrow override synchronization latency.",
            "s",
        ),
        (
            "override_arrow_pending_operations",
            pending,
            "Operations presented to an Arrow override synchronization.",
            "{operation}",
        ),
        (
            "override_arrow_projection_rows",
            projection_rows,
            "Projection rows returned by an Arrow override synchronization.",
            "{row}",
        ),
    )
    counters = (
        ("override_arrow_operations_accepted_total", accepted),
        ("override_arrow_operations_duplicate_total", duplicates),
        ("override_arrow_conflicts_total", conflicts),
    )
    try:
        for name, value, description, unit in measurements:
            instrument = _get_histogram(name, description, unit)
            if instrument is not None:
                instrument.record(value, attributes=attrs)
        for name, value in counters:
            instrument = _get_counter(name, "Arrow override synchronization outcomes.")
            if instrument is not None:
                instrument.add(value, attributes=attrs)
    except Exception:  # noqa: BLE001, S110 - telemetry is best-effort
        pass


def record_uploader_batch(
    *, table: str, subject: str, received: int, written: int
) -> None:
    """Record one uploader batch. `table` is the fully-qualified
    `<schema>.<name>` target table; `subject` is the NATS subject the
    batch came from."""
    if received < 0 or written < 0:
        return
    attrs = {"table": table, "subject": subject}
    received_c = _get_counter(
        "uploader_rows_received_total",
        "Rows received by the polars_hist_db uploader before delta filtering.",
    )
    written_c = _get_counter(
        "uploader_rows_written_total",
        "Rows surviving the delta filter, staged for write to the target table.",
    )
    dropped_c = _get_counter(
        "uploader_rows_dropped_total",
        "Rows dropped by the delta filter (received minus written).",
    )
    try:
        if received_c is not None:
            received_c.add(received, attributes=attrs)
        if written_c is not None:
            written_c.add(written, attributes=attrs)
        if dropped_c is not None:
            dropped_c.add(max(0, received - written), attributes=attrs)
    except Exception:  # noqa: BLE001, S110 - metrics are best-effort
        pass


def record_database_type_contract(
    *,
    backend: str,
    operation: str,
    expected_type: str,
    actual_type: str,
    forced: bool,
    outcome: str,
) -> None:
    """Record a database-boundary type violation or forced conversion."""
    attrs = {
        "backend": backend,
        "operation": operation,
        "expected_type": expected_type,
        "actual_type": actual_type,
        "forced": forced,
        "outcome": outcome,
    }
    counter_name = (
        "database_type_coercions_total"
        if forced
        else "database_type_contract_violations_total"
    )
    counter = _get_counter(
        counter_name,
        "Database-boundary type contract outcomes.",
    )
    try:
        if counter is not None:
            counter.add(1, attributes=attrs)
        from opentelemetry import trace

        span = trace.get_current_span()
        if span is not None:
            span.add_event("database.type_contract", attrs)
    except Exception:  # noqa: BLE001, S110 - telemetry is best-effort
        pass
