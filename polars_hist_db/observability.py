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

Alerts consuming these counters live in `private_consumer-iac` (PrometheusRule).
The canonical "uploader stuck" shape is `rate(received) > 0 AND
rate(written) == 0`.
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

_COUNTERS: dict[str, Any] = {}


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
