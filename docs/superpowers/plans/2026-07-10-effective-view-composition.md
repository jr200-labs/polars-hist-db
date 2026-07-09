# Effective View Composition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement deterministic effective-row composition for canonical cargo rows plus personal override operations.

**Architecture:** Add a focused `whengas_api.overrides` projection module that is independent of XTDB SQL details. The module receives canonical rows, replayable override operation records, read scope, and field specs, then returns effective rows and structured metadata. Later API tasks wire this pure projection into NATS request/reply, KV refreshes, exports, and permission checks.

**Tech Stack:** Python 3.12+, dataclasses, Polars, pytest, existing `whengas-api` NATS/API query patterns, `polars-hist-db` override operation semantics.

---

## File Structure

- Create: `app-team/whengas-api/src/whengas_api/overrides/__init__.py`
  Public imports for effective-view composition helpers.
- Create: `app-team/whengas-api/src/whengas_api/overrides/types.py`
  Read scopes, field specs, typed values, operation records, metadata records.
- Create: `app-team/whengas-api/src/whengas_api/overrides/cargo_fields.py`
  V1 cargo field registry and canonical/finality helpers.
- Create: `app-team/whengas-api/src/whengas_api/overrides/effective.py`
  Pure operation replay and canonical-row overlay logic.
- Create: `app-team/whengas-api/tests/test_effective_overrides.py`
  Tests for scope defaults, replay, overlay, finality, source match, drift, and metadata.
- Modify later: `app-team/whengas-api/src/whengas_api/db/cargos.py`
  Wire composition into cargo reads in WG-227, after the pure projection is implemented.
- Modify later: `app-team/whengas-api/src/whengas_api/register_tasks.py`
  Route NATS request/reply and KV refreshes through the effective read path in WG-227.

## Task 1: Public Types And Scope Defaults

**Files:**
- Create: `app-team/whengas-api/src/whengas_api/overrides/__init__.py`
- Create: `app-team/whengas-api/src/whengas_api/overrides/types.py`
- Test: `app-team/whengas-api/tests/test_effective_overrides.py`

- [ ] **Step 1: Write failing tests for read scope defaults**

Create `app-team/whengas-api/tests/test_effective_overrides.py`:

```python
from datetime import datetime, timezone

from whengas_api.overrides.types import (
    EffectiveViewRequest,
    OverrideReadScope,
    default_effective_scope,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 9, hour, tzinfo=timezone.utc)


def test_default_effective_scope_uses_requesting_user():
    scope = default_effective_scope("user-1")

    assert scope.mode == "effective"
    assert scope.owner_user_id == "user-1"


def test_effective_view_request_requires_timezone_aware_asof():
    request = EffectiveViewRequest(
        asof_utc=_utc(13),
        requester_user_id="user-1",
        scope=OverrideReadScope(mode="effective", owner_user_id="user-1"),
    )

    assert request.asof_utc == _utc(13)
    assert request.include_metadata is True
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: import failure for `whengas_api.overrides`.

- [ ] **Step 3: Add public type definitions**

Create `app-team/whengas-api/src/whengas_api/overrides/types.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence
from typing import Any, Literal

OverrideReadMode = Literal["effective", "canonical", "owner_layer"]
OverrideOperationType = Literal["set", "close", "system_close", "audit_correction"]
OverrideValueType = Literal["string", "enum", "datetime", "decimal", "location"]
OverrideFieldState = Literal["active", "source_drift", "source_matched", "closed"]


@dataclass(frozen=True)
class OverrideReadScope:
    mode: OverrideReadMode
    owner_user_id: str | None = None


def default_effective_scope(requester_user_id: str) -> OverrideReadScope:
    return OverrideReadScope(mode="effective", owner_user_id=requester_user_id)


@dataclass(frozen=True)
class EffectiveViewRequest:
    asof_utc: datetime
    requester_user_id: str
    scope: OverrideReadScope
    include_metadata: bool = True

    def __post_init__(self) -> None:
        if self.asof_utc.tzinfo is None or self.asof_utc.utcoffset() is None:
            raise ValueError("asof_utc must be timezone-aware")


@dataclass(frozen=True)
class OverrideTypedValue:
    value_type: OverrideValueType
    value_json: dict[str, Any]
    unit: str | None = None


@dataclass(frozen=True)
class OverrideFieldSpec:
    field_path: str
    value_type: OverrideValueType
    unit: str | None = None
    enum_values: frozenset[str] | None = None


@dataclass(frozen=True)
class OverrideOperation:
    operation_id: str
    change_set_id: str
    owner_user_id: str
    actor_user_id: str
    feed_id: str
    entity_id: str
    field_path: str
    operation_type: OverrideOperationType
    value: OverrideTypedValue | None
    observed_canonical_value_json: dict[str, Any] | None
    created_against_stale_source: bool
    valid_from: datetime
    valid_to: datetime | None = None
    reason: str | None = None
    comment: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FieldOverrideMetadata:
    field_path: str
    state: OverrideFieldState
    canonical_value_json: dict[str, Any] | None
    override_value_json: dict[str, Any] | None
    owner_user_id: str
    actor_user_id: str
    operation_id: str
    change_set_id: str
    valid_from: datetime
    valid_to: datetime | None
    created_against_stale_source: bool
    source_drift: bool
    reason: str | None
    comment: str | None
```

Create `app-team/whengas-api/src/whengas_api/overrides/__init__.py`:

```python
from .types import (
    EffectiveViewRequest,
    FieldOverrideMetadata,
    OverrideFieldSpec,
    OverrideOperation,
    OverrideReadScope,
    OverrideTypedValue,
    default_effective_scope,
)

__all__ = [
    "EffectiveViewRequest",
    "FieldOverrideMetadata",
    "OverrideFieldSpec",
    "OverrideOperation",
    "OverrideReadScope",
    "OverrideTypedValue",
    "default_effective_scope",
]
```

- [ ] **Step 4: Run the tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/whengas_api/overrides tests/test_effective_overrides.py
git commit -m "feat(wg-223): add effective override read types"
```

## Task 2: Cargo Field Registry And Canonical Helpers

**Files:**
- Create: `app-team/whengas-api/src/whengas_api/overrides/cargo_fields.py`
- Modify: `app-team/whengas-api/src/whengas_api/overrides/__init__.py`
- Test: `app-team/whengas-api/tests/test_effective_overrides.py`

- [ ] **Step 1: Add failing cargo field and finality tests**

Append to `app-team/whengas-api/tests/test_effective_overrides.py`:

```python
from whengas_api.overrides.cargo_fields import (
    CARGO_OVERRIDE_FIELDS,
    canonical_value_json,
    is_canonical_final,
)


def test_cargo_field_registry_contains_v1_fields():
    assert set(CARGO_OVERRIDE_FIELDS) == {
        "destination_date",
        "destination_location",
        "classification",
        "status",
        "origin_date",
        "origin_location",
        "cargo_mcm",
    }


def test_canonical_value_json_wraps_present_field_value():
    row = {"cargo_id": "cargo-1", "classification": "expected"}

    assert canonical_value_json(row, "classification") == {"value": "expected"}


def test_canonical_finality_detects_actual_delivered_and_deleted_rows():
    assert is_canonical_final({"status": "delivered"}) is True
    assert is_canonical_final({"classification": "actual"}) is True
    assert is_canonical_final({"status": "deleted"}) is True
    assert is_canonical_final({"status": "expected"}) is False
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: import failure for `whengas_api.overrides.cargo_fields`.

- [ ] **Step 3: Add cargo field registry and helpers**

Create `app-team/whengas-api/src/whengas_api/overrides/cargo_fields.py`:

```python
from __future__ import annotations

from typing import Any

from .types import OverrideFieldSpec

CARGO_CLASSIFICATIONS = frozenset(
    {"actual", "expected", "probable", "possible", "available"}
)
CARGO_STATUSES = frozenset(
    {"actual", "delivered", "expected", "probable", "possible", "cancelled", "deleted"}
)

CARGO_OVERRIDE_FIELDS: dict[str, OverrideFieldSpec] = {
    "destination_date": OverrideFieldSpec("destination_date", "datetime"),
    "destination_location": OverrideFieldSpec("destination_location", "location"),
    "classification": OverrideFieldSpec(
        "classification", "enum", enum_values=CARGO_CLASSIFICATIONS
    ),
    "status": OverrideFieldSpec("status", "enum", enum_values=CARGO_STATUSES),
    "origin_date": OverrideFieldSpec("origin_date", "datetime"),
    "origin_location": OverrideFieldSpec("origin_location", "location"),
    "cargo_mcm": OverrideFieldSpec("cargo_mcm", "decimal", unit="mcm"),
}


def canonical_value_json(row: dict[str, Any], field_path: str) -> dict[str, Any] | None:
    if field_path not in row:
        return None
    return {"value": row[field_path]}


def is_canonical_final(row: dict[str, Any]) -> bool:
    status = str(row.get("status", "")).lower()
    classification = str(row.get("classification", "")).lower()
    return status in {"delivered", "deleted", "actual"} or classification == "actual"
```

Modify `app-team/whengas-api/src/whengas_api/overrides/__init__.py`:

```python
from .cargo_fields import CARGO_OVERRIDE_FIELDS, canonical_value_json, is_canonical_final
from .types import (
    EffectiveViewRequest,
    FieldOverrideMetadata,
    OverrideFieldSpec,
    OverrideOperation,
    OverrideReadScope,
    OverrideTypedValue,
    default_effective_scope,
)

__all__ = [
    "CARGO_OVERRIDE_FIELDS",
    "EffectiveViewRequest",
    "FieldOverrideMetadata",
    "OverrideFieldSpec",
    "OverrideOperation",
    "OverrideReadScope",
    "OverrideTypedValue",
    "canonical_value_json",
    "default_effective_scope",
    "is_canonical_final",
]
```

- [ ] **Step 4: Run the tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/whengas_api/overrides tests/test_effective_overrides.py
git commit -m "feat(wg-223): add cargo effective-view field helpers"
```

## Task 3: Operation Replay

**Files:**
- Create: `app-team/whengas-api/src/whengas_api/overrides/effective.py`
- Modify: `app-team/whengas-api/src/whengas_api/overrides/__init__.py`
- Test: `app-team/whengas-api/tests/test_effective_overrides.py`

- [ ] **Step 1: Add failing replay tests**

Append to `app-team/whengas-api/tests/test_effective_overrides.py`:

```python
from whengas_api.overrides.effective import active_operations_as_of
from whengas_api.overrides.types import OverrideOperation, OverrideTypedValue


def _set_op(
    *,
    field_path: str = "classification",
    value: str = "probable",
    valid_from: datetime | None = None,
    operation_id: str = "op-set",
) -> OverrideOperation:
    return OverrideOperation(
        operation_id=operation_id,
        change_set_id="chg-1",
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="fakedata.cargos",
        entity_id="cargo-1",
        field_path=field_path,
        operation_type="set",
        value=OverrideTypedValue("enum", {"value": value}),
        observed_canonical_value_json={"value": "expected"},
        created_against_stale_source=False,
        valid_from=valid_from or _utc(13),
    )


def _close_op(*, valid_from: datetime, operation_id: str = "op-close") -> OverrideOperation:
    return OverrideOperation(
        operation_id=operation_id,
        change_set_id="chg-1",
        owner_user_id="user-1",
        actor_user_id="system",
        feed_id="fakedata.cargos",
        entity_id="cargo-1",
        field_path="classification",
        operation_type="system_close",
        value=None,
        observed_canonical_value_json=None,
        created_against_stale_source=False,
        valid_from=valid_from,
        valid_to=valid_from,
        reason="source_matched_override",
    )


def test_active_operations_replays_set_until_close():
    operations = [_set_op(valid_from=_utc(13)), _close_op(valid_from=_utc(15))]

    assert active_operations_as_of(operations, _utc(14))["classification"].value == (
        OverrideTypedValue("enum", {"value": "probable"})
    )
    assert active_operations_as_of(operations, _utc(16)) == {}


def test_active_operations_orders_close_before_set_at_same_timestamp():
    operations = [
        _set_op(value="probable", valid_from=_utc(13), operation_id="op-1"),
        _set_op(value="possible", valid_from=_utc(15), operation_id="op-3"),
        _close_op(valid_from=_utc(15), operation_id="op-2"),
    ]

    active = active_operations_as_of(operations, _utc(16))

    assert active["classification"].value == OverrideTypedValue(
        "enum", {"value": "possible"}
    )
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: import failure for `whengas_api.overrides.effective`.

- [ ] **Step 3: Add operation replay**

Create `app-team/whengas-api/src/whengas_api/overrides/effective.py`:

```python
from __future__ import annotations

from datetime import datetime

from .types import OverrideOperation


def _operation_priority(operation: OverrideOperation) -> int:
    if operation.operation_type in {"close", "system_close"}:
        return 0
    if operation.operation_type == "set":
        return 1
    return 2


def active_operations_as_of(
    operations: Sequence[OverrideOperation],
    asof_utc: datetime,
) -> dict[str, OverrideOperation]:
    if asof_utc.tzinfo is None or asof_utc.utcoffset() is None:
        raise ValueError("asof_utc must be timezone-aware")

    active: dict[str, OverrideOperation] = {}
    replayable = [operation for operation in operations if operation.valid_from <= asof_utc]
    for operation in sorted(
        replayable,
        key=lambda item: (
            item.valid_from,
            _operation_priority(item),
            item.change_set_id,
            item.operation_id,
        ),
    ):
        if operation.operation_type == "set":
            active[operation.field_path] = operation
        elif operation.operation_type in {"close", "system_close"}:
            active.pop(operation.field_path, None)
    return active
```

Modify `app-team/whengas-api/src/whengas_api/overrides/__init__.py`:

```python
from .cargo_fields import CARGO_OVERRIDE_FIELDS, canonical_value_json, is_canonical_final
from .effective import active_operations_as_of
from .types import (
    EffectiveViewRequest,
    FieldOverrideMetadata,
    OverrideFieldSpec,
    OverrideOperation,
    OverrideReadScope,
    OverrideTypedValue,
    default_effective_scope,
)

__all__ = [
    "CARGO_OVERRIDE_FIELDS",
    "EffectiveViewRequest",
    "FieldOverrideMetadata",
    "OverrideFieldSpec",
    "OverrideOperation",
    "OverrideReadScope",
    "OverrideTypedValue",
    "active_operations_as_of",
    "canonical_value_json",
    "default_effective_scope",
    "is_canonical_final",
]
```

- [ ] **Step 4: Run the tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/whengas_api/overrides tests/test_effective_overrides.py
git commit -m "feat(wg-223): replay override operations for effective views"
```

## Task 4: Effective Row Overlay And Metadata

**Files:**
- Modify: `app-team/whengas-api/src/whengas_api/overrides/effective.py`
- Test: `app-team/whengas-api/tests/test_effective_overrides.py`

- [ ] **Step 1: Add failing overlay and metadata tests**

Append to `app-team/whengas-api/tests/test_effective_overrides.py`:

```python
from whengas_api.overrides.cargo_fields import CARGO_OVERRIDE_FIELDS
from whengas_api.overrides.effective import compose_effective_rows


def test_compose_effective_rows_overlays_active_personal_value_with_metadata():
    rows = [{"cargo_id": "cargo-1", "classification": "expected", "status": "expected"}]
    request = EffectiveViewRequest(
        asof_utc=_utc(14),
        requester_user_id="user-1",
        scope=default_effective_scope("user-1"),
    )

    result = compose_effective_rows(rows, [_set_op(valid_from=_utc(13))], request, CARGO_OVERRIDE_FIELDS)

    assert result[0]["classification"] == "probable"
    metadata = result[0]["_override_metadata"]
    assert metadata["scope"] == "effective"
    assert metadata["owner_user_id"] == "user-1"
    assert metadata["fields"]["classification"]["state"] == "active"
    assert metadata["fields"]["classification"]["canonical_value_json"] == {"value": "expected"}
    assert metadata["fields"]["classification"]["override_value_json"] == {"value": "probable"}


def test_canonical_scope_does_not_overlay_private_values():
    rows = [{"cargo_id": "cargo-1", "classification": "expected", "status": "expected"}]
    request = EffectiveViewRequest(
        asof_utc=_utc(14),
        requester_user_id="user-1",
        scope=OverrideReadScope(mode="canonical"),
    )

    result = compose_effective_rows(rows, [_set_op(valid_from=_utc(13))], request, CARGO_OVERRIDE_FIELDS)

    assert result[0]["classification"] == "expected"
    assert result[0]["_override_metadata"]["scope"] == "canonical"
    assert result[0]["_override_metadata"]["fields"] == {}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: import failure for `compose_effective_rows`.

- [ ] **Step 3: Add effective overlay implementation**

Append to `app-team/whengas-api/src/whengas_api/overrides/effective.py`:

```python
from typing import Any, Mapping, Sequence

from .cargo_fields import canonical_value_json, is_canonical_final
from .types import EffectiveViewRequest, OverrideFieldSpec


def _metadata_for_row(request: EffectiveViewRequest) -> dict[str, Any]:
    return {
        "scope": request.scope.mode,
        "owner_user_id": request.scope.owner_user_id,
        "asof_utc": request.asof_utc.isoformat(),
        "fields": {},
    }


def _value_from_typed_json(operation: OverrideOperation) -> Any:
    if operation.value is None:
        return None
    return operation.value.value_json.get("value")


def compose_effective_rows(
    canonical_rows: Sequence[dict[str, Any]],
    override_operations: Sequence[OverrideOperation],
    request: EffectiveViewRequest,
    field_specs: Mapping[str, OverrideFieldSpec],
) -> list[dict[str, Any]]:
    if request.scope.mode == "canonical":
        return [
            {**row, "_override_metadata": _metadata_for_row(request)}
            for row in canonical_rows
        ]

    owner_user_id = request.scope.owner_user_id or request.requester_user_id
    result: list[dict[str, Any]] = []
    for row in canonical_rows:
        entity_id = str(row["cargo_id"])
        row_operations = [
            operation
            for operation in override_operations
            if operation.owner_user_id == owner_user_id and operation.entity_id == entity_id
        ]
        active = active_operations_as_of(row_operations, request.asof_utc)
        effective = dict(row)
        metadata = _metadata_for_row(request)
        if not is_canonical_final(row):
            for field_path, operation in active.items():
                if field_path not in field_specs:
                    raise ValueError(f"field {field_path!r} is not overrideable")
                canonical_json = canonical_value_json(row, field_path)
                override_json = operation.value.value_json if operation.value else None
                source_matched = canonical_json == override_json
                source_drift = (
                    canonical_json != operation.observed_canonical_value_json
                    and not source_matched
                )
                if not source_matched:
                    effective[field_path] = _value_from_typed_json(operation)
                state = "source_matched" if source_matched else (
                    "source_drift" if source_drift else "active"
                )
                metadata["fields"][field_path] = {
                    "state": state,
                    "canonical_value_json": canonical_json,
                    "override_value_json": override_json,
                    "owner_user_id": operation.owner_user_id,
                    "actor_user_id": operation.actor_user_id,
                    "operation_id": operation.operation_id,
                    "change_set_id": operation.change_set_id,
                    "valid_from": operation.valid_from.isoformat(),
                    "valid_to": operation.valid_to.isoformat()
                    if operation.valid_to is not None
                    else None,
                    "created_against_stale_source": operation.created_against_stale_source,
                    "source_drift": source_drift,
                    "reason": operation.reason,
                    "comment": operation.comment,
                }
        effective["_override_metadata"] = metadata
        result.append(effective)
    return result
```

- [ ] **Step 4: Run the tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/whengas_api/overrides/effective.py tests/test_effective_overrides.py
git commit -m "feat(wg-223): compose effective rows with metadata"
```

## Task 5: Source Match, Drift, And Finality

**Files:**
- Modify: `app-team/whengas-api/src/whengas_api/overrides/effective.py`
- Test: `app-team/whengas-api/tests/test_effective_overrides.py`

- [ ] **Step 1: Add failing source-match, drift, and finality tests**

Append to `app-team/whengas-api/tests/test_effective_overrides.py`:

```python
def test_source_match_uses_canonical_value_and_marks_metadata():
    rows = [{"cargo_id": "cargo-1", "classification": "probable", "status": "expected"}]
    request = EffectiveViewRequest(
        asof_utc=_utc(14),
        requester_user_id="user-1",
        scope=default_effective_scope("user-1"),
    )

    result = compose_effective_rows(rows, [_set_op(valid_from=_utc(13))], request, CARGO_OVERRIDE_FIELDS)

    assert result[0]["classification"] == "probable"
    assert result[0]["_override_metadata"]["fields"]["classification"]["state"] == "source_matched"


def test_source_drift_keeps_override_active_and_marks_metadata():
    rows = [{"cargo_id": "cargo-1", "classification": "available", "status": "expected"}]
    request = EffectiveViewRequest(
        asof_utc=_utc(14),
        requester_user_id="user-1",
        scope=default_effective_scope("user-1"),
    )

    result = compose_effective_rows(rows, [_set_op(valid_from=_utc(13))], request, CARGO_OVERRIDE_FIELDS)

    assert result[0]["classification"] == "probable"
    metadata = result[0]["_override_metadata"]["fields"]["classification"]
    assert metadata["state"] == "source_drift"
    assert metadata["source_drift"] is True


def test_final_canonical_row_defensively_suppresses_active_override():
    rows = [{"cargo_id": "cargo-1", "classification": "expected", "status": "delivered"}]
    request = EffectiveViewRequest(
        asof_utc=_utc(14),
        requester_user_id="user-1",
        scope=default_effective_scope("user-1"),
    )

    result = compose_effective_rows(rows, [_set_op(valid_from=_utc(13))], request, CARGO_OVERRIDE_FIELDS)

    assert result[0]["classification"] == "expected"
    assert result[0]["_override_metadata"]["fields"] == {}
```

- [ ] **Step 2: Run the tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py -q
```

Expected: tests should pass if Task 4 implemented source match, drift, and finality exactly. If any test fails, update only `compose_effective_rows` to match the contract.

- [ ] **Step 3: Commit**

Run:

```bash
git add src/whengas_api/overrides/effective.py tests/test_effective_overrides.py
git commit -m "test(wg-223): cover effective override edge cases"
```

## Task 6: Cargo Query Contract Tests

**Files:**
- Modify: `app-team/whengas-api/tests/test_xtdb_queries.py`
- Modify later: `app-team/whengas-api/src/whengas_api/db/cargos.py`

- [ ] **Step 1: Add skipped contract tests for WG-227 wiring**

Append to `app-team/whengas-api/tests/test_xtdb_queries.py`:

```python
import pytest


@pytest.mark.skip(reason="WG-227 wires effective override projection into cargo reads")
def test_cargo_query_uses_same_asof_for_canonical_and_override_operations():
    """Future wiring test:
    - canonical cargo query uses FOR VALID_TIME AS OF request.asof_utc
    - override operation query can see operation-log facts up to request.asof_utc
    - projection receives the same asof_utc for both inputs
    """


@pytest.mark.skip(reason="WG-227 wires canonical-only scope into cargo reads")
def test_cargo_query_supports_canonical_only_scope_without_private_values():
    """Future wiring test:
    - canonical scope returns source rows without overlay
    - response schema remains compatible with effective rows
    - private override metadata is not exposed
    """
```

- [ ] **Step 2: Run API tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py tests/test_xtdb_queries.py -q
```

Expected: effective override tests pass; two future wiring tests are skipped.

- [ ] **Step 3: Commit**

Run:

```bash
git add tests/test_xtdb_queries.py
git commit -m "test(wg-223): document future cargo query wiring contracts"
```

## Task 7: Final Verification And PR

**Files:**
- All files changed by prior tasks.

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests/test_effective_overrides.py tests/test_xtdb_queries.py -q
```

Expected: all non-skipped tests pass.

- [ ] **Step 2: Run broader API checks**

Run:

```bash
cd app-team/whengas-api
uv run pytest tests -q
uv run ruff check src tests
uv run mypy
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 3: Push branch**

Run:

```bash
cd app-team/whengas-api
git status --short
git push -u origin feat/wg-223-effective-view-composition
```

Expected: clean worktree before push and branch published to GitHub.

- [ ] **Step 4: Open PR**

Run:

```bash
cd app-team/whengas-api
gh pr create \
  --repo whengas/whengas-api \
  --base main \
  --head feat/wg-223-effective-view-composition \
  --title "feat(wg-223): add effective view composition" \
  --body "## Summary
- add pure effective-row composition for canonical rows plus personal override operations
- add read scopes, metadata, source-match, drift, and finality behavior
- add focused tests for replay and overlay semantics

## Verification
- uv run pytest tests/test_effective_overrides.py tests/test_xtdb_queries.py -q
- uv run pytest tests -q
- uv run ruff check src tests
- uv run mypy
- git diff --check

Linear: WG-223"
```

Expected: PR URL is printed. Do not merge the PR.

## Self-Review

Spec coverage:

- Deterministic current/historical composition: Tasks 3 and 4.
- Canonical-only scope: Task 4 and future wiring contracts in Task 6.
- Personal default scope and owner-layer shape: Task 1.
- Metadata explaining overridden fields: Task 4.
- Source match, drift, delivered/deleted finality: Task 5.
- Backend-independent replay semantics: Task 3.

The plan intentionally stops before NATS wiring, permission enforcement, UI, and
exports. Those belong in WG-224, WG-226, WG-227, and WG-228.
