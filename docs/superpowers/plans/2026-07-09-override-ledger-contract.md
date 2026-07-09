# Override Ledger Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a backend-independent temporal override ledger contract to `polars-hist-db` for WG-221.

**Architecture:** Implement the ledger as a small `polars_hist_db.overrides` package. It emits normal `TableConfig` and `ValidTimeConfig` objects for existing backend adapters and provides pure in-memory ledger semantics for unit tests and downstream API development. The first PR stops at library contract, config builders, semantics, tests, and docs.

**Tech Stack:** Python 3.12, dataclasses, `polars_hist_db.config.TableConfig`, `ValidTimeConfig`, pytest, ruff, mypy.

---

## File Structure

- Create: `polars_hist_db/overrides/__init__.py`
  Public imports for the override ledger package.
- Create: `polars_hist_db/overrides/types.py`
  Dataclasses and literals for typed values, operations, and config.
- Create: `polars_hist_db/overrides/config.py`
  Builders from `OverrideLedgerConfig` to `TableConfig` and `ValidTimeConfig`.
- Create: `polars_hist_db/overrides/ledger.py`
  Store protocol, in-memory store, and pure ledger interval behavior.
- Create: `tests/overrides/__init__.py`
  Test package marker.
- Create: `tests/overrides/test_override_table_config.py`
  Unit tests for generated table and valid-time config.
- Create: `tests/overrides/test_override_ledger_semantics.py`
  Unit tests for set, replace, close, grouped changes, and validation.
- Create: `tests/backends/test_override_table_contract.py`
  Backend-neutral contract tests that exercise existing table-preparation APIs.
- Modify: `docs/configuration.qmd`
  Document override ledger config, public API, backend notes, and use cases.

## Task 1: Override Public Types

**Files:**
- Create: `polars_hist_db/overrides/__init__.py`
- Create: `polars_hist_db/overrides/types.py`
- Test: `tests/overrides/test_override_table_config.py`

- [ ] **Step 1: Write failing import and config default test**

Create `tests/overrides/__init__.py` as an empty file.

Create `tests/overrides/test_override_table_config.py`:

```python
from polars_hist_db.overrides import OverrideLedgerConfig, OverrideTypedValue


def test_override_ledger_config_defaults_to_generic_table():
    config = OverrideLedgerConfig()

    assert config.schema == "overrides"
    assert config.table == "data_override_operations"
    assert config.valid_from_column == "valid_from"
    assert config.valid_to_column == "valid_to"


def test_override_typed_value_preserves_unit_and_json():
    value = OverrideTypedValue("decimal", {"value": "123.456"}, unit="mcm")

    assert value.value_type == "decimal"
    assert value.value_json == {"value": "123.456"}
    assert value.unit == "mcm"
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
uv run pytest tests/overrides/test_override_table_config.py -q
```

Expected: failure with `ModuleNotFoundError: No module named 'polars_hist_db.overrides'`.

- [ ] **Step 3: Add minimal public types**

Create `polars_hist_db/overrides/types.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

OverrideOperationType = Literal[
    "set",
    "close",
    "system_close",
    "audit_correction",
]


@dataclass(frozen=True)
class OverrideTypedValue:
    value_type: str
    value_json: dict[str, Any]
    unit: str | None = None


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
    valid_to: datetime | None
    reason: str | None = None
    comment: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OverrideLedgerConfig:
    schema: str = "overrides"
    table: str = "data_override_operations"
    valid_from_column: str = "valid_from"
    valid_to_column: str = "valid_to"
```

Create `polars_hist_db/overrides/__init__.py`:

```python
from .types import (
    OverrideLedgerConfig,
    OverrideOperation,
    OverrideOperationType,
    OverrideTypedValue,
)

__all__ = [
    "OverrideLedgerConfig",
    "OverrideOperation",
    "OverrideOperationType",
    "OverrideTypedValue",
]
```

- [ ] **Step 4: Run the test**

Run:

```bash
uv run pytest tests/overrides/test_override_table_config.py -q
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

Run:

```bash
git add polars_hist_db/overrides tests/overrides/test_override_table_config.py
git commit -m "feat(wg-221): add override ledger public types"
```

## Task 2: Table And Valid-Time Config Builders

**Files:**
- Create: `polars_hist_db/overrides/config.py`
- Modify: `polars_hist_db/overrides/__init__.py`
- Test: `tests/overrides/test_override_table_config.py`

- [ ] **Step 1: Add failing table config tests**

Append to `tests/overrides/test_override_table_config.py`:

```python
from polars_hist_db.overrides import (
    build_override_table_config,
    build_override_valid_time_config,
)


def test_build_override_table_config_contains_required_columns():
    table = build_override_table_config(OverrideLedgerConfig())

    assert table.schema == "overrides"
    assert table.name == "data_override_operations"
    assert table.primary_keys == ("operation_id",)
    assert {column.name for column in table.columns} == {
        "operation_id",
        "change_set_id",
        "owner_user_id",
        "actor_user_id",
        "feed_id",
        "entity_id",
        "field_path",
        "operation_type",
        "value_type",
        "value_json",
        "unit",
        "observed_canonical_value_json",
        "created_against_stale_source",
        "valid_from",
        "valid_to",
        "reason",
        "comment",
        "metadata_json",
    }


def test_build_override_valid_time_config_uses_business_columns():
    valid_time = build_override_valid_time_config(OverrideLedgerConfig())

    assert valid_time.schema == "overrides"
    assert valid_time.table == "data_override_operations"
    assert valid_time.from_column == "valid_from"
    assert valid_time.to_column == "valid_to"
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
uv run pytest tests/overrides/test_override_table_config.py -q
```

Expected: import failure for `build_override_table_config`.

- [ ] **Step 3: Add config builders**

Create `polars_hist_db/overrides/config.py`:

```python
from __future__ import annotations

from polars_hist_db.config import TableColumnConfig, TableConfig, ValidTimeConfig

from .types import OverrideLedgerConfig


def build_override_table_config(config: OverrideLedgerConfig) -> TableConfig:
    table = config.table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("operation_id",),
        columns=[
            TableColumnConfig(table, "operation_id", "VARCHAR(64)", nullable=False),
            TableColumnConfig(table, "change_set_id", "VARCHAR(64)", nullable=False),
            TableColumnConfig(table, "owner_user_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "actor_user_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "feed_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "entity_id", "VARCHAR(256)", nullable=False),
            TableColumnConfig(table, "field_path", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "operation_type", "VARCHAR(32)", nullable=False),
            TableColumnConfig(table, "value_type", "VARCHAR(32)"),
            TableColumnConfig(table, "value_json", "JSON"),
            TableColumnConfig(table, "unit", "VARCHAR(32)"),
            TableColumnConfig(table, "observed_canonical_value_json", "JSON"),
            TableColumnConfig(
                table,
                "created_against_stale_source",
                "BOOL",
                nullable=False,
            ),
            TableColumnConfig(
                table,
                config.valid_from_column,
                "DATETIME(6)",
                nullable=False,
            ),
            TableColumnConfig(table, config.valid_to_column, "DATETIME(6)"),
            TableColumnConfig(table, "reason", "VARCHAR(128)"),
            TableColumnConfig(table, "comment", "VARCHAR(2048)"),
            TableColumnConfig(table, "metadata_json", "JSON"),
        ],
    )


def build_override_valid_time_config(config: OverrideLedgerConfig) -> ValidTimeConfig:
    return ValidTimeConfig(
        schema=config.schema,
        table=config.table,
        from_column=config.valid_from_column,
        to_column=config.valid_to_column,
    )
```

Modify `polars_hist_db/overrides/__init__.py`:

```python
from .config import build_override_table_config, build_override_valid_time_config
from .types import (
    OverrideLedgerConfig,
    OverrideOperation,
    OverrideOperationType,
    OverrideTypedValue,
)

__all__ = [
    "OverrideLedgerConfig",
    "OverrideOperation",
    "OverrideOperationType",
    "OverrideTypedValue",
    "build_override_table_config",
    "build_override_valid_time_config",
]
```

- [ ] **Step 4: Run table config tests**

Run:

```bash
uv run pytest tests/overrides/test_override_table_config.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add polars_hist_db/overrides tests/overrides/test_override_table_config.py
git commit -m "feat(wg-221): add override ledger table config"
```

## Task 3: Pure Ledger Semantics

**Files:**
- Create: `polars_hist_db/overrides/ledger.py`
- Modify: `polars_hist_db/overrides/__init__.py`
- Test: `tests/overrides/test_override_ledger_semantics.py`

- [ ] **Step 1: Write failing cargo timeline test**

Create `tests/overrides/test_override_ledger_semantics.py`:

```python
from datetime import datetime, timezone

from polars_hist_db.overrides import (
    InMemoryOverrideLedgerStore,
    OverrideLedger,
    OverrideTypedValue,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 9, hour, tzinfo=timezone.utc)


def test_cargo_classification_set_replace_and_system_close_timeline():
    store = InMemoryOverrideLedgerStore()
    ledger = OverrideLedger(store)

    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        comment="first assessment",
    )
    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "possible"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(15),
    )
    ledger.close_field(
        owner_user_id="user-1",
        actor_user_id="system",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        valid_to=_utc(20),
        reason="source_match",
        system=True,
    )

    history = ledger.history_for_entity("user-1", "cargos", "cargo-1")

    assert [operation.operation_type for operation in history] == [
        "set",
        "close",
        "set",
        "system_close",
    ]
    assert history[0].valid_from == _utc(13)
    assert history[0].valid_to == _utc(15)
    assert history[1].valid_from == _utc(15)
    assert history[1].valid_to == _utc(15)
    assert history[1].reason == "replaced"
    assert history[2].value == OverrideTypedValue("enum", {"value": "possible"})
    assert history[2].valid_from == _utc(15)
    assert history[2].valid_to == _utc(20)
    assert history[3].valid_from == _utc(20)
    assert history[3].valid_to == _utc(20)
    assert history[3].reason == "source_match"
    assert ledger.active_for_entity("user-1", "cargos", "cargo-1") == []
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
uv run pytest tests/overrides/test_override_ledger_semantics.py -q
```

Expected: import failure for `InMemoryOverrideLedgerStore`.

- [ ] **Step 3: Implement in-memory ledger**

Create `polars_hist_db/overrides/ledger.py`:

```python
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from .types import OverrideOperation, OverrideOperationType, OverrideTypedValue


class OverrideLedgerStore(Protocol):
    def append(self, operation: OverrideOperation) -> None: ...

    def replace(self, operation: OverrideOperation) -> None: ...

    def history_for_entity(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> list[OverrideOperation]: ...

    def active_for_field(
        self, owner_user_id: str, feed_id: str, entity_id: str, field_path: str
    ) -> OverrideOperation | None: ...


class InMemoryOverrideLedgerStore:
    def __init__(self) -> None:
        self._operations: list[OverrideOperation] = []

    def append(self, operation: OverrideOperation) -> None:
        self._operations.append(operation)

    def replace(self, operation: OverrideOperation) -> None:
        for index, existing in enumerate(self._operations):
            if existing.operation_id == operation.operation_id:
                self._operations[index] = operation
                return
        raise ValueError(f"unknown override operation {operation.operation_id!r}")

    def history_for_entity(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> list[OverrideOperation]:
        return [
            operation
            for operation in self._operations
            if operation.owner_user_id == owner_user_id
            and operation.feed_id == feed_id
            and operation.entity_id == entity_id
        ]

    def active_for_field(
        self, owner_user_id: str, feed_id: str, entity_id: str, field_path: str
    ) -> OverrideOperation | None:
        active = [
            operation
            for operation in self.history_for_entity(owner_user_id, feed_id, entity_id)
            if operation.field_path == field_path
            and operation.operation_type == "set"
            and operation.valid_to is None
        ]
        if len(active) > 1:
            raise ValueError(
                "override ledger invariant violation: more than one active override"
            )
        return active[0] if active else None


class OverrideLedger:
    def __init__(self, store: OverrideLedgerStore) -> None:
        self.store = store

    def set_field(
        self,
        *,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        value: OverrideTypedValue,
        observed_canonical_value_json: dict[str, object] | None,
        valid_from: datetime,
        change_set_id: str | None = None,
        created_against_stale_source: bool = False,
        comment: str | None = None,
        metadata_json: dict[str, object] | None = None,
    ) -> OverrideOperation:
        self._require_timezone(valid_from, "valid_from")
        resolved_change_set_id = change_set_id or self._id("chg")
        active = self.store.active_for_field(
            owner_user_id, feed_id, entity_id, field_path
        )
        if active is not None:
            self.store.replace(replace(active, valid_to=valid_from))
            self.store.append(
                self._operation(
                    change_set_id=resolved_change_set_id,
                    owner_user_id=owner_user_id,
                    actor_user_id=actor_user_id,
                    feed_id=feed_id,
                    entity_id=entity_id,
                    field_path=field_path,
                    operation_type="close",
                    value=None,
                    observed_canonical_value_json=observed_canonical_value_json,
                    created_against_stale_source=created_against_stale_source,
                    valid_from=valid_from,
                    valid_to=valid_from,
                    reason="replaced",
                    comment=None,
                    metadata_json=metadata_json or {},
                )
            )

        operation = self._operation(
            change_set_id=resolved_change_set_id,
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type="set",
            value=value,
            observed_canonical_value_json=observed_canonical_value_json,
            created_against_stale_source=created_against_stale_source,
            valid_from=valid_from,
            valid_to=None,
            reason=None,
            comment=comment,
            metadata_json=metadata_json or {},
        )
        self.store.append(operation)
        return operation

    def close_field(
        self,
        *,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        valid_to: datetime,
        reason: str,
        change_set_id: str | None = None,
        comment: str | None = None,
        system: bool = False,
        metadata_json: dict[str, object] | None = None,
    ) -> OverrideOperation:
        self._require_timezone(valid_to, "valid_to")
        active = self.store.active_for_field(
            owner_user_id, feed_id, entity_id, field_path
        )
        if active is not None:
            self.store.replace(replace(active, valid_to=valid_to))

        operation = self._operation(
            change_set_id=change_set_id or self._id("chg"),
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type="system_close" if system else "close",
            value=None,
            observed_canonical_value_json=None,
            created_against_stale_source=False,
            valid_from=valid_to,
            valid_to=valid_to,
            reason=reason,
            comment=comment,
            metadata_json=metadata_json or {},
        )
        self.store.append(operation)
        return operation

    def active_for_entity(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> list[OverrideOperation]:
        return [
            operation
            for operation in self.history_for_entity(owner_user_id, feed_id, entity_id)
            if operation.operation_type == "set" and operation.valid_to is None
        ]

    def active_for_field(
        self, owner_user_id: str, feed_id: str, entity_id: str, field_path: str
    ) -> OverrideOperation | None:
        return self.store.active_for_field(owner_user_id, feed_id, entity_id, field_path)

    def history_for_entity(
        self, owner_user_id: str, feed_id: str, entity_id: str
    ) -> list[OverrideOperation]:
        return self.store.history_for_entity(owner_user_id, feed_id, entity_id)

    def _operation(
        self,
        *,
        change_set_id: str,
        owner_user_id: str,
        actor_user_id: str,
        feed_id: str,
        entity_id: str,
        field_path: str,
        operation_type: OverrideOperationType,
        value: OverrideTypedValue | None,
        observed_canonical_value_json: dict[str, object] | None,
        created_against_stale_source: bool,
        valid_from: datetime,
        valid_to: datetime | None,
        reason: str | None,
        comment: str | None,
        metadata_json: dict[str, object],
    ) -> OverrideOperation:
        if operation_type == "set" and value is None:
            raise ValueError("set override operations require a value")
        if operation_type in {"close", "system_close"} and value is not None:
            raise ValueError("close override operations must not carry a value")
        if valid_to is not None and valid_to < valid_from:
            raise ValueError("override valid_to must be greater than or equal to valid_from")
        return OverrideOperation(
            operation_id=self._id("op"),
            change_set_id=change_set_id,
            owner_user_id=owner_user_id,
            actor_user_id=actor_user_id,
            feed_id=feed_id,
            entity_id=entity_id,
            field_path=field_path,
            operation_type=operation_type,
            value=value,
            observed_canonical_value_json=observed_canonical_value_json,
            created_against_stale_source=created_against_stale_source,
            valid_from=valid_from,
            valid_to=valid_to,
            reason=reason,
            comment=comment,
            metadata_json=metadata_json,
        )

    @staticmethod
    def _id(prefix: str) -> str:
        return f"{prefix}_{uuid4().hex}"

    @staticmethod
    def _require_timezone(value: datetime, name: str) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
```

Modify `polars_hist_db/overrides/__init__.py` to export the new classes:

```python
from .config import build_override_table_config, build_override_valid_time_config
from .ledger import InMemoryOverrideLedgerStore, OverrideLedger
from .types import (
    OverrideLedgerConfig,
    OverrideOperation,
    OverrideOperationType,
    OverrideTypedValue,
)

__all__ = [
    "InMemoryOverrideLedgerStore",
    "OverrideLedger",
    "OverrideLedgerConfig",
    "OverrideOperation",
    "OverrideOperationType",
    "OverrideTypedValue",
    "build_override_table_config",
    "build_override_valid_time_config",
]
```

- [ ] **Step 4: Run the semantics test**

Run:

```bash
uv run pytest tests/overrides/test_override_ledger_semantics.py -q
```

Expected: test passes.

- [ ] **Step 5: Commit**

Run:

```bash
git add polars_hist_db/overrides tests/overrides/test_override_ledger_semantics.py
git commit -m "feat(wg-221): add override ledger semantics"
```

## Task 4: Validation And Grouped Changes

**Files:**
- Modify: `tests/overrides/test_override_ledger_semantics.py`
- Modify: `polars_hist_db/overrides/ledger.py`

- [ ] **Step 1: Add validation and grouping tests**

Append to `tests/overrides/test_override_ledger_semantics.py`:

```python
import pytest


def test_set_rejects_naive_valid_from():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    with pytest.raises(ValueError, match="timezone-aware"):
        ledger.set_field(
            owner_user_id="user-1",
            actor_user_id="user-1",
            feed_id="cargos",
            entity_id="cargo-1",
            field_path="status",
            value=OverrideTypedValue("enum", {"value": "deleted"}),
            observed_canonical_value_json={"value": "expected"},
            valid_from=datetime(2026, 7, 9, 13),
        )


def test_grouped_sets_share_change_set_id():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        change_set_id="change-1",
    )
    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="status",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        change_set_id="change-1",
    )

    history = ledger.history_for_entity("user-1", "cargos", "cargo-1")
    assert {operation.change_set_id for operation in history} == {"change-1"}
    assert len(ledger.active_for_entity("user-1", "cargos", "cargo-1")) == 2


def test_stale_source_flag_and_observed_canonical_value_are_preserved():
    ledger = OverrideLedger(InMemoryOverrideLedgerStore())

    operation = ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="cargo_mcm",
        value=OverrideTypedValue("decimal", {"value": "123.456"}, unit="mcm"),
        observed_canonical_value_json={"value": "100.000"},
        created_against_stale_source=True,
        valid_from=_utc(13),
    )

    assert operation.observed_canonical_value_json == {"value": "100.000"}
    assert operation.created_against_stale_source is True
    assert operation.value.unit == "mcm"
```

- [ ] **Step 2: Run the new tests**

Run:

```bash
uv run pytest tests/overrides/test_override_ledger_semantics.py -q
```

Expected: tests pass if Task 3 implementation included the validation and grouping behavior. If not, update `ledger.py` to match the test.

- [ ] **Step 3: Commit**

Run:

```bash
git add polars_hist_db/overrides/ledger.py tests/overrides/test_override_ledger_semantics.py
git commit -m "test(wg-221): cover override ledger invariants"
```

## Task 5: Backend Table Contract Tests

**Files:**
- Create: `tests/backends/test_override_table_contract.py`

- [ ] **Step 1: Write backend-neutral contract tests**

Create `tests/backends/test_override_table_contract.py`:

```python
from polars_hist_db.overrides import (
    OverrideLedgerConfig,
    build_override_table_config,
    build_override_valid_time_config,
)


def test_override_table_config_builds_sqlalchemy_columns_for_mariadb_path():
    table_config = build_override_table_config(OverrideLedgerConfig())

    columns = table_config.build_sqlalchemy_columns(is_delta_table=False)

    assert {column.name for column in columns} >= {
        "operation_id",
        "owner_user_id",
        "field_path",
        "valid_from",
        "valid_to",
        "metadata_json",
    }
    operation_id = next(column for column in columns if column.name == "operation_id")
    assert operation_id.primary_key is True


def test_override_valid_time_config_targets_business_window_for_xtdb_path():
    config = OverrideLedgerConfig()
    table_config = build_override_table_config(config)
    valid_time = build_override_valid_time_config(config)

    assert valid_time.matches(table_config.schema, table_config.name)
    assert valid_time.from_column == "valid_from"
    assert valid_time.to_column == "valid_to"
    assert {"valid_from", "valid_to"}.issubset(
        {column.name for column in table_config.columns}
    )
```

- [ ] **Step 2: Run the contract tests**

Run:

```bash
uv run pytest tests/backends/test_override_table_contract.py -q
```

Expected: tests pass.

- [ ] **Step 3: Commit**

Run:

```bash
git add tests/backends/test_override_table_contract.py
git commit -m "test(wg-221): prove override table backend contract"
```

## Task 6: Documentation

**Files:**
- Modify: `docs/configuration.qmd`

- [ ] **Step 1: Add documentation section**

Append this section to `docs/configuration.qmd`:

````markdown
## Override Ledgers

`polars-hist-db` exposes a backend-independent override ledger contract for
applications that need append-only user-authored corrections without mutating
canonical source-feed tables.

```python
from polars_hist_db.overrides import (
    OverrideLedgerConfig,
    build_override_table_config,
    build_override_valid_time_config,
)

config = OverrideLedgerConfig(
    schema="overrides",
    table="data_override_operations",
)

table_config = build_override_table_config(config)
valid_time = build_override_valid_time_config(config)
```

Equivalent YAML shape for downstream applications:

```yaml
override_ledgers:
  - schema: overrides
    table: data_override_operations
    valid_time:
      from_column: valid_from
      to_column: valid_to
```

`valid_from` and `valid_to` are explicit business-validity columns. XTDB also
has built-in `_valid_from` and `_valid_to` columns, but applications should not
depend on those system columns as the only representation of user correction
windows.

The generated table includes owner, actor, feed, entity, field path, operation
type, typed JSON value, observed canonical value, stale-source flag, validity
window, reason, comment, and metadata fields. The same table contract is
intended to work with MariaDB and XTDB adapters.
````

- [ ] **Step 2: Run docs-adjacent tests**

Run:

```bash
uv run pytest tests/overrides tests/backends/test_override_table_contract.py -q
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

Run:

```bash
git add docs/configuration.qmd
git commit -m "docs(wg-221): document override ledger contract"
```

## Task 7: Final Verification And PR

**Files:**
- All files changed by prior tasks.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run pytest tests/overrides tests/backends/test_override_table_contract.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run broader checks**

Run:

```bash
uv run pytest tests/config tests/test_parser.py tests/test_pipeline_projection.py -q
uv run ruff check polars_hist_db tests
uv run mypy
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 3: Push branch**

Run:

```bash
git status --short
git push -u origin feat/wg-221-override-ledger-contract
```

Expected: clean worktree before push and branch published to GitHub.

- [ ] **Step 4: Open PR**

Run:

```bash
gh pr create \
  --repo jr200-labs/polars-hist-db \
  --base master \
  --head feat/wg-221-override-ledger-contract \
  --title "feat(wg-221): add override ledger contract" \
  --body "## Summary
- add backend-independent override ledger config and public types
- add pure in-memory ledger semantics for set, replace, close, and system-close
- prove generated table config works through backend-neutral MariaDB/XTDB contract paths
- document override ledger API and config shape

## Verification
- uv run pytest tests/overrides tests/backends/test_override_table_contract.py -q
- uv run pytest tests/config tests/test_parser.py tests/test_pipeline_projection.py -q
- uv run ruff check polars_hist_db tests
- uv run mypy
- git diff --check

Linear: WG-221"
```

Expected: PR URL is printed. Do not merge the PR.

## Self-Review

Spec coverage:

- Public API: Tasks 1, 2, and 3.
- Config shape: Task 2 and Task 6.
- Backend independence: Task 5.
- Functionality and cargo timeline: Task 3.
- Validation and grouped use cases: Task 4.
- Documentation: Task 6.

The plan contains concrete file paths, commands, and code snippets. The
implementation is intentionally scoped to a reusable ledger contract; Whengas API
write/read/UI behavior is left for WG-226, WG-227, and WG-228.
