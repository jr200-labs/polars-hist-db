# Override Ledger Contract Design

## Goal

WG-221 introduces a backend-independent temporal override ledger contract in
`polars-hist-db`. The contract lets Whengas and future consumers store
user-authored field corrections without mutating canonical source-feed tables.

The first implementation is a library contract, not a Whengas API endpoint. It
defines schema, config, pure ledger semantics, and backend contract tests. Later
Whengas tickets can use it to implement write APIs, effective reads, UI, and
NATS invalidation.

## Design Principles

The override ledger is generic. Cargo is the first consumer, but the storage
shape must work for any feed, entity, and field path.

The ledger is backend independent. It emits normal `TableConfig` and
`ValidTimeConfig` values that the MariaDB and XTDB adapters can consume. No API
should require a caller to know whether the active backend is XTDB or MariaDB.

Business validity is explicit data. `valid_from` and `valid_to` are application
columns that define the user's correction window. XTDB `_valid_from` and
`_valid_to` remain backend/system valid-time columns and must not be the only
place where application validity is represented.

History is append-only. Closing or replacing an override appends operations; it
does not physically delete audit history.

## Public API

The public package surface is:

```python
from polars_hist_db.overrides import (
    InMemoryOverrideLedgerStore,
    OverrideLedger,
    OverrideLedgerConfig,
    OverrideOperation,
    OverrideOperationType,
    OverrideTypedValue,
    build_override_table_config,
    build_override_valid_time_config,
)
```

Core value objects:

```python
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
    reason: str | None
    comment: str | None
    metadata_json: dict[str, Any]
```

Configuration object:

```python
@dataclass(frozen=True)
class OverrideLedgerConfig:
    schema: str = "overrides"
    table: str = "data_override_operations"
    valid_from_column: str = "valid_from"
    valid_to_column: str = "valid_to"
```

Business API:

```python
ledger = OverrideLedger(store)

ledger.set_field(
    owner_user_id="user-1",
    actor_user_id="user-1",
    feed_id="cargos",
    entity_id="cargo-1",
    field_path="classification",
    value=OverrideTypedValue("enum", {"value": "probable"}),
    observed_canonical_value_json={"value": "expected"},
    valid_from=t1,
    comment="looks probable",
)

ledger.close_field(
    owner_user_id="user-1",
    actor_user_id="system",
    feed_id="cargos",
    entity_id="cargo-1",
    field_path="classification",
    valid_to=t3,
    reason="source_match",
)
```

Read API:

```python
ledger.active_for_entity(owner_user_id, feed_id, entity_id)
ledger.active_for_field(owner_user_id, feed_id, entity_id, field_path)
ledger.history_for_entity(owner_user_id, feed_id, entity_id)
```

## Config Shape

Top-level config can declare override ledgers without selecting a backend:

```yaml
override_ledgers:
  - schema: overrides
    table: data_override_operations
    valid_time:
      from_column: valid_from
      to_column: valid_to
```

The implementation should initially expose builders rather than forcing the
top-level parser to consume this YAML in the first PR:

```python
config = OverrideLedgerConfig(
    schema="overrides",
    table="data_override_operations",
)

table_config = build_override_table_config(config)
valid_time = build_override_valid_time_config(config)
```

The generated `TableConfig` must include:

```text
operation_id
change_set_id
owner_user_id
actor_user_id
feed_id
entity_id
field_path
operation_type
value_type
value_json
unit
observed_canonical_value_json
created_against_stale_source
valid_from
valid_to
reason
comment
metadata_json
```

The generated `ValidTimeConfig` must map:

```text
schema = overrides
table = data_override_operations
from_column = valid_from
to_column = valid_to
```

## Semantics

There is at most one active `set` operation per
`owner_user_id + feed_id + entity_id + field_path`.

`set_field` closes the previous active `set` for that key at the new operation's
`valid_from`, then appends a new active `set`.

`close_field` closes the previous active `set` for that key at `valid_to`, then
appends a close operation. The close operation has `valid_from == valid_to` and
`value is None`.

`system_close` follows the same interval behavior as `close`, but records that
the actor was system automation.

`audit_correction` is reserved for future correction of ledger mistakes. It is
part of the enum now so the table shape does not need to change later, but the
first PR does not need a high-level helper for it.

Grouped user/admin/system actions share a `change_set_id`. If callers do not
provide one, the ledger generates one. Comments and reasons remain fields on
each operation so per-field comments are available even when operations share a
group.

## Cargo Use Cases

The schema must express this example:

1. At 13:00, user sets `classification = probable`.
2. At 15:00, user changes `classification = possible`.
3. At 20:00, system closes the override because the canonical source now
   matches, deletes, or delivers the cargo.

Expected history:

```text
set(probable, valid_from=13:00, valid_to=15:00)
close(valid_from=15:00, valid_to=15:00, reason=replaced)
set(possible, valid_from=15:00, valid_to=20:00)
system_close(valid_from=20:00, valid_to=20:00, reason=source_match)
```

Expected active state after 20:00: no active classification override.

The first Whengas cargo fields are:

```text
destination_date
destination_location
classification
status
origin_date
origin_location
cargo_mcm
```

WG-221 should not hard-code those fields into the generic ledger. Cargo-specific
field validation belongs in Whengas API or a later optional domain helper.

## Validation Rules

`set` requires a non-null `OverrideTypedValue`.

`close` and `system_close` require `value is None`.

`valid_from` and `valid_to` must be timezone-aware datetimes.

An operation with both `valid_from` and `valid_to` must satisfy
`valid_to >= valid_from`.

`metadata_json` defaults to an empty dict and is reserved for future shared or
CRDT-compatible layer metadata.

## Test Strategy

Unit tests prove the public API and semantics without any database.

Config tests prove the table and valid-time config builders emit backend-neutral
config objects.

Backend contract tests prove the generated table config can be passed through
the existing MariaDB and XTDB table preparation paths without adding
backend-specific ledger logic.

No live database is required for the first PR. Existing live/integration tests
can be extended later once Whengas starts writing override rows.

## Non-Goals

This design does not implement `whengas-api` write handlers.

This design does not implement effective cargo reads.

This design does not implement UI editing, provenance display, or NATS
invalidation.

This design does not solve XTDB production transaction-log hardening. That
remains a production enablement gate before broad non-replayable override use.
