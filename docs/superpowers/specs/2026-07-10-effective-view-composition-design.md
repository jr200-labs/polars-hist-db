# Effective View Composition Design

## Goal

WG-223 defines deterministic rules for composing canonical source rows with
personal override layers. The first consumer is Whengas cargo data, but the
composition model should remain backend independent and reusable for other
feed/entity/field override layers.

This is a design contract, not a write API or UI implementation. Later tickets
can implement write handlers, effective read endpoints, permissions, UI editing,
and invalidation against this contract.

## Design Principles

The canonical row remains the identity source. An effective row never changes
the canonical primary key or pretends that an override operation is a canonical
source update.

The default user-facing scope is effective personal data: canonical values
overlaid with active overrides owned by the requesting user. Canonical-only
reads remain available for admin/debug/source inspection.

Composition is performed at the API boundary before data is returned to user
interfaces, downloads, exports, maps, timelines, and user-facing API responses.
That keeps every consumer on one effective-row contract instead of asking each
frontend or export path to reimplement overlay rules.

The override ledger is append-only. Effective reads derive active overrides by
replaying operation rows through the requested business valid time. They must
not depend on updating older set operations or on a backend-specific current-row
shortcut.

## Approaches Considered

### Recommended: API-side projection

`whengas-api` queries canonical rows and relevant override operation rows for
the same `asof_utc`, composes them in a shared projection module, and returns the
same effective rows to NATS request/reply, KV refreshes, exports, and future HTTP
surfaces.

This is the preferred V1 because it is deterministic, testable without a live
database, and permission-aware. It also keeps Whengas business rules out of
`polars-hist-db`.

### Database view projection

The database could expose SQL views that join canonical rows to active override
rows. This is attractive for simple reads, but it is a poor V1 fit because the
append-only operation log needs replay semantics, permissions vary by requester,
and the same logic must work across XTDB and non-XTDB test paths.

### Frontend-side projection

The API could return canonical rows and override rows separately and let the UI
compose them. This would duplicate business logic across map, table, detail,
timeline, and export paths. It also risks leaking private override values into
contexts where the requester is not allowed to inspect them.

## Public Contract

The later implementation should expose a small application-level module in
`whengas-api`, shaped like this:

```python
@dataclass(frozen=True)
class OverrideReadScope:
    mode: Literal["effective", "canonical", "owner_layer"]
    owner_user_id: str | None = None


@dataclass(frozen=True)
class EffectiveViewRequest:
    asof_utc: datetime
    requester_user_id: str
    scope: OverrideReadScope
    include_metadata: bool = True


@dataclass(frozen=True)
class FieldOverrideMetadata:
    field_path: str
    state: Literal[
        "active",
        "source_drift",
        "source_matched",
        "closed",
    ]
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


def compose_effective_rows(
    canonical_rows: pl.DataFrame,
    override_operations: Sequence[OverrideOperation],
    request: EffectiveViewRequest,
    field_specs: Mapping[str, OverrideFieldSpec],
) -> pl.DataFrame:
    raise NotImplementedError
```

The exact Python names may change during implementation, but the behavior should
not: one function receives canonical rows, operation rows, request scope, and a
field registry, then returns effective rows with metadata.

## Read Scopes

`canonical` returns canonical rows exactly as read from the source table at
`asof_utc`. It does not overlay personal values. It may include an empty
metadata column to keep response schemas stable, but it must not expose private
override details.

`effective` is the normal user default. It overlays active operations owned by
`requester_user_id`.

`owner_layer` lets admin/support/internal callers inspect another user's layer.
Read permission for this mode is separate from elevated write permission. Normal
users may not select arbitrary owners.

If scope is omitted by a user-facing endpoint, it resolves to:

```text
OverrideReadScope(mode="effective", owner_user_id=requester_user_id)
```

## Operation Replay

All composition uses a single `asof_utc` for both canonical rows and override
operations.

The canonical query reads the source table at `asof_utc`.

The override query reads operation-log rows for the selected owner, feed, entity
set, and all operation rows with `valid_from <= asof_utc`. For XTDB this should
use an operation-log query that can see historical operation facts, not only rows
that are valid at the selected time. `FOR VALID_TIME ALL` plus a normal
`valid_from <= asof_utc` predicate is acceptable if it is the clearest path.

Rows are replayed in this deterministic order:

```text
valid_from ascending
operation priority: close/system_close before set before audit_correction
change_set_id ascending
operation_id ascending
```

The write API should prevent multiple active `set` operations for the same
`owner_user_id + feed_id + entity_id + field_path`. If historical data violates
that invariant, composition must fail closed with a clear error rather than
guessing which value wins.

Replay state per field:

```text
set            -> active[field_path] = operation
close          -> remove active[field_path]
system_close   -> remove active[field_path]
audit_correction -> reserved for later; ignore in V1 projection unless it
                    explicitly supersedes an operation in a later spec
```

## Overlay Rules

For each canonical row:

1. Resolve the entity ID from the canonical row's cargo ID.
2. Replay owner-layer operations for that entity at `asof_utc`.
3. For each active field operation, validate that `field_path` is in the cargo
   override field registry.
4. Compare the canonical field value at `asof_utc` with the override value.
5. If the canonical row is final or deleted, do not overlay active normal-user
   overrides.
6. If the canonical value equals the override value, use the canonical value and
   mark metadata as `source_matched`.
7. If the canonical value differs from the operation's
   `observed_canonical_value_json`, overlay the override value and mark
   `source_drift = true`.
8. Otherwise overlay the override value and mark metadata as `active`.

The field column in the returned row always contains the value the requester
should see. Metadata explains why it is effective.

## Canonical Finality

Canonical delivered/actual or deleted rows are final for normal user overrides.

When a canonical row reaches finality, `whengas-api` must write system-close
operations for all active personal overrides on that entity. Effective reads
also apply a defensive finality rule so a delayed close job cannot show an
override after finality.

Normal users cannot create active overrides after finality. The write API should
reject with a clear domain error. Elevated admin/support writes after finality
are audit-correction records only in V1 and do not become active user-facing
overrides.

## Source Match And Drift

If an active override value matches the canonical source value at `asof_utc`,
the field is treated as source matched. `whengas-api` should system-close that
field override with reason `source_matched_override`. Effective reads should
return the canonical value and metadata state `source_matched` even if the
system-close operation has not been written yet.

If the canonical source value changes away from the operation's
`observed_canonical_value_json` and does not match the override value, the
override remains active but metadata sets `source_drift = true`.

## Response Metadata

Effective responses include one reserved metadata column:

```text
_override_metadata
```

For JSON responses this is an object. For Arrow IPC responses the preferred
shape is a struct containing a list or map of per-field metadata. If Arrow map
support is awkward for the UI stack, a JSON string column is acceptable for V1
as long as the schema is documented and shared by API/UI tests.

Suggested JSON shape:

```json
{
  "scope": "effective",
  "owner_user_id": "user-1",
  "asof_utc": "2026-07-09T13:00:00Z",
  "fields": {
    "classification": {
      "state": "source_drift",
      "canonical_value_json": {"value": "expected"},
      "override_value_json": {"value": "probable"},
      "operation_id": "op_abc",
      "change_set_id": "chg_abc",
      "actor_user_id": "user-1",
      "valid_from": "2026-07-09T13:00:00Z",
      "valid_to": null,
      "created_against_stale_source": false,
      "source_drift": true,
      "reason": null,
      "comment": "looks probable"
    }
  }
}
```

User display labels are optional and permission-gated. The stable contract uses
IDs; UI label enrichment can be added when identity lookup is available.

## Historical Views

Historical reads use the same composition algorithm with a different
`asof_utc`. A field override only affects historical rows at times after its
`valid_from` and before a replayed close/system-close operation.

Timelines that need both canonical and effective histories should request both
canonical and effective scopes explicitly. The effective view must not remove
the ability to inspect canonical source history.

## Exports And UI Consumers

Exports/downloads use the same effective read path as map, table, detail, and
timeline views. They must not silently fall back to canonical-only data.

The UI should continue treating the row's cargo ID as the identity. Override
metadata is auxiliary data for badges, provenance panels, stale-source warnings,
and editor defaults.

## Non-Goals

WG-223 does not implement write endpoints, permission checks, UI components,
NATS invalidation, or CRDT/shared-layer behavior.

WG-223 does not define every cargo field validator. Field validation belongs in
the later cargo-specific write API implementation.

WG-223 does not require database-specific SQL views. The V1 contract is an
application projection that can be tested in memory.
