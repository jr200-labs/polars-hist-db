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


@dataclass(frozen=True)
class CrdtDocumentStoreConfig:
    schema: str = "overrides"
    documents_table: str = "crdt_documents"
    updates_table: str = "crdt_updates"
