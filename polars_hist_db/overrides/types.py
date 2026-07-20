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
    recorded_at: datetime | None = None
    actor_display_name: str | None = None
    document_id: str | None = None
    generation: int = 1


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


@dataclass(frozen=True)
class DocumentAccessStoreConfig:
    schema: str = "overrides"
    documents_table: str = "document_access"
    grants_table: str = "document_access_grants"
    commands_table: str = "document_access_commands"


@dataclass(frozen=True)
class LayerCompositionStoreConfig:
    schema: str = "overrides"
    revisions_table: str = "layer_composition_revisions"


@dataclass(frozen=True)
class OverridePurgeStoreConfig:
    schema: str = "overrides"
    metadata_table: str = "override_purge_metadata"


@dataclass(frozen=True)
class ArrowOverrideStoreConfig:
    schema: str = "overrides"
    heads_table: str = "arrow_override_layer_heads"
    operations_table: str = "arrow_override_operations"
    references_table: str = "arrow_override_operation_references"
    string_list_values_table: str = "arrow_override_string_list_values"
