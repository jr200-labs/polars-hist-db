from __future__ import annotations

from sqlalchemy import Connection, inspect, text
from sqlalchemy.schema import CreateColumn

from polars_hist_db.config import TableColumnConfig, TableConfig, ValidTimeConfig

from .types import (
    ArrowOverrideStoreConfig,
    CrdtDocumentStoreConfig,
    DocumentAccessStoreConfig,
    LayerCompositionStoreConfig,
    OverrideLedgerConfig,
    OverridePurgeStoreConfig,
)


def build_arrow_override_table_configs(
    config: ArrowOverrideStoreConfig,
) -> tuple[TableConfig, TableConfig, TableConfig, TableConfig]:
    heads = TableConfig(
        name=config.heads_table,
        schema=config.schema,
        primary_keys=("layer_id",),
        columns=[
            TableColumnConfig(
                config.heads_table, "layer_id", "BINARY(16)", nullable=False
            ),
            TableColumnConfig(
                config.heads_table, "generation", "BIGINT", nullable=False
            ),
            TableColumnConfig(config.heads_table, "revision", "BIGINT", nullable=False),
            TableColumnConfig(
                config.heads_table,
                "updated_at",
                "TIMESTAMP WITH TIME ZONE",
                nullable=False,
            ),
        ],
    )
    value_columns = [
        ("kind", "VARCHAR(32)"),
        ("string", "TEXT"),
        ("boolean", "BOOL"),
        ("integer", "BIGINT"),
        ("float", "DOUBLE"),
        ("decimal", "DECIMAL(38,12)"),
        ("timestamp", "TIMESTAMP WITH TIME ZONE"),
        ("date", "DATE"),
        ("time", "TIME"),
        ("duration_us", "BIGINT"),
        ("binary", "BLOB"),
        ("extension_schema_id", "VARCHAR(255)"),
        ("extension_payload", "BLOB"),
    ]
    operations = TableConfig(
        name=config.operations_table,
        schema=config.schema,
        primary_keys=("operation_id",),
        columns=[
            TableColumnConfig(
                config.operations_table, "operation_id", "BINARY(16)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "format_version", "INT", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "change_set_id", "BINARY(16)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "layer_id", "BINARY(16)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "generation", "BIGINT", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "layer_revision", "BIGINT", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "feed_id", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "entity_id", "VARCHAR(256)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "field_path", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "operation_type", "VARCHAR(16)", nullable=False
            ),
            *[
                TableColumnConfig(config.operations_table, f"value_{name}", data_type)
                for name, data_type in value_columns
            ],
            TableColumnConfig(config.operations_table, "unit", "VARCHAR(64)"),
            *[
                TableColumnConfig(
                    config.operations_table, f"observed_{name}", data_type
                )
                for name, data_type in value_columns
            ],
            TableColumnConfig(
                config.operations_table, "source_drift", "BOOL", nullable=False
            ),
            TableColumnConfig(
                config.operations_table,
                "valid_from",
                "TIMESTAMP WITH TIME ZONE",
                nullable=False,
            ),
            TableColumnConfig(
                config.operations_table, "valid_to", "TIMESTAMP WITH TIME ZONE"
            ),
            TableColumnConfig(config.operations_table, "comment", "TEXT"),
            TableColumnConfig(
                config.operations_table, "actor_subject", "VARCHAR(255)", nullable=False
            ),
            TableColumnConfig(
                config.operations_table, "actor_display_name", "VARCHAR(255)"
            ),
            TableColumnConfig(
                config.operations_table,
                "recorded_at",
                "TIMESTAMP WITH TIME ZONE",
                nullable=False,
            ),
            TableColumnConfig(
                config.operations_table, "payload_hash", "BINARY(32)", nullable=False
            ),
        ],
    )
    references = TableConfig(
        name=config.references_table,
        schema=config.schema,
        primary_keys=("operation_id", "reference_kind", "ordinal"),
        columns=[
            TableColumnConfig(
                config.references_table, "operation_id", "BINARY(16)", nullable=False
            ),
            TableColumnConfig(
                config.references_table, "reference_kind", "VARCHAR(16)", nullable=False
            ),
            TableColumnConfig(
                config.references_table, "ordinal", "INT", nullable=False
            ),
            TableColumnConfig(
                config.references_table,
                "referenced_operation_id",
                "BINARY(16)",
                nullable=False,
            ),
        ],
    )
    string_lists = TableConfig(
        name=config.string_list_values_table,
        schema=config.schema,
        primary_keys=("operation_id", "value_role", "ordinal"),
        columns=[
            TableColumnConfig(
                config.string_list_values_table,
                "operation_id",
                "BINARY(16)",
                nullable=False,
            ),
            TableColumnConfig(
                config.string_list_values_table,
                "value_role",
                "VARCHAR(16)",
                nullable=False,
            ),
            TableColumnConfig(
                config.string_list_values_table, "ordinal", "INT", nullable=False
            ),
            TableColumnConfig(
                config.string_list_values_table, "value", "TEXT", nullable=False
            ),
        ],
    )
    return heads, operations, references, string_lists


def build_layer_composition_table_config(
    config: LayerCompositionStoreConfig,
) -> TableConfig:
    table = config.revisions_table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("revision_id",),
        columns=[
            TableColumnConfig(table, "revision_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "layer_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "child_layer_ids_json", "JSON", nullable=False),
            TableColumnConfig(table, "valid_from", "DATETIME(6)", nullable=False),
            TableColumnConfig(table, "valid_to", "DATETIME(6)"),
            TableColumnConfig(table, "recorded_at", "DATETIME(6)", nullable=False),
            TableColumnConfig(table, "actor_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "supersedes_revision_id", "VARCHAR(128)"),
        ],
    )


def build_override_purge_table_config(config: OverridePurgeStoreConfig) -> TableConfig:
    table = config.metadata_table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("request_id",),
        columns=[
            TableColumnConfig(table, "request_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "document_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "layer_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "actor_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "reason", "VARCHAR(2048)", nullable=False),
            TableColumnConfig(table, "old_generation", "BIGINT", nullable=False),
            TableColumnConfig(table, "generation", "BIGINT", nullable=False),
            TableColumnConfig(table, "erased_count", "BIGINT", nullable=False),
            TableColumnConfig(table, "rebuilt_count", "BIGINT", nullable=False),
            TableColumnConfig(table, "tombstone_count", "BIGINT", nullable=False),
            TableColumnConfig(table, "status", "VARCHAR(32)", nullable=False),
            TableColumnConfig(table, "completed_at", "DATETIME(6)", nullable=False),
        ],
    )


def build_document_access_table_configs(
    config: DocumentAccessStoreConfig,
) -> tuple[TableConfig, TableConfig, TableConfig]:
    documents = TableConfig(
        name=config.documents_table,
        schema=config.schema,
        primary_keys=("document_id",),
        columns=[
            TableColumnConfig(
                config.documents_table, "document_id", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.documents_table, "name", "VARCHAR(255)", nullable=False
            ),
            TableColumnConfig(
                config.documents_table,
                "normalized_name",
                "VARCHAR(255)",
                nullable=False,
                unique_constraint=["document_access_group_name"],
            ),
            TableColumnConfig(config.documents_table, "description", "MEDIUMTEXT"),
            TableColumnConfig(
                config.documents_table, "status", "VARCHAR(16)", nullable=False
            ),
            TableColumnConfig(
                config.documents_table, "revision", "BIGINT", nullable=False
            ),
            TableColumnConfig(
                config.documents_table, "created_by", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.documents_table, "created_at", "DATETIME(6)", nullable=False
            ),
            TableColumnConfig(
                config.documents_table,
                "owning_group",
                "VARCHAR(255)",
                unique_constraint=["document_access_group_name"],
            ),
            TableColumnConfig(
                config.documents_table,
                "generation",
                "BIGINT",
                default_value="1",
                nullable=False,
            ),
            TableColumnConfig(config.documents_table, "archived_by", "VARCHAR(128)"),
            TableColumnConfig(config.documents_table, "archived_at", "DATETIME(6)"),
        ],
    )
    grants = TableConfig(
        name=config.grants_table,
        schema=config.schema,
        primary_keys=("grant_id",),
        columns=[
            TableColumnConfig(
                config.grants_table, "grant_id", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table, "document_id", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table, "group_name", "VARCHAR(255)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table,
                "active_group_key",
                "VARCHAR(512)",
                unique_constraint=["document_access_active_group"],
            ),
            TableColumnConfig(
                config.grants_table, "role", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table, "granted_by", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table, "granted_at", "DATETIME(6)", nullable=False
            ),
            TableColumnConfig(
                config.grants_table, "document_revision", "BIGINT", nullable=False
            ),
            TableColumnConfig(config.grants_table, "revoked_by", "VARCHAR(128)"),
            TableColumnConfig(config.grants_table, "revoked_at", "DATETIME(6)"),
        ],
    )
    commands = TableConfig(
        name=config.commands_table,
        schema=config.schema,
        primary_keys=("idempotency_key",),
        columns=[
            TableColumnConfig(
                config.commands_table, "idempotency_key", "VARCHAR(128)", nullable=False
            ),
            TableColumnConfig(
                config.commands_table, "payload_hash", "VARCHAR(64)", nullable=False
            ),
            TableColumnConfig(
                config.commands_table, "command_kind", "VARCHAR(32)", nullable=False
            ),
            TableColumnConfig(
                config.commands_table, "result_json", "JSON", nullable=False
            ),
            TableColumnConfig(
                config.commands_table, "recorded_at", "DATETIME(6)", nullable=False
            ),
        ],
    )
    return documents, grants, commands


def build_crdt_document_table_config(config: CrdtDocumentStoreConfig) -> TableConfig:
    table = config.documents_table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("document_id",),
        columns=[
            TableColumnConfig(
                table,
                "document_id",
                "VARCHAR(128)",
                nullable=False,
                unique_constraint=["document_source_update_hash"],
            ),
            TableColumnConfig(table, "revision", "BIGINT", nullable=False),
            TableColumnConfig(
                table, "generation", "BIGINT", default_value="1", nullable=False
            ),
            TableColumnConfig(table, "head_state_vector_base64", "MEDIUMTEXT"),
            TableColumnConfig(table, "snapshot_update_base64", "MEDIUMTEXT"),
            TableColumnConfig(table, "snapshot_update_hash", "VARCHAR(64)"),
            TableColumnConfig(table, "snapshot_state_vector_base64", "MEDIUMTEXT"),
            TableColumnConfig(table, "snapshot_through_revision", "BIGINT"),
            TableColumnConfig(table, "updated_at", "DATETIME(6)", nullable=False),
        ],
    )


def build_crdt_update_table_config(config: CrdtDocumentStoreConfig) -> TableConfig:
    table = config.updates_table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("document_id", "revision"),
        columns=[
            TableColumnConfig(
                table,
                "document_id",
                "VARCHAR(128)",
                nullable=False,
                unique_constraint=["document_source_update_hash"],
            ),
            TableColumnConfig(table, "revision", "BIGINT", nullable=False),
            TableColumnConfig(
                table, "generation", "BIGINT", default_value="1", nullable=False
            ),
            TableColumnConfig(
                table,
                "source_update_hash",
                "VARCHAR(64)",
                nullable=False,
                unique_constraint=["document_source_update_hash"],
            ),
            TableColumnConfig(
                table, "accepted_update_hash", "VARCHAR(64)", nullable=False
            ),
            TableColumnConfig(table, "update_base64", "MEDIUMTEXT", nullable=False),
            TableColumnConfig(table, "accepted_at", "DATETIME(6)", nullable=False),
            TableColumnConfig(table, "metadata_json", "JSON"),
        ],
    )


def build_override_table_config(config: OverrideLedgerConfig) -> TableConfig:
    table = config.table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("operation_id",),
        columns=[
            TableColumnConfig(table, "operation_id", "VARCHAR(64)", nullable=False),
            TableColumnConfig(table, "change_set_id", "VARCHAR(64)", nullable=False),
            # Personal rows retain an owner; shared rows are scoped by layer_id.
            TableColumnConfig(table, "owner_user_id", "VARCHAR(128)"),
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
            # Nullable fields preserve compatibility with existing personal rows.
            TableColumnConfig(table, "format_version", "INT"),
            TableColumnConfig(table, "layer_id", "VARCHAR(128)"),
            TableColumnConfig(table, "actor_id", "VARCHAR(128)"),
            TableColumnConfig(table, "actor_display_name", "VARCHAR(255)"),
            TableColumnConfig(table, "supersedes_operation_ids_json", "JSON"),
            TableColumnConfig(table, "removes_operation_ids_json", "JSON"),
            TableColumnConfig(table, "recorded_at", "DATETIME(6)"),
            TableColumnConfig(table, "payload_hash", "VARCHAR(64)"),
            TableColumnConfig(table, "crdt_document_id", "VARCHAR(128)"),
            TableColumnConfig(table, "crdt_document_revision", "BIGINT"),
            TableColumnConfig(
                table, "generation", "BIGINT", default_value="1", nullable=False
            ),
        ],
    )


def build_override_valid_time_config(config: OverrideLedgerConfig) -> ValidTimeConfig:
    return ValidTimeConfig(
        schema=config.schema,
        table=config.table,
        from_column=config.valid_from_column,
        to_column=config.valid_to_column,
    )


def migrate_override_owner_nullable(
    connection: Connection, config: OverrideLedgerConfig
) -> None:
    """Relax the legacy personal-owner column for shared projection rows."""

    inspector = inspect(connection)
    if not inspector.has_table(config.table, schema=config.schema):
        return
    owner_column = next(
        column
        for column in inspector.get_columns(config.table, schema=config.schema)
        if column["name"] == "owner_user_id"
    )
    if owner_column["nullable"]:
        return
    table_config = build_override_table_config(config)
    column = next(
        column
        for column in table_config.build_sqlalchemy_columns(is_delta_table=False)
        if column.name == "owner_user_id"
    )
    column_sql = str(CreateColumn(column).compile(connection))
    connection.execute(
        text(f"ALTER TABLE {config.schema}.{config.table} MODIFY COLUMN {column_sql}")
    )
