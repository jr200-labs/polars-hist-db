from __future__ import annotations

from polars_hist_db.config import TableColumnConfig, TableConfig, ValidTimeConfig

from .types import CrdtDocumentStoreConfig, OverrideLedgerConfig


def build_crdt_document_table_config(config: CrdtDocumentStoreConfig) -> TableConfig:
    table = config.documents_table
    return TableConfig(
        name=table,
        schema=config.schema,
        primary_keys=("document_id",),
        columns=[
            TableColumnConfig(table, "document_id", "VARCHAR(128)", nullable=False),
            TableColumnConfig(table, "revision", "BIGINT", nullable=False),
            TableColumnConfig(table, "state_vector_base64", "MEDIUMTEXT"),
            TableColumnConfig(table, "snapshot_update_base64", "MEDIUMTEXT"),
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
                unique_constraint=["document_update_hash"],
            ),
            TableColumnConfig(table, "revision", "BIGINT", nullable=False),
            TableColumnConfig(
                table,
                "update_hash",
                "VARCHAR(64)",
                nullable=False,
                unique_constraint=["document_update_hash"],
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
            # Nullable fields preserve compatibility with existing personal rows.
            TableColumnConfig(table, "format_version", "INT"),
            TableColumnConfig(table, "layer_id", "VARCHAR(128)"),
            TableColumnConfig(table, "actor_id", "VARCHAR(128)"),
            TableColumnConfig(table, "supersedes_operation_ids_json", "JSON"),
            TableColumnConfig(table, "removes_operation_ids_json", "JSON"),
            TableColumnConfig(table, "recorded_at", "DATETIME(6)"),
            TableColumnConfig(table, "payload_hash", "VARCHAR(64)"),
        ],
    )


def build_override_valid_time_config(config: OverrideLedgerConfig) -> ValidTimeConfig:
    return ValidTimeConfig(
        schema=config.schema,
        table=config.table,
        from_column=config.valid_from_column,
        to_column=config.valid_to_column,
    )
