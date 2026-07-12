from polars_hist_db.overrides import (
    OverrideLedgerConfig,
    OverrideTypedValue,
    build_override_table_config,
    build_override_valid_time_config,
)


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
        "format_version",
        "layer_id",
        "actor_id",
        "supersedes_operation_ids_json",
        "removes_operation_ids_json",
        "recorded_at",
        "payload_hash",
        "crdt_document_id",
        "crdt_document_revision",
    }


def test_build_override_valid_time_config_uses_business_columns():
    valid_time = build_override_valid_time_config(OverrideLedgerConfig())

    assert valid_time.schema == "overrides"
    assert valid_time.table == "data_override_operations"
    assert valid_time.from_column == "valid_from"
    assert valid_time.to_column == "valid_to"
