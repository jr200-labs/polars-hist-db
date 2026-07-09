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
