from polars_hist_db.config import DatasetConfig, ValidTimeConfig
from polars_hist_db.config.config import ParityConfig


def _dataset_config(**overrides):
    config = {
        "name": "record_stream",
        "delta_table_schema": "fakedata",
        "input_config": {"type": "dsv", "search_paths": []},
        "pipeline": [
            {
                "table": "records",
                "schema": "fakedata",
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                ],
            }
        ],
    }
    config.update(overrides)
    return DatasetConfig(**config)


def test_dataset_config_parses_valid_time_mappings():
    dataset = _dataset_config(
        valid_time=[
            {
                "schema": "fakedata",
                "table": "records",
                "from_column": "msg_timestamp",
                "to_column": "valid_until",
            }
        ]
    )

    assert dataset.valid_time == [
        ValidTimeConfig(
            schema="fakedata",
            table="records",
            from_column="msg_timestamp",
            to_column="valid_until",
        )
    ]
    assert dataset.valid_time_for_table("fakedata", "records") == dataset.valid_time[0]


def test_dataset_valid_time_lookup_allows_schema_omission():
    dataset = _dataset_config(
        valid_time=[
            {
                "table": "records",
                "from_column": "_asof_dt",
            }
        ]
    )

    assert dataset.valid_time_for_table("fakedata", "records") == ValidTimeConfig(
        table="records",
        from_column="_asof_dt",
    )
    assert dataset.valid_time_for_table("other", "entities") is None


def test_parity_config_parses_ignore_columns_and_semantic_foreign_keys():
    parity = ParityConfig(
        ignore_columns=["sample.location_info.id"],
        semantic_foreign_keys=[
            {
                "source": "sample.trades.origin_location_id",
                "target": "sample.location_info.id",
                "columns": ["name", "country"],
            }
        ],
    )

    assert parity.ignore_columns == ("sample.location_info.id",)
    assert parity.semantic_foreign_keys[0].source == (
        "sample.trades.origin_location_id"
    )
    assert parity.semantic_foreign_keys[0].target == "sample.location_info.id"
    assert parity.semantic_foreign_keys[0].columns == ("name", "country")
