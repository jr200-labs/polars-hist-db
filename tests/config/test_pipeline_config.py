from polars_hist_db.config import TableConfigs
from polars_hist_db.config.dataset import (
    Pipeline,
    PipelineColumn,
    PipelineExtractColumn,
)


def test_pipeline_reuses_cached_lookups():
    pipeline = Pipeline(
        [
            {
                "schema": "ref",
                "table": "countries",
                "columns": [{"source": "country", "target": "name"}],
            },
            {
                "schema": "sample",
                "table": "events",
                "type": "primary",
                "columns": [
                    {"source": "id", "target": "id", "required": True},
                    {"source": "country", "target": "country_id"},
                ],
            },
        ]
    )

    extract_items = pipeline.extract_items(1)
    header_map = pipeline.get_header_map("events")
    header_map["mutated"] = "outside"

    assert pipeline.get_main_table_name() == ("sample", "events")
    assert isinstance(pipeline.items, tuple)
    assert all(isinstance(column, PipelineColumn) for column in pipeline.items)
    assert pipeline.get_table_names() == ["countries", "events"]
    assert pipeline.get_pipeline_items() == {
        0: ("ref", "countries"),
        1: ("sample", "events"),
    }
    assert pipeline.item_type("countries") == "extract"
    assert pipeline.get_header_map("events") == {"id": "id", "country_id": "country"}
    assert pipeline.extract_items(1) is extract_items
    assert extract_items == (
        PipelineExtractColumn("events", "id", "id", required=True),
        PipelineExtractColumn("events", "country", "country_id"),
    )


def test_pipeline_builds_normalized_table_metadata_without_dataframes():
    pipeline = Pipeline(
        [
            {
                "schema": "ref",
                "table": "countries",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country", "target": "name"},
                ],
            },
            {
                "schema": "sample",
                "table": "events",
                "type": "primary",
                "columns": [
                    {"source": "id", "target": "id", "required": True},
                    {"source": "country_id", "target": "country_id"},
                    {"source": "raw_date", "ingestion_data_type": "VARCHAR(10)"},
                    {
                        "target": "event_date",
                        "column_type": "time_partition_only",
                        "target_data_type": "DATE",
                        "transforms": {"parse_date": []},
                    },
                ],
            },
        ]
    )
    tables = TableConfigs(
        items=[
            {
                "schema": "ref",
                "name": "countries",
                "primary_keys": ["id"],
                "columns": [
                    {"name": "id", "data_type": "INT", "nullable": False},
                    {"name": "name", "data_type": "VARCHAR(64)"},
                ],
            },
            {
                "schema": "sample",
                "name": "events",
                "primary_keys": ["id"],
                "columns": [
                    {"name": "id", "data_type": "INT", "nullable": False},
                    {"name": "country_id", "data_type": "INT"},
                ],
            },
        ]
    )

    definitions = {
        (column.schema, column.source or column.target): column
        for column in pipeline.build_ingestion_column_definitions(tables)
    }
    delta_columns = pipeline.build_delta_table_column_configs(tables, "upload")

    assert definitions[("ref", "country_id")].deduce_foreign_key is True
    assert definitions[("ref", "country")].target_data_type == "VARCHAR(64)"
    assert definitions[("sample", "id")].required is True
    assert definitions[("sample", "raw_date")].column_type == "dsv_only"
    assert definitions[("sample", "raw_date")].table is None
    assert definitions[("sample", "event_date")].transforms == {"parse_date": []}
    assert definitions[("sample", "event_date")].table is None
    assert [(column.name, column.data_type) for column in delta_columns] == [
        ("country", "VARCHAR(64)"),
        ("id", "INT"),
        ("country_id", "INT"),
    ]
