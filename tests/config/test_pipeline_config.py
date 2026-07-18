from polars_hist_db.config.dataset import Pipeline


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
    assert pipeline.get_table_names() == ["countries", "events"]
    assert pipeline.get_pipeline_items() == {
        0: ("ref", "countries"),
        1: ("sample", "events"),
    }
    assert pipeline.item_type("countries") == "extract"
    assert pipeline.get_header_map("events") == {"id": "id", "country_id": "country"}
    assert pipeline.extract_items(1) is extract_items
    assert extract_items.to_dict(as_series=False) == {
        "table": ["events", "events"],
        "source": ["id", "country"],
        "target": ["id", "country_id"],
        "required": [True, False],
        "deduce_foreign_key": [False, False],
    }
