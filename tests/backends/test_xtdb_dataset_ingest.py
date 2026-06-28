from datetime import datetime, timezone
from types import SimpleNamespace

import polars as pl

from polars_hist_db.config import (
    DatasetConfig,
    TableConfigs,
    ValidTimeConfig,
)
from polars_hist_db.dataset.extract_item import scrape_extract_item
from polars_hist_db.dataset.primary_item import scrape_primary_item


class _FakeTableConfigOps:
    def __init__(self):
        self.created = []

    def create(self, table_config):
        self.created.append(table_config)
        return table_config


class _FakeXtdbBackend:
    name = "xtdb"

    def __init__(self):
        self.table_config_ops = _FakeTableConfigOps()
        self.temporal_upsert_calls = []

    def table_configs(self, connection):
        return self.table_config_ops

    def temporal_upsert(self, *args, **kwargs):
        self.temporal_upsert_calls.append((args, kwargs))
        return 1


class _FakeStagingOps:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.prepare_calls = []

    def prepare_pipeline_item_dataframe(self, *args, **kwargs):
        self.prepare_calls.append((args, kwargs))
        return self.dataframe


def test_xtdb_primary_ingest_uses_staged_dataframe_for_temporal_upsert():
    tables = TableConfigs(
        items=[
            {
                "schema": "fakedata",
                "name": "cargos",
                "primary_keys": ["cargo_id"],
                "columns": [
                    {"name": "cargo_id", "data_type": "INT", "nullable": False},
                    {"name": "destination", "data_type": "VARCHAR(64)"},
                ],
                "is_temporal": True,
            }
        ]
    )
    cargo_table = tables["cargos"]
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
        valid_time=[
            {
                "schema": "fakedata",
                "table": "cargos",
                "from_column": "msg_timestamp",
            }
        ],
        pipeline=[
            {
                "schema": "fakedata",
                "table": "cargos",
                "type": "primary",
                "columns": [
                    {"source": "cargo_id", "target": "cargo_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    msg_timestamp = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    update_time = datetime(2030, 1, 1, 12, 5, tzinfo=timezone.utc)
    staged_df = pl.DataFrame(
        {
            "cargo_id": [1],
            "destination": ["Tokyo"],
            "msg_timestamp": [msg_timestamp],
        }
    )
    backend = _FakeXtdbBackend()
    staging = _FakeStagingOps(staged_df)

    did_modify = scrape_primary_item(
        0,
        dataset,
        tables,
        update_time,
        SimpleNamespace(),
        stage_run_id="stage-1",
        staging=staging,
        backend=backend,
    )

    assert did_modify is True
    assert backend.table_config_ops.created == [cargo_table]
    assert staging.prepare_calls == [
        (
            ("stage-1", dataset, 0, cargo_table),
            {
                "valid_time": ValidTimeConfig(
                    schema="fakedata",
                    table="cargos",
                    from_column="msg_timestamp",
                )
            },
        )
    ]
    args, kwargs = backend.temporal_upsert_calls[0]
    assert args[1:] == ("fakedata", "cargos")
    assert args[0].to_dict(as_series=False) == {
        "cargo_id": [1],
        "destination": ["Tokyo"],
        "msg_timestamp": [msg_timestamp],
    }
    assert kwargs["table_config"] == cargo_table
    assert kwargs["delta_config"] == dataset.delta_config
    assert kwargs["update_time"] == update_time
    assert kwargs["valid_time"] == ValidTimeConfig(
        schema="fakedata",
        table="cargos",
        from_column="msg_timestamp",
    )


def test_xtdb_extract_ingest_uses_staged_dataframe_for_temporal_upsert():
    tables = TableConfigs(
        items=[
            {
                "schema": "spire",
                "name": "vectors",
                "primary_keys": ["mmsi"],
                "columns": [
                    {"name": "mmsi", "data_type": "INT", "nullable": False},
                    {"name": "latitude", "data_type": "DOUBLE"},
                ],
                "is_temporal": True,
            },
            {
                "schema": "spire",
                "name": "vessel_info",
                "primary_keys": ["mmsi"],
                "columns": [
                    {"name": "mmsi", "data_type": "INT", "nullable": False},
                    {"name": "name", "data_type": "VARCHAR(64)"},
                ],
            },
        ]
    )
    vessel_info = tables["vessel_info"]
    dataset = DatasetConfig(
        name="spire_stream",
        delta_table_schema="spire",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "spire",
                "table": "vectors",
                "type": "primary",
                "columns": [
                    {"source": "source_mmsi", "target": "mmsi"},
                    {"source": "source_latitude", "target": "latitude"},
                ],
            },
            {
                "schema": "spire",
                "table": "vessel_info",
                "columns": [
                    {"source": "source_mmsi", "target": "mmsi"},
                    {"source": "source_name", "target": "name"},
                ],
            },
        ],
    )
    update_time = datetime(2030, 1, 1, 12, 5, tzinfo=timezone.utc)
    staged_df = pl.DataFrame(
        {
            "mmsi": [123],
            "name": ["LNG Test"],
        }
    )
    backend = _FakeXtdbBackend()
    staging = _FakeStagingOps(staged_df)

    did_modify = scrape_extract_item(
        1,
        dataset,
        "vessel_info",
        tables,
        update_time,
        SimpleNamespace(),
        stage_run_id="stage-1",
        staging=staging,
        backend=backend,
    )

    assert did_modify is True
    assert backend.table_config_ops.created == [vessel_info]
    assert staging.prepare_calls == [
        (("stage-1", dataset, 1, vessel_info), {"valid_time": None})
    ]
    args, kwargs = backend.temporal_upsert_calls[0]
    assert args[1:] == ("spire", "vessel_info")
    assert args[0].to_dict(as_series=False) == {
        "mmsi": [123],
        "name": ["LNG Test"],
    }
    assert kwargs["table_config"] == vessel_info
    assert kwargs["delta_config"] == dataset.delta_config
    assert kwargs["update_time"] == update_time
    assert kwargs["valid_time"] is None
