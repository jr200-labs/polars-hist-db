from datetime import datetime, timezone
from unittest.mock import Mock

import polars as pl

from polars_hist_db.backends.xtdb import XtdbStagingOps
from polars_hist_db.config import (
    DatasetConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)


def test_xtdb_staging_insert_partition_uses_retained_stage_table(monkeypatch):
    created_configs = []
    inserted = {}

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbTableConfigOps.create",
        lambda self, table_config: created_configs.append(table_config) or table_config,
    )

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )

    delta_table_config = TableConfig(
        schema="fakedata",
        name="cargo_stream",
        columns=[
            TableColumnConfig("cargo_stream", "cargo_id", "INT"),
            TableColumnConfig("cargo_stream", "destination_name", "VARCHAR(64)"),
        ],
    )
    partition_time = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    staging = XtdbStagingOps(object())

    staging.ensure_table(delta_table_config)
    inserted_count = staging.insert_partition(
        pl.DataFrame(
            {
                "cargo_id": [1, 1],
                "destination_name": ["Osaka", "Tokyo"],
            }
        ),
        delta_table_config,
        "stage-1",
        partition_time,
        uniqueness_col_set=["cargo_id"],
        prefill_nulls_with_default=True,
    )

    assert inserted_count == 1
    stage_config = created_configs[0]
    assert stage_config.schema == "fakedata"
    assert stage_config.name == "__cargo_stream_stage"
    assert list(stage_config.primary_keys) == ["stage_run_id", "stage_row_index"]
    assert inserted["table_schema"] == "fakedata"
    assert inserted["table_name"] == "__cargo_stream_stage"
    assert inserted["table_config"].name == "__cargo_stream_stage"
    assert inserted["df"].to_dict(as_series=False) == {
        "stage_row_index": [0],
        "cargo_id": [1],
        "destination_name": ["Tokyo"],
        "stage_run_id": ["stage-1"],
        "stage_partition_time": [partition_time],
    }


def test_xtdb_staging_projects_pipeline_item_with_valid_time_mapping(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "cargo_id": [1],
            "destination_name": ["Tokyo"],
            "msg_timestamp": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
            "ignored": ["not written"],
        }
    )
    from_raw_sql = Mock(return_value=stage_df)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        from_raw_sql,
    )
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="fakedata",
        input_config={"type": "dsv", "search_paths": []},
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
    table_config = TableConfig(
        schema="fakedata",
        name="cargos",
        primary_keys=["cargo_id"],
        columns=[
            TableColumnConfig("cargos", "cargo_id", "INT"),
            TableColumnConfig("cargos", "destination", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=ValidTimeConfig(table="cargos", from_column="msg_timestamp"),
    )

    assert result.to_dict(as_series=False) == {
        "cargo_id": [1],
        "destination": ["Tokyo"],
        "msg_timestamp": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
    }
    from_raw_sql.assert_called_once_with(
        "SELECT * FROM fakedata.__cargo_stream_stage "
        "WHERE stage_run_id = 'stage-1'::TEXT"
    )


def test_xtdb_staging_cleanup_deletes_only_batch_run():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    delta_table_config = TableConfig(
        schema="fakedata",
        name="cargo_stream",
        columns=[
            TableColumnConfig("cargo_stream", "cargo_id", "INT"),
        ],
    )

    XtdbStagingOps(connection).cleanup_run("stage-1", delta_table_config)

    assert driver_connection.execute.call_args.args == (
        "DELETE FROM fakedata.__cargo_stream_stage "
        "WHERE stage_run_id = 'stage-1'::TEXT",
    )


def test_xtdb_staging_deduces_foreign_keys_and_inserts_missing_parent(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": [None],
            "country_name": ["Japan"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "country_id": pl.String,
            "country_name": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "_id": pl.String,
            "country_id": pl.String,
            "name": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    generated_id = 'xtdb-fk-v1:ref.countries:[["name","Japan"]]'
    assert inserted["table_schema"] == "ref"
    assert inserted["table_name"] == "countries"
    assert inserted["table_config"] == table_config
    assert inserted["df"].to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }
    assert result.to_dict(as_series=False) == {
        "country_id": [generated_id],
        "name": ["Japan"],
    }


def test_xtdb_staging_deduces_explicit_numeric_foreign_keys(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "origin_location_id": [1001],
            "origin_name": ["Bonny"],
            "origin_country": ["Nigeria"],
        }
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        inserted["table_schema"] = table_schema
        inserted["table_name"] = table_name
        inserted["table_config"] = table_config
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="kpler",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "kpler",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "origin_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "origin_name", "target": "name"},
                    {"source": "origin_country", "target": "country"},
                ],
            },
            {
                "schema": "kpler",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "origin_location_id", "target": "origin_location_id"},
                ],
            },
        ],
    )
    table_config = TableConfig(
        schema="kpler",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert inserted["table_schema"] == "kpler"
    assert inserted["table_name"] == "location_info"
    assert inserted["table_config"] == table_config
    assert inserted["df"].to_dict(as_series=False) == {
        "id": [1001],
        "name": ["Bonny"],
        "country": ["Nigeria"],
    }
    assert result.to_dict(as_series=False) == {
        "id": [1001],
        "name": ["Bonny"],
        "country": ["Nigeria"],
    }


def test_xtdb_staging_generates_numeric_foreign_key_when_source_key_is_missing(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "destination_location_id": [None],
            "destination_name": ["Southern Europe"],
            "destination_country": ["#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    inserted = {}

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted["df"] = df
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="kpler",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "kpler",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "kpler",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "destination_location_id",
                    },
                ],
            },
        ],
    )
    table_config = TableConfig(
        schema="kpler",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    generated_id = result[0, "id"]
    assert isinstance(generated_id, int)
    assert generated_id < 0
    assert inserted["df"].to_dict(as_series=False) == {
        "id": [generated_id],
        "name": ["Southern Europe"],
        "country": ["#Unspecified"],
    }
    assert result.to_dict(as_series=False) == {
        "id": [generated_id],
        "name": ["Southern Europe"],
        "country": ["#Unspecified"],
    }


def test_xtdb_staging_reuses_deduced_foreign_key_for_later_primary_item(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "trade_id": [308327],
            "destination_location_id": [None],
            "destination_name": ["#Unspecified"],
            "destination_country": ["#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "trade_id": pl.Int64,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        lambda self, df, table_schema, table_name, table_config=None: df.height,
    )
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="kpler",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "kpler",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "kpler",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "trade_id", "target": "id"},
                    {
                        "source": "destination_location_id",
                        "target": "destination_location_id",
                    },
                ],
            },
        ],
    )
    location_config = TableConfig(
        schema="kpler",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )
    trades_config = TableConfig(
        schema="kpler",
        name="trades",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("trades", "id", "INT", nullable=False),
            TableColumnConfig("trades", "destination_location_id", "INT"),
        ],
    )
    staging = XtdbStagingOps(object())

    location_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        location_config,
        valid_time=None,
    )
    trades_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        1,
        trades_config,
        valid_time=None,
    )

    generated_id = location_df[0, "id"]
    assert trades_df.to_dict(as_series=False) == {
        "id": [308327],
        "destination_location_id": [generated_id],
    }


def test_xtdb_staging_does_not_insert_same_generated_parent_twice(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "origin_location_id": [None],
            "origin_name": ["#Unspecified"],
            "origin_country": ["#Unspecified"],
            "destination_location_id": [None],
            "destination_name": ["#Unspecified"],
            "destination_country": ["#Unspecified"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "origin_location_id": pl.Int64,
            "origin_name": pl.String,
            "origin_country": pl.String,
            "destination_location_id": pl.Int64,
            "destination_name": pl.String,
            "destination_country": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        schema={
            "id": pl.Int64,
            "name": pl.String,
            "country": pl.String,
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    inserted = []

    def table_insert(self, df, table_schema, table_name, table_config=None):
        inserted.append(df)
        return df.height

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="cargo_stream",
        delta_table_schema="kpler",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "kpler",
                "table": "location_info",
                "type": "extract",
                "columns": [
                    {
                        "source": "origin_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "origin_name", "target": "name"},
                    {"source": "origin_country", "target": "country"},
                ],
            },
            {
                "schema": "kpler",
                "table": "location_info",
                "columns": [
                    {
                        "source": "destination_location_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "destination_name", "target": "name"},
                    {"source": "destination_country", "target": "country"},
                ],
            },
            {
                "schema": "kpler",
                "table": "trades",
                "type": "primary",
                "columns": [
                    {"source": "origin_location_id", "target": "origin_location_id"},
                ],
            },
        ],
    )
    location_config = TableConfig(
        schema="kpler",
        name="location_info",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("location_info", "id", "INT", nullable=False),
            TableColumnConfig("location_info", "name", "VARCHAR(64)"),
            TableColumnConfig("location_info", "country", "VARCHAR(64)"),
        ],
    )
    staging = XtdbStagingOps(object())

    origin_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        location_config,
        valid_time=None,
    )
    destination_df = staging.prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        1,
        location_config,
        valid_time=None,
    )

    assert len(inserted) == 1
    assert origin_df[0, "id"] == inserted[0][0, "id"]
    assert destination_df.is_empty()


def test_xtdb_staging_deduces_existing_foreign_key_without_insert(monkeypatch):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": [None],
            "country_name": ["Japan"],
        },
        schema={
            "stage_run_id": pl.String,
            "stage_row_index": pl.Int64,
            "country_id": pl.String,
            "country_name": pl.String,
        },
    )
    parent_df = pl.DataFrame(
        {
            "country_id": ["country:japan"],
            "name": ["Japan"],
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    table_insert = Mock(return_value=0)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    table_insert.assert_not_called()
    assert result.to_dict(as_series=False) == {
        "country_id": ["country:japan"],
        "name": ["Japan"],
    }


def test_xtdb_staging_deduces_foreign_keys_with_null_typed_empty_parent(
    monkeypatch,
):
    stage_df = pl.DataFrame(
        {
            "stage_run_id": ["stage-1"],
            "stage_row_index": [0],
            "country_id": ["country:japan"],
            "country_name": ["Japan"],
        }
    )
    parent_df = pl.DataFrame(
        {
            "country_id": pl.Series("country_id", [], dtype=pl.Null),
            "name": pl.Series("name", [], dtype=pl.Null),
        }
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_raw_sql",
        Mock(return_value=stage_df),
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.from_table",
        Mock(return_value=parent_df),
    )
    table_insert = Mock(return_value=1)
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.XtdbDataframeOps.table_insert",
        table_insert,
    )
    dataset = DatasetConfig(
        name="country_stream",
        delta_table_schema="ref",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "ref",
                "table": "countries",
                "type": "primary",
                "columns": [
                    {
                        "source": "country_id",
                        "target": "country_id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "country_name", "target": "name"},
                ],
            }
        ],
    )
    table_config = TableConfig(
        schema="ref",
        name="countries",
        primary_keys=["country_id"],
        columns=[
            TableColumnConfig("countries", "country_id", "VARCHAR(128)"),
            TableColumnConfig("countries", "name", "VARCHAR(64)"),
        ],
    )

    result = XtdbStagingOps(object()).prepare_pipeline_item_dataframe(
        "stage-1",
        dataset,
        0,
        table_config,
        valid_time=None,
    )

    assert result.to_dict(as_series=False) == {
        "country_id": ["country:japan"],
        "name": ["Japan"],
    }
