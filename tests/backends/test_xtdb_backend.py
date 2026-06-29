from unittest.mock import Mock
from datetime import date, datetime, time, timezone
from decimal import Decimal
from types import SimpleNamespace

import polars as pl
import pytest

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.backends.xtdb import (
    XtdbDataframeOps,
    XtdbTableConfigOps,
    _xtdb_declared_columns,
)
from polars_hist_db.config import (
    DeltaConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)


def test_xtdb_temporal_upsert_delegates_to_dataframe_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 2
    df = pl.DataFrame({"id": [1, 2], "destination": ["Alpha", "Beta"]})
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        df,
        table_config.schema,
        table_config.name,
        dataframe_ops=ops,
        table_config=table_config,
    )

    assert result == 2
    ops.table_insert.assert_called_once_with(
        df,
        "test",
        "records",
        table_config=table_config,
    )


def test_xtdb_temporal_upsert_passes_system_time_to_dataframe_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    update_time = datetime(2030, 1, 1, tzinfo=timezone.utc)
    df = pl.DataFrame({"id": [1]})

    result = backend.temporal_upsert(
        df,
        "test",
        "records",
        dataframe_ops=ops,
        update_time=update_time,
    )

    assert result == 1
    ops.table_insert.assert_called_once_with(
        df,
        "test",
        "records",
        table_config=None,
        update_time=update_time,
    )


def test_xtdb_temporal_upsert_rejects_manual_finality():
    backend = XtdbBackend()

    with pytest.raises(NotImplementedError, match="row_finality='disabled'"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            delta_config=DeltaConfig(row_finality="manual"),
        )


def test_xtdb_temporal_upsert_dropout_deletes_missing_current_keys():
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(
        return_value=pl.DataFrame(
            {
                "_id": [1, 2],
                "destination": ["Alpha", "Beta"],
            }
        )
    )
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
    )

    assert result == 2
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == "DELETE FROM test.records WHERE _id IN (2::BIGINT)"
    assert executed_sql[3] == (
        "INSERT INTO test.records (_id, destination) VALUES (1::BIGINT, 'Alpha'::TEXT)"
    )


def test_xtdb_temporal_upsert_dropout_closes_missing_keys_at_valid_time():
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"_id": [1, 2]}))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 1, 2, tzinfo=timezone.utc)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
    )

    assert result == 2
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == (
        "DELETE FROM test.records FOR PORTION OF VALID_TIME FROM "
        "TIMESTAMP '2030-01-02T00:00:00+00:00' TO NULL "
        "WHERE _id IN (2::BIGINT)"
    )


def test_xtdb_temporal_upsert_dropout_uses_explicit_close_time_for_empty_batches():
    backend = XtdbBackend()
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"_id": [1, 2]}))
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(schema={"id": pl.Int64, "destination": pl.String}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(row_finality="dropout"),
        dropout_close_time=datetime(2030, 1, 3, tzinfo=timezone.utc),
    )

    assert result == 2
    executed_sql = [call.args[0] for call in driver_connection.execute.call_args_list]
    assert executed_sql[1] == (
        "DELETE FROM test.records FOR PORTION OF VALID_TIME FROM "
        "TIMESTAMP '2030-01-03T00:00:00+00:00' TO NULL "
        "WHERE _id IN (1::BIGINT, 2::BIGINT)"
    )


def test_xtdb_temporal_upsert_rejects_duplicate_source_keys_by_default():
    backend = XtdbBackend()
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate source keys"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            table_config=table_config,
            delta_config=DeltaConfig(),
        )


def test_xtdb_temporal_upsert_takes_first_duplicate_source_key():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(on_duplicate_key="take_first"),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
    }


def test_xtdb_temporal_upsert_takes_last_duplicate_source_key():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1, 1], "destination": ["Alpha", "Beta"]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(on_duplicate_key="take_last"),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Beta"],
    }


def test_xtdb_temporal_upsert_treats_explicit_valid_time_change_as_changed():
    backend = XtdbBackend()
    ops = Mock()
    ops.from_raw_sql.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "_valid_from": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
            "_valid_to": [datetime(2030, 2, 1, tzinfo=timezone.utc)],
        }
    )
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
                "_valid_to": [datetime(2030, 3, 1, tzinfo=timezone.utc)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
        update_time=datetime(2030, 1, 2, tzinfo=timezone.utc),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "_valid_from": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
        "_valid_to": [datetime(2030, 3, 1, tzinfo=timezone.utc)],
    }


def test_xtdb_temporal_upsert_ignores_valid_from_when_filtering_unchanged_rows():
    backend = XtdbBackend()
    ops = Mock()
    ops.from_raw_sql.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "_valid_from": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
        }
    )
    ops.table_insert.return_value = 0
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "_valid_from": [datetime(2030, 2, 1, tzinfo=timezone.utc)],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 0
    ops.table_insert.assert_called_once()
    assert ops.table_insert.call_args.args[0].is_empty()


def test_xtdb_temporal_upsert_normalizes_types_when_filtering_unchanged_rows():
    backend = XtdbBackend()
    ops = Mock()
    ops.from_raw_sql.return_value = pl.DataFrame(
        {
            "_id": [1],
            "destination": ["Alpha"],
            "float_col": [12.34],
        }
    )
    ops.table_insert.return_value = 0
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "INT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "float_col", "FLOAT"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "float_col": [12.34],
            },
            schema_overrides={"float_col": pl.Float32},
        ),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 0
    ops.table_insert.assert_called_once()
    assert ops.table_insert.call_args.args[0].is_empty()


def test_xtdb_temporal_upsert_prefills_configured_defaults_before_insert():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="defaults",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("defaults", "id", "INT", nullable=False),
            TableColumnConfig("defaults", "enabled", "BOOLEAN", default_value="0"),
            TableColumnConfig("defaults", "as_of", "DATE", default_value="1985-10-26"),
            TableColumnConfig("defaults", "cutoff", "TIME", default_value="01:20:00"),
            TableColumnConfig(
                "defaults",
                "price",
                "DECIMAL(10,2)",
                default_value="2.71",
            ),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "enabled": [None],
                "as_of": [None],
                "cutoff": [None],
                "price": [None],
            },
            schema_overrides={
                "id": pl.Int32,
                "enabled": pl.Boolean,
                "as_of": pl.Date,
                "cutoff": pl.Time,
                "price": pl.Decimal(10, 2),
            },
        ),
        "test",
        "defaults",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(prefill_nulls_with_default=True),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "enabled": [False],
        "as_of": [date(1985, 10, 26)],
        "cutoff": [time(1, 20)],
        "price": [Decimal("2.71")],
    }


def test_xtdb_temporal_upsert_treats_null_to_value_as_changed():
    backend = XtdbBackend()
    ops = Mock()
    ops.from_raw_sql.return_value = pl.DataFrame(
        {
            "_id": [1],
            "amount_value": [None],
        },
        schema_overrides={"amount_value": pl.Float64},
    )
    ops.table_insert.return_value = 1
    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    result = backend.temporal_upsert(
        pl.DataFrame({"id": [1], "amount_value": [330.33]}),
        "test",
        "records",
        dataframe_ops=ops,
        table_config=table_config,
        delta_config=DeltaConfig(drop_unchanged_rows=True),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "amount_value": [330.33],
    }


def test_xtdb_temporal_upsert_maps_configured_valid_time_columns():
    backend = XtdbBackend()
    ops = Mock()
    ops.table_insert.return_value = 1
    asof_time = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
    expiry_time = datetime(2030, 2, 1, 12, 0, tzinfo=timezone.utc)

    result = backend.temporal_upsert(
        pl.DataFrame(
            {
                "id": [1],
                "destination": ["Alpha"],
                "msg_timestamp": [asof_time],
                "valid_until": [expiry_time],
            }
        ),
        "test",
        "records",
        dataframe_ops=ops,
        valid_time=ValidTimeConfig(
            table="records",
            from_column="msg_timestamp",
            to_column="valid_until",
        ),
    )

    assert result == 1
    written_df = ops.table_insert.call_args.args[0]
    assert written_df.to_dict(as_series=False) == {
        "id": [1],
        "destination": ["Alpha"],
        "msg_timestamp": [asof_time],
        "valid_until": [expiry_time],
        "_valid_from": [asof_time],
        "_valid_to": [expiry_time],
    }


def test_xtdb_temporal_upsert_rejects_missing_valid_time_source_column():
    backend = XtdbBackend()

    with pytest.raises(ValueError, match="missing source column"):
        backend.temporal_upsert(
            pl.DataFrame({"id": [1], "destination": ["Alpha"]}),
            "test",
            "records",
            dataframe_ops=Mock(),
            valid_time=ValidTimeConfig(
                table="records",
                from_column="msg_timestamp",
            ),
        )


def test_xtdb_temporal_upsert_rejects_valid_time_target_conflict():
    backend = XtdbBackend()

    with pytest.raises(ValueError, match="already contains that column"):
        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "msg_timestamp": [datetime(2030, 1, 1, tzinfo=timezone.utc)],
                    "_valid_from": [datetime(2030, 1, 2, tzinfo=timezone.utc)],
                }
            ),
            "test",
            "records",
            dataframe_ops=Mock(),
            valid_time=ValidTimeConfig(
                table="records",
                from_column="msg_timestamp",
            ),
        )


def test_xtdb_table_creation_maps_mysql_compatibility_types(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="compat_types",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("compat_types", "id", "INT", nullable=False),
            TableColumnConfig("compat_types", "bool_col", "BOOL"),
            TableColumnConfig("compat_types", "bit_col", "BIT"),
            TableColumnConfig("compat_types", "tinyint_col", "TINYINT"),
            TableColumnConfig("compat_types", "mediumint_col", "MEDIUMINT"),
            TableColumnConfig("compat_types", "datetime_col", "DATETIME"),
            TableColumnConfig("compat_types", "time_col", "TIME"),
        ],
    )

    ops.create(table_config)

    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
        "(_id, table_schema, table_name, primary_keys_json, id_policy, "
        "columns_json, foreign_keys_json) "
        "VALUES ('test.compat_types'::TEXT, 'test'::TEXT, 'compat_types'::TEXT, "
        "'[\"id\"]'::TEXT, 'single-key'::TEXT, "
        '\'[{"table":"compat_types","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":false,"nullable":false,'
        '"unique_constraint":[]},{"table":"compat_types","name":"bool_col",'
        '"data_type":"BOOL","default_value":null,"autoincrement":false,'
        '"nullable":true,"unique_constraint":[]},{"table":"compat_types",'
        '"name":"bit_col","data_type":"BIT","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
        '{"table":"compat_types","name":"tinyint_col","data_type":"TINYINT",'
        '"default_value":null,"autoincrement":false,"nullable":true,'
        '"unique_constraint":[]},{"table":"compat_types","name":"mediumint_col",'
        '"data_type":"MEDIUMINT","default_value":null,"autoincrement":false,'
        '"nullable":true,"unique_constraint":[]},{"table":"compat_types",'
        '"name":"datetime_col","data_type":"DATETIME","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
        '{"table":"compat_types","name":"time_col","data_type":"TIME",'
        '"default_value":null,"autoincrement":false,"nullable":true,'
        "\"unique_constraint\":[]}]'::TEXT, '[]'::TEXT)",
    ]
    assert _xtdb_declared_columns(table_config) == [
        "_id",
        "bool_col",
        "bit_col",
        "tinyint_col",
        "mediumint_col",
        "datetime_col",
        "time_col",
    ]
    assert [column.data_type for column in table_config.columns[1:]] == [
        "BOOL",
        "BIT",
        "TINYINT",
        "MEDIUMINT",
        "DATETIME",
        "TIME",
    ]


def test_xtdb_backend_returns_explicit_table_config_ops():
    connection = object()
    ops = XtdbBackend().table_configs(connection)

    assert isinstance(ops, XtdbTableConfigOps)


def test_xtdb_table_config_ops_drop_all_erases_configured_tables(monkeypatch):
    executed = []

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb._execute_xtdb_dml",
        lambda _connection, sql, **_kwargs: executed.append(sql),
    )

    table_config = TableConfig(
        schema="market",
        name="prices",
        primary_keys=["id"],
        columns=[TableColumnConfig("prices", "id", "INT", nullable=False)],
    )

    ops = XtdbTableConfigOps(object())
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=True))

    ops.drop_all(Mock(items=[table_config]))

    assert executed == [
        "ERASE FROM market.prices WHERE TRUE",
        "DELETE FROM market.__polars_hist_db_xtdb_table_configs "
        "WHERE _id = 'market.prices'::TEXT",
    ]


def test_xtdb_table_config_ops_drop_removes_metadata_without_data_table(monkeypatch):
    executed = []

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb._execute_xtdb_dml",
        lambda _connection, sql, **_kwargs: executed.append(sql),
    )

    table_config = TableConfig(
        schema="market",
        name="prices",
        primary_keys=["id"],
        columns=[TableColumnConfig("prices", "id", "INT", nullable=False)],
    )

    ops = XtdbTableConfigOps(object())
    monkeypatch.setattr(
        ops,
        "table_exists",
        Mock(
            side_effect=lambda _schema, table: (
                table == "__polars_hist_db_xtdb_table_configs"
            )
        ),
    )

    ops.drop(table_config)

    assert executed == [
        "DELETE FROM market.__polars_hist_db_xtdb_table_configs "
        "WHERE _id = 'market.prices'::TEXT",
    ]


def test_xtdb_backend_create_engine_targets_xtdb_database_by_default(monkeypatch):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    XtdbBackend().create_engine(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=15432)
    )

    assert calls[0][0] == "postgresql+psycopg://127.0.0.1:15432/xtdb"


def test_xtdb_backend_create_engine_disables_psycopg_prepared_statements(
    monkeypatch,
):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    result = XtdbBackend().create_engine(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=15432)
    )

    assert result is engine
    assert calls[0][1]["connect_args"] == {"prepare_threshold": None}


def test_xtdb_backend_create_engine_uses_configured_database(monkeypatch):
    calls = []
    engine = Mock()
    engine.dialect = SimpleNamespace()

    def fake_create_engine(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb.create_engine", fake_create_engine
    )

    XtdbBackend().create_engine(
        DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            port=15432,
            database="analytics",
        )
    )

    assert calls[0][0] == "postgresql+psycopg://127.0.0.1:15432/analytics"


def test_xtdb_table_creation_records_configured_columns_without_ddl(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DECIMAL(20,6)"),
        ],
    )
    result = ops.create(table_config)

    assert result is table_config
    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
        "(_id, table_schema, table_name, primary_keys_json, id_policy, "
        "columns_json, foreign_keys_json) "
        "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
        "'[\"id\"]'::TEXT, 'single-key'::TEXT, "
        '\'[{"table":"records","name":"id","data_type":"BIGINT",'
        '"default_value":null,"autoincrement":false,"nullable":false,'
        '"unique_constraint":[]},{"table":"records","name":"destination",'
        '"data_type":"VARCHAR(255)","default_value":null,"autoincrement":false,'
        '"nullable":true,"unique_constraint":[]},{"table":"records",'
        '"name":"amount_value","data_type":"DECIMAL(20,6)","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]}]'
        "'::TEXT, '[]'::TEXT)",
    ]


def test_xtdb_table_creation_is_idempotent_when_table_exists(monkeypatch):
    connection = Mock()
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=True))
    existing_config = TableConfig(schema="test", name="records", columns=[])
    monkeypatch.setattr(ops, "from_table", Mock(return_value=existing_config))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["_id"],
        columns=[TableColumnConfig("records", "_id", "BIGINT", nullable=False)],
    )

    result = ops.create(table_config)

    assert result is existing_config
    connection.execute.assert_not_called()
    ops.from_table.assert_called_once_with("test", "records")


def test_xtdb_table_creation_records_composite_primary_key_columns(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    result = ops.create(table_config)

    assert result is table_config
    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert all(not sql.startswith("CREATE TABLE") for sql in executed_sql)
    assert executed_sql == [
        "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
        "(_id, table_schema, table_name, primary_keys_json, id_policy, "
        "columns_json, foreign_keys_json) "
        "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
        "'[\"entity_id\",\"record_id\"]'::TEXT, 'xtdb-pk-v1'::TEXT, "
        '\'[{"table":"records","name":"entity_id","data_type":"BIGINT",'
        '"default_value":null,"autoincrement":false,"nullable":false,'
        '"unique_constraint":[]},{"table":"records","name":"record_id",'
        '"data_type":"BIGINT","default_value":null,"autoincrement":false,'
        '"nullable":false,"unique_constraint":[]},{"table":"records",'
        '"name":"destination","data_type":"VARCHAR(255)","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]}]'
        "'::TEXT, '[]'::TEXT)",
    ]


def test_xtdb_table_creation_records_primary_key_metadata(monkeypatch):
    connection = Mock()
    connection.connection = None
    ops = XtdbTableConfigOps(connection)
    monkeypatch.setattr(ops, "table_exists", Mock(return_value=False))

    table_config = TableConfig(
        schema="test",
        name="records",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "VARCHAR(255)", nullable=False),
        ],
    )

    ops.create(table_config)

    executed_sql = [call.args[0].text for call in connection.execute.call_args_list]
    assert executed_sql == [
        "INSERT INTO test.__polars_hist_db_xtdb_table_configs "
        "(_id, table_schema, table_name, primary_keys_json, id_policy, "
        "columns_json, foreign_keys_json) "
        "VALUES ('test.records'::TEXT, 'test'::TEXT, 'records'::TEXT, "
        "'[\"entity_id\",\"record_id\"]'::TEXT, 'xtdb-pk-v1'::TEXT, "
        '\'[{"table":"records","name":"entity_id","data_type":"BIGINT",'
        '"default_value":null,"autoincrement":false,"nullable":false,'
        '"unique_constraint":[]},{"table":"records","name":"record_id",'
        '"data_type":"VARCHAR(255)","default_value":null,"autoincrement":false,'
        '"nullable":false,"unique_constraint":[]}]\'::TEXT, \'[]\'::TEXT)',
    ]


def test_xtdb_table_reflection_builds_table_config_from_information_schema(
    monkeypatch,
):
    read_database = Mock(
        return_value=pl.DataFrame(
            {
                "column_name": [
                    "_id",
                    "destination",
                    "amount_value",
                    "_system_from",
                    "_system_to",
                ],
                "data_type": [
                    ":i64",
                    ":utf8",
                    "DECIMAL(10,4)",
                    "TIMESTAMP",
                    "TIMESTAMP",
                ],
                "is_nullable": ["NO", "YES", "YES", "YES", "YES"],
            }
        )
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    connection = object()
    ops = XtdbTableConfigOps(connection)

    table_config = ops.from_table("test", "records")

    assert table_config.schema == "test"
    assert table_config.name == "records"
    assert list(table_config.primary_keys) == ["_id"]
    assert [
        (col.name, col.data_type, col.nullable) for col in table_config.columns
    ] == [
        ("_id", "BIGINT", False),
        ("destination", "VARCHAR(255)", True),
        ("amount_value", "DECIMAL(10,4)", True),
    ]
    assert read_database.call_args_list[0].args == (
        """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'test'
              AND table_name = 'records'
            ORDER BY ordinal_position
        """,
        connection,
    )


def test_xtdb_table_reflection_prefers_configured_column_metadata(monkeypatch):
    columns_json = (
        '[{"table":"all_types","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":true,"nullable":false,'
        '"unique_constraint":[]},{"table":"all_types","name":"decimal_col",'
        '"data_type":"DECIMAL(10,2)","default_value":null,'
        '"autoincrement":false,"nullable":true,"unique_constraint":[]},'
        '{"table":"all_types","name":"real_col","data_type":"REAL",'
        '"default_value":null,"autoincrement":false,"nullable":true,'
        '"unique_constraint":[]}]'
    )
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": ["_id", "decimal_col", "real_col"],
                    "data_type": [":i32", "[:DECIMAL 38 2]", ":f32"],
                    "is_nullable": ["NO", "YES", "YES"],
                }
            ),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                }
            ),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "all_types")

    assert list(table_config.primary_keys) == ["id"]
    assert [(col.name, col.data_type) for col in table_config.columns] == [
        ("id", "INT"),
        ("decimal_col", "DECIMAL(10,2)"),
        ("real_col", "REAL"),
    ]


def test_xtdb_table_reflection_restores_foreign_key_metadata(monkeypatch):
    columns_json = (
        '[{"table":"trading_pairs","name":"id","data_type":"INT",'
        '"default_value":null,"autoincrement":true,"nullable":false,'
        '"unique_constraint":[]},{"table":"trading_pairs",'
        '"name":"exchange_id","data_type":"INT","default_value":null,'
        '"autoincrement":false,"nullable":false,"unique_constraint":[]}]'
    )
    foreign_keys_json = (
        '[{"name":"exchange_id","references":{"schema":"test",'
        '"table":"exchanges","column":"id"}}]'
    )
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": ["_id", "exchange_id"],
                    "data_type": [":i32", ":i32"],
                    "is_nullable": ["NO", "NO"],
                }
            ),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["id"]'],
                    "id_policy": ["single-key"],
                    "columns_json": [columns_json],
                    "foreign_keys_json": [foreign_keys_json],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "trading_pairs")

    assert list(table_config.primary_keys) == ["id"]
    assert [fk.name for fk in table_config.foreign_keys] == ["exchange_id"]
    fk = table_config.foreign_keys[0]
    assert fk.references.schema == "test"
    assert fk.references.table == "exchanges"
    assert fk.references.column == "id"


def test_xtdb_table_reflection_recovers_composite_primary_keys_from_metadata(
    monkeypatch,
):
    read_database = Mock(
        side_effect=[
            pl.DataFrame(
                {
                    "column_name": [
                        "_id",
                        "entity_id",
                        "record_id",
                        "destination",
                    ],
                    "data_type": [":utf8", ":i64", ":utf8", ":utf8"],
                    "is_nullable": ["NO", "NO", "NO", "YES"],
                }
            ),
            pl.DataFrame(
                {
                    "primary_keys_json": ['["entity_id","record_id"]'],
                    "id_policy": ["xtdb-pk-v1"],
                }
            ),
        ]
    )
    monkeypatch.setattr(pl, "read_database", read_database)

    table_config = XtdbTableConfigOps(object()).from_table("test", "records")

    assert table_config.schema == "test"
    assert table_config.name == "records"
    assert list(table_config.primary_keys) == ["entity_id", "record_id"]
    assert [
        (col.name, col.data_type, col.nullable) for col in table_config.columns
    ] == [
        ("_id", "VARCHAR(255)", False),
        ("entity_id", "BIGINT", False),
        ("record_id", "VARCHAR(255)", False),
        ("destination", "VARCHAR(255)", True),
    ]


def test_xtdb_table_reflection_errors_when_table_has_no_metadata(monkeypatch):
    monkeypatch.setattr(pl, "read_database", Mock(return_value=pl.DataFrame()))
    ops = XtdbTableConfigOps(object())

    with pytest.raises(
        ValueError, match="XTDB table metadata not found for test.records"
    ):
        ops.from_table("test", "records")


def test_xtdb_declared_columns_quotes_non_identifier_column_names():
    from polars_hist_db.backends.xtdb import _xtdb_declared_columns

    table_config = TableConfig(
        schema="sample",
        name="__record_stage",
        primary_keys=["stage_run_id", "stage_row_index"],
        columns=[
            TableColumnConfig("__record_stage", "stage_run_id", "VARCHAR(128)"),
            TableColumnConfig("__record_stage", "stage_row_index", "BIGINT"),
            TableColumnConfig("__record_stage", "Entity", "VARCHAR(64)"),
            TableColumnConfig("__record_stage", "External Ref (entity)", "VARCHAR(16)"),
            TableColumnConfig("__record_stage", "timestamp", "DATETIME"),
        ],
    )

    assert _xtdb_declared_columns(table_config) == [
        "_id",
        "stage_run_id",
        "stage_row_index",
        '"Entity"',
        '"External Ref (entity)"',
        '"timestamp"',
    ]
