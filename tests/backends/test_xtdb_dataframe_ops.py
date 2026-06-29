from unittest.mock import Mock
from datetime import datetime, timezone
from types import SimpleNamespace

import polars as pl

from polars_hist_db.backends.xtdb import (
    XtdbDataframeOps,
    XtdbTableConfigOps,
    _execute_xtdb_dml,
)
from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.core import TimeHint


def test_xtdb_dataframe_ops_reads_raw_sql_with_schema_overrides(monkeypatch):
    expected_df = pl.DataFrame({"id": [1]})
    read_database = Mock(return_value=expected_df)
    monkeypatch.setattr(pl, "read_database", read_database)

    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.from_raw_sql("select * from test.cargos", {"id": pl.Int64})

    assert result is expected_df
    read_database.assert_called_once_with(
        "select * from test.cargos",
        connection,
        schema_overrides={"id": pl.Int64},
    )


def test_xtdb_dml_retries_with_append_time_when_system_time_is_too_old():
    class _DriverConnection:
        def __init__(self):
            self.executed = []
            self.rollback_count = 0
            self.commit_count = 0

        def execute(self, sql):
            self.executed.append(sql)

        def commit(self):
            self.commit_count += 1
            if self.commit_count == 1:
                raise RuntimeError("invalid-system-time: specified system-time older")

        def rollback(self):
            self.rollback_count += 1

    class _Connection:
        def __init__(self):
            self.connection = SimpleNamespace(driver_connection=_DriverConnection())
            self.info = {}

        def in_transaction(self):
            return False

    connection = _Connection()

    row_count = _execute_xtdb_dml(
        connection,
        "INSERT INTO test.cargos (_id) VALUES (1)",
        system_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert row_count == 0
    assert connection.connection.driver_connection.rollback_count == 1
    assert connection.connection.driver_connection.executed == [
        "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2025-01-01T00:00:00+00:00')",
        "INSERT INTO test.cargos (_id) VALUES (1)",
        "BEGIN READ WRITE",
        "INSERT INTO test.cargos (_id) VALUES (1)",
    ]


def test_xtdb_dataframe_ops_reads_table_as_sql():
    connection = object()
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"id": [1]}))

    result = ops.from_table("test", "cargos", {"id": pl.Int64})

    assert result.to_dict(as_series=False) == {"id": [1]}
    ops.from_raw_sql.assert_called_once_with(
        "SELECT * FROM test.cargos",
        {"id": pl.Int64},
    )


def test_xtdb_dataframe_ops_applies_table_time_hint():
    connection = object()
    ops = XtdbDataframeOps(connection)
    ops.from_raw_sql = Mock(return_value=pl.DataFrame({"id": [1]}))

    result = ops.from_table(
        "test",
        "cargos",
        {"id": pl.Int64},
        time_hint=TimeHint(
            mode="asof",
            asof_utc=datetime.fromisoformat("2026-01-02T03:04:05+00:00"),
        ),
    )

    assert result.to_dict(as_series=False) == {"id": [1]}
    ops.from_raw_sql.assert_called_once_with(
        "SELECT * FROM test.cargos FOR SYSTEM_TIME AS OF '2026-01-02T03:04:05+00:00'",
        {"id": pl.Int64},
    )


def test_xtdb_dataframe_ops_reads_table_with_configured_schema_overrides(monkeypatch):
    read_database = Mock(return_value=pl.DataFrame())
    monkeypatch.setattr(pl, "read_database", read_database)
    table_config = TableConfig(
        schema="test",
        name="cargos",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("cargos", "id", "BIGINT", nullable=False),
            TableColumnConfig("cargos", "destination_date", "DATETIME"),
            TableColumnConfig("cargos", "cargo_mcm", "DOUBLE"),
        ],
    )
    monkeypatch.setattr(
        XtdbTableConfigOps,
        "from_table",
        lambda self, table_schema, table_name: table_config,
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    ops.from_table("test", "cargos")

    read_database.assert_called_once_with(
        "SELECT * FROM test.cargos",
        connection,
        schema_overrides={
            "_id": pl.Int64,
            "destination_date": pl.Datetime(),
            "cargo_mcm": pl.Float64,
            "_valid_from": pl.Datetime("us", "UTC"),
            "_valid_to": pl.Datetime("us", "UTC"),
        },
    )


def test_xtdb_dataframe_ops_writes_dataframe_via_polars_write_database():
    df = Mock()
    df.write_database.return_value = 7
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "cargos")

    assert result == 7
    df.write_database.assert_called_once_with(
        table_name="test.cargos",
        connection=connection,
        if_table_exists="append",
    )


def test_xtdb_dataframe_ops_maps_configured_primary_key_to_id(monkeypatch):
    captured = {}

    def write_database(self, **kwargs):
        captured["df"] = self
        captured["kwargs"] = kwargs
        return 2

    monkeypatch.setattr(pl.DataFrame, "write_database", write_database)

    df = pl.DataFrame({"id": [1, 2], "destination": ["A", "B"]})
    table_config = TableConfig(
        schema="test",
        name="cargos",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("cargos", "id", "BIGINT", nullable=False),
            TableColumnConfig("cargos", "destination", "VARCHAR(255)"),
        ],
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "cargos", table_config=table_config)

    assert result == 2
    written_df = captured["df"]
    assert written_df.to_dict(as_series=False) == {
        "_id": [1, 2],
        "destination": ["A", "B"],
    }
    assert captured["kwargs"] == {
        "table_name": "test.cargos",
        "connection": connection,
        "if_table_exists": "append",
    }


def test_xtdb_dataframe_ops_maps_composite_primary_key_to_synthetic_id(monkeypatch):
    captured = {}

    def write_database(self, **kwargs):
        captured["df"] = self
        captured["kwargs"] = kwargs
        return 2

    monkeypatch.setattr(pl.DataFrame, "write_database", write_database)

    df = pl.DataFrame(
        {
            "vessel_id": [10, 10],
            "cargo_id": ["A", "B"],
            "destination": ["Tokyo", "Osaka"],
        }
    )
    table_config = TableConfig(
        schema="test",
        name="cargos",
        primary_keys=["vessel_id", "cargo_id"],
        columns=[
            TableColumnConfig("cargos", "vessel_id", "BIGINT", nullable=False),
            TableColumnConfig("cargos", "cargo_id", "VARCHAR(255)", nullable=False),
            TableColumnConfig("cargos", "destination", "VARCHAR(255)"),
        ],
    )
    connection = object()
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(df, "test", "cargos", table_config=table_config)

    assert result == 2
    written_df = captured["df"]
    assert written_df.to_dict(as_series=False) == {
        "_id": [
            'xtdb-pk-v1:[["vessel_id",10],["cargo_id","A"]]',
            'xtdb-pk-v1:[["vessel_id",10],["cargo_id","B"]]',
        ],
        "vessel_id": [10, 10],
        "cargo_id": ["A", "B"],
        "destination": ["Tokyo", "Osaka"],
    }
    assert captured["kwargs"] == {
        "table_name": "test.cargos",
        "connection": connection,
        "if_table_exists": "append",
    }


def test_xtdb_dataframe_ops_uses_system_time_transaction_for_update_time():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)

    result = ops.table_insert(
        pl.DataFrame({"_id": [1], "destination": ["Tokyo"]}),
        "test",
        "cargos",
        update_time=datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert result == 1
    assert driver_connection.execute.call_args_list[0].args == (
        "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2030-01-01T12:00:00+00:00')",
    )


def test_xtdb_dml_advances_reused_system_time_on_same_connection():
    from polars_hist_db.backends.xtdb import _execute_xtdb_dml

    class _DriverConnection:
        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)

        def commit(self):
            self.statements.append("COMMIT")

        def rollback(self):
            self.statements.append("ROLLBACK")

    class _Connection:
        def __init__(self):
            self.connection = SimpleNamespace(driver_connection=_DriverConnection())
            self.info = {}

        def in_transaction(self):
            return False

    connection = _Connection()
    system_time = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)

    _execute_xtdb_dml(
        connection, "CREATE TABLE test.one (_id)", system_time=system_time
    )
    _execute_xtdb_dml(
        connection, "CREATE TABLE test.two (_id)", system_time=system_time
    )

    begin_statements = [
        statement
        for statement in connection.connection.driver_connection.statements
        if statement.startswith("BEGIN READ WRITE")
    ]
    assert begin_statements == [
        "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '2030-01-01T12:00:00+00:00')",
        "BEGIN READ WRITE WITH "
        "(SYSTEM_TIME = TIMESTAMP '2030-01-01T12:00:00.000001+00:00')",
    ]


def test_xtdb_dataframe_ops_uses_native_casts_for_mysql_compatibility_types():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="compat_types",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("compat_types", "id", "INT", nullable=False),
            TableColumnConfig("compat_types", "bool_col", "BOOL"),
            TableColumnConfig("compat_types", "bit_col", "BIT"),
            TableColumnConfig("compat_types", "tinyint_col", "TINYINT"),
            TableColumnConfig("compat_types", "smallint_col", "SMALLINT"),
            TableColumnConfig("compat_types", "mediumint_col", "MEDIUMINT"),
            TableColumnConfig("compat_types", "datetime_col", "DATETIME"),
            TableColumnConfig("compat_types", "time_col", "TIME"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "bool_col": [True],
                "bit_col": [1],
                "tinyint_col": [2],
                "smallint_col": [738221],
                "mediumint_col": [4],
                "datetime_col": [datetime(2030, 1, 1, 12, 0)],
                "time_col": [datetime(2030, 1, 1, 12, 30).time()],
            }
        ),
        "test",
        "compat_types",
        table_config=table_config,
    )

    insert_sql = driver_connection.execute.call_args_list[1].args[0]
    assert "TRUE::BOOLEAN" in insert_sql
    assert "1::INTEGER" in insert_sql
    assert "2::INTEGER" in insert_sql
    assert "738221::INTEGER" in insert_sql
    assert "4::INTEGER" in insert_sql
    assert "'2030-01-01T12:00:00'::TIMESTAMP" in insert_sql
    assert "'12:30:00'::TIME" in insert_sql


def test_xtdb_dataframe_ops_casts_null_values_to_configured_types():
    driver_connection = Mock()
    connection = Mock()
    connection.connection.driver_connection = driver_connection
    connection.in_transaction.return_value = False
    ops = XtdbDataframeOps(connection)
    table_config = TableConfig(
        schema="test",
        name="cargos",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("cargos", "id", "BIGINT", nullable=False),
            TableColumnConfig("cargos", "destination_date", "DATETIME"),
            TableColumnConfig("cargos", "cargo_mcm", "DOUBLE"),
        ],
    )

    ops.table_insert(
        pl.DataFrame(
            {
                "id": [1],
                "destination_date": [None],
                "cargo_mcm": [None],
            }
        ),
        "test",
        "cargos",
        table_config=table_config,
    )

    insert_sql = driver_connection.execute.call_args_list[1].args[0]
    assert "NULL::TIMESTAMP" in insert_sql
    assert "NULL::DOUBLE PRECISION" in insert_sql


def test_xtdb_backend_returns_xtdb_dataframe_ops():
    from polars_hist_db.backends import XtdbBackend

    connection = object()
    ops = XtdbBackend().dataframes(connection)

    assert isinstance(ops, XtdbDataframeOps)
