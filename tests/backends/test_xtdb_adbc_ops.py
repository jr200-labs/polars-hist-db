from unittest.mock import Mock

import polars as pl
import pyarrow as pa

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.backends.xtdb import XtdbAdbcDataframeOps
from polars_hist_db.config import TableColumnConfig, TableConfig


class _FakeCursor:
    def __init__(self):
        self.ingests = []
        self.executed = []
        self.arrow_result = pa.table({"_id": [1], "destination": ["Tokyo"]})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def adbc_ingest(self, table_name, arrow_table, mode, **kwargs):
        self.ingests.append((table_name, arrow_table, mode, kwargs))

    def execute(self, query):
        self.executed.append(query)

    def fetch_arrow_table(self):
        return self.arrow_result


class _FakeAdbcConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()

    def cursor(self):
        return self.cursor_instance


def _cargo_config(schema="public"):
    return TableConfig(
        schema=schema,
        name="cargos",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("cargos", "id", "BIGINT", nullable=False),
            TableColumnConfig("cargos", "destination", "VARCHAR(255)"),
        ],
    )


def test_xtdb_adbc_dataframe_ops_ingests_arrow_with_id_mapping():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    result = ops.table_insert(
        pl.DataFrame({"id": [1, 2], "destination": ["Tokyo", "Osaka"]}),
        "public",
        "cargos",
        table_config=_cargo_config(),
    )

    assert result == 2
    table_name, arrow_table, mode, kwargs = connection.cursor_instance.ingests[0]
    assert table_name == "cargos"
    assert mode == "create_append"
    assert kwargs == {"db_schema_name": "public"}
    assert arrow_table.schema.field("destination").type == pa.string()
    assert arrow_table.to_pydict() == {
        "_id": [1, 2],
        "destination": ["Tokyo", "Osaka"],
    }


def test_xtdb_adbc_dataframe_ops_reads_arrow_into_polars():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    result = ops.from_table("public", "cargos")

    assert result.to_dict(as_series=False) == {"_id": [1], "destination": ["Tokyo"]}
    assert connection.cursor_instance.executed == ["SELECT * FROM public.cargos"]


def test_xtdb_adbc_dataframe_ops_targets_configured_schema():
    connection = _FakeAdbcConnection()
    ops = XtdbAdbcDataframeOps(connection)

    ops.table_insert(
        pl.DataFrame({"id": [1]}),
        "analytics",
        "cargos",
        table_config=_cargo_config(schema="analytics"),
    )

    assert connection.cursor_instance.ingests[0][3] == {"db_schema_name": "analytics"}


def test_xtdb_backend_builds_adbc_uri():
    uri = XtdbBackend().adbc_uri(
        DbEngineConfig(backend="xtdb", hostname="127.0.0.1", adbc_port=19832)
    )

    assert uri == "grpc://127.0.0.1:19832"


def test_xtdb_backend_creates_adbc_connection(monkeypatch):
    fake_flight_sql = Mock()
    fake_flight_sql.connect.return_value = object()
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb._load_flight_sql",
        lambda: fake_flight_sql,
    )

    config = DbEngineConfig(backend="xtdb", hostname="127.0.0.1", adbc_port=19832)
    result = XtdbBackend().create_adbc_connection(config)

    assert result is fake_flight_sql.connect.return_value
    fake_flight_sql.connect.assert_called_once_with(
        "grpc://127.0.0.1:19832",
        autocommit=True,
    )
