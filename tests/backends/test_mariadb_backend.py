from polars_hist_db.backends import MariaDbBackend
from polars_hist_db.core import DataframeOps, TableConfigOps, TableOps


def test_mariadb_backend_returns_existing_operation_wrappers():
    backend = MariaDbBackend()
    connection = object()

    assert isinstance(backend.dataframes(connection), DataframeOps)
    assert isinstance(backend.table_configs(connection), TableConfigOps)
    assert isinstance(backend.tables("schema", "table", connection), TableOps)
