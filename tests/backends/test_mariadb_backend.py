from polars_hist_db.backends import MariaDbBackend
from polars_hist_db.core import DataframeOps, TableConfigOps, TableOps
from polars_hist_db.overrides import CrdtDocumentStoreConfig, OverrideLedgerConfig


def test_mariadb_backend_returns_existing_operation_wrappers():
    backend = MariaDbBackend()
    connection = object()

    assert isinstance(backend.dataframes(connection), DataframeOps)
    assert isinstance(backend.table_configs(connection), TableConfigOps)
    assert isinstance(backend.tables("schema", "table", connection), TableOps)


def test_mariadb_backend_builds_crdt_document_store(monkeypatch):
    class Store:
        def __init__(self, connection, document_store, projection):
            self.connection = connection
            self.document_store = document_store
            self.projection = projection

    monkeypatch.setattr("polars_hist_db.overrides.sql.MariaDbCrdtDocumentStore", Store)
    store = MariaDbBackend().crdt_documents(
        object(), CrdtDocumentStoreConfig(), OverrideLedgerConfig()
    )

    assert isinstance(store, Store)
