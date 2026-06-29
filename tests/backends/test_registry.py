import pytest

from polars_hist_db.backends import (
    DbEngineConfig,
    MariaDbBackend,
    XtdbBackend,
    backend_from_config,
    get_backend,
)


def test_registry_returns_mariadb_backend_by_default_name():
    backend = get_backend("mariadb")

    assert isinstance(backend, MariaDbBackend)
    assert backend.name == "mariadb"


def test_registry_returns_xtdb_backend():
    backend = get_backend("xtdb")

    assert isinstance(backend, XtdbBackend)
    assert backend.name == "xtdb"


def test_registry_rejects_unknown_backend():
    with pytest.raises(ValueError, match="Unsupported database backend 'oracle'"):
        get_backend("oracle")


def test_backend_from_config_uses_db_engine_config_backend():
    backend = backend_from_config(DbEngineConfig(backend="xtdb"))

    assert isinstance(backend, XtdbBackend)
    assert backend.max_rows_per_insert == 10_000


def test_backend_from_config_passes_xtdb_max_rows_per_insert():
    backend = backend_from_config(
        DbEngineConfig(backend="xtdb", max_rows_per_insert=2500)
    )

    assert isinstance(backend, XtdbBackend)
    assert backend.max_rows_per_insert == 2500
