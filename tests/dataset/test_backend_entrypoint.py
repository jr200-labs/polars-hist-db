from types import SimpleNamespace

import pytest

from polars_hist_db.backends import DbEngineConfig
from polars_hist_db.dataset import run_datasets
from polars_hist_db.dataset.entrypoint import _create_config_tables


class _FakeBackend:
    def __init__(self):
        self.created_with = None

    def create_engine(self, config):
        self.created_with = config
        return object()


class _ContextManager:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self):
        self.connection = _FakeConnection()
        self.used_begin = False
        self.used_connect = False

    def begin(self):
        self.used_begin = True
        return _ContextManager(self.connection)

    def connect(self):
        self.used_connect = True
        return _ContextManager(self.connection)


class _FakeConnection:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True


class _FakeTableConfigOps:
    def __init__(self):
        self.created_all = None

    def create_all(self, tables):
        self.created_all = tables


class _FakeBackendWithTableConfigOps:
    def __init__(self, name="mariadb"):
        self.name = name
        self.ops = _FakeTableConfigOps()
        self.connection = None

    def table_configs(self, connection):
        self.connection = connection
        return self.ops


class _FailingInputSource:
    cleaned = False

    async def next_df(self, engine):
        raise RuntimeError("input failed")

    async def cleanup(self):
        type(self).cleaned = True


@pytest.mark.asyncio
async def test_run_datasets_creates_engine_from_config_when_engine_is_omitted(
    monkeypatch,
):
    fake_backend = _FakeBackend()

    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint.backend_from_config",
        lambda config: fake_backend,
    )

    config = SimpleNamespace(
        db_config=DbEngineConfig(backend="mariadb"),
        datasets=SimpleNamespace(datasets=[]),
    )

    await run_datasets(config)

    assert fake_backend.created_with == config.db_config


@pytest.mark.asyncio
async def test_run_datasets_can_raise_input_source_errors(monkeypatch):
    backend = _FakeBackendWithTableConfigOps()
    engine = _FakeEngine()
    dataset = SimpleNamespace(
        name="bad_dataset",
        input_config=SimpleNamespace(type="dsv"),
    )
    config = SimpleNamespace(
        db_config=DbEngineConfig(backend="mariadb"),
        datasets=SimpleNamespace(datasets=[dataset]),
        tables=object(),
    )
    _FailingInputSource.cleaned = False

    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint.backend_from_config",
        lambda config: backend,
    )
    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint._build_delta_table_config",
        lambda tables, dataset: object(),
    )
    monkeypatch.setattr(
        "polars_hist_db.dataset.entrypoint.InputSourceFactory.create_input_source",
        lambda *args, **kwargs: _FailingInputSource(),
    )

    with pytest.raises(RuntimeError, match="input failed"):
        await run_datasets(config, engine, raise_on_error=True)

    assert _FailingInputSource.cleaned is True


def test_create_config_tables_uses_selected_backend_table_config_ops():
    engine = _FakeEngine()
    backend = _FakeBackendWithTableConfigOps()
    tables = object()

    _create_config_tables(engine, tables, backend)

    assert backend.connection is engine.connection
    assert backend.ops.created_all is tables
    assert engine.used_begin is True
    assert engine.used_connect is False
    assert engine.connection.committed is False


def test_create_config_tables_uses_plain_connection_for_xtdb():
    engine = _FakeEngine()
    backend = _FakeBackendWithTableConfigOps(name="xtdb")
    tables = object()

    _create_config_tables(engine, tables, backend)

    assert backend.connection is engine.connection
    assert backend.ops.created_all is tables
    assert engine.used_begin is False
    assert engine.used_connect is True
    assert engine.connection.committed is True
