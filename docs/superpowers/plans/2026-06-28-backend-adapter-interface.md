# Backend Adapter Interface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a pluggable historical database backend interface with MariaDB as the default complete backend and XTDB as an experimental adapter.

**Architecture:** Add a `polars_hist_db.backends` package with config, protocol, registry, MariaDB adapter, and XTDB adapter. Keep existing concrete MariaDB operation classes working, and have the MariaDB adapter delegate to them. Parse `db` YAML into a first-class `DbEngineConfig` so callers can select `mariadb`, `xtdb`, or future `mssql`.

**Tech Stack:** Python 3.12, Polars, SQLAlchemy, PyMySQL for MariaDB, optional PostgreSQL-wire SQLAlchemy connection for XTDB, pytest.

---

### Task 1: Parse Database Backend Config

**Files:**
- Create: `tests/config/test_db_engine_config.py`
- Modify: `polars_hist_db/config/config.py`
- Modify: `polars_hist_db/config/__init__.py`

- [x] **Step 1: Write failing tests**

```python
from polars_hist_db.config import PolarsHistDbConfig
from polars_hist_db.backends import DbEngineConfig


def test_config_parses_db_section():
    config = PolarsHistDbConfig.from_yaml("tests/_data/config/simple.yaml")

    assert isinstance(config.db_config, DbEngineConfig)
    assert config.db_config.backend == "mariadb"
    assert config.db_config.hostname == "127.0.0.1"
    assert config.db_config.username == "root"
    assert config.db_config.password == "admin"


def test_config_defaults_to_mariadb_when_db_section_missing():
    config = PolarsHistDbConfig(
        {"table_configs": [], "datasets": []},
        table_configs_path=["table_configs"],
        datasets_path=["datasets"],
    )

    assert config.db_config.backend == "mariadb"
    assert config.db_config.port == 3306
```

- [x] **Step 2: Run tests and verify RED**

Run: `uv run pytest tests/config/test_db_engine_config.py -q`

Expected: fails because `polars_hist_db.backends` or `db_config` does not exist.

- [x] **Step 3: Add `DbEngineConfig` and config parsing**

Create `polars_hist_db/backends/config.py` with a dataclass that parses `backend`, `hostname`, `port`, `username`, `password`, `ssl_config`, `pool_size`, and `max_overflow`. Update `PolarsHistDbConfig` to set `self.db_config`.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run pytest tests/config/test_db_engine_config.py -q`

Expected: passes.

### Task 2: Add Backend Protocol And Registry

**Files:**
- Create: `tests/backends/test_registry.py`
- Create: `polars_hist_db/backends/base.py`
- Create: `polars_hist_db/backends/registry.py`
- Create: `polars_hist_db/backends/__init__.py`
- Create: `polars_hist_db/backends/mariadb.py`
- Create: `polars_hist_db/backends/xtdb.py`

- [x] **Step 1: Write failing tests**

```python
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
```

- [x] **Step 2: Run tests and verify RED**

Run: `uv run pytest tests/backends/test_registry.py -q`

Expected: fails because backend modules do not exist.

- [x] **Step 3: Implement registry and adapter shells**

Define `HistoricalDbBackend` as a `Protocol`. Implement `MariaDbBackend` and `XtdbBackend` dataclasses with `name` fields. Implement `get_backend` and `backend_from_config`.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run pytest tests/backends/test_registry.py -q`

Expected: passes.

### Task 3: Delegate MariaDB Adapter To Existing Operations

**Files:**
- Create: `tests/backends/test_mariadb_backend.py`
- Modify: `polars_hist_db/backends/mariadb.py`

- [x] **Step 1: Write failing tests**

```python
from polars_hist_db.backends import MariaDbBackend
from polars_hist_db.core import DataframeOps, TableConfigOps, TableOps


def test_mariadb_backend_returns_existing_operation_wrappers():
    backend = MariaDbBackend()
    connection = object()

    assert isinstance(backend.dataframes(connection), DataframeOps)
    assert isinstance(backend.table_configs(connection), TableConfigOps)
    assert isinstance(backend.tables("schema", "table", connection), TableOps)
```

- [x] **Step 2: Run tests and verify RED**

Run: `uv run pytest tests/backends/test_mariadb_backend.py -q`

Expected: fails because adapter methods are missing.

- [x] **Step 3: Implement delegation methods**

Add `dataframes(connection)`, `table_configs(connection)`, and `tables(schema, table, connection)` methods to `MariaDbBackend`.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run pytest tests/backends/test_mariadb_backend.py -q`

Expected: passes.

### Task 4: Add Explicit XTDB Experimental Unsupported Methods

**Files:**
- Create: `tests/backends/test_xtdb_backend.py`
- Modify: `polars_hist_db/backends/xtdb.py`

- [x] **Step 1: Write failing tests**

```python
import pytest

from polars_hist_db.backends import XtdbBackend


def test_xtdb_temporal_upsert_is_explicitly_unsupported():
    backend = XtdbBackend()

    with pytest.raises(NotImplementedError, match="XTDB backend does not yet implement temporal upsert"):
        backend.temporal_upsert(None)
```

- [x] **Step 2: Run tests and verify RED**

Run: `uv run pytest tests/backends/test_xtdb_backend.py -q`

Expected: fails because `temporal_upsert` is missing.

- [x] **Step 3: Implement explicit unsupported method**

Add `temporal_upsert(*args, **kwargs)` to `XtdbBackend` raising the tested `NotImplementedError`.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run pytest tests/backends/test_xtdb_backend.py -q`

Expected: passes.

### Task 5: Document Backend Selection

**Files:**
- Modify: `docs/backend-contract.qmd`
- Modify: `docs/configuration.qmd`
- Modify: `docs/api.qmd`

- [x] **Step 1: Update docs**

Describe the `db.backend` selection model, default `mariadb`, experimental `xtdb`, and future `mssql`.

- [x] **Step 2: Verify docs source links**

Run: `rg "backend: mariadb|xtdb|mssql|backend_from_config|DbEngineConfig" docs polars_hist_db`

Expected: source references exist and no generated `_site` edit is required.

### Task 6: Run Focused Verification

**Files:**
- No new files.

- [x] **Step 1: Run backend/config tests**

Run: `uv run pytest tests/config/test_db_engine_config.py tests/backends -q`

Expected: all pass.

- [x] **Step 2: Run static checks for touched package**

Run: `uv run ruff check polars_hist_db tests/config tests/backends`

Expected: no errors.

- [x] **Step 3: Review git diff**

Run: `git diff --stat && git diff -- polars_hist_db tests docs`

Expected: changes are limited to backend interface, tests, and docs.

### Task 7: Wire Config-Selected Engine Entry Points

**Files:**
- Create: `tests/config/test_config_engine_factory.py`
- Create: `tests/dataset/test_backend_entrypoint.py`
- Modify: `polars_hist_db/config/config.py`
- Modify: `polars_hist_db/dataset/entrypoint.py`

- [x] **Step 1: Write failing tests**

Add tests proving `PolarsHistDbConfig.create_engine()` creates an engine via the selected backend and `run_datasets(config)` creates an engine when none is passed.

- [x] **Step 2: Run tests and verify RED**

Run: `uv run python -m pytest tests/config/test_config_engine_factory.py tests/dataset/test_backend_entrypoint.py -q`

Expected: fails because `create_engine` and backend-aware entrypoint wiring are missing.

- [x] **Step 3: Implement config-selected engine creation**

Add `PolarsHistDbConfig.create_engine()` and make `run_datasets` accept `engine=None`, creating the configured backend engine when omitted.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run python -m pytest tests/config/test_config_engine_factory.py tests/dataset/test_backend_entrypoint.py -q`

Expected: passes.

### Task 8: Add XTDB Typed Dataframe Spike

**Files:**
- Create: `tests/backends/test_xtdb_dataframe_ops.py`
- Modify: `polars_hist_db/backends/xtdb.py`
- Modify: `docs/api.qmd`
- Modify: `docs/backend-contract.qmd`

- [x] **Step 1: Write failing tests**

Add tests for `XtdbDataframeOps.from_raw_sql`, `from_table`, `table_insert`, and `XtdbBackend.dataframes`.

- [x] **Step 2: Run tests and verify RED**

Run: `uv run python -m pytest tests/backends/test_xtdb_dataframe_ops.py -q`

Expected: fails because `XtdbDataframeOps` is missing.

- [x] **Step 3: Implement minimal XTDB dataframe helper**

Add `XtdbDataframeOps` using `polars.read_database` for reads and `DataFrame.write_database` for append writes. Keep temporal upsert explicitly unsupported.

- [x] **Step 4: Run tests and verify GREEN**

Run: `uv run python -m pytest tests/backends/test_xtdb_dataframe_ops.py tests/backends/test_xtdb_backend.py -q`

Expected: passes.

- [x] **Step 5: Run full verification**

Run:

```bash
uv run python -m pytest -q
uv run ruff check polars_hist_db tests/config tests/backends tests/dataset/test_backend_entrypoint.py
uv run mypy polars_hist_db
```

Expected: all pass.
