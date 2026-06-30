# Backend Adapter Interface Design

## Goal

Extract a storage backend interface for `polars-hist-db` so MariaDB remains the
default complete backend, XTDB can be added experimentally, and future backends
such as MS SQL can be introduced without changing downstream API callers again.

## Context

The current implementation exposes storage operations through concrete classes
such as `DataframeOps`, `TableOps`, `TableConfigOps`, `DeltaTableOps`, `AuditOps`,
and `TimeHint`. Those classes work today, but their implementation is tightly
coupled to MariaDB and SQLAlchemy.

The YAML configuration already has a `db` section with `backend: mariadb`, but
`PolarsHistDbConfig` does not currently parse that section into a first-class
database configuration object. That means backend selection exists in the config
shape but not in the runtime model.

## Design

Add a new `polars_hist_db.backends` package containing:

- `DbEngineConfig`: parsed database connection and backend settings.
- `HistoricalDbBackend`: protocol for the backend surface used by ingestion and
  API services.
- `BackendName`: supported backend names: `mariadb`, `xtdb`, and future `mssql`.
- `get_backend(name)`: registry lookup with clear validation errors.
- `backend_from_config(config)`: construct the backend selected by
  `DbEngineConfig`.
- `MariaDbBackend`: complete adapter that delegates to the existing MariaDB
  operations rather than duplicating them.
- `XtdbBackend`: experimental adapter with engine creation and explicitly
  unsupported temporal/delta/audit methods until the spike proves parity.

`PolarsHistDbConfig.create_engine()` should create an engine through the
selected backend. `run_datasets(config)` should also create the configured
engine when the caller does not pass one, while continuing to accept the old
`run_datasets(config, engine)` shape.

The existing concrete operation classes stay in place for compatibility. The
first pass introduces the backend layer beside them and routes new backend-aware
code through the adapter. Later tasks can migrate dataset and API entry points
incrementally.

## Backend Contract

The protocol should cover the real behavioural surface:

- Create an engine or connection handle.
- Create/drop/reflect configured tables.
- Read typed dataframes from tables, selectables, and raw SQL where supported.
- Insert/update/upsert/delete dataframes.
- Apply temporal query modes: latest, all, as-of, and span.
- Record and query audit entries.
- Expose update visibility hooks.

Backends may raise `NotImplementedError` for unsupported contract methods, but
they must fail with backend-specific messages. Silent partial support is not
acceptable.

## Compatibility

MariaDB remains the default when `db` is omitted or `db.backend` is omitted.
Existing direct usage such as `DataframeOps(connection)` and
`TableConfigOps(connection)` continues to work. New backend-aware callers should
prefer the adapter.

## XTDB Scope For WG-238

XTDB is experimental in WG-238. The goal is to establish the adapter path and
typed read/write spike surface, not to claim full temporal parity. Full XTDB
adoption remains gated by WG-239, WG-240, WG-241, and WG-243.

The WG-238 XTDB adapter should expose typed dataframe raw SQL reads, table
reads, and append writes. Temporal upsert, delta/finality, schema management,
audit tracking, and update visibility stay explicitly unsupported until parity
work is complete.

## Testing

Tests must prove:

- Config parsing preserves existing table/dataset parsing and adds `db_config`.
- Missing `db` defaults to MariaDB.
- Backend registry returns MariaDB and XTDB adapters.
- Unknown backend names raise a clear `ValueError`.
- MariaDB adapter delegates to existing operation classes.
- XTDB unsupported contract methods fail explicitly.
