# polars-hist-db

A Python library for building bitemporal data pipelines with [Polars](https://pola.rs/),
MariaDB, and optional XTDB backend support.

It ingests data from DSV files or [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) subjects, tracks history through a typed temporal table contract, and exposes everything as strongly-typed Polars DataFrames.

### Features

- **Typed uploads** — push Polars DataFrames into MariaDB or XTDB with automatic type mapping between Polars, SQL, and SQLAlchemy/ADBC.
- **Typed queries** — read tables back into DataFrames with column types inferred from the configured backend schema. Temporal query hints (`asof`, `span`, `all`) let you slice history without writing SQL.
- **YAML-driven pipelines** — define scrape specifications that handle column typing, enrichment via custom transform functions, normalization across tables, and foreign-key deduction.
- **Deduplication** — an audit log tracks what has already been ingested so re-runs skip previously processed files or messages.
- **Transactional ingestion** — each file or message is processed in its own transaction; failures roll back cleanly without affecting other items.
- **Dual input sources** — crawl directories for DSV files, or consume messages from NATS JetStream with configurable fetch/subscription strategies.
- **Delta upserts** — staging tables detect changed rows, handle duplicates (`take_first`, `take_last`, `error`), and optionally mark disappeared rows as dropped.

Full documentation: [jr200.github.io/polars-hist-db](https://jr200.github.io/polars-hist-db)

## Quick Start

```bash
uv sync
make test
```

## Development

Development and CI use Python 3.15, selected by `.python-version`; uv installs
that interpreter automatically. The library continues to support Python 3.12+.
With the current lockfile, Python 3.15 builds `pycrdt`, `scandir-rs`, and the
optional ADBC driver manager from source. Install a Rust toolchain supporting
the 2024 edition and a C/C++ compiler before running
`uv sync --group dev --extra xtdb`.

```bash
make check       # ruff + mypy
make docs        # render and preview quarto docs
make bump PART=patch
make release
```

## License

This project is licensed under the [MIT](LICENSE) license.
