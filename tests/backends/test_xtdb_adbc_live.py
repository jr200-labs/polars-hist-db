from contextlib import contextmanager
from collections.abc import Iterator
from datetime import datetime, timezone
import os
import subprocess
import time
from typing import Any

import polars as pl
import pytest
from sqlalchemy import text

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.core import TimeHint


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("POLARS_HIST_DB_XTDB_LIVE") != "1",
        reason="set POLARS_HIST_DB_XTDB_LIVE=1 to run live XTDB tests",
    ),
]


try:
    import adbc_driver_flightsql  # noqa: F401
    import psycopg  # noqa: F401
except ImportError:
    pytestmark = [
        *pytestmark,
        pytest.mark.skip(reason="install the xtdb extra to run live XTDB ADBC tests"),
    ]


def _docker(args: list[str]) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def _published_port(container_id: str, container_port: str) -> int:
    output = _docker(["port", container_id, container_port])
    first_binding = output.splitlines()[0]
    return int(first_binding.rsplit(":", 1)[1])


@contextmanager
def _xtdb_adbc_connection() -> Iterator[Any]:
    container_id = _docker(
        ["run", "--rm", "-d", "-p", "9832", "ghcr.io/xtdb/xtdb:nightly"]
    )
    connection = None
    try:
        backend = XtdbBackend()
        config = DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            adbc_port=_published_port(container_id, "9832/tcp"),
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                connection = backend.create_adbc_connection(config)
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetch_arrow_table()
                break
            except Exception:
                if connection is not None:
                    connection.close()
                    connection = None
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)

        yield connection
    finally:
        if connection is not None:
            connection.close()
        subprocess.run(["docker", "rm", "-f", container_id], check=False)


@contextmanager
def _xtdb_pgwire_and_adbc_connections() -> Iterator[tuple[Any, Any]]:
    container_id = _docker(
        [
            "run",
            "--rm",
            "-d",
            "-p",
            "5432",
            "-p",
            "9832",
            "ghcr.io/xtdb/xtdb:nightly",
        ]
    )
    engine = None
    adbc_connection = None
    try:
        backend = XtdbBackend()
        config = DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            port=_published_port(container_id, "5432/tcp"),
            adbc_port=_published_port(container_id, "9832/tcp"),
        )
        engine = backend.create_engine(config)
        deadline = time.monotonic() + 60
        while True:
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                adbc_connection = backend.create_adbc_connection(config)
                with adbc_connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetch_arrow_table()
                break
            except Exception:
                if adbc_connection is not None:
                    adbc_connection.close()
                    adbc_connection = None
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)

        with engine.connect() as pgwire_connection:
            yield pgwire_connection, adbc_connection
    finally:
        if adbc_connection is not None:
            adbc_connection.close()
        if engine is not None:
            engine.dispose()
        subprocess.run(["docker", "rm", "-f", container_id], check=False)


def test_xtdb_adbc_live_arrow_ingest_read_roundtrip():
    table_config = TableConfig(
        schema="public",
        name=f"live_adbc_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_adbc_connection() as connection:
        backend = XtdbBackend()
        ops = backend.adbc_dataframes(connection)
        ops.table_insert(
            pl.DataFrame(
                {
                    "id": [1, 2],
                    "destination": ["Alpha", "Beta"],
                    "amount_value": [10.5, 20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            table_config=table_config,
        )

        result = ops.from_table(table_config.schema, table_config.name)

    assert result.sort("_id").select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "amount_value": [10.5, 20.25],
    }


def test_xtdb_adbc_live_staging_partition_roundtrip():
    stream_name = f"live_adbc_stage_records_{int(time.time())}"
    delta_table_config = TableConfig(
        schema="sample",
        name=stream_name,
        columns=[
            TableColumnConfig(stream_name, "record_id", "BIGINT", nullable=False),
            TableColumnConfig(stream_name, "destination", "VARCHAR(255)"),
            TableColumnConfig(stream_name, "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_pgwire_and_adbc_connections() as (
        pgwire_connection,
        adbc_connection,
    ):
        backend = XtdbBackend(max_rows_per_insert=2)
        staging = backend.staging(
            pgwire_connection,
            adbc_connection=adbc_connection,
        )
        stage_config = staging.ensure_table(delta_table_config)

        inserted_count = staging.insert_partition(
            pl.DataFrame(
                {
                    "record_id": [1, 2, 3],
                    "destination": ["Alpha", "Beta", "Gamma"],
                    "amount_value": [10.5, 20.25, 30.75],
                }
            ),
            delta_table_config,
            "stage-1",
            datetime(2030, 1, 1, tzinfo=timezone.utc),
            uniqueness_col_set=["record_id"],
            prefill_nulls_with_default=True,
        )
        persisted = backend.dataframes(pgwire_connection).from_table(
            stage_config.schema,
            stage_config.name,
        )

    assert inserted_count == 3
    assert persisted.sort("record_id").select(
        ["record_id", "destination", "amount_value", "stage_run_id"]
    ).to_dict(as_series=False) == {
        "record_id": [1, 2, 3],
        "destination": ["Alpha", "Beta", "Gamma"],
        "amount_value": [10.5, 20.25, 30.75],
        "stage_run_id": ["stage-1", "stage-1", "stage-1"],
    }


def test_xtdb_adbc_live_temporal_upsert_supports_system_time_asof():
    table_config = TableConfig(
        schema="public",
        name=f"live_adbc_temporal_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_adbc_connection() as connection:
        backend = XtdbBackend()
        ops = backend.adbc_dataframes(connection)
        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Alpha"],
                    "amount_value": [10.5],
                }
            ),
            table_config.schema,
            table_config.name,
            dataframe_ops=ops,
            table_config=table_config,
        )

        time.sleep(0.2)
        checkpoint = datetime.now(timezone.utc)
        time.sleep(0.2)

        backend.temporal_upsert(
            pl.DataFrame(
                {
                    "id": [1],
                    "destination": ["Beta"],
                    "amount_value": [20.25],
                }
            ),
            table_config.schema,
            table_config.name,
            dataframe_ops=ops,
            table_config=table_config,
        )

        latest = ops.from_table(table_config.schema, table_config.name)
        asof_checkpoint = ops.from_table(
            table_config.schema,
            table_config.name,
            time_hint=TimeHint(mode="asof", asof_utc=checkpoint),
        )

    assert latest.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }
    assert asof_checkpoint.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }
