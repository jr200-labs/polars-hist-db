from contextlib import contextmanager
from datetime import datetime, timezone
import os
import subprocess
import time
from collections.abc import Iterator

import polars as pl
import pytest
from sqlalchemy import Engine, text

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.config import (
    DatasetConfig,
    DeltaConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("POLARS_HIST_DB_XTDB_LIVE") != "1",
        reason="set POLARS_HIST_DB_XTDB_LIVE=1 to run live XTDB tests",
    ),
]


try:
    import psycopg  # noqa: F401
except ImportError:
    pytestmark = [
        *pytestmark,
        pytest.mark.skip(reason="install the xtdb extra to run live XTDB tests"),
    ]


def _docker(args: list[str]) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


def _published_port(container_id: str) -> int:
    output = _docker(["port", container_id, "5432/tcp"])
    first_binding = output.splitlines()[0]
    return int(first_binding.rsplit(":", 1)[1])


@contextmanager
def _xtdb_engine() -> Iterator[Engine]:
    container_id = _docker(
        ["run", "--rm", "-d", "-p", "5432", "ghcr.io/xtdb/xtdb:nightly"]
    )
    engine = None
    try:
        config = DbEngineConfig(
            backend="xtdb",
            hostname="127.0.0.1",
            port=_published_port(container_id),
        )
        engine = XtdbBackend().create_engine(config)
        deadline = time.monotonic() + 60
        while True:
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)

        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        subprocess.run(["docker", "rm", "-f", container_id], check=False)


def test_xtdb_live_create_append_read_roundtrip():
    table_config = TableConfig(
        schema="public",
        name=f"live_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            backend.dataframes(connection).table_insert(
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

            result = backend.dataframes(connection).from_table(
                table_config.schema,
                table_config.name,
            )

    assert result.sort("_id").select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "amount_value": [10.5, 20.25],
    }


def test_xtdb_live_insert_non_public_table_with_reserved_column_name():
    table_config = TableConfig(
        schema="source_a",
        name=f"live_entity_info_{int(time.time())}",
        primary_keys=["entity_id"],
        columns=[
            TableColumnConfig("entity_info", "entity_id", "INT", nullable=False),
            TableColumnConfig("entity_info", "name", "VARCHAR(64)"),
            TableColumnConfig("entity_info", "flag", "VARCHAR(64)"),
        ],
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            backend.dataframes(connection).table_insert(
                pl.DataFrame(
                    {
                        "entity_id": [311038700],
                        "name": ["ALPHA"],
                        "flag": ["BS"],
                    }
                ),
                table_config.schema,
                table_config.name,
                table_config=table_config,
            )

            result = backend.dataframes(connection).from_table(
                table_config.schema,
                table_config.name,
            )

    assert result.sort("_id").select(["_id", "name", "flag"]).to_dict(
        as_series=False
    ) == {
        "_id": [311038700],
        "name": ["ALPHA"],
        "flag": ["BS"],
    }


def test_xtdb_live_composite_primary_key_roundtrip():
    table_config = TableConfig(
        schema="public",
        name=f"live_composite_records_{int(time.time())}",
        primary_keys=["entity_id", "record_id"],
        columns=[
            TableColumnConfig("records", "entity_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "record_id", "VARCHAR(255)", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            backend.dataframes(connection).table_insert(
                pl.DataFrame(
                    {
                        "entity_id": [10, 10],
                        "record_id": ["A", "B"],
                        "destination": ["Alpha", "Beta"],
                    }
                ),
                table_config.schema,
                table_config.name,
                table_config=table_config,
            )

            result = backend.dataframes(connection).from_table(
                table_config.schema,
                table_config.name,
            )
            reflected_config = backend.table_configs(connection).from_table(
                table_config.schema,
                table_config.name,
            )

    assert result.sort("record_id").select(
        ["_id", "entity_id", "record_id", "destination"]
    ).to_dict(as_series=False) == {
        "_id": [
            'xtdb-pk-v1:[["entity_id",10],["record_id","A"]]',
            'xtdb-pk-v1:[["entity_id",10],["record_id","B"]]',
        ],
        "entity_id": [10, 10],
        "record_id": ["A", "B"],
        "destination": ["Alpha", "Beta"],
    }
    assert list(reflected_config.primary_keys) == ["entity_id", "record_id"]


def test_xtdb_live_temporal_upsert_honours_update_time_system_time():
    table_config = TableConfig(
        schema="public",
        name=f"live_temporal_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

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
                connection=connection,
                table_config=table_config,
                update_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
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
                connection=connection,
                table_config=table_config,
                update_time=datetime(2030, 1, 2, tzinfo=timezone.utc),
            )

            table_sql = f"{table_config.schema}.{table_config.name}"

            def read_at(asof: datetime) -> pl.DataFrame:
                timestamp = asof.isoformat()
                return backend.dataframes(connection).from_raw_sql(
                    "SELECT _id, destination, amount_value "
                    f"FROM {table_sql} "
                    f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                    f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
                )

            asof_after_second = read_at(datetime(2030, 1, 3, tzinfo=timezone.utc))
            asof_before_first = read_at(
                datetime(2029, 12, 31, 23, 59, tzinfo=timezone.utc)
            )
            asof_between_updates = read_at(
                datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
            )

    assert asof_before_first.is_empty()
    assert asof_after_second.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }
    assert asof_between_updates.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }


def test_xtdb_live_delta_upsert_drops_unchanged_rows():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(
        drop_unchanged_rows=True,
        row_finality="disabled",
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

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
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
            unchanged_count = backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "destination": ["Alpha"],
                        "amount_value": [10.5],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 2, tzinfo=timezone.utc),
            )
            changed_count = backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "destination": ["Beta"],
                        "amount_value": [20.25],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 3, tzinfo=timezone.utc),
            )

            history = backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value, _system_from "
                f"FROM {table_config.schema}.{table_config.name} "
                "FOR VALID_TIME ALL FOR SYSTEM_TIME ALL "
                "ORDER BY _system_from"
            )
            table_sql = f"{table_config.schema}.{table_config.name}"

            def read_at(asof: datetime) -> pl.DataFrame:
                timestamp = asof.isoformat()
                return backend.dataframes(connection).from_raw_sql(
                    "SELECT _id, destination, amount_value "
                    f"FROM {table_sql} "
                    f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                    f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
                )

            asof_after_unchanged = read_at(
                datetime(2030, 1, 2, 12, 0, tzinfo=timezone.utc)
            )
            asof_after_changed = read_at(datetime(2030, 1, 4, tzinfo=timezone.utc))

    assert unchanged_count == 0
    assert changed_count == 1
    system_from_values = [str(value) for value in history["_system_from"].to_list()]
    assert not any(value.startswith("2030-01-02") for value in system_from_values)
    assert asof_after_unchanged.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }
    assert asof_after_changed.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }


def test_xtdb_live_delta_upsert_takes_last_duplicate_source_key():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_duplicate_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(
        on_duplicate_key="take_last",
        row_finality="disabled",
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            row_count = backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1, 1],
                        "destination": ["Alpha", "Beta"],
                        "amount_value": [10.5, 20.25],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )

            timestamp = datetime(2030, 1, 2, tzinfo=timezone.utc).isoformat()
            result = backend.dataframes(connection).from_raw_sql(
                "SELECT _id, destination, amount_value "
                f"FROM {table_config.schema}.{table_config.name} "
                f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
            )

    assert row_count == 1
    assert result.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Beta"],
        "amount_value": [20.25],
    }


def test_xtdb_live_delta_upsert_dropout_deletes_missing_rows():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_dropout_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )
    delta_config = DeltaConfig(row_finality="dropout")

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1, 2],
                        "destination": ["Alpha", "Beta"],
                        "amount_value": [10.5, 20.25],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
            changed_count = backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "destination": ["Alpha"],
                        "amount_value": [10.5],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
                update_time=datetime(2030, 1, 2, tzinfo=timezone.utc),
            )

            table_sql = f"{table_config.schema}.{table_config.name}"

            def read_at(asof: datetime) -> pl.DataFrame:
                timestamp = asof.isoformat()
                return backend.dataframes(connection).from_raw_sql(
                    "SELECT _id, destination, amount_value "
                    f"FROM {table_sql} "
                    f"FOR VALID_TIME AS OF TIMESTAMP '{timestamp}' "
                    f"FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}' "
                    "ORDER BY _id"
                )

            before_dropout = read_at(datetime(2030, 1, 1, 12, tzinfo=timezone.utc))
            after_dropout = read_at(datetime(2030, 1, 3, tzinfo=timezone.utc))

    assert changed_count == 2
    assert before_dropout.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1, 2],
        "destination": ["Alpha", "Beta"],
        "amount_value": [10.5, 20.25],
    }
    assert after_dropout.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }


def test_xtdb_live_delta_upsert_dropout_closes_missing_rows_at_valid_time():
    table_config = TableConfig(
        schema="public",
        name=f"live_delta_dropout_valid_close_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )
    delta_config = DeltaConfig(row_finality="dropout")

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1, 2],
                        "destination": ["Alpha", "Beta"],
                        "_valid_from": [datetime(1985, 1, 1)] * 2,
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
            )
            backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "destination": ["Alpha"],
                        "_valid_from": [datetime(1986, 1, 1)],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                delta_config=delta_config,
            )

            history = backend.dataframes(connection).from_raw_sql(
                f"""
                SELECT _id, destination, _valid_from, _valid_to
                FROM {table_config.schema}.{table_config.name}
                FOR VALID_TIME ALL
                WHERE _id = 2
                ORDER BY _valid_from
                """
            )
            connection.commit()

    history = history.with_columns(
        pl.col(column).dt.replace_time_zone(None)
        for column in ["_valid_from", "_valid_to"]
    )
    assert history.select(["_id", "destination", "_valid_from", "_valid_to"]).to_dict(
        as_series=False
    ) == {
        "_id": [2],
        "destination": ["Beta"],
        "_valid_from": [datetime(1985, 1, 1)],
        "_valid_to": [datetime(1986, 1, 1)],
    }


def test_xtdb_live_temporal_upsert_honours_explicit_valid_time_window():
    table_config = TableConfig(
        schema="public",
        name=f"live_valid_window_records_{int(time.time())}",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
            TableColumnConfig("records", "amount_value", "DOUBLE"),
        ],
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            backend.table_configs(connection).create(table_config)

            row_count = backend.temporal_upsert(
                pl.DataFrame(
                    {
                        "id": [1],
                        "destination": ["Alpha"],
                        "amount_value": [10.5],
                        "_valid_from": [datetime(2030, 1, 10, tzinfo=timezone.utc)],
                        "_valid_to": [datetime(2030, 1, 20, tzinfo=timezone.utc)],
                    }
                ),
                table_config.schema,
                table_config.name,
                connection=connection,
                table_config=table_config,
                update_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )

            table_sql = f"{table_config.schema}.{table_config.name}"

            def read_valid_at(valid_asof: datetime) -> pl.DataFrame:
                valid_timestamp = valid_asof.isoformat()
                system_timestamp = datetime(2030, 1, 2, tzinfo=timezone.utc).isoformat()
                return backend.dataframes(connection).from_raw_sql(
                    "SELECT _id, destination, amount_value "
                    f"FROM {table_sql} "
                    f"FOR VALID_TIME AS OF TIMESTAMP '{valid_timestamp}' "
                    f"FOR SYSTEM_TIME AS OF TIMESTAMP '{system_timestamp}'"
                )

            before_window = read_valid_at(datetime(2030, 1, 5, tzinfo=timezone.utc))
            inside_window = read_valid_at(datetime(2030, 1, 15, tzinfo=timezone.utc))
            after_window = read_valid_at(datetime(2030, 1, 25, tzinfo=timezone.utc))

    assert row_count == 1
    assert before_window.is_empty()
    assert inside_window.select(["_id", "destination", "amount_value"]).to_dict(
        as_series=False
    ) == {
        "_id": [1],
        "destination": ["Alpha"],
        "amount_value": [10.5],
    }
    assert after_window.is_empty()


def test_xtdb_live_staging_roundtrip_projects_and_cleans_batch():
    suffix = int(time.time())
    delta_table_config = TableConfig(
        schema="public",
        name=f"live_stage_record_stream_{suffix}",
        columns=[
            TableColumnConfig("stage", "record_id", "BIGINT"),
            TableColumnConfig("stage", "destination_name", "VARCHAR(255)"),
            TableColumnConfig("stage", "msg_timestamp", "TIMESTAMP"),
        ],
    )
    target_table_config = TableConfig(
        schema="public",
        name=f"live_stage_records_{suffix}",
        primary_keys=["record_id"],
        columns=[
            TableColumnConfig("records", "record_id", "BIGINT", nullable=False),
            TableColumnConfig("records", "destination", "VARCHAR(255)"),
        ],
    )
    dataset = DatasetConfig(
        name=delta_table_config.name,
        delta_table_schema="public",
        input_config={"type": "dsv", "search_paths": []},
        pipeline=[
            {
                "schema": "public",
                "table": target_table_config.name,
                "type": "primary",
                "columns": [
                    {"source": "record_id", "target": "record_id"},
                    {"source": "destination_name", "target": "destination"},
                ],
            }
        ],
    )
    partition_time = datetime(2030, 1, 1, tzinfo=timezone.utc)
    projected_partition_time = datetime(2030, 1, 1)

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            staging = backend.staging(connection)

            staging.ensure_table(delta_table_config)
            inserted_count = staging.insert_partition(
                pl.DataFrame(
                    {
                        "record_id": [1, 1],
                        "destination_name": ["Beta", "Alpha"],
                        "msg_timestamp": [partition_time, partition_time],
                    }
                ),
                delta_table_config,
                "stage-live-1",
                partition_time,
                uniqueness_col_set=["record_id"],
                prefill_nulls_with_default=True,
            )
            projected = staging.prepare_pipeline_item_dataframe(
                "stage-live-1",
                dataset,
                0,
                target_table_config,
                valid_time=ValidTimeConfig(
                    table=target_table_config.name,
                    from_column="msg_timestamp",
                ),
            )
            staging.cleanup_run("stage-live-1", delta_table_config)
            remaining = backend.dataframes(connection).from_raw_sql(
                "SELECT * FROM "
                f"public.__{delta_table_config.name}_stage "
                "WHERE stage_run_id = 'stage-live-1'::TEXT"
            )

    assert inserted_count == 1
    assert projected.to_dict(as_series=False) == {
        "record_id": [1],
        "destination": ["Alpha"],
        "msg_timestamp": [projected_partition_time],
    }
    assert remaining.is_empty()
