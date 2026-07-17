from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from uuid import uuid4
import subprocess
import time

import polars as pl
from pycrdt import Doc, Map
import pytest
from sqlalchemy import Engine, text

from polars_hist_db.backends import DbEngineConfig, XtdbBackend
from polars_hist_db.overrides import (
    CrdtDocumentStoreConfig,
    OverrideLedgerConfig,
    build_crdt_document_table_config,
    build_crdt_update_table_config,
    build_override_table_config,
    override_recorded_order_sql,
    prepare_crdt_update,
)

pytestmark = pytest.mark.integration


def _docker(args: list[str]) -> str:
    return subprocess.check_output(["docker", *args], text=True).strip()


@contextmanager
def _xtdb_engine() -> Iterator[Engine]:
    container_id = _docker(
        ["run", "--rm", "-d", "-p", "5432", "ghcr.io/xtdb/xtdb:nightly"]
    )
    engine = None
    try:
        port = int(_docker(["port", container_id, "5432/tcp"]).rsplit(":", 1)[1])
        engine = XtdbBackend().create_engine(
            DbEngineConfig(backend="xtdb", hostname="127.0.0.1", port=port)
        )
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


def _operation_update(operation_id: str, valid_from: datetime) -> bytes:
    document: Any = Doc()
    document["operations"] = Map(
        {
            operation_id: {
                "format_version": 1,
                "operation_id": operation_id,
                "change_set_id": str(uuid4()),
                "layer_id": "shared",
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": {"value_type": "enum", "value_json": {"value": "open"}},
                "supersedes_operation_ids": [],
                "removes_operation_ids": [],
                "valid_from": valid_from.isoformat(),
                "valid_to": None,
            }
        }
    )
    return document.get_update()


def test_xtdb_crdt_store_persists_projection_at_operation_valid_time():
    suffix = str(uuid4()).replace("-", "")
    document_config = CrdtDocumentStoreConfig(
        schema="public",
        documents_table=f"crdt_documents_{suffix}",
        updates_table=f"crdt_updates_{suffix}",
    )
    projection_config = OverrideLedgerConfig(
        schema="public", table=f"crdt_operations_{suffix}"
    )
    valid_from = datetime.now(timezone.utc) - timedelta(minutes=1)
    prepared = prepare_crdt_update(
        "document-1",
        None,
        _operation_update(str(uuid4()), valid_from),
        actor_id="user-1",
        recorded_at=datetime.now(timezone.utc),
    )

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            backend = XtdbBackend()
            for config in (
                build_crdt_document_table_config(document_config),
                build_crdt_update_table_config(document_config),
                build_override_table_config(projection_config),
            ):
                backend.table_configs(connection).create(config)

            store = backend.crdt_documents(
                connection, document_config, projection_config
            )
            accepted = store.commit(prepared)
            duplicate = store.commit(prepared)
            operation = (
                connection.execute(
                    text(
                        "SELECT actor_id, crdt_document_revision "
                        f"FROM public.{projection_config.table} "
                        f"FOR VALID_TIME AS OF TIMESTAMP '{(valid_from + timedelta(seconds=1)).isoformat()}'"
                    )
                )
                .mappings()
                .one()
            )

    assert accepted.accepted is True
    assert accepted.revision == 1
    assert duplicate.duplicate is True
    assert dict(operation) == {"actor_id": "user-1", "crdt_document_revision": 1}


def test_xtdb_override_bulk_write_uses_native_timestamp_instants():
    table = f"override_order_{uuid4().hex}"
    config = OverrideLedgerConfig(schema="public", table=table)
    table_config = build_override_table_config(config)
    order = override_recorded_order_sql("xtdb")
    at = datetime(2026, 7, 17, tzinfo=timezone.utc)

    with _xtdb_engine() as engine:
        with engine.connect() as connection:
            XtdbBackend().dataframes(connection).table_insert(
                pl.DataFrame(
                    {
                        "operation_id": ["op-a-set", "op-z-close"],
                        "operation_type": ["set", "close"],
                        "recorded_at": [at, at],
                        "valid_from": [at, at],
                        "valid_to": [None, None],
                    }
                ),
                "public",
                table,
                table_config=table_config,
            )
            operations = (
                connection.execute(
                    text(f"SELECT operation_id FROM public.{table} ORDER BY {order}")
                )
                .scalars()
                .all()
            )
            timestamp_types = dict(
                connection.execute(
                    text(
                        "SELECT column_name, data_type "
                        "FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = :table "
                        "AND column_name IN ('recorded_at', 'valid_from', 'valid_to')"
                    ),
                    {"table": table},
                ).all()
            )

    assert operations == ["op-z-close", "op-a-set"]
    assert all(":utf8" not in str(data_type) for data_type in timestamp_types.values())
    assert all(
        "timestamp-tz" in str(data_type) for data_type in timestamp_types.values()
    )
