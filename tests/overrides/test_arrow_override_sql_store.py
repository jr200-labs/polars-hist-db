from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
from sqlalchemy import MetaData, Table, create_engine, text

from polars_hist_db.overrides import (
    ArrowOverrideStoreConfig,
    MariaDbArrowOverrideRepository,
    RepositoryArrowOverrideOperationStore,
    arrow_override_operation_schema,
    build_arrow_override_table_configs,
)


def _value(value: str) -> dict[str, object]:
    return {
        field.name: value
        if field.name == "string_value"
        else "string"
        if field.name == "kind"
        else None
        for field in arrow_override_operation_schema().field("value").type
    }


def _proposal(value: str) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "format_version": 1,
                "operation_id": uuid4().bytes,
                "change_set_id": uuid4().bytes,
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": _value(value),
                "supersedes_ids": [],
                "removes_ids": [],
                "valid_from": datetime(2026, 7, 19, tzinfo=timezone.utc),
                "source_drift": False,
            }
        ],
        schema=arrow_override_operation_schema(),
    )


def test_sql_repository_obeys_arrow_sync_contract() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    layer_id = uuid4()
    now = datetime(2026, 7, 19, 2, tzinfo=timezone.utc)
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS overrides"))
        config = ArrowOverrideStoreConfig()
        metadata = MetaData(schema=config.schema)
        for table_config in build_arrow_override_table_configs(config):
            Table(
                table_config.name,
                metadata,
                *table_config.build_sqlalchemy_columns(is_delta_table=False),
            )
        metadata.create_all(connection)
        repository = MariaDbArrowOverrideRepository(connection, config)
        repository.create_layer(layer_id)
        store = RepositoryArrowOverrideOperationStore(repository)
        proposal = _proposal("ready")

        first = store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=0,
            pending=proposal,
            actor_subject="subject-1",
            actor_display_name="Verified Name",
            recorded_at=now,
        )
        duplicate = store.sync(
            layer_id=layer_id,
            generation=1,
            known_revision=1,
            pending=proposal,
            actor_subject="subject-1",
            actor_display_name="Verified Name",
            recorded_at=now,
        )

        assert first.revision == duplicate.revision == 1
        assert first.projection_delta["frontier_state"].to_pylist() == ["clean"]
        assert duplicate.acknowledgements["status"].to_pylist() == ["duplicate"]
        assert duplicate.projection_delta.num_rows == 0
