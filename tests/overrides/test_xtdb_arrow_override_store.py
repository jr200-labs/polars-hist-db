from datetime import datetime, timezone
from uuid import UUID, uuid4

import pyarrow as pa

from polars_hist_db.overrides import (
    ArrowOverrideStoreConfig,
    DocumentAccessStoreConfig,
    RowGuard,
    XtdbArrowOverrideRepository,
    arrow_override_operation_schema,
    build_document_access_table_configs,
    finalize_arrow_override_operations,
)


def _committed(layer_id: UUID) -> pa.Table:
    value = {
        field.name: "ready"
        if field.name == "string_value"
        else "string"
        if field.name == "kind"
        else None
        for field in arrow_override_operation_schema().field("value").type
    }
    proposal = pa.Table.from_pylist(
        [
            {
                "format_version": 1,
                "operation_id": uuid4().bytes,
                "change_set_id": uuid4().bytes,
                "feed_id": "records",
                "entity_id": "record-1",
                "field_path": "status",
                "operation_type": "set",
                "value": value,
                "supersedes_ids": [],
                "removes_ids": [],
                "valid_from": datetime(2026, 7, 19, tzinfo=timezone.utc),
                "source_drift": False,
            }
        ],
        schema=arrow_override_operation_schema(),
    )
    return finalize_arrow_override_operations(
        proposal,
        layer_id=layer_id,
        generation=1,
        layer_revision=1,
        actor_subject="subject-1",
        actor_display_name=None,
        recorded_at=datetime(2026, 7, 19, 2, tzinfo=timezone.utc),
    )


def test_xtdb_append_is_one_asserted_typed_transaction(monkeypatch) -> None:
    transactions: list[list[str]] = []
    monkeypatch.setattr(
        "polars_hist_db.overrides.arrow_xtdb._execute_xtdb_transaction",
        lambda _connection, statements: transactions.append(list(statements)),
    )
    layer_id = uuid4()
    committed = _committed(layer_id)
    repository = XtdbArrowOverrideRepository(object(), ArrowOverrideStoreConfig())
    access = build_document_access_table_configs(DocumentAccessStoreConfig())[0]

    assert repository.append_if_revision(
        layer_id,
        1,
        0,
        committed,
        datetime(2026, 7, 19, 2, tzinfo=timezone.utc),
        (
            RowGuard(
                access,
                {"document_id": str(layer_id)},
                {"status": "active", "revision": 1},
            ),
        ),
    )
    statements = transactions[0]

    assert "document_access" in statements[0]
    assert statements[1].startswith("ASSERT EXISTS")
    assert statements[2].startswith("ASSERT NOT EXISTS")
    assert statements[3].startswith("UPDATE")
    assert "::VARBINARY" in "\n".join(statements)
    assert "::TIMESTAMP WITH TIME ZONE" in "\n".join(statements)
