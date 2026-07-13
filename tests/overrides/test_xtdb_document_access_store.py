from datetime import datetime, timezone

from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentAccessStoreConfig,
    XtdbDocumentAccessStore,
)


def test_xtdb_access_create_submits_one_asserted_transaction(monkeypatch):
    statements: list[str] = []
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())
    monkeypatch.setattr(store, "_duplicate", lambda *_: None)
    monkeypatch.setattr(store, "_ensure_tables", lambda: None)
    monkeypatch.setattr(
        "polars_hist_db.overrides.xtdb._execute_xtdb_transaction",
        lambda _, transaction: statements.extend(transaction),
    )

    result = store.create(
        "document-1",
        "Shared corrections",
        None,
        "user-1",
        datetime(2026, 7, 12, 11, tzinfo=timezone.utc),
        initial_grants=(AccessGrantInput("grant-1", "operators", "editor"),),
        idempotency_key="command-1",
    )

    assert result.accepted is True
    assert result.document.revision == 1
    assert statements[0].startswith("ASSERT NOT EXISTS")
    assert any("INSERT INTO overrides.document_access" in item for item in statements)
    assert any(
        "INSERT INTO overrides.document_access_grants" in item for item in statements
    )
    assert any(
        "INSERT INTO overrides.document_access_commands" in item for item in statements
    )


def test_xtdb_access_guard_uses_active_revision():
    store = XtdbDocumentAccessStore(object(), DocumentAccessStoreConfig())

    guard = store.guard("document-1", 3)

    assert guard.key_values == {"document_id": "document-1"}
    assert guard.expected_values == {"status": "active", "revision": 3}
