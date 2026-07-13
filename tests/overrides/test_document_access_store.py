from datetime import datetime, timezone

import pytest

from polars_hist_db.overrides import (
    AccessGrantInput,
    DocumentArchived,
    DocumentRevisionConflict,
    IdempotencyConflict,
    InMemoryDocumentAccessStore,
)


TIME = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)


def test_lifecycle_is_versioned_and_preserves_grant_history():
    store = InMemoryDocumentAccessStore()
    created = store.create(
        "doc-1",
        "Analysts",
        None,
        "admin",
        TIME,
        initial_grants=[AccessGrantInput("grant-1", "analysts", "editor")],
        idempotency_key="create-1",
    )
    revoked = store.revoke(
        "doc-1", "analysts", "admin", TIME, 1, idempotency_key="revoke-1"
    )
    granted = store.grant(
        "doc-1",
        AccessGrantInput("grant-2", "analysts", "resolver"),
        "admin",
        TIME,
        2,
        idempotency_key="grant-2",
    )
    archived = store.archive("doc-1", "admin", TIME, 3, idempotency_key="archive-1")

    assert created.document.revision == 1
    assert revoked.document.revision == 2
    assert revoked.grants[0].active is False
    assert granted.document.revision == 3
    assert archived.document.status == "archived"
    assert archived.document.revision == 4
    assert len(store.grants("doc-1", include_revoked=True)) == 2
    assert store.grants("doc-1")[0].role == "resolver"
    with pytest.raises(DocumentArchived):
        store.grant(
            "doc-1",
            AccessGrantInput("grant-3", "reviewers", "viewer"),
            "admin",
            TIME,
            4,
            idempotency_key="grant-3",
        )


def test_idempotency_and_revision_conflicts_are_explicit():
    store = InMemoryDocumentAccessStore()
    accepted = store.create(
        "doc-1", "Analysts", None, "admin", TIME, idempotency_key="create-1"
    )
    duplicate = store.create(
        "doc-1", "Analysts", None, "admin", TIME, idempotency_key="create-1"
    )

    assert accepted.accepted
    assert duplicate.duplicate
    with pytest.raises(IdempotencyConflict):
        store.create("doc-2", "Other", None, "admin", TIME, idempotency_key="create-1")
    with pytest.raises(DocumentRevisionConflict):
        store.archive("doc-1", "admin", TIME, 2, idempotency_key="archive-1")


def test_guard_matches_the_portable_active_revision_contract():
    guard = InMemoryDocumentAccessStore().guard("doc-1", 4)

    assert guard.key_values == {"document_id": "doc-1"}
    assert guard.expected_values == {"status": "active", "revision": 4}
