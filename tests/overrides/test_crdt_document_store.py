from pycrdt import Doc, Map
import pytest
from typing import Any

from polars_hist_db.overrides import InMemoryCrdtDocumentStore


def _document_update() -> bytes:
    document: Any = Doc()
    document["drafts"] = Map({"title": "Initial"})
    return document.get_update()


def test_document_store_merges_offline_updates_and_rebuilds_from_snapshot():
    base_update = _document_update()
    left = Doc()
    right = Doc()
    left.apply_update(base_update)
    right.apply_update(base_update)
    base_state = left.get_state()

    left.get("drafts", type=Map)["left"] = "offline"
    right.get("drafts", type=Map)["right"] = "offline"

    store = InMemoryCrdtDocumentStore()
    store.append_update("document-1", base_update)
    store.append_update("document-1", left.get_update(base_state), expected_revision=1)
    store.append_update("document-1", right.get_update(base_state), expected_revision=2)
    snapshot = store.write_snapshot("document-1")

    rebuilt = Doc()
    rebuilt.apply_update(snapshot.update)
    caught_up = Doc()
    caught_up.apply_update(base_update)
    caught_up.apply_update(store.diff("document-1", base_state))

    assert rebuilt.get("drafts", type=Map).to_py() == {
        "title": "Initial",
        "left": "offline",
        "right": "offline",
    }
    assert snapshot.revision == 3
    assert store.snapshot("document-1") == snapshot
    assert (
        caught_up.get("drafts", type=Map).to_py()
        == rebuilt.get("drafts", type=Map).to_py()
    )


def test_document_store_deduplicates_exact_updates_and_checks_revisions():
    update = _document_update()
    store = InMemoryCrdtDocumentStore()

    first = store.append_update("document-1", update, expected_revision=0)
    duplicate = store.append_update("document-1", update, expected_revision=0)

    assert first.accepted is True
    assert duplicate.accepted is False
    assert duplicate.document.revision == 1

    with pytest.raises(ValueError, match="expected revision"):
        store.append_update("document-2", update, expected_revision=1)
    with pytest.raises(KeyError):
        store.load_document("document-2")
