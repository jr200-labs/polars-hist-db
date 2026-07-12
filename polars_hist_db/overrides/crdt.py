from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any


@dataclass(frozen=True)
class CrdtDocument:
    document_id: str
    revision: int
    state_vector: bytes
    update: bytes


@dataclass(frozen=True)
class CrdtAppendResult:
    document: CrdtDocument
    accepted: bool


class InMemoryCrdtDocumentStore:
    """Yjs-compatible document storage for tests and embedded applications."""

    def __init__(self) -> None:
        self._documents: dict[str, Any] = {}
        self._revisions: dict[str, int] = {}
        self._update_hashes: dict[str, set[str]] = {}
        self._snapshots: dict[str, CrdtDocument] = {}

    def append_update(
        self,
        document_id: str,
        update: bytes,
        *,
        expected_revision: int | None = None,
    ) -> CrdtAppendResult:
        document = self._documents.get(document_id)
        revision = self._revisions.get(document_id, 0)
        if document is None:
            if expected_revision not in {None, 0}:
                raise ValueError(
                    "CRDT document revision does not match expected revision"
                )
            document = _doc()
            self._documents[document_id] = document
            self._revisions[document_id] = revision
        update_hash = sha256(update).hexdigest()
        hashes = self._update_hashes.setdefault(document_id, set())

        if update_hash in hashes:
            return CrdtAppendResult(self.load_document(document_id), accepted=False)
        if expected_revision is not None and expected_revision != revision:
            raise ValueError("CRDT document revision does not match expected revision")

        document.apply_update(update)
        hashes.add(update_hash)
        self._revisions[document_id] = revision + 1
        return CrdtAppendResult(self.load_document(document_id), accepted=True)

    def load_document(self, document_id: str) -> CrdtDocument:
        document = self._documents.get(document_id)
        if document is None:
            raise KeyError(f"unknown CRDT document: {document_id}")
        return CrdtDocument(
            document_id=document_id,
            revision=self._revisions[document_id],
            state_vector=document.get_state(),
            update=document.get_update(),
        )

    def diff(self, document_id: str, state_vector: bytes) -> bytes:
        return self._documents[document_id].get_update(state_vector)

    def write_snapshot(self, document_id: str) -> CrdtDocument:
        snapshot = self.load_document(document_id)
        self._snapshots[document_id] = snapshot
        return snapshot

    def snapshot(self, document_id: str) -> CrdtDocument | None:
        return self._snapshots.get(document_id)


def _doc() -> Any:
    try:
        from pycrdt import Doc
    except ImportError as exc:
        raise RuntimeError(
            "CRDT support requires the 'crdt' extra: pip install polars-hist-db[crdt]"
        ) from exc
    return Doc()
