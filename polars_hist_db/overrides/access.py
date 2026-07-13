from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256
import json
from typing import Iterable

from .config import build_document_access_table_configs
from .crdt import RowGuard
from .types import DocumentAccessStoreConfig


class DocumentAccessError(ValueError):
    pass


class DocumentNotFound(DocumentAccessError):
    pass


class DocumentArchived(DocumentAccessError):
    pass


class DocumentRevisionConflict(DocumentAccessError):
    pass


class IdempotencyConflict(DocumentAccessError):
    pass


@dataclass(frozen=True)
class AccessDocument:
    document_id: str
    name: str
    normalized_name: str
    description: str | None
    status: str
    revision: int
    created_by: str
    created_at: datetime
    archived_by: str | None = None
    archived_at: datetime | None = None


@dataclass(frozen=True)
class AccessGrant:
    grant_id: str
    document_id: str
    group_name: str
    role: str
    granted_by: str
    granted_at: datetime
    document_revision: int
    revoked_by: str | None = None
    revoked_at: datetime | None = None

    @property
    def active(self) -> bool:
        return self.revoked_at is None


@dataclass(frozen=True)
class AccessGrantInput:
    grant_id: str
    group_name: str
    role: str


@dataclass(frozen=True)
class AccessMutationResult:
    document: AccessDocument
    grants: tuple[AccessGrant, ...]
    accepted: bool
    duplicate: bool = False


class InMemoryDocumentAccessStore:
    def __init__(self) -> None:
        self._documents: dict[str, AccessDocument] = {}
        self._grants: dict[str, AccessGrant] = {}
        self._commands: dict[str, tuple[str, AccessMutationResult]] = {}

    def create(
        self,
        document_id: str,
        name: str,
        description: str | None,
        actor_id: str,
        recorded_at: datetime,
        *,
        initial_grants: Iterable[AccessGrantInput] = (),
        idempotency_key: str,
    ) -> AccessMutationResult:
        grants = tuple(initial_grants)
        payload = _payload("create", document_id, name, description, actor_id, grants)
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate is not None:
            return duplicate
        if document_id in self._documents:
            raise DocumentAccessError("document already exists")
        normalized_name = _normalized(name)
        if any(
            doc.normalized_name == normalized_name for doc in self._documents.values()
        ):
            raise DocumentAccessError("document name already exists")
        _require_time(recorded_at)
        document = AccessDocument(
            document_id,
            name,
            normalized_name,
            description,
            "active",
            1,
            actor_id,
            recorded_at,
        )
        self._documents[document_id] = document
        for grant in grants:
            self._insert_grant(document, grant, actor_id, recorded_at)
        return self._record(idempotency_key, payload, document)

    def get(self, document_id: str) -> AccessDocument | None:
        return self._documents.get(document_id)

    def guard(self, document_id: str, expected_revision: int) -> RowGuard:
        return RowGuard(
            build_document_access_table_configs(DocumentAccessStoreConfig())[0],
            {"document_id": document_id},
            {"status": "active", "revision": expected_revision},
        )

    def list_for_groups(
        self, groups: Iterable[str], *, include_archived: bool = False
    ) -> list[AccessDocument]:
        group_set = set(groups)
        document_ids = {
            grant.document_id
            for grant in self._grants.values()
            if grant.active and grant.group_name in group_set
        }
        return [
            doc
            for doc in self._documents.values()
            if doc.document_id in document_ids
            and (include_archived or doc.status == "active")
        ]

    def list_all(self, *, include_archived: bool = False) -> list[AccessDocument]:
        return [
            doc
            for doc in self._documents.values()
            if include_archived or doc.status == "active"
        ]

    def grants(
        self, document_id: str, *, include_revoked: bool = False
    ) -> tuple[AccessGrant, ...]:
        return tuple(
            grant
            for grant in self._grants.values()
            if grant.document_id == document_id and (include_revoked or grant.active)
        )

    def grant(
        self,
        document_id: str,
        grant: AccessGrantInput,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload("grant", document_id, grant, actor_id, expected_revision)
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        if any(
            item.active and item.group_name == grant.group_name
            for item in self.grants(document_id)
        ):
            raise DocumentAccessError("group already has an active grant")
        document = self._advance(document)
        self._documents[document_id] = document
        self._insert_grant(document, grant, actor_id, recorded_at)
        return self._record(idempotency_key, payload, document)

    def revoke(
        self,
        document_id: str,
        group_name: str,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload(
            "revoke", document_id, group_name, actor_id, expected_revision
        )
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        grant = next(
            (
                item
                for item in self.grants(document_id)
                if item.group_name == group_name
            ),
            None,
        )
        if grant is None:
            raise DocumentAccessError("active group grant not found")
        document = self._advance(document)
        self._documents[document_id] = document
        self._grants[grant.grant_id] = replace(
            grant,
            revoked_by=actor_id,
            revoked_at=recorded_at,
            document_revision=document.revision,
        )
        return self._record(idempotency_key, payload, document)

    def archive(
        self,
        document_id: str,
        actor_id: str,
        recorded_at: datetime,
        expected_revision: int,
        *,
        idempotency_key: str,
    ) -> AccessMutationResult:
        payload = _payload("archive", document_id, actor_id, expected_revision)
        duplicate = self._duplicate(idempotency_key, payload)
        if duplicate is not None:
            return duplicate
        document = self._active(document_id, expected_revision)
        _require_time(recorded_at)
        document = replace(
            self._advance(document),
            status="archived",
            archived_by=actor_id,
            archived_at=recorded_at,
        )
        self._documents[document_id] = document
        return self._record(idempotency_key, payload, document)

    def _active(self, document_id: str, expected_revision: int) -> AccessDocument:
        document = self._documents.get(document_id)
        if document is None:
            raise DocumentNotFound("document not found")
        if document.status != "active":
            raise DocumentArchived("document archived")
        if document.revision != expected_revision:
            raise DocumentRevisionConflict("document revision changed")
        return document

    def _insert_grant(
        self,
        document: AccessDocument,
        grant: AccessGrantInput,
        actor_id: str,
        recorded_at: datetime,
    ) -> None:
        if grant.grant_id in self._grants:
            raise DocumentAccessError("grant already exists")
        self._grants[grant.grant_id] = AccessGrant(
            grant.grant_id,
            document.document_id,
            grant.group_name,
            grant.role,
            actor_id,
            recorded_at,
            document.revision,
        )

    def _advance(self, document: AccessDocument) -> AccessDocument:
        return replace(document, revision=document.revision + 1)

    def _duplicate(
        self, idempotency_key: str, payload: str
    ) -> AccessMutationResult | None:
        prior = self._commands.get(idempotency_key)
        if prior is None:
            return None
        if prior[0] != payload:
            raise IdempotencyConflict(
                "idempotency key was reused with different content"
            )
        return replace(prior[1], accepted=False, duplicate=True)

    def _record(
        self, idempotency_key: str, payload: str, document: AccessDocument
    ) -> AccessMutationResult:
        result = AccessMutationResult(
            document,
            self.grants(document.document_id, include_revoked=True),
            accepted=True,
        )
        self._commands[idempotency_key] = (payload, result)
        return result


def _normalized(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        raise DocumentAccessError("name is required")
    return normalized


def _require_time(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DocumentAccessError("recorded_at must be timezone-aware")


def _payload(action: str, *values: object) -> str:
    return sha256(
        json.dumps(
            [action, *values], default=lambda value: value.__dict__, sort_keys=True
        ).encode()
    ).hexdigest()
