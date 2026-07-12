from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Sequence

from sqlalchemy import Connection, MetaData, Table, and_, select
from sqlalchemy.exc import IntegrityError

from polars_hist_db.config import TableConfig

from .config import (
    build_crdt_document_table_config,
    build_crdt_update_table_config,
    build_override_table_config,
)
from .crdt import (
    AtomicInsert,
    CrdtCommitResult,
    CrdtDocument,
    CrdtPreconditionFailed,
    CrdtRevisionConflict,
    PreparedCrdtCommit,
    RowGuard,
    _doc,
)
from .types import CrdtDocumentStoreConfig, OverrideLedgerConfig


class MariaDbCrdtDocumentStore:
    """SQLAlchemy repository for the prepared CRDT document contract."""

    def __init__(
        self,
        connection: Connection,
        config: CrdtDocumentStoreConfig,
        projection_config: OverrideLedgerConfig,
    ) -> None:
        self.connection = connection
        self.config = config
        self.documents = _table(connection, build_crdt_document_table_config(config))
        self.updates = _table(connection, build_crdt_update_table_config(config))
        self.projection = _table(
            connection, build_override_table_config(projection_config)
        )

    def load_document(self, document_id: str) -> CrdtDocument | None:
        head = (
            self.connection.execute(
                select(self.documents).where(
                    self.documents.c.document_id == document_id
                )
            )
            .mappings()
            .first()
        )
        if head is None:
            return None
        document = _doc()
        rows = self.connection.execute(
            select(
                self.updates.c.update_base64,
                self.updates.c.accepted_update_hash,
            )
            .where(self.updates.c.document_id == document_id)
            .order_by(self.updates.c.revision)
        ).mappings()
        for row in rows:
            update = _decode(row["update_base64"])
            if sha256(update).hexdigest() != row["accepted_update_hash"]:
                raise ValueError(
                    "stored accepted CRDT update hash does not match update"
                )
            document.apply_update(update)
        return CrdtDocument(
            document_id=document_id,
            revision=head["revision"],
            state_vector=document.get_state(),
            update=document.get_update(),
        )

    def commit(
        self,
        prepared: PreparedCrdtCommit,
        *,
        guards: Sequence[RowGuard] = (),
        inserts: Sequence[AtomicInsert] = (),
    ) -> CrdtCommitResult:
        if (
            sha256(prepared.accepted_update).hexdigest()
            != prepared.accepted_update_hash
        ):
            raise ValueError("accepted CRDT update hash does not match update")
        duplicate = self._duplicate_result(prepared)
        if duplicate is not None:
            return duplicate
        if prepared.is_noop:
            current = self.load_document(prepared.document_id)
            return CrdtCommitResult(
                current,
                0 if current is None else current.revision,
                b"",
                accepted=False,
                duplicate=True,
            )

        try:
            with self.connection.begin_nested():
                return self._commit(prepared, guards=guards, inserts=inserts)
        except IntegrityError as exc:
            duplicate = self._duplicate_result(prepared)
            if duplicate is not None:
                return duplicate
            current = self.load_document(prepared.document_id)
            if (0 if current is None else current.revision) != prepared.base_revision:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                ) from exc
            raise ValueError("CRDT commit conflicts with an immutable row") from exc

    def _commit(
        self,
        prepared: PreparedCrdtCommit,
        *,
        guards: Sequence[RowGuard],
        inserts: Sequence[AtomicInsert],
    ) -> CrdtCommitResult:
        self._check_guards(guards)
        current = self.load_document(prepared.document_id)
        revision = 0 if current is None else current.revision
        if revision != prepared.base_revision:
            raise CrdtRevisionConflict(
                "CRDT document revision does not match prepared revision"
            )
        next_revision = revision + 1
        if current is None:
            self.connection.execute(
                self.documents.insert().values(
                    document_id=prepared.document_id,
                    revision=next_revision,
                    head_state_vector_base64=_encode(prepared.state_vector),
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            result = self.connection.execute(
                self.documents.update()
                .where(self.documents.c.document_id == prepared.document_id)
                .where(self.documents.c.revision == prepared.base_revision)
                .values(
                    revision=next_revision,
                    head_state_vector_base64=_encode(prepared.state_vector),
                    updated_at=datetime.now(timezone.utc),
                )
            )
            if result.rowcount != 1:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                )
        self.connection.execute(
            self.updates.insert().values(
                document_id=prepared.document_id,
                revision=next_revision,
                source_update_hash=prepared.source_update_hash,
                accepted_update_hash=prepared.accepted_update_hash,
                update_base64=_encode(prepared.accepted_update),
                accepted_at=datetime.now(timezone.utc),
            )
        )
        self._insert_operations(prepared, next_revision)
        for insert in inserts:
            self.connection.execute(
                _table(self.connection, insert.table_config).insert().values(insert.row)
            )
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            next_revision,
            prepared.accepted_update,
            accepted=True,
        )

    def _duplicate_result(
        self, prepared: PreparedCrdtCommit
    ) -> CrdtCommitResult | None:
        duplicate = (
            self.connection.execute(
                select(self.updates)
                .where(self.updates.c.document_id == prepared.document_id)
                .where(self.updates.c.source_update_hash == prepared.source_update_hash)
            )
            .mappings()
            .first()
        )
        if duplicate is None:
            return None
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            duplicate["revision"],
            _decode(duplicate["update_base64"]),
            accepted=False,
            duplicate=True,
        )

    def _check_guards(self, guards: Sequence[RowGuard]) -> None:
        for guard in guards:
            table = _table(self.connection, guard.table_config)
            predicates = [
                table.c[name] == value for name, value in guard.key_values.items()
            ]
            predicates.extend(
                table.c[name] == value for name, value in guard.expected_values.items()
            )
            if (
                self.connection.execute(
                    select(table).where(and_(*predicates)).with_for_update()
                ).first()
                is None
            ):
                raise CrdtPreconditionFailed("CRDT commit guard no longer matches")

    def _insert_operations(self, prepared: PreparedCrdtCommit, revision: int) -> None:
        for operation in prepared.operations:
            self.connection.execute(
                self.projection.insert().values(
                    operation_id=operation.operation_id,
                    change_set_id=operation.change_set_id,
                    owner_user_id=None,
                    actor_user_id=operation.actor_id,
                    feed_id=operation.feed_id,
                    entity_id=operation.entity_id,
                    field_path=operation.field_path,
                    operation_type=operation.operation_type,
                    value_type=None
                    if operation.value is None
                    else operation.value.value_type,
                    value_json=_json(
                        None if operation.value is None else operation.value.value_json
                    ),
                    unit=None if operation.value is None else operation.value.unit,
                    observed_canonical_value_json=_json(
                        operation.observed_canonical_value_json
                    ),
                    created_against_stale_source=False,
                    valid_from=operation.valid_from,
                    valid_to=operation.valid_to,
                    comment=operation.comment,
                    metadata_json=_json(operation.metadata_json or {}),
                    format_version=operation.format_version,
                    layer_id=operation.layer_id,
                    actor_id=operation.actor_id,
                    supersedes_operation_ids_json=_json(
                        list(operation.supersedes_operation_ids)
                    ),
                    removes_operation_ids_json=_json(
                        list(operation.removes_operation_ids)
                    ),
                    recorded_at=operation.recorded_at,
                    payload_hash=operation.payload_hash,
                    crdt_document_id=prepared.document_id,
                    crdt_document_revision=revision,
                )
            )


def _table(connection: Connection, config: TableConfig) -> Table:
    return Table(config.name, MetaData(schema=config.schema), autoload_with=connection)


def _encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode(value: str) -> bytes:
    return base64.b64decode(value)


def _json(value: object | None) -> str | None:
    return None if value is None else json.dumps(value, separators=(",", ":"))
