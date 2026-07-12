from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

from sqlalchemy import text

from polars_hist_db.backends.xtdb import (
    _execute_xtdb_transaction,
    _is_xtdb_table_not_found_error,
    _qualified_table_name,
    _rollback_xtdb_connection,
    _xtdb_cast_type,
    _xtdb_column_identifier,
    _xtdb_composite_document_id,
    _xtdb_sql_literal,
)
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


class XtdbCrdtDocumentStore:
    """XTDB repository for the backend-neutral prepared CRDT contract."""

    def __init__(
        self,
        connection: Any,
        config: CrdtDocumentStoreConfig,
        projection_config: OverrideLedgerConfig,
    ) -> None:
        self.connection = connection
        self.documents = build_crdt_document_table_config(config)
        self.updates = build_crdt_update_table_config(config)
        self.projection = build_override_table_config(projection_config)

    def load_document(self, document_id: str) -> CrdtDocument | None:
        document_id_sql = _literal(document_id, "VARCHAR(128)")
        head = self._rows(
            f"SELECT document_id, revision FROM {_table_name(self.documents)} "
            f"WHERE _id = {document_id_sql}"
        )
        if not head:
            return None
        document = _doc()
        for row in self._rows(
            "SELECT update_base64, accepted_update_hash "
            f"FROM {_table_name(self.updates)} "
            f"WHERE document_id = {document_id_sql} ORDER BY revision"
        ):
            update = _decode(str(row["update_base64"]))
            if sha256(update).hexdigest() != row["accepted_update_hash"]:
                raise ValueError(
                    "stored accepted CRDT update hash does not match update"
                )
            document.apply_update(update)
        return CrdtDocument(
            document_id=document_id,
            revision=int(str(head[0]["revision"])),
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
        self._ensure_tables()
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

        statements = [
            self._source_assertion(prepared),
            *(self._guard_assertion(guard) for guard in guards),
            self._revision_assertion(prepared),
            _insert_statement(
                self.documents,
                {
                    "document_id": prepared.document_id,
                    "revision": prepared.base_revision + 1,
                    "head_state_vector_base64": _encode(prepared.state_vector),
                    "updated_at": _accepted_at(prepared),
                },
            ),
            _insert_statement(
                self.updates,
                {
                    "document_id": prepared.document_id,
                    "revision": prepared.base_revision + 1,
                    "source_update_hash": prepared.source_update_hash,
                    "accepted_update_hash": prepared.accepted_update_hash,
                    "update_base64": _encode(prepared.accepted_update),
                    "accepted_at": _accepted_at(prepared),
                },
            ),
            *(
                self._operation_statement(operation, prepared)
                for operation in prepared.operations
            ),
            *(_insert_statement(insert.table_config, insert.row) for insert in inserts),
        ]
        try:
            _execute_xtdb_transaction(self.connection, statements)
        except Exception:
            duplicate = self._duplicate_result(prepared)
            if duplicate is not None:
                return duplicate
            current = self.load_document(prepared.document_id)
            if (0 if current is None else current.revision) != prepared.base_revision:
                raise CrdtRevisionConflict(
                    "CRDT document revision changed during commit"
                ) from None
            if any(not self._guard_matches(guard) for guard in guards):
                raise CrdtPreconditionFailed(
                    "CRDT commit guard no longer matches"
                ) from None
            raise

        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            prepared.base_revision + 1,
            prepared.accepted_update,
            accepted=True,
        )

    def _duplicate_result(
        self, prepared: PreparedCrdtCommit
    ) -> CrdtCommitResult | None:
        rows = self._rows(
            "SELECT revision, update_base64 "
            f"FROM {_table_name(self.updates)} WHERE "
            f"document_id = {_literal(prepared.document_id, 'VARCHAR(128)')} AND "
            f"source_update_hash = {_literal(prepared.source_update_hash, 'VARCHAR(64)')}"
        )
        if not rows:
            return None
        return CrdtCommitResult(
            self.load_document(prepared.document_id),
            int(str(rows[0]["revision"])),
            _decode(str(rows[0]["update_base64"])),
            accepted=False,
            duplicate=True,
        )

    def _source_assertion(self, prepared: PreparedCrdtCommit) -> str:
        return (
            "ASSERT NOT EXISTS (SELECT 1 FROM "
            f"{_table_name(self.updates)} WHERE "
            f"document_id = {_literal(prepared.document_id, 'VARCHAR(128)')} AND "
            f"source_update_hash = {_literal(prepared.source_update_hash, 'VARCHAR(64)')}"
            "), 'CRDT source update already accepted'"
        )

    def _revision_assertion(self, prepared: PreparedCrdtCommit) -> str:
        document_id = _literal(prepared.document_id, "VARCHAR(128)")
        if prepared.base_revision == 0:
            predicate = f"NOT EXISTS (SELECT 1 FROM {_table_name(self.documents)} WHERE _id = {document_id})"
        else:
            predicate = (
                "EXISTS (SELECT 1 FROM "
                f"{_table_name(self.documents)} WHERE _id = {document_id} "
                f"AND revision = {prepared.base_revision}::BIGINT)"
            )
        return f"ASSERT {predicate}, 'CRDT document revision changed'"

    def _guard_assertion(self, guard: RowGuard) -> str:
        return f"ASSERT EXISTS (SELECT 1 FROM {_table_name(guard.table_config)} WHERE {_where(guard.table_config, {**guard.key_values, **guard.expected_values})}), 'CRDT commit guard failed'"

    def _guard_matches(self, guard: RowGuard) -> bool:
        return bool(
            self._rows(
                f"SELECT 1 FROM {_table_name(guard.table_config)} "
                f"WHERE {_where(guard.table_config, {**guard.key_values, **guard.expected_values})}"
            )
        )

    def _operation_statement(self, operation: Any, prepared: PreparedCrdtCommit) -> str:
        row = {
            "operation_id": operation.operation_id,
            "change_set_id": operation.change_set_id,
            "owner_user_id": None,
            "actor_user_id": operation.actor_id,
            "feed_id": operation.feed_id,
            "entity_id": operation.entity_id,
            "field_path": operation.field_path,
            "operation_type": operation.operation_type,
            "value_type": None
            if operation.value is None
            else operation.value.value_type,
            "value_json": None
            if operation.value is None
            else operation.value.value_json,
            "unit": None if operation.value is None else operation.value.unit,
            "observed_canonical_value_json": operation.observed_canonical_value_json,
            "created_against_stale_source": False,
            "valid_from": operation.valid_from,
            "valid_to": operation.valid_to,
            "comment": operation.comment,
            "metadata_json": operation.metadata_json or {},
            "format_version": operation.format_version,
            "layer_id": operation.layer_id,
            "actor_id": operation.actor_id,
            "supersedes_operation_ids_json": list(operation.supersedes_operation_ids),
            "removes_operation_ids_json": list(operation.removes_operation_ids),
            "recorded_at": operation.recorded_at,
            "payload_hash": operation.payload_hash,
            "crdt_document_id": prepared.document_id,
            "crdt_document_revision": prepared.base_revision + 1,
        }
        return _insert_statement(
            self.projection,
            row,
            valid_from=operation.valid_from,
            valid_to=operation.valid_to,
        )

    def _ensure_tables(self) -> None:
        for config in (self.documents, self.updates, self.projection):
            if self._rows(
                "SELECT table_name FROM information_schema.tables WHERE "
                f"table_schema = {_literal(config.schema, 'TEXT')} AND "
                f"table_name = {_literal(config.name, 'TEXT')}"
            ):
                continue
            bootstrap_id = "__polars_hist_db_crdt_bootstrap__"
            table_name = _table_name(config)
            _execute_xtdb_transaction(
                self.connection,
                [
                    f"INSERT INTO {table_name} (_id) VALUES ({_literal(bootstrap_id, 'TEXT')})",
                    f"ERASE FROM {table_name} WHERE _id = {_literal(bootstrap_id, 'TEXT')}",
                ],
            )

    def _rows(self, sql: str) -> list[Mapping[str, object]]:
        try:
            return [dict(row) for row in self.connection.execute(text(sql)).mappings()]
        except Exception as exc:
            if not _is_xtdb_table_not_found_error(exc):
                raise
            _rollback_xtdb_connection(self.connection)
            return []


def _insert_statement(
    config: TableConfig,
    row: Mapping[str, object],
    *,
    valid_from: object | None = None,
    valid_to: object | None = None,
) -> str:
    columns = {column.name: column for column in config.columns}
    missing_keys = [key for key in config.primary_keys if key not in row]
    if missing_keys:
        raise ValueError("atomic rows require every configured primary key")
    document_id = _document_id(config, row)
    values = {"_id": document_id, **row}
    types = {
        "_id": "TEXT",
        **{name: column.data_type for name, column in columns.items()},
    }
    if valid_from is not None:
        values["_valid_from"] = valid_from
        values["_valid_to"] = valid_to
        types.update({"_valid_from": "TIMESTAMP", "_valid_to": "TIMESTAMP"})
    column_sql = ", ".join(_xtdb_column_identifier(name) for name in values)
    value_sql = ", ".join(
        _literal(value, types[name]) for name, value in values.items()
    )
    return f"INSERT INTO {_table_name(config)} ({column_sql}) VALUES ({value_sql})"


def _document_id(config: TableConfig, row: Mapping[str, object]) -> object:
    keys = list(config.primary_keys)
    if len(keys) == 1:
        return row[keys[0]]
    return _xtdb_composite_document_id(keys, tuple(row[key] for key in keys))


def _where(config: TableConfig, values: Mapping[str, object]) -> str:
    types = {column.name: column.data_type for column in config.columns}
    predicates = []
    for name, value in values.items():
        if value is None:
            predicates.append(f"{_xtdb_column_identifier(name)} IS NULL")
        else:
            predicates.append(
                f"{_xtdb_column_identifier(name)} = {_literal(value, types[name])}"
            )
    return " AND ".join(predicates)


def _table_name(config: TableConfig) -> str:
    return _qualified_table_name(config.schema, config.name)


def _literal(value: object, data_type: str) -> str:
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, separators=(",", ":"))
    return _xtdb_sql_literal(value, _xtdb_cast_type(data_type))


def _encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode(value: str) -> bytes:
    return base64.b64decode(value)


def _accepted_at(prepared: PreparedCrdtCommit) -> datetime:
    if prepared.operations:
        return prepared.operations[0].recorded_at
    return datetime.now(timezone.utc)
