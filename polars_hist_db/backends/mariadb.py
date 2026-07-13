from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..core import DataframeOps, TableConfigOps, TableOps
from ..core import TimeHint
from .config import DbEngineConfig
from .temporal import system_time_hint_clause

if TYPE_CHECKING:
    from ..config import TableConfig
    from ..overrides import (
        CrdtDocumentStoreConfig,
        DocumentAccessStoreConfig,
        OverrideLedgerConfig,
    )
    from ..overrides.sql import MariaDbDocumentAccessStore
    from ..overrides.sql import MariaDbCrdtDocumentStore


@dataclass(frozen=True)
class MariaDbBackend:
    name: str = "mariadb"

    def create_engine(self, config: DbEngineConfig) -> Engine:
        auth = ""
        if config.username:
            auth = config.username
            if config.password:
                auth = f"{auth}:{config.password}"
            auth = f"{auth}@"

        url = f"mariadb+pymysql://{auth}{config.hostname}:{config.port}"
        return create_engine(
            url,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
        )

    def dataframes(self, connection: Any) -> DataframeOps:
        return DataframeOps(connection)

    def table_configs(self, connection: Any) -> TableConfigOps:
        return TableConfigOps(connection)

    def crdt_documents(
        self,
        connection: Any,
        document_store: "CrdtDocumentStoreConfig",
        projection: "OverrideLedgerConfig",
    ) -> "MariaDbCrdtDocumentStore":
        from ..overrides.sql import MariaDbCrdtDocumentStore

        return MariaDbCrdtDocumentStore(connection, document_store, projection)

    def document_access(
        self, connection: Any, config: "DocumentAccessStoreConfig"
    ) -> "MariaDbDocumentAccessStore":
        from ..overrides.sql import MariaDbDocumentAccessStore

        return MariaDbDocumentAccessStore(connection, config)

    def tables(self, table_schema: str, table_name: str, connection: Any) -> TableOps:
        return TableOps(table_schema, table_name, connection)

    def time_hint_clause(self, time_hint: TimeHint) -> str | None:
        return system_time_hint_clause(time_hint)

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        connection.execute(
            text(f"DELETE FROM {delta_table_config.schema}.{delta_table_config.name}")
        )
