from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime
from urllib.parse import quote
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Optional,
)

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..config import (
    DeltaConfig,
    TableConfig,
    ValidTimeConfig,
)
from ..core import TimeHint
from .config import DbEngineConfig
from .base import (
    TableHealthResult,
    bounded_table_health_query,
    execute_table_health_query,
)
from .temporal import system_time_hint_clause
from . import xtdb_arrow as _xtdb_arrow
from . import xtdb_delta as _xtdb_delta
from . import xtdb_dataframe as _xtdb_dataframe
from . import xtdb_query as _xtdb_query
from . import xtdb_schema as _xtdb_schema
from . import xtdb_staging as _xtdb_staging
from . import xtdb_transport as _xtdb_transport
from .xtdb_schema import XtdbTableConfigOps
from .xtdb_delta import _xtdb_temporal_upsert
from .xtdb_staging import XtdbStagingOps
from .xtdb_dataframe import (
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
)

if TYPE_CHECKING:
    from ..overrides import (
        ArrowOverrideStoreConfig,
        CrdtDocumentStoreConfig,
        DocumentAccessStoreConfig,
        LayerCompositionStoreConfig,
        OverrideLedgerConfig,
    )
    from ..overrides.xtdb import XtdbDocumentAccessStore
    from ..overrides.arrow import RepositoryArrowOverrideOperationStore
    from ..overrides.xtdb import XtdbLayerCompositionStore
    from ..overrides.xtdb import XtdbCrdtDocumentStore


def __getattr__(name: str) -> Any:
    """Keep legacy private XTDB imports working during the module split."""
    try:
        return getattr(_xtdb_dataframe, name)
    except AttributeError:
        try:
            return getattr(_xtdb_arrow, name)
        except AttributeError:
            try:
                return getattr(_xtdb_query, name)
            except AttributeError:
                try:
                    return getattr(_xtdb_delta, name)
                except AttributeError:
                    try:
                        return getattr(_xtdb_staging, name)
                    except AttributeError:
                        try:
                            return getattr(_xtdb_schema, name)
                        except AttributeError:
                            return getattr(_xtdb_transport, name)


def _load_flight_sql() -> Any:
    try:
        from adbc_driver_flightsql import dbapi as flight_sql
    except ImportError as exc:
        raise RuntimeError(
            "XTDB ADBC support requires the 'xtdb' extra "
            "(adbc-driver-flightsql). Install with polars-hist-db[xtdb]."
        ) from exc
    return flight_sql


@dataclass(frozen=True)
class XtdbBackend:
    name: str = "xtdb"
    max_rows_per_insert: Optional[int] = 10_000

    def create_engine(self, config: DbEngineConfig) -> Engine:
        database = config.database or "xtdb"
        auth = ""
        if config.username:
            auth = quote(config.username, safe="")
            if config.password:
                auth = f"{auth}:{quote(config.password, safe='')}"
            auth = f"{auth}@"
        url = f"postgresql+psycopg://{auth}{config.hostname}:{config.port}/{database}"
        engine = create_engine(
            url,
            connect_args={"prepare_threshold": None},
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            use_native_hstore=False,
        )
        # XTDB pgwire currently trips SQLAlchemy's PostgreSQL dialect when it
        # probes SHOW standard_conforming_strings. XTDB uses standard strings,
        # so skip that PostgreSQL-specific initialization query.
        engine.dialect._set_backslash_escapes = lambda connection: setattr(  # type: ignore[attr-defined, method-assign]
            engine.dialect, "_backslash_escapes", False
        )
        return engine

    @contextmanager
    def connection_scope(self, engine: Engine) -> Iterator[Any]:
        """Open a scope for XTDB stores, which manage their own transactions."""
        with engine.connect() as connection:
            yield connection
            connection.commit()

    def adbc_uri(self, config: DbEngineConfig) -> str:
        adbc_port = config.adbc_port or 9832
        return f"grpc://{config.hostname}:{adbc_port}"

    def create_adbc_connection(self, config: DbEngineConfig) -> Any:
        return _load_flight_sql().connect(self.adbc_uri(config), autocommit=True)

    def open_ingest_connection(self, config: DbEngineConfig) -> Any:
        return self.create_adbc_connection(config)

    def close_ingest_connection(self, connection: Any | None) -> None:
        if connection is not None:
            connection.close()

    def dataframes(self, connection: Any) -> XtdbDataframeOps:
        return XtdbDataframeOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
        )

    def adbc_dataframes(self, connection: Any) -> XtdbAdbcDataframeOps:
        return XtdbAdbcDataframeOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
        )

    def table_configs(self, connection: Any) -> XtdbTableConfigOps:
        return XtdbTableConfigOps(connection)

    def table_health_query(
        self, table_schema: str, table_name: str, minimum_rows: int = 0
    ) -> str:
        return bounded_table_health_query(
            table_schema, table_name, minimum_rows, quote='"'
        )

    def check_table_resource(
        self,
        connection: Any,
        table_schema: str,
        table_name: str,
        minimum_rows: int = 0,
    ) -> TableHealthResult:
        return execute_table_health_query(
            connection,
            self.table_health_query(table_schema, table_name, minimum_rows),
            minimum_rows,
        )

    def crdt_documents(
        self,
        connection: Any,
        document_store: "CrdtDocumentStoreConfig",
        projection: "OverrideLedgerConfig",
    ) -> "XtdbCrdtDocumentStore":
        from ..overrides.xtdb import XtdbCrdtDocumentStore

        return XtdbCrdtDocumentStore(connection, document_store, projection)

    def arrow_overrides(
        self, connection: Any, config: "ArrowOverrideStoreConfig"
    ) -> "RepositoryArrowOverrideOperationStore":
        from ..overrides.arrow import RepositoryArrowOverrideOperationStore
        from ..overrides.arrow_xtdb import XtdbArrowOverrideRepository

        return RepositoryArrowOverrideOperationStore(
            XtdbArrowOverrideRepository(connection, config)
        )

    def document_access(
        self, connection: Any, config: "DocumentAccessStoreConfig"
    ) -> "XtdbDocumentAccessStore":
        from ..overrides.xtdb import XtdbDocumentAccessStore

        return XtdbDocumentAccessStore(connection, config)

    def layer_compositions(
        self, connection: Any, config: "LayerCompositionStoreConfig"
    ) -> "XtdbLayerCompositionStore":
        from ..overrides.xtdb import XtdbLayerCompositionStore

        return XtdbLayerCompositionStore(connection, config)

    def staging(
        self,
        connection: Any,
        *,
        ingest_connection: Any | None = None,
    ) -> XtdbStagingOps:
        return XtdbStagingOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
            adbc_connection=ingest_connection,
        )

    def time_hint_clause(self, time_hint: TimeHint) -> str | None:
        return system_time_hint_clause(time_hint)

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        # Stage rows are already erased per partition by cleanup_run.
        return None

    def temporal_upsert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        *,
        connection: Any | None = None,
        dataframe_ops: XtdbDataframeOps | XtdbAdbcDataframeOps | None = None,
        table_config: Optional[TableConfig] = None,
        delta_config: Optional[DeltaConfig] = None,
        update_time: Optional[datetime] = None,
        valid_time: Optional[ValidTimeConfig] = None,
        dropout_close_time: Optional[datetime] = None,
    ) -> int:
        return _xtdb_temporal_upsert(
            df,
            table_schema,
            table_name,
            connection=connection,
            dataframe_ops=dataframe_ops,
            table_config=table_config,
            delta_config=delta_config,
            update_time=update_time,
            valid_time=valid_time,
            dropout_close_time=dropout_close_time,
            max_rows_per_insert=self.max_rows_per_insert,
            dataframe_factory=self.dataframes,
        )
