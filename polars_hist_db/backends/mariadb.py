from dataclasses import dataclass
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..core import DataframeOps, TableConfigOps, TableOps
from ..core import TimeHint
from .config import DbEngineConfig
from .temporal import system_time_hint_clause


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

    def tables(self, table_schema: str, table_name: str, connection: Any) -> TableOps:
        return TableOps(table_schema, table_name, connection)

    def time_hint_clause(self, time_hint: TimeHint) -> str | None:
        return system_time_hint_clause(time_hint)
