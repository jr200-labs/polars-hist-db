from typing import Any, Protocol

from ..config import TableConfig


class HistoricalDbBackend(Protocol):
    name: str

    def dataframes(self, connection: Any) -> Any: ...

    def table_configs(self, connection: Any) -> Any: ...

    def tables(self, table_schema: str, table_name: str, connection: Any) -> Any: ...

    def finalize_ingest_run(
        self, connection: Any, delta_table_config: TableConfig
    ) -> None:
        """Ensure the delta/stage table carries no rows once ingest returns."""
        ...
