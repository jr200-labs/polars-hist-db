from typing import Any, Protocol


class HistoricalDbBackend(Protocol):
    name: str

    def dataframes(self, connection: Any) -> Any: ...

    def table_configs(self, connection: Any) -> Any: ...

    def tables(self, table_schema: str, table_name: str, connection: Any) -> Any: ...
