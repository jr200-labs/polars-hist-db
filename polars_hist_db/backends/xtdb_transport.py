from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import re
from typing import Any, Iterable, Mapping, Optional, cast

import polars as pl
import pyarrow as pa
from sqlalchemy import text

from ..config import TableConfig
from ..core import TimeHint
from ..types import PolarsType, is_polars_type
from .temporal import system_time_hint_clause


_XTDB_SYSTEM_COLUMNS = {"_valid_from", "_valid_to", "_system_from", "_system_to"}
_XTDB_LAST_SYSTEM_TIME_KEY = "polars_hist_db_xtdb_last_system_time"
_XTDB_RESERVED_IDENTIFIERS = {"flag", "timestamp"}
_XTDB_QUERY_ROWS_PER_CHUNK = 10_000
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _table_config_ops(connection: Any) -> Any:
    # Imported lazily because xtdb.py re-exports this transport module.
    from .xtdb import XtdbTableConfigOps

    return XtdbTableConfigOps(connection)


def _validate_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsupported XTDB identifier: {identifier!r}")
    return identifier


def _quote_identifier(identifier: str) -> str:
    if (
        _IDENTIFIER_RE.match(identifier)
        and identifier == identifier.lower()
        and identifier not in _XTDB_RESERVED_IDENTIFIERS
    ):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def _xtdb_physical_column_name(column_name: str) -> str:
    return column_name.replace(".", "_").replace("/", "_").replace("-", "_").lower()


def _xtdb_column_identifier(column_name: str) -> str:
    return _quote_identifier(_xtdb_physical_column_name(column_name))


def _xtdb_physical_column_map(table_config: TableConfig) -> dict[str, str]:
    column_map = {}
    reverse_map: dict[str, str] = {}
    for column in table_config.columns:
        physical_name = _xtdb_physical_column_name(column.name)
        existing_logical_name = reverse_map.get(physical_name)
        if existing_logical_name is not None and existing_logical_name != column.name:
            raise ValueError(
                "XTDB physical column name collision: "
                f"{existing_logical_name!r} and {column.name!r} both map to "
                f"{physical_name!r}"
            )
        column_map[column.name] = physical_name
        reverse_map[physical_name] = column.name
    return column_map


def _restore_xtdb_logical_columns(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
) -> pl.DataFrame:
    if table_config is None:
        return df

    rename_map = {
        physical: logical
        for logical, physical in _xtdb_physical_column_map(table_config).items()
        if physical != logical and physical in df.columns
    }
    if not rename_map:
        return df
    return df.rename(rename_map)


def _qualified_table_name(table_schema: str, table_name: str) -> str:
    return f"{_validate_identifier(table_schema)}.{_validate_identifier(table_name)}"


def _apply_schema_overrides(
    df: pl.DataFrame,
    schema_overrides: Optional[Mapping[str, pl.DataType]],
) -> pl.DataFrame:
    if not schema_overrides:
        return df

    casts = {
        column: dtype
        for column, dtype in schema_overrides.items()
        if column in df.columns
    }
    if not casts:
        return df
    return df.with_columns(
        pl.col(column).cast(dtype) for column, dtype in casts.items()
    )


def _normalize_xtdb_ingest_arrow(table: pa.Table) -> pa.Table:
    for index, field in enumerate(table.schema):
        if pa.types.is_large_string(field.type):
            table = table.set_column(
                index,
                field.with_type(pa.string()),
                table.column(field.name).cast(pa.string()),
            )
        elif pa.types.is_dictionary(field.type) and pa.types.is_large_string(
            field.type.value_type
        ):
            table = table.set_column(
                index,
                field.with_type(pa.string()),
                table.column(field.name).cast(pa.string()),
            )
    return table


def _xtdb_document_id_columns(table_config: TableConfig) -> list[str]:
    primary_keys = list(table_config.primary_keys)
    configured_columns = [column.name for column in table_config.columns]

    if "_id" in configured_columns:
        return ["_id"]
    if primary_keys:
        return primary_keys

    raise ValueError(
        "XTDB backend requires at least one primary key or an explicit _id column"
    )


def _xtdb_json_safe_key_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _xtdb_composite_document_id(
    primary_keys: list[str],
    row: tuple[Any, ...],
) -> str:
    encoded_parts = [
        [key, _xtdb_json_safe_key_value(value)]
        for key, value in zip(primary_keys, row, strict=True)
    ]
    return "xtdb-pk-v1:" + json.dumps(
        encoded_parts,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _xtdb_cast_type(data_type: str) -> str:
    normalized = data_type.upper()
    if normalized.startswith(("BINARY", "VARBINARY")) or normalized.endswith("BLOB"):
        return "VARBINARY"
    if normalized.startswith(("VARCHAR", "CHAR")) or "TEXT" in normalized:
        return "TEXT"
    if normalized in {"JSON", "JSONB"}:
        return "TEXT"
    if normalized in {"DOUBLE", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    if normalized == "FLOAT":
        return "FLOAT"
    if normalized == "REAL":
        # MariaDB REAL is DOUBLE PRECISION unless REAL_AS_FLOAT is enabled.
        # The package contract likewise exposes REAL as Polars Float64.
        return "DOUBLE PRECISION"
    if normalized in {"INT", "INTEGER"}:
        return "INTEGER"
    if normalized in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    if normalized in {"BIT", "TINYINT", "SMALLINT", "MEDIUMINT"}:
        return "INTEGER"
    if normalized in {"BIGINT", "DATE", "TIME"}:
        return normalized
    if normalized.startswith("DATETIME"):
        return "TIMESTAMP WITH TIME ZONE"
    if normalized.startswith("TIMESTAMPTZ"):
        return "TIMESTAMP WITH TIME ZONE"
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return normalized
    if normalized.startswith("TIMESTAMP"):
        return normalized
    raise ValueError(f"Unsupported XTDB column type: {data_type}")


def _xtdb_cast_type_from_polars(dtype: pl.DataType) -> str:
    if is_polars_type(dtype, pl.Int8, pl.Int16, pl.Int32):
        return "INTEGER"
    if dtype == pl.Int64:
        return "BIGINT"
    if dtype == pl.Float32:
        return "FLOAT"
    if dtype == pl.Float64:
        return "DOUBLE PRECISION"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Time:
        return "TIME"
    if isinstance(dtype, pl.Datetime):
        return (
            "TIMESTAMP WITH TIME ZONE" if dtype.time_zone is not None else "TIMESTAMP"
        )
    if isinstance(dtype, pl.Decimal):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if dtype == pl.Binary:
        return "VARBINARY"
    if is_polars_type(dtype, pl.String, pl.Utf8, pl.Categorical):
        return "TEXT"
    raise ValueError(
        f"Unsupported XTDB Polars type {dtype}; provide a supported typed "
        "DataFrame or an explicit TableConfig"
    )


def _xtdb_insert_casts(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
) -> list[str]:
    configured_types = {}
    if table_config is not None:
        document_id_columns = _xtdb_document_id_columns(table_config)
        if len(document_id_columns) > 1:
            configured_types["_id"] = "TEXT"
        elif document_id_columns != ["_id"]:
            for column in table_config.columns:
                if column.name == document_id_columns[0]:
                    configured_types["_id"] = _xtdb_cast_type(column.data_type)
                    break
        for column in table_config.columns:
            configured_types[column.name] = _xtdb_cast_type(column.data_type)

    casts = []
    for name, dtype in df.schema.items():
        casts.append(
            configured_types[name]
            if name in configured_types
            else _xtdb_cast_type_from_polars(dtype)
        )
    return casts


def _xtdb_configured_column_dtypes(
    table_config: TableConfig,
) -> dict[str, pl.DataType]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    dtypes: dict[str, pl.DataType] = {}

    for column in table_config.columns:
        dtype = _xtdb_polars_type_or_none(column.data_type)
        if dtype is None:
            continue
        dtypes[column.name] = dtype
        if document_id_columns != ["_id"] and document_id_columns == [column.name]:
            dtypes["_id"] = dtype

    if len(document_id_columns) > 1:
        dtypes["_id"] = pl.String()
    dtypes["_valid_from"] = pl.Datetime("us", "UTC")
    dtypes["_valid_to"] = pl.Datetime("us", "UTC")
    return dtypes


def _xtdb_physical_configured_column_dtypes(
    table_config: TableConfig,
) -> dict[str, pl.DataType]:
    physical_column_map = _xtdb_physical_column_map(table_config)
    return {
        physical_column_map.get(column, _xtdb_physical_column_name(column)): dtype
        for column, dtype in _xtdb_configured_column_dtypes(table_config).items()
    }


def _apply_xtdb_configured_column_dtypes(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
    *,
    force_type_coercion: bool = False,
) -> pl.DataFrame:
    if table_config is None:
        return df

    return PolarsType.enforce_database_schema(
        df,
        _xtdb_configured_column_dtypes(table_config),
        backend="xtdb",
        operation="table_insert",
        force_type_coercion=force_type_coercion,
    )


def _iter_xtdb_insert_chunks(
    df: pl.DataFrame,
    max_rows_per_insert: Optional[int],
) -> Iterable[pl.DataFrame]:
    if max_rows_per_insert is None or df.height <= max_rows_per_insert:
        yield df
        return

    for offset in range(0, df.height, max_rows_per_insert):
        yield df.slice(offset, max_rows_per_insert)


def _prepare_xtdb_insert_dataframe(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
) -> pl.DataFrame:
    if table_config is None:
        return df

    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns == ["_id"]:
        return df
    if "_id" in df.columns:
        raise ValueError(
            "XTDB insert dataframe already contains _id and cannot also map "
            "configured primary keys to _id"
        )
    missing_keys = [key for key in document_id_columns if key not in df.columns]
    if missing_keys:
        raise ValueError(
            "XTDB insert dataframe is missing configured primary key columns "
            f"{missing_keys!r}"
        )

    if len(document_id_columns) == 1:
        return df.with_columns(pl.col(document_id_columns[0]).alias("_id")).select(
            ["_id", *df.columns]
        )

    document_ids = [
        _xtdb_composite_document_id(document_id_columns, row)
        for row in df.select(document_id_columns).iter_rows()
    ]

    return df.with_columns(pl.Series("_id", document_ids)).select(["_id", *df.columns])


def _driver_connection(connection: Any) -> Any | None:
    proxied_connection = getattr(connection, "connection", None)
    return getattr(proxied_connection, "driver_connection", None)


def _xtdb_timestamp_literal(value: datetime) -> str:
    escaped = value.isoformat().replace("'", "''")
    timestamp_type = "TIMESTAMP WITH TIME ZONE" if value.tzinfo else "TIMESTAMP"
    return f"{timestamp_type} '{escaped}'"


def _next_xtdb_system_time(connection: Any, system_time: datetime) -> datetime:
    info = getattr(connection, "info", None)
    if not isinstance(info, dict):
        return system_time

    last_system_time = info.get(_XTDB_LAST_SYSTEM_TIME_KEY)
    if isinstance(last_system_time, datetime) and system_time <= last_system_time:
        system_time = last_system_time + timedelta(microseconds=1)
    info[_XTDB_LAST_SYSTEM_TIME_KEY] = system_time
    return system_time


def _is_xtdb_invalid_system_time_error(exc: Exception) -> bool:
    message = str(exc)
    return "invalid-system-time" in message or "specified system-time older" in message


def _rollback_xtdb_connection(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _configure_xtdb_pgwire_parameter_adapters(driver_connection: Any) -> None:
    adapters = getattr(driver_connection, "adapters", None)
    register_dumper = getattr(adapters, "register_dumper", None)
    if not callable(register_dumper):
        return

    try:
        from psycopg.types.string import StrDumper
    except ModuleNotFoundError:
        return

    register_dumper(str, StrDumper)


def _xtdb_parameter_value(value: Any, cast_type: str) -> Any:
    if (
        isinstance(value, datetime)
        and cast_type.upper().startswith("TIMESTAMP")
        and value.tzinfo is not None
    ):
        value = value.astimezone(timezone.utc)
        if "WITH TIME ZONE" not in cast_type.upper():
            value = value.replace(tzinfo=None)
    return value


def _normalize_xtdb_timestamp_columns(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
) -> pl.DataFrame:
    casts = _xtdb_insert_casts(df, table_config)
    expressions = []
    for column, cast_type in zip(df.columns, casts, strict=True):
        dtype = df.schema[column]
        if (
            isinstance(dtype, pl.Datetime)
            and dtype.time_zone is not None
            and cast_type.upper().startswith("TIMESTAMP")
        ):
            expression = pl.col(column).dt.convert_time_zone("UTC")
            if "WITH TIME ZONE" not in cast_type.upper():
                expression = expression.dt.replace_time_zone(None)
            expressions.append(expression)
    if not expressions:
        return df
    return df.with_columns(expressions)


def _execute_xtdb_dml(
    connection: Any,
    sql: str,
    rows: list[tuple[Any, ...]] | None = None,
    *,
    system_time: Optional[datetime] = None,
) -> int:
    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        if rows is not None or system_time is not None:
            raise ValueError(
                "XTDB dataframe writes with rows or system-time require a live "
                "DBAPI connection"
            )
        connection.execute(text(sql))
        return 0

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        begin_sql = (
            "BEGIN READ WRITE WITH "
            f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
        )
    driver_connection.execute(begin_sql)
    try:
        if rows is None:
            driver_connection.execute(sql)
            row_count = 0
        else:
            _configure_xtdb_pgwire_parameter_adapters(driver_connection)
            cursor = driver_connection.cursor()
            try:
                cursor.executemany(sql, rows)
            finally:
                close = getattr(cursor, "close", None)
                if callable(close):
                    close()
            row_count = len(rows)
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            return _execute_xtdb_dml(connection, sql, rows, system_time=None)
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit
    return row_count


def _execute_xtdb_arrow_copy(
    connection: Any,
    table_sql: str,
    df: pl.DataFrame,
    *,
    system_time: Optional[datetime] = None,
) -> None:
    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB Arrow COPY requires a live DBAPI connection")

    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    begin_sql = "BEGIN READ WRITE"
    if system_time is not None:
        system_time = _next_xtdb_system_time(connection, system_time)
        begin_sql = (
            "BEGIN READ WRITE WITH "
            f"(SYSTEM_TIME = {_xtdb_timestamp_literal(system_time)})"
        )

    arrow_table = _normalize_xtdb_ingest_arrow(df.to_arrow())
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
        writer.write_table(arrow_table)

    driver_connection.execute(begin_sql)
    try:
        cursor = driver_connection.cursor()
        try:
            with cursor.copy(
                f"COPY {table_sql} FROM STDIN WITH (FORMAT 'arrow-stream')"
            ) as copy:
                copy.write(sink.getvalue().to_pybytes())
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        driver_connection.execute("COMMIT")
    except Exception as exc:
        driver_connection.execute("ROLLBACK")
        driver_connection.rollback()
        if system_time is not None and _is_xtdb_invalid_system_time_error(exc):
            _execute_xtdb_arrow_copy(connection, table_sql, df, system_time=None)
            return
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit


def _xtdb_temporal_basis_clause(update_time: Optional[datetime]) -> str:
    if update_time is None:
        return ""
    return (
        f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(update_time)}"
        f" FOR SYSTEM_TIME AS OF {_xtdb_timestamp_literal(update_time)}"
    )


def _xtdb_valid_time_clause(time_hint: Optional[TimeHint]) -> str:
    if time_hint is None or time_hint.mode == "none":
        return ""
    if time_hint.mode == "all":
        return " FOR VALID_TIME ALL"
    if time_hint.mode == "asof":
        assert isinstance(time_hint.asof_utc, datetime)
        return f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(time_hint.asof_utc)}"
    if time_hint.mode == "span":
        assert isinstance(time_hint.asof_utc, datetime)
        assert time_hint.history_span is not None
        if time_hint.history_span.total_seconds() == 0:
            return (
                f" FOR VALID_TIME AS OF {_xtdb_timestamp_literal(time_hint.asof_utc)}"
            )
        start_date_utc = time_hint.asof_utc - time_hint.history_span
        return (
            " FOR VALID_TIME BETWEEN "
            f"{_xtdb_timestamp_literal(start_date_utc)} AND "
            f"{_xtdb_timestamp_literal(time_hint.asof_utc)}"
        )

    raise ValueError(f"invalid TimeHint mode: {time_hint.mode}")


def _xtdb_single_primary_key_alias(table_config: TableConfig) -> str | None:
    primary_keys = list(table_config.primary_keys)
    if len(primary_keys) != 1:
        return None
    primary_key = primary_keys[0]
    if primary_key != "_id":
        return primary_key
    return None


def _xtdb_table_query_output_columns(
    table_config: TableConfig,
    column_selection: Optional[list[str]],
) -> list[str]:
    if column_selection is not None:
        return column_selection

    columns = []
    single_key_alias = _xtdb_single_primary_key_alias(table_config)
    if single_key_alias is not None:
        columns.append(single_key_alias)

    columns.extend(
        column.name
        for column in table_config.columns
        if column.name not in {"_id", single_key_alias, *_XTDB_SYSTEM_COLUMNS}
    )
    columns.extend(["__valid_from", "__valid_to"])
    return columns


def _xtdb_table_query_select_expr(column: str, table_config: TableConfig) -> str:
    single_key_alias = _xtdb_single_primary_key_alias(table_config)
    if single_key_alias == column:
        return f"t._id AS {_xtdb_column_identifier(column)}"
    if column == "__valid_from":
        return "t._valid_from AS __valid_from"
    if column == "__valid_to":
        return "t._valid_to AS __valid_to"
    return f"t.{_xtdb_column_identifier(column)}"


def _xtdb_table_query_target_column(column: str, table_config: TableConfig) -> str:
    if _xtdb_single_primary_key_alias(table_config) == column:
        return "_id"
    return _xtdb_column_identifier(column)


def _xtdb_values_cte(name: str, df: pl.DataFrame) -> tuple[str, dict[str, Any]]:
    if df.is_empty():
        raise ValueError("XTDB table_query requires at least one query row")

    cte_name = _validate_identifier(name)
    columns = [_xtdb_column_identifier(column) for column in df.columns]
    casts = [_xtdb_cast_type_from_polars(dtype) for dtype in df.schema.values()]
    parameters = {}
    row_queries = []
    for row_index, row in enumerate(df.rows()):
        projections = []
        for column_index, (value, cast_type) in enumerate(zip(row, casts, strict=True)):
            parameter = f"q_{row_index}_{column_index}"
            parameters[parameter] = _xtdb_parameter_value(value, cast_type)
            projections.append(
                f"CAST(:{parameter} AS {cast_type}) AS {columns[column_index]}"
            )
        row_queries.append(f"SELECT {', '.join(projections)}")
    return (
        f"{cte_name} AS ({' UNION ALL '.join(row_queries)})",
        parameters,
    )


def _xtdb_polars_type_or_none(data_type: str) -> pl.DataType | None:
    normalized = data_type.upper()
    if normalized.startswith(("DATETIME", "TIMESTAMPTZ")) or normalized.startswith(
        "TIMESTAMP WITH TIME ZONE"
    ):
        return pl.Datetime("us", "UTC")
    try:
        return PolarsType.from_sql(data_type)
    except ValueError:
        return None


class XtdbDataframeOps:
    def __init__(
        self,
        connection: Any,
        max_rows_per_insert: Optional[int] = None,
    ):
        self.connection = connection
        self.max_rows_per_insert = max_rows_per_insert

    def from_raw_sql(
        self,
        query: str,
        schema_overrides: Optional[Mapping[str, pl.DataType]] = None,
        execute_options: Optional[dict[str, Any]] = None,
    ) -> pl.DataFrame:
        if schema_overrides is None:
            schema_overrides = {}
        kwargs: dict[str, Any] = {"schema_overrides": schema_overrides}
        if execute_options is not None:
            kwargs["execute_options"] = execute_options
        return pl.read_database(
            query,
            self.connection,
            **kwargs,
        )

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        schema_overrides: Optional[Mapping[str, pl.DataType]] = None,
        time_hint: Optional[TimeHint] = None,
    ) -> pl.DataFrame:
        table_config: TableConfig | None = None
        if schema_overrides is None:
            table_config = _table_config_ops(self.connection).from_table(
                table_schema,
                table_name,
            )
            schema_overrides = _xtdb_physical_configured_column_dtypes(table_config)

        hint_clause = system_time_hint_clause(time_hint)
        query = f"SELECT * FROM {table_schema}.{table_name}"
        if hint_clause:
            query = f"{query} {hint_clause}"

        return _restore_xtdb_logical_columns(
            self.from_raw_sql(
                query,
                schema_overrides,
            ),
            table_config,
        )

    def table_query(
        self,
        table_schema: str,
        table_name: str,
        query_df: pl.DataFrame,
        column_selection: Optional[list[str]],
        time_hint: TimeHint | None = None,
        table_config: TableConfig | None = None,
        basis_time: datetime | None = None,
    ) -> pl.DataFrame:
        if table_config is None:
            table_config = _table_config_ops(self.connection).from_table(
                table_schema,
                table_name,
            )
        output_columns = _xtdb_table_query_output_columns(
            table_config,
            column_selection,
        )
        schema_overrides = {}
        for column in table_config.columns:
            if column.name not in output_columns:
                continue
            dtype = _xtdb_polars_type_or_none(column.data_type)
            if dtype is not None:
                schema_overrides[column.name] = dtype
        if "_id" in output_columns:
            document_id_columns = _xtdb_document_id_columns(table_config)
            if len(document_id_columns) > 1:
                schema_overrides["_id"] = pl.String()
            else:
                id_config = next(
                    (
                        column
                        for column in table_config.columns
                        if column.name == document_id_columns[0]
                    ),
                    None,
                )
                if id_config is not None:
                    schema_overrides["_id"] = (
                        _xtdb_polars_type_or_none(id_config.data_type) or pl.String()
                    )
        for temporal_column in ["__valid_from", "__valid_to"]:
            if temporal_column in output_columns:
                schema_overrides[temporal_column] = pl.Datetime("us")
        if query_df.is_empty():
            return pl.DataFrame(schema=schema_overrides)
        select_sql = ", ".join(
            _xtdb_table_query_select_expr(column, table_config)
            for column in output_columns
        )
        table_sql = _qualified_table_name(table_schema, table_name)
        valid_time_clause = (
            _xtdb_temporal_basis_clause(basis_time)
            if basis_time is not None
            else _xtdb_valid_time_clause(time_hint)
        )
        single_key_alias = _xtdb_single_primary_key_alias(table_config)
        if single_key_alias is not None and single_key_alias in output_columns:
            id_column = next(
                column
                for column in table_config.columns
                if column.name == single_key_alias
            )
            id_dtype = _xtdb_polars_type_or_none(id_column.data_type)
            if id_dtype is not None:
                schema_overrides[single_key_alias] = id_dtype
        chunk_size = self.max_rows_per_insert or _XTDB_QUERY_ROWS_PER_CHUNK
        chunks = []
        for query_chunk in query_df.iter_slices(chunk_size):
            values_cte, parameters = _xtdb_values_cte("query_df", query_chunk)
            join_clause = " AND ".join(
                "t."
                f"{_xtdb_table_query_target_column(column, table_config)} = "
                f"q.{_xtdb_column_identifier(column)}"
                for column in query_chunk.columns
            )
            query = (
                f"WITH {values_cte} "
                f"SELECT {select_sql} "
                "FROM ("
                "SELECT *, _valid_from, _valid_to "
                f"FROM {table_sql}{valid_time_clause}"
                ") AS t "
                "JOIN query_df AS q "
                f"ON {join_clause}"
            )
            chunks.append(
                self.from_raw_sql(
                    query,
                    schema_overrides,
                    {"parameters": parameters},
                )
            )
        df = pl.concat(chunks, how="vertical_relaxed")
        for temporal_column in ["__valid_from", "__valid_to"]:
            if (
                temporal_column in df.columns
                and isinstance(df[temporal_column].dtype, pl.Datetime)
                and getattr(df[temporal_column].dtype, "time_zone", None) is not None
            ):
                df = df.with_columns(pl.col(temporal_column).dt.replace_time_zone(None))
        if "__valid_to" in df.columns:
            df = df.with_columns(
                pl.col("__valid_to").fill_null(
                    datetime.fromisoformat("2106-02-07T06:28:15.999999")
                )
            )
        return df.pipe(PolarsType.cast_str_to_cat)

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        table_config: Optional[TableConfig] = None,
        update_time: Optional[datetime] = None,
        force_type_coercion: bool = False,
    ) -> int:
        if table_config is not None:
            _xtdb_physical_column_map(table_config)
        df = _prepare_xtdb_insert_dataframe(df, table_config)
        df = _apply_xtdb_configured_column_dtypes(
            df,
            table_config,
            force_type_coercion=force_type_coercion,
        )
        if df.is_empty():
            return 0

        driver_connection = _driver_connection(self.connection)
        if driver_connection is not None:
            inserted_count = 0
            for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
                table_sql = _qualified_table_name(table_schema, table_name)
                if chunk.height > 1:
                    physical_columns = {
                        column: _xtdb_physical_column_name(column)
                        for column in chunk.columns
                        if column != _xtdb_physical_column_name(column)
                    }
                    _execute_xtdb_arrow_copy(
                        self.connection,
                        table_sql,
                        chunk.rename(physical_columns),
                        system_time=update_time,
                    )
                    inserted_count += chunk.height
                    continue

                columns = [_xtdb_column_identifier(column) for column in chunk.columns]
                column_sql = ", ".join(columns)
                casts = _xtdb_insert_casts(chunk, table_config)
                rows = [
                    tuple(
                        _xtdb_parameter_value(value, cast)
                        for value, cast in zip(row, casts, strict=True)
                    )
                    for row in chunk.rows()
                ]
                values_sql = "(" + ", ".join(f"%s::{cast}" for cast in casts) + ")"
                insert_sql = (
                    f"INSERT INTO {table_sql} ({column_sql}) VALUES {values_sql}"
                )
                _execute_xtdb_dml(
                    self.connection,
                    insert_sql,
                    rows,
                    system_time=update_time,
                )
                inserted_count += len(rows)
            return inserted_count

        if update_time is not None:
            raise ValueError(
                "XTDB pgwire system-time writes require a live DBAPI connection"
            )

        inserted_count = 0
        for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
            result = chunk.write_database(
                table_name=f"{table_schema}.{table_name}",
                connection=self.connection,
                if_table_exists="append",
            )
            inserted_count += int(result) if result is not None else 0
        return inserted_count


class XtdbAdbcDataframeOps:
    def __init__(
        self,
        connection: Any,
        max_rows_per_insert: Optional[int] = None,
    ):
        self.connection = connection
        self.max_rows_per_insert = max_rows_per_insert

    def from_raw_sql(
        self,
        query: str,
        schema_overrides: Optional[Mapping[str, pl.DataType]] = None,
    ) -> pl.DataFrame:
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            arrow_table = cursor.fetch_arrow_table()

        return _apply_schema_overrides(
            cast(pl.DataFrame, pl.from_arrow(arrow_table)),
            schema_overrides,
        )

    def from_table(
        self,
        table_schema: str,
        table_name: str,
        schema_overrides: Optional[Mapping[str, pl.DataType]] = None,
        time_hint: Optional[TimeHint] = None,
    ) -> pl.DataFrame:
        table_sql = _qualified_table_name(table_schema, table_name)
        hint_clause = system_time_hint_clause(time_hint)
        query = f"SELECT * FROM {table_sql}"
        if hint_clause:
            query = f"{query} {hint_clause}"

        return self.from_raw_sql(
            query,
            schema_overrides,
        )

    table_query = XtdbDataframeOps.table_query

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        table_config: Optional[TableConfig] = None,
        update_time: Optional[datetime] = None,
        force_type_coercion: bool = False,
    ) -> int:
        if update_time is not None:
            raise NotImplementedError(
                "XTDB ADBC dataframe ingest does not yet support transaction "
                "SYSTEM_TIME; use the pgwire dataframe path for update_time"
            )

        table_schema = _validate_identifier(table_schema)
        table_name = _validate_identifier(table_name)
        if df.is_empty():
            return 0

        df = _prepare_xtdb_insert_dataframe(df, table_config)
        df = _apply_xtdb_configured_column_dtypes(
            df,
            table_config,
            force_type_coercion=force_type_coercion,
        )
        for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
            arrow_table = _normalize_xtdb_ingest_arrow(chunk.to_arrow())
            with self.connection.cursor() as cursor:
                cursor.adbc_ingest(
                    table_name,
                    arrow_table,
                    mode="create_append",
                    db_schema_name=table_schema,
                )
        return df.height
