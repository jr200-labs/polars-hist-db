from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import logging
import math
import re
from urllib.parse import quote
from typing import TYPE_CHECKING, Any, Iterable, Literal, Mapping, Optional, cast

import polars as pl
import pyarrow as pa
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..config import (
    DeltaConfig,
    ForeignKeyConfig,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)
from ..core import TimeHint
from ..pipeline_projection import project_staged_pipeline_item_dataframe
from ..types import PolarsType
from .config import DbEngineConfig
from .temporal import system_time_hint_clause

if TYPE_CHECKING:
    from ..overrides import (
        CrdtDocumentStoreConfig,
        DocumentAccessStoreConfig,
        LayerCompositionStoreConfig,
        OverrideLedgerConfig,
    )
    from ..overrides.xtdb import XtdbDocumentAccessStore
    from ..overrides.xtdb import XtdbLayerCompositionStore
    from ..overrides.xtdb import XtdbCrdtDocumentStore

LOGGER = logging.getLogger(__name__)

_XTDB_SYSTEM_COLUMNS = {"_valid_from", "_valid_to", "_system_from", "_system_to"}
_XTDB_READONLY_SYSTEM_COLUMNS = {"_system_from", "_system_to"}
_XTDB_TABLE_CONFIG_METADATA_TABLE = "__polars_hist_db_xtdb_table_configs"
_XTDB_STAGE_RUN_ID_COLUMN = "stage_run_id"
_XTDB_STAGE_ROW_INDEX_COLUMN = "stage_row_index"
_XTDB_STAGE_PARTITION_TIME_COLUMN = "stage_partition_time"
_XTDB_LAST_SYSTEM_TIME_KEY = "polars_hist_db_xtdb_last_system_time"
_XTDB_RESERVED_IDENTIFIERS = {"flag", "timestamp"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_flight_sql() -> Any:
    try:
        from adbc_driver_flightsql import dbapi as flight_sql
    except ImportError as exc:
        raise RuntimeError(
            "XTDB ADBC support requires the 'xtdb' extra "
            "(adbc-driver-flightsql). Install with polars-hist-db[xtdb]."
        ) from exc
    return flight_sql


def _is_xtdb_adbc_ingest_unavailable(exc: Exception) -> bool:
    message = str(exc).lower()
    class_name = exc.__class__.__name__
    return (
        class_name in {"NotImplementedError", "NotSupportedError"}
        and "executeingest" in message
        and ("not implemented" in message or "not_implemented" in message)
    )


def _xtdb_type_to_config_type(data_type: str) -> str:
    normalized = data_type.upper()
    xtdb_types = {
        ":I8": "TINYINT",
        ":I16": "SMALLINT",
        ":I32": "INT",
        ":I64": "BIGINT",
        ":F32": "FLOAT",
        ":F64": "DOUBLE",
        ":UTF8": "VARCHAR(255)",
        ":BOOL": "BOOLEAN",
        ":BOOLEAN": "BOOLEAN",
        ":DATE": "DATE",
        ":TIMESTAMP": "TIMESTAMP",
    }
    return xtdb_types.get(normalized, normalized)


def _is_nullable(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).upper() in {"YES", "TRUE", "1"}


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


def _xtdb_table_config_metadata_table(table_schema: str) -> str:
    return _qualified_table_name(table_schema, _XTDB_TABLE_CONFIG_METADATA_TABLE)


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


def _xtdb_declared_columns(table_config: TableConfig) -> list[str]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    uses_explicit_id = document_id_columns == ["_id"]

    declared_columns = ["_id"]
    for column in table_config.columns:
        if column.name in _XTDB_SYSTEM_COLUMNS:
            continue
        if uses_explicit_id and column.name == "_id":
            continue
        declared_columns.append(column.name)

    return [_xtdb_column_identifier(column) for column in declared_columns]


def _xtdb_stage_table_name(dataset_name: str) -> str:
    return f"__{dataset_name}_stage"


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


def _xtdb_document_id_source(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if len(document_id_columns) == 1:
        return document_id_columns[0]

    raise ValueError(
        "XTDB backend requires a single document id source for this operation"
    )


def _xtdb_json_safe_key_value(value: Any) -> Any:
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


def _xtdb_table_config_metadata(
    connection: Any,
    table_schema: str,
    table_name: str,
) -> pl.DataFrame | None:
    escaped_schema = table_schema.replace("'", "''")
    escaped_name = table_name.replace("'", "''")
    try:
        return pl.read_database(
            f"""
            SELECT *
            FROM {_xtdb_table_config_metadata_table(table_schema)}
            WHERE table_schema = '{escaped_schema}'
              AND table_name = '{escaped_name}'
        """,
            connection,
        )
    except Exception:
        return None


def _xtdb_primary_keys_from_metadata(
    metadata: pl.DataFrame | None,
    table_schema: str,
    table_name: str,
) -> list[str] | None:
    if (
        metadata is None
        or metadata.is_empty()
        or "primary_keys_json" not in metadata.columns
    ):
        return None

    primary_keys = json.loads(str(metadata[0, "primary_keys_json"]))
    if not isinstance(primary_keys, list) or not all(
        isinstance(key, str) for key in primary_keys
    ):
        raise ValueError(
            f"Invalid XTDB table-config metadata for {table_schema}.{table_name}"
        )
    return primary_keys


def _xtdb_table_config_from_metadata(
    metadata: pl.DataFrame | None,
    table_schema: str,
    table_name: str,
) -> TableConfig | None:
    if (
        metadata is None
        or metadata.is_empty()
        or "columns_json" not in metadata.columns
    ):
        return None

    columns_json = metadata[0, "columns_json"]
    if columns_json is None:
        return None

    raw_columns = json.loads(str(columns_json))
    if not isinstance(raw_columns, list):
        raise ValueError(
            f"Invalid XTDB column metadata for {table_schema}.{table_name}"
        )

    columns = [
        TableColumnConfig(**column)
        for column in raw_columns
        if isinstance(column, dict)
    ]
    primary_keys = _xtdb_primary_keys_from_metadata(
        metadata,
        table_schema,
        table_name,
    )
    foreign_keys: list[ForeignKeyConfig] = []
    if "foreign_keys_json" in metadata.columns:
        foreign_keys_json = metadata[0, "foreign_keys_json"]
        if foreign_keys_json is not None:
            raw_foreign_keys = json.loads(str(foreign_keys_json))
            if not isinstance(raw_foreign_keys, list):
                raise ValueError(
                    f"Invalid XTDB foreign-key metadata for {table_schema}.{table_name}"
                )
            foreign_keys = [
                ForeignKeyConfig(**foreign_key)
                for foreign_key in raw_foreign_keys
                if isinstance(foreign_key, dict)
            ]
    return TableConfig(
        name=table_name,
        schema=table_schema,
        columns=columns,
        foreign_keys=foreign_keys,
        primary_keys=primary_keys or [],
        is_temporal=True,
    )


def _xtdb_foreign_keys_json(table_config: TableConfig) -> str:
    foreign_keys = []
    for foreign_key in table_config.foreign_keys:
        ref_table = foreign_key.references.table
        ref_table_name = (
            ref_table.name if isinstance(ref_table, TableConfig) else str(ref_table)
        )
        foreign_keys.append(
            {
                "name": foreign_key.name,
                "references": {
                    "schema": foreign_key.references.schema,
                    "table": ref_table_name,
                    "column": foreign_key.references.column,
                },
            }
        )
    return json.dumps(foreign_keys, separators=(",", ":"))


def _xtdb_id_policy(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if document_id_columns == ["_id"]:
        return "explicit-id"
    if len(document_id_columns) == 1:
        return "single-key"
    return "xtdb-pk-v1"


def _xtdb_cast_type(data_type: str) -> str:
    normalized = data_type.upper()
    if normalized.startswith(("VARCHAR", "CHAR")) or "TEXT" in normalized:
        return "TEXT"
    if normalized in {"DOUBLE", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    if normalized in {"FLOAT", "REAL"}:
        return "FLOAT"
    if normalized in {"INT", "INTEGER"}:
        return "INTEGER"
    if normalized in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    if normalized in {"BIT", "TINYINT", "SMALLINT", "MEDIUMINT"}:
        return "INTEGER"
    if normalized in {"BIGINT", "DATE", "TIME"}:
        return normalized
    if normalized == "DATETIME":
        return "TIMESTAMP"
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return normalized
    if normalized.startswith("TIMESTAMP"):
        return normalized
    return "TEXT"


def _xtdb_cast_type_from_polars(dtype: pl.DataType) -> str:
    if dtype in {pl.Int8, pl.Int16, pl.Int32}:
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
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
    return "TEXT"


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
        casts.append(configured_types.get(name, _xtdb_cast_type_from_polars(dtype)))
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
) -> pl.DataFrame:
    if table_config is None:
        return df

    configured_dtypes = _xtdb_configured_column_dtypes(table_config)
    casts = []
    for column, dtype in configured_dtypes.items():
        if column not in df.columns:
            continue

        source_expr = pl.col(column)
        if df.schema[column] == pl.Categorical and dtype not in {pl.String, pl.Utf8}:
            source_expr = source_expr.cast(pl.String)
        casts.append(source_expr.cast(dtype, strict=False))

    if not casts:
        return df
    return df.with_columns(casts)


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


def _apply_xtdb_duplicate_policy(
    df: pl.DataFrame,
    table_config: TableConfig,
    delta_config: DeltaConfig,
) -> pl.DataFrame:
    if df.is_empty():
        return df

    row_index = "__xtdb_duplicate_row_index"
    indexed_df = df.with_row_index(row_index)
    prepared = _prepare_xtdb_insert_dataframe(indexed_df, table_config)

    duplicate_count_column = "__xtdb_duplicate_count"
    duplicate_keys = (
        prepared.group_by("_id")
        .agg(pl.len().alias(duplicate_count_column))
        .filter(pl.col(duplicate_count_column) > 1)
    )
    if duplicate_keys.is_empty():
        return df

    if delta_config.on_duplicate_key == "error":
        raise ValueError("XTDB delta upsert found duplicate source keys")

    keep: Literal["first", "last"] = (
        "first" if delta_config.on_duplicate_key == "take_first" else "last"
    )
    selected_rows = (
        prepared.select([row_index, "_id"])
        .unique(subset=["_id"], keep=keep, maintain_order=True)
        .select(row_index)
    )
    return indexed_df.join(selected_rows, on=row_index, how="inner").drop(row_index)


def _xtdb_document_id_cast_type(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if len(document_id_columns) > 1:
        return "TEXT"
    source_key = document_id_columns[0]
    for column in table_config.columns:
        if column.name == source_key:
            return _xtdb_cast_type(column.data_type)
    return "TEXT"


def _delete_xtdb_missing_rows(
    df: pl.DataFrame,
    table_schema: str,
    table_name: str,
    table_config: TableConfig,
    dataframe_ops: "XtdbDataframeOps",
    update_time: Optional[datetime],
    dropout_close_time: Optional[datetime],
) -> int:
    try:
        current = dataframe_ops.from_raw_sql(
            "SELECT _id FROM "
            f"{_qualified_table_name(table_schema, table_name)}"
            f"{_xtdb_temporal_basis_clause(update_time)}"
        )
    except Exception as exc:
        if _is_xtdb_table_not_found_error(exc):
            _rollback_xtdb_connection(dataframe_ops.connection)
            return 0
        raise
    if current.is_empty():
        return 0

    document_id_columns = _xtdb_document_id_columns(table_config)

    incoming_ids = _prepare_xtdb_insert_dataframe(
        df.select(document_id_columns).unique(maintain_order=True),
        table_config,
    )
    missing_ids = (
        current.select("_id")
        .join(incoming_ids.select("_id"), on="_id", how="anti")
        .get_column("_id")
        .to_list()
    )
    if not missing_ids:
        return 0

    id_cast_type = _xtdb_document_id_cast_type(table_config)
    ids_sql = ", ".join(_xtdb_sql_literal(value, id_cast_type) for value in missing_ids)
    valid_time_clause = ""
    close_time = _xtdb_dropout_close_time(df, dropout_close_time, update_time)
    if close_time is not None:
        valid_time_clause = (
            " FOR PORTION OF VALID_TIME FROM "
            f"{_xtdb_timestamp_literal(close_time)} TO NULL"
        )
    delete_sql = (
        f"DELETE FROM {_qualified_table_name(table_schema, table_name)}"
        f"{valid_time_clause} "
        f"WHERE _id IN ({ids_sql})"
    )
    _execute_xtdb_dml(
        dataframe_ops.connection,
        delete_sql,
        system_time=update_time,
    )
    return len(missing_ids)


def _xtdb_dropout_close_time(
    df: pl.DataFrame,
    dropout_close_time: Optional[datetime],
    update_time: Optional[datetime],
) -> datetime | None:
    if dropout_close_time is not None:
        return dropout_close_time
    if "_valid_from" not in df.columns or df.is_empty():
        return update_time

    close_times = df.select("_valid_from").drop_nulls().unique()
    if close_times.height == 0:
        return update_time
    if close_times.height > 1:
        raise ValueError(
            "XTDB row_finality='dropout' requires a single _valid_from value "
            "when update_time is not provided"
        )

    close_time = close_times.item()
    if not isinstance(close_time, datetime):
        raise ValueError(
            "XTDB row_finality='dropout' _valid_from values must be datetimes"
        )
    return close_time


def _driver_connection(connection: Any) -> Any | None:
    proxied_connection = getattr(connection, "connection", None)
    return getattr(proxied_connection, "driver_connection", None)


def _xtdb_timestamp_literal(value: datetime) -> str:
    escaped = value.isoformat().replace("'", "''")
    return f"TIMESTAMP '{escaped}'"


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


def _is_xtdb_table_not_found_error(exc: Exception) -> bool:
    return "Table not found:" in str(exc)


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
        return value.astimezone(timezone.utc).replace(tzinfo=None)
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
            expressions.append(
                pl.col(column).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            )
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


def _execute_xtdb_transaction(connection: Any, statements: Iterable[str]) -> None:
    """Submit one serialized XTDB DML transaction."""

    driver_connection = _driver_connection(connection)
    if driver_connection is None:
        raise ValueError("XTDB transactions require a live DBAPI connection")
    _rollback_xtdb_connection(connection)
    autocommit = getattr(driver_connection, "autocommit", None)
    if autocommit is not None:
        driver_connection.autocommit = True

    driver_connection.execute("BEGIN READ WRITE")
    try:
        for statement in statements:
            driver_connection.execute(statement)
        driver_connection.execute("COMMIT")
    except Exception:
        driver_connection.execute("ROLLBACK")
        raise
    finally:
        if autocommit is not None:
            driver_connection.autocommit = autocommit


def _xtdb_sql_literal(value: Any, cast_type: str) -> str:
    if value is None:
        return f"NULL::{cast_type}"
    if isinstance(value, bool):
        return f"{'TRUE' if value else 'FALSE'}::{cast_type}"
    if isinstance(value, int):
        return f"{value}::{cast_type}"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("XTDB insert does not support non-finite float values")
        return f"{value}::{cast_type}"
    if isinstance(value, Decimal):
        return f"{value}::{cast_type}"
    if isinstance(value, datetime):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"
    if isinstance(value, date):
        escaped = value.isoformat().replace("'", "''")
        return f"'{escaped}'::{cast_type}"

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'::{cast_type}"


def _xtdb_temporal_basis_clause(update_time: Optional[datetime]) -> str:
    if update_time is None:
        return ""
    timestamp = update_time.isoformat().replace("'", "''")
    return (
        f" FOR VALID_TIME AS OF TIMESTAMP '{timestamp}'"
        f" FOR SYSTEM_TIME AS OF TIMESTAMP '{timestamp}'"
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


def _xtdb_values_cte(name: str, df: pl.DataFrame) -> str:
    if df.is_empty():
        raise ValueError("XTDB table_query requires at least one query row")

    cte_name = _validate_identifier(name)
    columns = [_xtdb_column_identifier(column) for column in df.columns]
    casts = [_xtdb_cast_type_from_polars(dtype) for dtype in df.schema.values()]
    row_queries = []
    for row in df.rows():
        select_sql = ", ".join(
            f"{_xtdb_sql_literal(value, cast)} AS {column}"
            for value, cast, column in zip(row, casts, columns, strict=True)
        )
        row_queries.append(f"SELECT {select_sql}")
    return f"{cte_name} AS ({' UNION ALL '.join(row_queries)})"


def _xtdb_polars_type_or_none(data_type: str) -> pl.DataType | None:
    try:
        return PolarsType.from_sql(data_type)
    except ValueError:
        return None


def _xtdb_default_value(default_value: object, data_type: str) -> object:
    if default_value is None:
        return None

    dtype = PolarsType.from_sql(data_type)
    value = str(default_value)
    if dtype == pl.Boolean:
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    if dtype == pl.Date:
        return date.fromisoformat(value)
    if dtype == pl.Time:
        return time.fromisoformat(value)
    if isinstance(dtype, pl.Datetime):
        return datetime.fromisoformat(value)
    if isinstance(dtype, pl.Decimal):
        return Decimal(value)
    if dtype.is_integer():
        return int(value)
    if dtype.is_float():
        return float(value)
    return default_value


def _fill_xtdb_defaults(df: pl.DataFrame, table_config: TableConfig) -> pl.DataFrame:
    for column in table_config.columns:
        if column.name not in df.columns or column.default_value is None:
            continue

        target_dtype = PolarsType.from_sql(column.data_type)
        fill_dtype = df[column.name].dtype
        if fill_dtype == pl.Null:
            fill_dtype = target_dtype

        default_value = _xtdb_default_value(column.default_value, column.data_type)
        df = df.with_columns(
            pl.col(column.name)
            .cast(fill_dtype)
            .fill_null(pl.lit(default_value).cast(fill_dtype))
        )
    return df


def _xtdb_value_compare_dtypes(table_config: TableConfig) -> dict[str, pl.DataType]:
    document_id_columns = _xtdb_document_id_columns(table_config)
    uses_single_source_key = len(document_id_columns) == 1
    dtypes = {}
    for column in table_config.columns:
        target_name = (
            "_id"
            if uses_single_source_key and column.name == document_id_columns[0]
            else column.name
        )
        dtypes[target_name] = PolarsType.from_sql(column.data_type)
    return dtypes


def _apply_xtdb_compare_dtypes(
    df: pl.DataFrame,
    columns: Iterable[str],
    dtypes: Mapping[str, pl.DataType],
) -> pl.DataFrame:
    casts = [
        pl.col(column).cast(dtypes[column])
        for column in columns
        if column in df.columns and column in dtypes
    ]
    if not casts:
        return df
    return df.with_columns(casts)


def _filter_xtdb_unchanged_rows(
    df: pl.DataFrame,
    table_schema: str,
    table_name: str,
    table_config: TableConfig,
    dataframe_ops: Any,
    update_time: Optional[datetime],
) -> pl.DataFrame:
    if df.is_empty():
        return df

    row_index = "__xtdb_row_index"
    exists_column = "__xtdb_exists"
    indexed_df = df.with_row_index(row_index)
    incoming = _prepare_xtdb_insert_dataframe(indexed_df, table_config)

    table_sql = _qualified_table_name(table_schema, table_name)
    try:
        current = dataframe_ops.from_raw_sql(
            f"SELECT * FROM {table_sql}{_xtdb_temporal_basis_clause(update_time)}"
        )
    except Exception as exc:
        if _is_xtdb_table_not_found_error(exc):
            _rollback_xtdb_connection(dataframe_ops.connection)
            return df
        raise
    current = _restore_xtdb_logical_columns(current, table_config)
    if current.is_empty():
        return df

    compare_columns = [
        column
        for column in incoming.columns
        if column not in {row_index, "_valid_from", *_XTDB_READONLY_SYSTEM_COLUMNS}
        and column in current.columns
    ]
    if "_id" not in compare_columns:
        raise ValueError("XTDB delta upsert requires current rows to include _id")

    value_columns = [column for column in compare_columns if column != "_id"]
    compare_dtypes = _xtdb_value_compare_dtypes(table_config)
    incoming = _apply_xtdb_compare_dtypes(incoming, value_columns, compare_dtypes)
    current = _apply_xtdb_compare_dtypes(current, value_columns, compare_dtypes)
    current_projection = current.select(compare_columns).with_columns(
        pl.lit(True).alias(exists_column)
    )
    current_projection = current_projection.rename(
        {column: f"{column}__xtdb_current" for column in value_columns}
    )

    joined = incoming.join(current_projection, on="_id", how="left")
    keep_expr = pl.col(exists_column).is_null()
    for column in value_columns:
        current_column = f"{column}__xtdb_current"
        equal_expr = (
            (pl.col(column) == pl.col(current_column))
            | (pl.col(column).is_null() & pl.col(current_column).is_null())
        ).fill_null(False)
        keep_expr = keep_expr | equal_expr.not_()

    changed_row_indexes = joined.filter(keep_expr).select(row_index)
    return indexed_df.join(changed_row_indexes, on=row_index, how="inner").drop(
        row_index
    )


_XTDB_NON_TEMPORAL_VALID_FROM = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _apply_xtdb_valid_time_mapping(
    df: pl.DataFrame, valid_time: Optional[ValidTimeConfig]
) -> pl.DataFrame:
    if valid_time is None:
        return df

    mappings = {
        valid_time.from_column: "_valid_from",
    }
    if valid_time.to_column is not None:
        mappings[valid_time.to_column] = "_valid_to"

    missing_columns = [source for source in mappings if source not in df.columns]
    if missing_columns:
        raise ValueError(
            "XTDB valid-time mapping references missing source column(s): "
            + ", ".join(missing_columns)
        )

    null_columns = [source for source in mappings if df[source].null_count() > 0]
    if null_columns:
        raise ValueError(
            "XTDB valid-time mapping references null source value(s): "
            + ", ".join(null_columns)
        )

    for source, target in mappings.items():
        if target in df.columns and source != target:
            raise ValueError(
                f"XTDB valid-time mapping cannot write {target}; "
                "the dataframe already contains that column"
            )

    return df.with_columns(
        pl.col(source).alias(target)
        for source, target in mappings.items()
        if source != target
    )


def _apply_xtdb_non_temporal_valid_from(
    df: pl.DataFrame,
    table_config: Optional[TableConfig],
    valid_time: Optional[ValidTimeConfig],
) -> pl.DataFrame:
    """Reference tables (``is_temporal: false``) with no ``valid_time`` mapping
    must be readable as-of any historical timestamp — they represent an
    always-valid dimension, not a bitemporal fact.

    XTDB otherwise defaults an unspecified ``_valid_from`` to transaction
    time, so ``FOR VALID_TIME AS OF <t>`` on any ``t`` before the upload
    returns zero rows. Pin ``_valid_from`` to the unix epoch so those
    queries resolve.
    """
    if table_config is None or table_config.is_temporal:
        return df
    if valid_time is not None:
        return df
    if "_valid_from" in df.columns:
        return df
    return df.with_columns(pl.lit(_XTDB_NON_TEMPORAL_VALID_FROM).alias("_valid_from"))


def _fill_xtdb_staging_defaults(
    df: pl.DataFrame, table_config: TableConfig
) -> pl.DataFrame:
    return _fill_xtdb_defaults(df, table_config)


def _materialize_xtdb_missing_staging_columns(
    df: pl.DataFrame, table_config: TableConfig
) -> pl.DataFrame:
    missing_columns = [
        column for column in table_config.columns if column.name not in df.columns
    ]
    if not missing_columns:
        return df

    return df.with_columns(
        pl.lit(_xtdb_default_value(column.default_value, column.data_type))
        .cast(PolarsType.from_sql(column.data_type))
        .alias(column.name)
        for column in missing_columns
    )


def _dedupe_xtdb_staging_dataframe(
    df: pl.DataFrame, uniqueness_col_set: Iterable[str]
) -> pl.DataFrame:
    unique_columns = [column for column in uniqueness_col_set if column in df.columns]
    if not unique_columns:
        return df.unique(keep="last", maintain_order=True)
    return df.unique(subset=unique_columns, keep="last", maintain_order=True)


def _xtdb_deduced_foreign_key_columns(col_info: pl.DataFrame) -> dict[str, str]:
    return dict(
        col_info.filter("deduce_foreign_key").select("source", "target").iter_rows()
    )


def _xtdb_deduced_value_columns(col_info: pl.DataFrame) -> dict[str, str]:
    return dict(
        col_info.filter(pl.col("deduce_foreign_key").not_())
        .select("source", "target")
        .iter_rows()
    )


def _xtdb_is_textual_config_column(table_config: TableConfig, column_name: str) -> bool:
    column = next(
        (column for column in table_config.columns if column.name == column_name), None
    )
    if column is None:
        return False
    data_type = column.data_type.upper()
    return any(token in data_type for token in ["CHAR", "TEXT", "STRING", "VARCHAR"])


def _xtdb_is_integer_config_column(table_config: TableConfig, column_name: str) -> bool:
    column = next(
        (column for column in table_config.columns if column.name == column_name), None
    )
    if column is None:
        return False
    data_type = column.data_type.upper()
    return data_type in {"BIGINT", "INT", "INTEGER", "MEDIUMINT", "SMALLINT", "TINYINT"}


def _xtdb_deduced_foreign_key_id(
    table_config: TableConfig,
    value_targets: list[str],
    row: dict[str, Any],
) -> str:
    encoded_parts = [
        [column, _xtdb_json_safe_key_value(row[column])] for column in value_targets
    ]
    return f"xtdb-fk-v1:{table_config.schema}.{table_config.name}:" + json.dumps(
        encoded_parts, separators=(",", ":"), ensure_ascii=False
    )


def _xtdb_deduced_numeric_foreign_key_id(
    table_config: TableConfig,
    value_targets: list[str],
    row: dict[str, Any],
) -> int:
    encoded_parts = [
        [column, _xtdb_json_safe_key_value(row[column])] for column in value_targets
    ]
    payload = f"xtdb-fk-v1:{table_config.schema}.{table_config.name}:" + json.dumps(
        encoded_parts, separators=(",", ":"), ensure_ascii=False
    )
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    generated = int.from_bytes(digest[:4], "big") & 0x7FFFFFFF
    return -(generated or 1)


def _xtdb_parent_row_cache_key(
    table_config: TableConfig,
    columns: Iterable[str],
    row: dict[str, Any],
) -> tuple[str, str, str]:
    encoded_parts = [
        [column, _xtdb_json_safe_key_value(row[column])] for column in columns
    ]
    return (
        table_config.schema,
        table_config.name,
        json.dumps(encoded_parts, separators=(",", ":"), ensure_ascii=False),
    )


def _xtdb_fk_needs_generated_values(stage_df: pl.DataFrame, source_column: str) -> bool:
    return (
        source_column not in stage_df.columns
        or stage_df[source_column].null_count() > 0
    )


def _cast_null_parent_lookup_columns(
    parent_lookup: pl.DataFrame,
    generated: pl.DataFrame,
    lookup_columns: Iterable[str],
) -> pl.DataFrame:
    casts = [
        pl.col(column).cast(generated.schema[column])
        for column in lookup_columns
        if column in parent_lookup.columns
        and column in generated.columns
        and parent_lookup.schema[column] == pl.Null
    ]
    if not casts:
        return parent_lookup
    return parent_lookup.with_columns(casts)


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
    ) -> pl.DataFrame:
        if schema_overrides is None:
            schema_overrides = {}
        return pl.read_database(
            query,
            self.connection,
            schema_overrides=schema_overrides,
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
            table_config = XtdbTableConfigOps(self.connection).from_table(
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
    ) -> pl.DataFrame:
        table_config = XtdbTableConfigOps(self.connection).from_table(
            table_schema,
            table_name,
        )
        output_columns = _xtdb_table_query_output_columns(
            table_config,
            column_selection,
        )
        select_sql = ", ".join(
            _xtdb_table_query_select_expr(column, table_config)
            for column in output_columns
        )
        join_clause = " AND ".join(
            "t."
            f"{_xtdb_table_query_target_column(column, table_config)} = "
            f"q.{_xtdb_column_identifier(column)}"
            for column in query_df.columns
        )
        table_sql = _qualified_table_name(table_schema, table_name)
        valid_time_clause = _xtdb_valid_time_clause(time_hint)
        query = (
            f"WITH {_xtdb_values_cte('query_df', query_df)} "
            f"SELECT {select_sql} "
            "FROM ("
            "SELECT *, _valid_from, _valid_to "
            f"FROM {table_sql}{valid_time_clause}"
            ") AS t "
            "JOIN query_df AS q "
            f"ON {join_clause}"
        )
        schema_overrides = {}
        for column in table_config.columns:
            if column.name not in output_columns:
                continue
            dtype = _xtdb_polars_type_or_none(column.data_type)
            if dtype is not None:
                schema_overrides[column.name] = dtype
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
        for temporal_column in ["__valid_from", "__valid_to"]:
            if temporal_column in output_columns:
                schema_overrides[temporal_column] = pl.Datetime("us")

        df = self.from_raw_sql(query, schema_overrides)
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
    ) -> int:
        if table_config is not None:
            _xtdb_physical_column_map(table_config)
        df = _prepare_xtdb_insert_dataframe(df, table_config)
        df = _apply_xtdb_configured_column_dtypes(df, table_config)
        if df.is_empty():
            return 0

        driver_connection = _driver_connection(self.connection)
        if driver_connection is not None:
            inserted_count = 0
            for chunk in _iter_xtdb_insert_chunks(df, self.max_rows_per_insert):
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
                table_sql = _qualified_table_name(table_schema, table_name)
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

    def table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        table_config: Optional[TableConfig] = None,
        update_time: Optional[datetime] = None,
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
        df = _apply_xtdb_configured_column_dtypes(df, table_config)
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


class XtdbTableConfigOps:
    def __init__(self, connection: Any):
        self.connection = connection

    def create_all(self, tcs: Any) -> None:
        for table_config in tcs.items:
            self.create(table_config)

    def drop_all(self, tcs: Any) -> None:
        for table_config in reversed(tcs.items):
            self.drop(table_config)

    def drop(self, table_config: TableConfig) -> None:
        data_table_exists = self.table_exists(table_config.schema, table_config.name)

        if data_table_exists:
            table_name = _qualified_table_name(table_config.schema, table_config.name)
            _execute_xtdb_dml(self.connection, f"ERASE FROM {table_name} WHERE TRUE")

        if self.table_exists(table_config.schema, _XTDB_TABLE_CONFIG_METADATA_TABLE):
            metadata_table = _xtdb_table_config_metadata_table(table_config.schema)
            metadata_id = _xtdb_sql_literal(
                f"{table_config.schema}.{table_config.name}", "TEXT"
            )
            _execute_xtdb_dml(
                self.connection,
                f"DELETE FROM {metadata_table} WHERE _id = {metadata_id}",
            )

    def create(
        self,
        table_config: TableConfig,
        *args,
        **kwargs,
    ) -> TableConfig:
        if self.table_exists(table_config.schema, table_config.name):
            return self.from_table(table_config.schema, table_config.name)

        self._record_table_config_metadata(table_config)
        return table_config

    def _record_table_config_metadata(self, table_config: TableConfig) -> None:
        primary_keys_json = json.dumps(
            list(table_config.primary_keys),
            separators=(",", ":"),
        )
        columns_json = json.dumps(
            [column.__dict__ for column in table_config.columns],
            separators=(",", ":"),
        )
        values = [
            f"{table_config.schema}.{table_config.name}",
            table_config.schema,
            table_config.name,
            primary_keys_json,
            _xtdb_id_policy(table_config),
            columns_json,
            _xtdb_foreign_keys_json(table_config),
        ]
        values_sql = ", ".join(_xtdb_sql_literal(value, "TEXT") for value in values)
        metadata_table = _xtdb_table_config_metadata_table(table_config.schema)
        _execute_xtdb_dml(
            self.connection,
            f"INSERT INTO {metadata_table} "
            "(_id, table_schema, table_name, primary_keys_json, "
            "id_policy, columns_json, foreign_keys_json) "
            f"VALUES ({values_sql})",
        )

    def table_exists(self, table_schema: str, table_name: str) -> bool:
        table_schema = _validate_identifier(table_schema)
        table_name = _validate_identifier(table_name)
        metadata = pl.read_database(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{table_schema}'
              AND table_name = '{table_name}'
        """,
            self.connection,
        )
        return not metadata.is_empty()

    def from_table(self, table_schema: str, table_name: str) -> TableConfig:
        table_schema = _validate_identifier(table_schema)
        table_name = _validate_identifier(table_name)
        metadata = pl.read_database(
            f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{table_schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """,
            self.connection,
        )
        if metadata.is_empty():
            raise ValueError(
                f"XTDB table metadata not found for {table_schema}.{table_name}"
            )

        config_metadata = _xtdb_table_config_metadata(
            self.connection,
            table_schema,
            table_name,
        )
        configured_table = _xtdb_table_config_from_metadata(
            config_metadata,
            table_schema,
            table_name,
        )
        if configured_table is not None:
            return configured_table

        columns = []
        primary_keys = []
        for row in metadata.iter_rows(named=True):
            col_name = str(row["column_name"])
            if col_name in _XTDB_SYSTEM_COLUMNS:
                continue
            if col_name == "_id":
                primary_keys.append(col_name)

            columns.append(
                TableColumnConfig(
                    table=table_name,
                    name=col_name,
                    data_type=_xtdb_type_to_config_type(str(row["data_type"])),
                    nullable=_is_nullable(row["is_nullable"]),
                )
            )

        configured_primary_keys = _xtdb_primary_keys_from_metadata(
            config_metadata,
            table_schema,
            table_name,
        )
        if configured_primary_keys is not None:
            primary_keys = configured_primary_keys

        return TableConfig(
            name=table_name,
            schema=table_schema,
            columns=columns,
            primary_keys=primary_keys,
            is_temporal=True,
        )


class XtdbStagingOps:
    def __init__(
        self,
        connection: Any,
        max_rows_per_insert: Optional[int] = None,
        adbc_connection: Any | None = None,
    ):
        self.connection = connection
        self.adbc_connection = adbc_connection
        self.max_rows_per_insert = max_rows_per_insert
        self._stage_run_cache: dict[str, pl.DataFrame] = {}
        self._inserted_parent_row_cache: set[tuple[str, str, str]] = set()
        self._projected_parent_row_cache: set[tuple[str, str, str]] = set()

    def _dataframes(self) -> XtdbDataframeOps:
        return XtdbDataframeOps(
            self.connection,
            max_rows_per_insert=self.max_rows_per_insert,
        )

    def _bulk_dataframes(self) -> XtdbDataframeOps | XtdbAdbcDataframeOps:
        if self.adbc_connection is not None:
            return XtdbAdbcDataframeOps(
                self.adbc_connection,
                max_rows_per_insert=self.max_rows_per_insert,
            )
        return self._dataframes()

    def _bulk_table_insert(
        self,
        df: pl.DataFrame,
        table_schema: str,
        table_name: str,
        *,
        table_config: TableConfig,
    ) -> int:
        if self.adbc_connection is None:
            return self._dataframes().table_insert(
                df,
                table_schema,
                table_name,
                table_config=table_config,
            )

        try:
            return self._bulk_dataframes().table_insert(
                df,
                table_schema,
                table_name,
                table_config=table_config,
            )
        except Exception as exc:
            if not _is_xtdb_adbc_ingest_unavailable(exc):
                raise

        return self._dataframes().table_insert(
            df,
            table_schema,
            table_name,
            table_config=table_config,
        )

    def bulk_dataframes(self) -> XtdbDataframeOps | XtdbAdbcDataframeOps:
        return self._bulk_dataframes()

    def stage_table_config(self, delta_table_config: TableConfig) -> TableConfig:
        stage_table = _xtdb_stage_table_name(delta_table_config.name)
        existing_column_names = {column.name for column in delta_table_config.columns}
        columns = [
            TableColumnConfig(
                table=stage_table,
                name=column.name,
                data_type=column.data_type,
                default_value=column.default_value,
                autoincrement=column.autoincrement,
                nullable=column.nullable,
                unique_constraint=column.unique_constraint,
            )
            for column in delta_table_config.columns
        ]
        metadata_columns = [
            TableColumnConfig(
                stage_table,
                _XTDB_STAGE_RUN_ID_COLUMN,
                "VARCHAR(128)",
                nullable=False,
            ),
            TableColumnConfig(
                stage_table,
                _XTDB_STAGE_ROW_INDEX_COLUMN,
                "BIGINT",
                nullable=False,
            ),
            TableColumnConfig(
                stage_table,
                _XTDB_STAGE_PARTITION_TIME_COLUMN,
                "DATETIME",
                nullable=False,
            ),
        ]
        columns.extend(
            column
            for column in metadata_columns
            if column.name not in existing_column_names
        )
        return TableConfig(
            name=stage_table,
            schema=delta_table_config.schema,
            columns=columns,
            primary_keys=[_XTDB_STAGE_RUN_ID_COLUMN, _XTDB_STAGE_ROW_INDEX_COLUMN],
        )

    def ensure_table(self, delta_table_config: TableConfig) -> TableConfig:
        stage_config = self.stage_table_config(delta_table_config)
        return XtdbTableConfigOps(self.connection).create(stage_config)

    def insert_partition(
        self,
        df: pl.DataFrame,
        delta_table_config: TableConfig,
        stage_run_id: str,
        partition_time: datetime,
        *,
        uniqueness_col_set: Iterable[str],
        prefill_nulls_with_default: bool,
    ) -> int:
        if prefill_nulls_with_default:
            df = _fill_xtdb_staging_defaults(df, delta_table_config)
        df = _dedupe_xtdb_staging_dataframe(df, uniqueness_col_set)
        df = _materialize_xtdb_missing_staging_columns(df, delta_table_config)
        df = df.with_row_index(_XTDB_STAGE_ROW_INDEX_COLUMN).with_columns(
            pl.lit(stage_run_id).alias(_XTDB_STAGE_RUN_ID_COLUMN),
            pl.lit(partition_time).alias(_XTDB_STAGE_PARTITION_TIME_COLUMN),
        )
        if df.is_empty():
            self._stage_run_cache[stage_run_id] = df
            return 0

        stage_config = self.stage_table_config(delta_table_config)
        df = _normalize_xtdb_timestamp_columns(df, stage_config)
        inserted_count = self._bulk_table_insert(
            df,
            stage_config.schema,
            stage_config.name,
            table_config=stage_config,
        )
        if inserted_count > 0:
            self._stage_run_cache[stage_run_id] = df
        return inserted_count

    def prepare_pipeline_item_dataframe(
        self,
        stage_run_id: str,
        dataset: Any,
        pipeline_id: int,
        table_config: TableConfig,
        *,
        valid_time: Optional[ValidTimeConfig],
    ) -> pl.DataFrame:
        table_name = _qualified_table_name(
            dataset.delta_table_schema, _xtdb_stage_table_name(dataset.name)
        )
        stage_run_literal = _xtdb_sql_literal(stage_run_id, "TEXT")
        stage_df = self._stage_run_cache.get(stage_run_id)
        if stage_df is None:
            stage_df = self._dataframes().from_raw_sql(
                f"SELECT * FROM {table_name} "
                f"WHERE {_XTDB_STAGE_RUN_ID_COLUMN} = {stage_run_literal}"
            )
        stage_df = self._deduce_foreign_keys(
            stage_df,
            dataset,
            pipeline_id,
            table_config,
        )
        self._stage_run_cache[stage_run_id] = stage_df
        projected_df = project_staged_pipeline_item_dataframe(
            stage_df, dataset, pipeline_id, table_config, valid_time
        )
        return self._filter_duplicate_projected_parent_rows(
            projected_df,
            dataset,
            pipeline_id,
            table_config,
        )

    def _filter_duplicate_projected_parent_rows(
        self,
        projected_df: pl.DataFrame,
        dataset: Any,
        pipeline_id: int,
        table_config: TableConfig,
    ) -> pl.DataFrame:
        if projected_df.is_empty():
            return projected_df

        col_info = dataset.pipeline.extract_items(pipeline_id)
        fk_map = _xtdb_deduced_foreign_key_columns(col_info)
        if not fk_map:
            return projected_df

        value_map = _xtdb_deduced_value_columns(col_info)
        parent_columns = [
            column
            for column in [*fk_map.values(), *value_map.values()]
            if column in projected_df.columns
        ]
        rows_to_return = []
        new_parent_keys = set()
        for row in projected_df.iter_rows(named=True):
            parent_key = _xtdb_parent_row_cache_key(
                table_config,
                parent_columns,
                row,
            )
            if parent_key in self._projected_parent_row_cache:
                continue
            rows_to_return.append(row)
            new_parent_keys.add(parent_key)

        self._projected_parent_row_cache.update(new_parent_keys)
        return pl.DataFrame(rows_to_return, schema=projected_df.schema)

    def _deduce_foreign_keys(
        self,
        stage_df: pl.DataFrame,
        dataset: Any,
        pipeline_id: int,
        table_config: TableConfig,
    ) -> pl.DataFrame:
        col_info = dataset.pipeline.extract_items(pipeline_id)
        fk_map = _xtdb_deduced_foreign_key_columns(col_info)
        if not fk_map:
            return stage_df

        value_map = _xtdb_deduced_value_columns(col_info)
        if not value_map:
            raise ValueError(
                "XTDB foreign-key deduction requires at least one natural key column"
            )

        value_sources = list(value_map.keys())
        value_targets = list(value_map.values())
        fk_sources = list(fk_map.keys())
        fk_targets = list(fk_map.values())

        missing_columns = sorted(set(value_sources).difference(stage_df.columns))
        if missing_columns:
            raise ValueError(
                "XTDB foreign-key deduction references missing source column(s): "
                + ", ".join(missing_columns)
            )

        for source_column, target_column in fk_map.items():
            if (
                _xtdb_fk_needs_generated_values(stage_df, source_column)
                and not _xtdb_is_textual_config_column(table_config, target_column)
                and not _xtdb_is_integer_config_column(table_config, target_column)
            ):
                raise NotImplementedError(
                    "XTDB foreign-key deduction needs to generate a key for "
                    f"{table_config.name}.{target_column}, but only textual and "
                    "integer generated keys are currently supported"
                )

        candidate_sources = [
            column
            for column in [*value_sources, *fk_sources]
            if column in stage_df.columns
        ]
        candidates = stage_df.select(candidate_sources).unique(maintain_order=True)
        generated = candidates.rename(
            {
                **{
                    source: target
                    for source, target in value_map.items()
                    if source != target
                },
                **{
                    source: target
                    for source, target in fk_map.items()
                    if source in candidates.columns and source != target
                },
            }
        )
        for source_column, target_column in fk_map.items():
            if _xtdb_is_integer_config_column(table_config, target_column):
                generated_value = (
                    pl.struct(value_targets)
                    .map_elements(
                        lambda row: _xtdb_deduced_numeric_foreign_key_id(
                            table_config, value_targets, row
                        ),
                        return_dtype=pl.Int64,
                    )
                    .alias(target_column)
                )
            else:
                generated_value = (
                    pl.struct(value_targets)
                    .map_elements(
                        lambda row: _xtdb_deduced_foreign_key_id(
                            table_config, value_targets, row
                        ),
                        return_dtype=pl.String,
                    )
                    .alias(target_column)
                )
            if source_column not in candidates.columns:
                generated = generated.with_columns(generated_value)
                continue
            if _xtdb_is_textual_config_column(
                table_config, target_column
            ) or _xtdb_is_integer_config_column(table_config, target_column):
                generated = generated.with_columns(
                    pl.coalesce(target_column, generated_value).alias(target_column)
                )

        parent_df = self._dataframes().from_table(
            table_config.schema,
            table_config.name,
        )
        parent_lookup_columns = [
            column
            for column in [*fk_targets, *value_targets]
            if column in parent_df.columns
        ]
        if parent_lookup_columns:
            parent_lookup = parent_df.select(parent_lookup_columns)
        else:
            parent_lookup = pl.DataFrame(schema=generated.schema)
        parent_lookup = _cast_null_parent_lookup_columns(
            parent_lookup,
            generated,
            [*fk_targets, *value_targets],
        )

        joined = generated.join(
            parent_lookup,
            on=value_targets,
            how="left",
            suffix="__existing",
        )
        for target_column in fk_targets:
            existing_column = f"{target_column}__existing"
            if existing_column in joined.columns:
                joined = joined.with_columns(
                    pl.coalesce(existing_column, target_column).alias(target_column)
                ).drop(existing_column)

        first_fk = fk_targets[0]
        existing_first_fk = f"{first_fk}__existing"
        if (
            existing_first_fk
            in generated.join(
                parent_lookup,
                on=value_targets,
                how="left",
                suffix="__existing",
            ).columns
        ):
            missing_parent_rows = joined.filter(pl.col(first_fk).is_not_null())
            if not parent_lookup.is_empty():
                existing_keys = parent_lookup.select(value_targets).unique()
                missing_parent_rows = missing_parent_rows.join(
                    existing_keys,
                    on=value_targets,
                    how="anti",
                )
        else:
            missing_parent_rows = joined

        parent_rows_to_insert = missing_parent_rows.select(
            [*fk_targets, *value_targets]
        ).unique(maintain_order=True)
        if not parent_rows_to_insert.is_empty():
            insert_columns = [*fk_targets, *value_targets]
            rows_to_insert = []
            new_parent_keys = set()
            for row in parent_rows_to_insert.iter_rows(named=True):
                parent_key = _xtdb_parent_row_cache_key(
                    table_config,
                    insert_columns,
                    row,
                )
                if parent_key in self._inserted_parent_row_cache:
                    continue
                rows_to_insert.append(row)
                new_parent_keys.add(parent_key)

            parent_rows_to_insert = pl.DataFrame(
                rows_to_insert,
                schema=parent_rows_to_insert.schema,
            )
        if not parent_rows_to_insert.is_empty():
            self._bulk_table_insert(
                parent_rows_to_insert,
                table_config.schema,
                table_config.name,
                table_config=table_config,
            )
            self._inserted_parent_row_cache.update(new_parent_keys)

        resolver = joined.select([*value_targets, *fk_targets]).rename(
            {
                **{target: source for source, target in value_map.items()},
                **{target: source for source, target in fk_map.items()},
            }
        )
        return stage_df.drop(
            [column for column in fk_sources if column in stage_df.columns]
        ).join(resolver, on=value_sources, how="left")

    def cleanup_run(self, stage_run_id: str, delta_table_config: TableConfig) -> None:
        self._stage_run_cache.pop(stage_run_id, None)
        table_name = _qualified_table_name(
            delta_table_config.schema, _xtdb_stage_table_name(delta_table_config.name)
        )
        stage_run_literal = _xtdb_sql_literal(stage_run_id, "TEXT")
        _execute_xtdb_dml(
            self.connection,
            f"ERASE FROM {table_name} "
            f"WHERE {_XTDB_STAGE_RUN_ID_COLUMN} = {stage_run_literal}",
        )


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

    def adbc_uri(self, config: DbEngineConfig) -> str:
        adbc_port = config.adbc_port or 9832
        return f"grpc://{config.hostname}:{adbc_port}"

    def create_adbc_connection(self, config: DbEngineConfig) -> Any:
        return _load_flight_sql().connect(self.adbc_uri(config), autocommit=True)

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

    def crdt_documents(
        self,
        connection: Any,
        document_store: "CrdtDocumentStoreConfig",
        projection: "OverrideLedgerConfig",
    ) -> "XtdbCrdtDocumentStore":
        from ..overrides.xtdb import XtdbCrdtDocumentStore

        return XtdbCrdtDocumentStore(connection, document_store, projection)

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
        adbc_connection: Any | None = None,
    ) -> XtdbStagingOps:
        return XtdbStagingOps(
            connection,
            max_rows_per_insert=self.max_rows_per_insert,
            adbc_connection=adbc_connection,
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
        if dataframe_ops is None:
            if connection is None:
                raise ValueError(
                    "XTDB temporal upsert requires connection or dataframe_ops"
                )
            dataframe_ops = self.dataframes(connection)

        df = _apply_xtdb_valid_time_mapping(df, valid_time)
        df = _apply_xtdb_non_temporal_valid_from(df, table_config, valid_time)

        deleted_count = 0
        if delta_config is not None:
            if delta_config.row_finality not in {"disabled", "dropout"}:
                raise NotImplementedError(
                    "XTDB temporal upsert currently only supports "
                    "row_finality='disabled' or 'dropout'"
                )
            if table_config is None:
                raise ValueError("XTDB delta upsert requires table_config")
            if delta_config.prefill_nulls_with_default:
                df = _fill_xtdb_defaults(df, table_config)
            df = _apply_xtdb_duplicate_policy(df, table_config, delta_config)
            if delta_config.row_finality == "dropout":
                if not isinstance(dataframe_ops, XtdbDataframeOps):
                    raise NotImplementedError(
                        "XTDB row_finality='dropout' currently requires the "
                        "pgwire dataframe path"
                    )
                deleted_count = _delete_xtdb_missing_rows(
                    df,
                    table_schema,
                    table_name,
                    table_config,
                    dataframe_ops,
                    update_time,
                    dropout_close_time,
                )
            if delta_config.drop_unchanged_rows:
                df = _filter_xtdb_unchanged_rows(
                    df,
                    table_schema,
                    table_name,
                    table_config,
                    dataframe_ops,
                    update_time,
                )

        insert_kwargs: dict[str, Any] = {"table_config": table_config}
        if update_time is not None:
            insert_kwargs["update_time"] = update_time

        try:
            inserted_count = dataframe_ops.table_insert(
                df,
                table_schema,
                table_name,
                **insert_kwargs,
            )
        except Exception as exc:
            if not (
                isinstance(dataframe_ops, XtdbAdbcDataframeOps)
                and _is_xtdb_adbc_ingest_unavailable(exc)
            ):
                raise
            if connection is None:
                raise
            LOGGER.warning(
                "XTDB ADBC ingest unavailable — falling back to pgwire (%s)", exc
            )
            inserted_count = self.dataframes(connection).table_insert(
                df,
                table_schema,
                table_name,
                **insert_kwargs,
            )
        return deleted_count + inserted_count
