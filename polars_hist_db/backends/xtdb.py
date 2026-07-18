from dataclasses import dataclass
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from decimal import Decimal
from functools import partial
import hashlib
import json
import logging
import math
import re
from urllib.parse import quote
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    cast,
)

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..config import (
    DeltaConfig,
    ForeignKeyConfig,
    PipelineExtractColumn,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)
from ..core import TimeHint
from ..pipeline_projection import project_staged_pipeline_item_dataframe
from ..types import PolarsType
from .config import DbEngineConfig
from .base import (
    TableHealthResult,
    bounded_table_health_query,
    execute_table_health_query,
)
from .temporal import system_time_hint_clause
from . import xtdb_transport as _xtdb_transport
from .xtdb_transport import (
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
    _driver_connection,
    _execute_xtdb_dml,
    _normalize_xtdb_timestamp_columns,
    _prepare_xtdb_insert_dataframe,
    _qualified_table_name,
    _rollback_xtdb_connection,
    _validate_identifier,
    _xtdb_cast_type,
    _xtdb_column_identifier,
    _xtdb_document_id_columns,
    _xtdb_json_safe_key_value,
    _xtdb_table_query_target_column,
    _xtdb_temporal_basis_clause,
    _xtdb_timestamp_literal,
    _xtdb_values_cte,
)

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


def __getattr__(name: str) -> Any:
    """Keep legacy private transport imports working during the module split."""
    return getattr(_xtdb_transport, name)


_XTDB_SYSTEM_COLUMNS = {"_valid_from", "_valid_to", "_system_from", "_system_to"}
_XTDB_READONLY_SYSTEM_COLUMNS = {"_system_from", "_system_to"}
_XTDB_TABLE_CONFIG_METADATA_TABLE = "__polars_hist_db_xtdb_table_configs"
_XTDB_STAGE_RUN_ID_COLUMN = "stage_run_id"
_XTDB_STAGE_ROW_INDEX_COLUMN = "stage_row_index"
_XTDB_STAGE_PARTITION_TIME_COLUMN = "stage_partition_time"
_XTDB_LAST_SYSTEM_TIME_KEY = "polars_hist_db_xtdb_last_system_time"
_XTDB_RESERVED_IDENTIFIERS = {"flag", "timestamp"}
_XTDB_QUERY_ROWS_PER_CHUNK = 10_000
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


def _xtdb_table_config_metadata_table(table_schema: str) -> str:
    return _qualified_table_name(table_schema, _XTDB_TABLE_CONFIG_METADATA_TABLE)


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


def _xtdb_document_id_source(table_config: TableConfig) -> str:
    document_id_columns = _xtdb_document_id_columns(table_config)
    if len(document_id_columns) == 1:
        return document_id_columns[0]

    raise ValueError(
        "XTDB backend requires a single document id source for this operation"
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
    document_id_columns = _xtdb_document_id_columns(table_config)
    incoming_ids = _prepare_xtdb_insert_dataframe(
        df.select(document_id_columns).unique(maintain_order=True),
        table_config,
    ).get_column("_id")
    id_cast_type = _xtdb_document_id_cast_type(table_config)
    ids_sql = ", ".join(
        _xtdb_sql_literal(value, id_cast_type) for value in incoming_ids
    )
    missing_predicate = f"_id NOT IN ({ids_sql})" if ids_sql else "TRUE"
    table_sql = _qualified_table_name(table_schema, table_name)
    try:
        missing_count = int(
            dataframe_ops.from_raw_sql(
                "SELECT COUNT(*) AS missing_count FROM "
                f"{table_sql}{_xtdb_temporal_basis_clause(update_time)} "
                f"WHERE {missing_predicate}"
            ).item()
        )
    except Exception as exc:
        if _is_xtdb_table_not_found_error(exc):
            _rollback_xtdb_connection(dataframe_ops.connection)
            return 0
        raise
    if missing_count == 0:
        return 0
    valid_time_clause = ""
    close_time = _xtdb_dropout_close_time(df, dropout_close_time, update_time)
    if close_time is not None:
        valid_time_clause = (
            " FOR PORTION OF VALID_TIME FROM "
            f"{_xtdb_timestamp_literal(close_time)} TO NULL"
        )
    delete_sql = f"DELETE FROM {table_sql}{valid_time_clause} WHERE {missing_predicate}"
    _execute_xtdb_dml(
        dataframe_ops.connection,
        delete_sql,
        system_time=update_time,
    )
    return missing_count


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


def _is_xtdb_table_not_found_error(exc: Exception) -> bool:
    return "Table not found:" in str(exc)


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

    compare_columns = [
        column
        for column in incoming.columns
        if column not in {row_index, "_valid_from", *_XTDB_READONLY_SYSTEM_COLUMNS}
    ]
    query_columns = [
        "__valid_to" if column == "_valid_to" else column for column in compare_columns
    ]
    try:
        current = dataframe_ops.table_query(
            table_schema,
            table_name,
            incoming.select("_id").unique(maintain_order=True),
            query_columns,
            table_config=table_config,
            basis_time=update_time,
        )
    except Exception as exc:
        if _is_xtdb_table_not_found_error(exc):
            _rollback_xtdb_connection(dataframe_ops.connection)
            return df
        raise
    if "__valid_to" in current.columns:
        current = current.rename({"__valid_to": "_valid_to"})
    if current.is_empty():
        return df

    compare_columns = [
        column for column in compare_columns if column in current.columns
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


def _xtdb_deduced_foreign_key_columns(
    col_info: Iterable[PipelineExtractColumn],
) -> dict[str, str]:
    return {
        column.source: column.target for column in col_info if column.deduce_foreign_key
    }


def _xtdb_deduced_value_columns(
    col_info: Iterable[PipelineExtractColumn],
) -> dict[str, str]:
    return {
        column.source: column.target
        for column in col_info
        if not column.deduce_foreign_key
    }


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


def _xtdb_deduced_foreign_key_payload(
    table_config: TableConfig,
    value_targets: list[str],
    schema: Mapping[str, pl.DataType],
) -> pl.Expr:
    encoded_parts = []
    for column in value_targets:
        value = pl.col(column)
        dtype = schema[column]
        if isinstance(dtype, pl.Datetime):
            timezone_suffix = "%:z" if dtype.time_zone else ""
            value = value.dt.strftime(f"%Y-%m-%dT%H:%M:%S%.f{timezone_suffix}")
        elif dtype == pl.Date or dtype.is_decimal():
            value = value.cast(pl.String)

        encoded_column = json.dumps(column, ensure_ascii=False)
        encoded_value = (
            pl.struct(value.alias(column))
            .struct.json_encode()
            .str.strip_prefix(f"{{{encoded_column}:")
            .str.strip_suffix("}")
        )
        encoded_parts.append(
            pl.concat_str(
                pl.lit("["),
                pl.lit(encoded_column),
                pl.lit(","),
                encoded_value,
                pl.lit("]"),
            )
        )

    return pl.concat_str(
        pl.lit(f"xtdb-fk-v1:{table_config.schema}.{table_config.name}:["),
        pl.concat_str(encoded_parts, separator=","),
        pl.lit("]"),
    )


def _xtdb_deduced_numeric_foreign_key_ids(payloads: pl.Series, bits: int) -> pl.Series:
    mask = (1 << bits) - 1

    def generated_id(payload: str) -> int:
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        generated = int.from_bytes(digest[:8], "big") & mask
        return -(generated or 1)

    return pl.Series(
        [generated_id(payload) for payload in payloads],
        dtype=pl.Int64,
    )


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


def _cast_parent_lookup_columns(
    parent_lookup: pl.DataFrame,
    generated: pl.DataFrame,
    lookup_columns: Iterable[str],
) -> pl.DataFrame:
    casts = [
        pl.col(column).cast(generated.schema[column])
        for column in lookup_columns
        if column in parent_lookup.columns
        and column in generated.columns
        and parent_lookup.schema[column] != generated.schema[column]
    ]
    if not casts:
        return parent_lookup
    return parent_lookup.with_columns(casts)


def _xtdb_integer_min(table_config: TableConfig, column_name: str) -> int:
    data_type = next(
        column.data_type.upper()
        for column in table_config.columns
        if column.name == column_name
    )
    bits = {
        "TINYINT": 8,
        "SMALLINT": 16,
        "MEDIUMINT": 24,
        "INT": 32,
        "INTEGER": 32,
        "BIGINT": 64,
    }[data_type]
    return -(1 << (bits - 1))


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
            self.adbc_connection = None

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
        self._stage_run_cache[stage_run_id] = df
        return df.height

    def prepare_pipeline_item_dataframe(
        self,
        stage_run_id: str,
        dataset: Any,
        pipeline_id: int,
        table_config: TableConfig,
        *,
        valid_time: Optional[ValidTimeConfig],
    ) -> pl.DataFrame:
        stage_df = self._stage_run_cache.get(stage_run_id)
        if stage_df is None:
            raise ValueError(
                f"XTDB staged partition is unavailable for run {stage_run_id!r}"
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
            if (
                parent_key in self._projected_parent_row_cache
                or parent_key in new_parent_keys
            ):
                continue
            rows_to_return.append(row)
            new_parent_keys.add(parent_key)

        self._projected_parent_row_cache.update(new_parent_keys)
        return pl.DataFrame(rows_to_return, schema=projected_df.schema)

    def _resolve_numeric_foreign_key_collisions(
        self,
        rows: pl.DataFrame,
        table_config: TableConfig,
        fk_targets: list[str],
    ) -> pl.DataFrame:
        if rows.is_empty():
            return rows
        resolved = rows
        for target in fk_targets:
            marker = f"__xtdb_generated_{target}"
            if marker not in resolved.columns or not _xtdb_is_integer_config_column(
                table_config, target
            ):
                continue

            candidates = resolved.select(target).unique(maintain_order=True)
            values_cte, parameters = _xtdb_values_cte("candidate_keys", candidates)
            target_column = _xtdb_column_identifier(target)
            physical_target = _xtdb_table_query_target_column(target, table_config)
            table_name = _qualified_table_name(table_config.schema, table_config.name)
            minimum_column = "__xtdb_minimum_id"
            existing = self._dataframes().from_raw_sql(
                f"WITH {values_cte}, "
                "occupied AS ("
                f"SELECT t.{physical_target} AS {target_column} "
                f"FROM {table_name} AS t JOIN candidate_keys AS q "
                f"ON t.{physical_target} = q.{target_column}"
                "), bounds AS ("
                f"SELECT MIN(t.{physical_target}) AS {minimum_column} "
                f"FROM {table_name} AS t"
                ") "
                f"SELECT occupied.{target_column}, bounds.{minimum_column} "
                "FROM bounds LEFT JOIN occupied ON TRUE",
                {
                    target: resolved.schema[target],
                    minimum_column: resolved.schema[target],
                },
                {"parameters": parameters},
            )
            occupied = set(existing.get_column(target).drop_nulls().to_list())
            minimum_values = existing.get_column(minimum_column).drop_nulls()
            database_minimum = (
                cast(int, minimum_values.min())
                if not minimum_values.is_empty()
                else None
            )

            used: set[int] = set()
            values: list[int] = []
            minimum = _xtdb_integer_min(table_config, target)
            for row in resolved.select([target, marker]).iter_rows(named=True):
                value = int(row[target])
                generated = bool(row[marker])
                if value in occupied or value in used:
                    if not generated:
                        raise ValueError(
                            f"Foreign key {table_config.name}.{target} is already in use"
                        )
                    lower_values = [*used]
                    if database_minimum is not None:
                        lower_values.append(database_minimum)
                    floor = min(lower_values)
                    if floor <= minimum:
                        raise ValueError(
                            f"No generated foreign keys remain for "
                            f"{table_config.name}.{target}"
                        )
                    value = floor - 1
                used.add(value)
                values.append(value)

            resolved = resolved.with_columns(pl.Series(target, values))

        return resolved

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
            generated = generated.with_columns(
                (
                    pl.lit(True)
                    if source_column not in candidates.columns
                    else pl.col(target_column).is_null()
                ).alias(f"__xtdb_generated_{target_column}")
            )
            generated_payload = _xtdb_deduced_foreign_key_payload(
                table_config, value_targets, generated.schema
            )
            if _xtdb_is_integer_config_column(table_config, target_column):
                bits = (
                    63
                    if any(
                        column.name == target_column
                        and column.data_type.upper() == "BIGINT"
                        for column in table_config.columns
                    )
                    else 31
                )
                generated_value = generated_payload.map_batches(
                    partial(_xtdb_deduced_numeric_foreign_key_ids, bits=bits),
                    return_dtype=pl.Int64,
                ).alias(target_column)
            else:
                generated_value = generated_payload.alias(target_column)
            if source_column not in candidates.columns:
                generated = generated.with_columns(generated_value)
                continue
            if _xtdb_is_textual_config_column(
                table_config, target_column
            ) or _xtdb_is_integer_config_column(table_config, target_column):
                generated = generated.with_columns(
                    pl.coalesce(target_column, generated_value).alias(target_column)
                )

        parent_lookup = self._dataframes().table_query(
            table_config.schema,
            table_config.name,
            generated.select(value_targets).unique(maintain_order=True),
            [*fk_targets, *value_targets],
            table_config=table_config,
        )
        parent_lookup = _cast_parent_lookup_columns(
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
        first_fk = fk_targets[0]
        has_existing_fk = f"{first_fk}__existing" in joined.columns
        for target_column in fk_targets:
            existing_column = f"{target_column}__existing"
            if existing_column in joined.columns:
                joined = joined.with_columns(
                    pl.coalesce(existing_column, target_column).alias(target_column)
                ).drop(existing_column)

        if has_existing_fk:
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

        missing_parent_rows = self._resolve_numeric_foreign_key_collisions(
            missing_parent_rows,
            table_config,
            fk_targets,
        )
        resolved_keys = missing_parent_rows.select(
            [*value_targets, *fk_targets]
        ).unique(maintain_order=True)
        joined = joined.join(
            resolved_keys,
            on=value_targets,
            how="left",
            suffix="__resolved",
        )
        for target_column in fk_targets:
            resolved_column = f"{target_column}__resolved"
            if resolved_column in joined.columns:
                joined = joined.with_columns(
                    pl.coalesce(resolved_column, target_column).alias(target_column)
                ).drop(resolved_column)

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

    def cleanup_run(self, stage_run_id: str) -> None:
        self._stage_run_cache.pop(stage_run_id, None)


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
