from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Mapping, Optional

import polars as pl

from ..config import DeltaConfig, TableConfig, ValidTimeConfig
from ..types import PolarsType
from .xtdb_arrow import (
    _prepare_xtdb_insert_dataframe,
    _xtdb_cast_type,
    _xtdb_document_id_columns,
)
from .xtdb_query import _xtdb_temporal_basis_clause
from .xtdb_dataframe import XtdbDataframeOps
from .xtdb_transport import (
    _execute_xtdb_dml,
    _is_xtdb_table_not_found_error,
    _qualified_table_name,
    _rollback_xtdb_connection,
    _xtdb_sql_literal,
    _xtdb_timestamp_literal,
)


_XTDB_READONLY_SYSTEM_COLUMNS = {"_system_from", "_system_to"}


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
