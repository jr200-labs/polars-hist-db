from datetime import date, datetime, time
from decimal import Decimal
from functools import partial
import hashlib
import json
from typing import Any, Iterable, Mapping, Optional, cast

import polars as pl

from ..config import (
    PipelineExtractColumn,
    TableColumnConfig,
    TableConfig,
    ValidTimeConfig,
)
from ..pipeline_projection import project_staged_pipeline_item_dataframe
from ..types import PolarsType
from .xtdb_arrow import _xtdb_json_safe_key_value
from .xtdb_query import _xtdb_table_query_target_column
from .xtdb_dataframe import (
    _uploaded_xtdb_relation,
    XtdbAdbcDataframeOps,
    XtdbDataframeOps,
)
from .xtdb_transport import (
    _is_xtdb_adbc_ingest_unavailable,
    _normalize_xtdb_timestamp_columns,
    _qualified_table_name,
    _xtdb_column_identifier,
)


_XTDB_STAGE_RUN_ID_COLUMN = "stage_run_id"
_XTDB_STAGE_ROW_INDEX_COLUMN = "stage_row_index"
_XTDB_STAGE_PARTITION_TIME_COLUMN = "stage_partition_time"


def _xtdb_stage_table_name(dataset_name: str) -> str:
    return f"__{dataset_name}_stage"


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


def _fill_xtdb_staging_defaults(
    df: pl.DataFrame, table_config: TableConfig
) -> pl.DataFrame:
    return _fill_xtdb_defaults(df, table_config)


def _materialize_xtdb_missing_columns(
    df: pl.DataFrame,
    table_config: TableConfig,
    *,
    use_defaults: bool,
) -> pl.DataFrame:
    missing_columns = [
        column for column in table_config.columns if column.name not in df.columns
    ]
    if not missing_columns:
        return df

    materialized = df.with_columns(
        pl.lit(
            _xtdb_default_value(column.default_value, column.data_type)
            if use_defaults
            else None
        )
        .cast(PolarsType.from_sql(column.data_type))
        .alias(column.name)
        for column in missing_columns
    )
    configured_columns = [column.name for column in table_config.columns]
    extra_columns = [
        column for column in materialized.columns if column not in configured_columns
    ]
    return materialized.select([*configured_columns, *extra_columns])


def _cast_xtdb_configured_columns(
    df: pl.DataFrame,
    table_config: TableConfig,
    column_names: Iterable[str],
) -> pl.DataFrame:
    configured_types = {
        column.name: PolarsType.from_sql(column.data_type)
        for column in table_config.columns
    }
    return df.with_columns(
        pl.col(column_name).cast(configured_types[column_name])
        for column_name in column_names
        if column_name in df.columns and column_name in configured_types
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
        df = _materialize_xtdb_missing_columns(
            df,
            delta_table_config,
            use_defaults=prefill_nulls_with_default,
        )
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
            target_column = _xtdb_column_identifier(target)
            physical_target = _xtdb_table_query_target_column(target, table_config)
            table_name = _qualified_table_name(table_config.schema, table_config.name)
            minimum_column = "__xtdb_minimum_id"
            dataframe_ops = self._dataframes()
            with _uploaded_xtdb_relation(
                dataframe_ops, candidates, table_config.schema
            ) as candidate_table:
                existing = dataframe_ops.from_raw_sql(
                    "WITH occupied AS ("
                    f"SELECT t.{physical_target} AS {target_column} "
                    f"FROM {table_name} AS t JOIN {candidate_table} AS q "
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
                target_dtype = PolarsType.from_sql(
                    next(
                        column.data_type
                        for column in table_config.columns
                        if column.name == target_column
                    )
                )
                bits = (
                    63
                    if any(
                        column.name == target_column
                        and column.data_type.upper() == "BIGINT"
                        for column in table_config.columns
                    )
                    else 31
                )
                generated_value = (
                    generated_payload.map_batches(
                        partial(_xtdb_deduced_numeric_foreign_key_ids, bits=bits),
                        return_dtype=pl.Int64,
                    )
                    .cast(target_dtype)
                    .alias(target_column)
                )
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
        missing_parent_rows = _cast_xtdb_configured_columns(
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
        joined = _cast_xtdb_configured_columns(joined, table_config, fk_targets)

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
        resolved_stage = stage_df.drop(
            [column for column in fk_sources if column in stage_df.columns]
        ).join(resolver, on=value_sources, how="left")
        target_dtypes = {
            source: PolarsType.from_sql(
                next(
                    column.data_type
                    for column in table_config.columns
                    if column.name == target
                )
            )
            for source, target in fk_map.items()
        }
        return resolved_stage.with_columns(
            pl.col(source).cast(dtype) for source, dtype in target_dtypes.items()
        )

    def cleanup_run(self, stage_run_id: str) -> None:
        self._stage_run_cache.pop(stage_run_id, None)
