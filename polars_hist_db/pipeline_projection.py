from typing import Any, Optional

import polars as pl

from .config import TableConfig, ValidTimeConfig
from .types import PolarsType


def _include_valid_time_source_columns(
    selected_columns: list[str],
    src_tgt_colname_map: dict[str, str],
    valid_time: Optional[ValidTimeConfig],
    stage_df: pl.DataFrame,
) -> list[str]:
    result = list(selected_columns)
    if valid_time is None:
        return result

    target_to_source = {
        target: source for source, target in src_tgt_colname_map.items()
    }
    for valid_time_column in [valid_time.from_column, valid_time.to_column]:
        if valid_time_column is None:
            continue
        source_column = target_to_source.get(valid_time_column, valid_time_column)
        if source_column in stage_df.columns and source_column not in result:
            result.append(source_column)
    return result


def project_staged_pipeline_item_dataframe(
    stage_df: pl.DataFrame,
    dataset: Any,
    pipeline_id: int,
    table_config: TableConfig,
    valid_time: Optional[ValidTimeConfig],
) -> pl.DataFrame:
    upload_items = dataset.pipeline.extract_items(pipeline_id)
    src_tgt_colname_map = dict(upload_items.select("source", "target").iter_rows())
    selected_columns = upload_items["source"].to_list()
    selected_columns = _include_valid_time_source_columns(
        selected_columns,
        src_tgt_colname_map,
        valid_time,
        stage_df,
    )

    missing_columns = sorted(set(selected_columns).difference(stage_df.columns))
    if missing_columns:
        nullable_target_columns = {
            column.name: column
            for column in table_config.columns
            if column.nullable and not column.autoincrement
        }
        fill_columns = []
        rejected_columns = []
        for source_column in missing_columns:
            target_column_name = src_tgt_colname_map[source_column]
            target_column = nullable_target_columns.get(target_column_name)
            if target_column is None:
                rejected_columns.append(source_column)
                continue

            fill_columns.append(
                pl.lit(None)
                .cast(PolarsType.from_sql(target_column.data_type))
                .alias(source_column)
            )

        if rejected_columns:
            raise ValueError(
                "column mismatch on "
                f"{set(rejected_columns)} in {upload_items['source'].to_list()}"
            )

        stage_df = stage_df.with_columns(fill_columns)

    return stage_df.select(selected_columns).rename(
        {
            source: target
            for source, target in src_tgt_colname_map.items()
            if source in selected_columns and source != target
        }
    )
