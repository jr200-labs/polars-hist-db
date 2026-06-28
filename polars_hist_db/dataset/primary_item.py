from datetime import datetime
import logging
from typing import Any, Optional

import polars as pl
from sqlalchemy import Connection

from ..config import TableConfig, TableConfigs, DatasetConfig
from ..core import TableConfigOps, DeltaTableOps, TableOps

LOGGER = logging.getLogger(__name__)


def scrape_primary_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
    partition_df: Optional[pl.DataFrame] = None,
    stage_run_id: Optional[str] = None,
    staging: Any = None,
    backend: Any = None,
) -> bool:
    pipeline = dataset.pipeline
    delta_table_schema = dataset.delta_table_schema
    delta_table_name = dataset.name
    main_table_config = tables[pipeline.get_main_table_name()[1]]
    LOGGER.debug("(item %d) scraping item %s", pipeline_id, main_table_config.name)

    if getattr(backend, "name", None) == "xtdb":
        return scrape_xtdb_pipeline_item(
            pipeline_id,
            dataset,
            main_table_config,
            upload_time,
            connection,
            stage_run_id,
            staging,
            backend,
        )

    upload_items = pipeline.extract_items(pipeline_id)
    selected_columns = upload_items["source"].to_list()

    TableConfigOps(connection).create(main_table_config)
    tbo = TableOps(delta_table_schema, delta_table_name, connection)
    common_columns = [c.name for c in tbo.get_column_intersection(selected_columns)]

    if selected_columns is not None:
        if len(common_columns) != len(selected_columns):
            cols_not_configured = set(common_columns).symmetric_difference(
                selected_columns
            )
            raise ValueError(
                f"column mismatch on {cols_not_configured} in {selected_columns}"
            )

    ni, nu, nd = DeltaTableOps(
        delta_table_schema, delta_table_name, dataset.delta_config, connection
    ).upsert(
        main_table_config.name,
        upload_time,
        is_main_table=True,
        source_columns=common_columns,
        src_tgt_colname_map=dict(upload_items.select("source", "target").iter_rows()),
    )

    LOGGER.debug("(item %d) upserted %d rows", -1, ni + nu)

    # TODO: trigger table mod notification
    return (ni + nu + nd) > 0


def scrape_xtdb_pipeline_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    table_config: TableConfig,
    upload_time: datetime,
    connection: Connection,
    stage_run_id: Optional[str],
    staging: Any,
    backend: Any,
) -> bool:
    if stage_run_id is None or staging is None:
        raise ValueError("XTDB ingest requires a staged partition")

    valid_time = dataset.valid_time_for_table(table_config.schema, table_config.name)
    target_df = staging.prepare_pipeline_item_dataframe(
        stage_run_id,
        dataset,
        pipeline_id,
        table_config,
        valid_time=valid_time,
    )

    backend.table_configs(connection).create(table_config)
    modified_count = backend.temporal_upsert(
        target_df,
        table_config.schema,
        table_config.name,
        connection=connection,
        table_config=table_config,
        delta_config=dataset.delta_config,
        update_time=upload_time,
        valid_time=valid_time,
    )

    LOGGER.debug("(item %d) XTDB upserted %d rows", pipeline_id, modified_count)
    return modified_count > 0
