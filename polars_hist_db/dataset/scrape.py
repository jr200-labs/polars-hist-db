import asyncio
from datetime import datetime
from contextlib import nullcontext
import logging
from random import uniform
from typing import Any, List, Optional, Set, Tuple
from uuid import uuid4

import polars as pl
from sqlalchemy import Connection, Engine

from ..config import TableConfig, TableConfigs, DatasetConfig
from ..core import DataframeOps, TableConfigOps
from ..utils import NonRetryableException
from ..loaders.input_source import BatchFinalizer

from .extract_item import scrape_extract_item
from .primary_item import scrape_primary_item

LOGGER = logging.getLogger(__name__)


def _pipeline_transaction_context(connection: Connection, is_xtdb: bool):
    if is_xtdb:
        return nullcontext()
    return connection.begin()


def _scrape_pipeline_item(
    pipeline_id: int,
    dataset: DatasetConfig,
    target_schema: str,
    target_table: str,
    tables: TableConfigs,
    upload_time: datetime,
    connection: Connection,
    partition_df: Optional[pl.DataFrame] = None,
    stage_run_id: Optional[str] = None,
    staging: Any = None,
    backend: Any = None,
) -> bool:
    item_type = dataset.pipeline.item_type(target_table)
    if item_type == "primary":
        return scrape_primary_item(
            pipeline_id,
            dataset,
            tables,
            upload_time,
            connection,
            partition_df=partition_df,
            stage_run_id=stage_run_id,
            staging=staging,
            backend=backend,
        )
    elif item_type == "extract":
        return scrape_extract_item(
            pipeline_id,
            dataset,
            target_table,
            tables,
            upload_time,
            connection,
            partition_df=partition_df,
            stage_run_id=stage_run_id,
            staging=staging,
            backend=backend,
        )
    else:
        raise ValueError(f"unknown item type: {item_type}")


def _ensure_delta_table(
    connection: Connection,
    delta_table_config: TableConfig,
    is_temporary_table: bool,
):
    """Ensure the delta table exists in the given connection.

    For temporary tables this must run in the same connection that will
    use the table, since TEMPORARY TABLEs are session-scoped.
    """
    TableConfigOps(connection).create(
        delta_table_config,
        is_delta_table=True,
        is_temporary_table=is_temporary_table,
    )


def _run_pipeline_as_transaction(
    partitions: List[Tuple[datetime, pl.DataFrame]],
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    finalizer: BatchFinalizer,
    delta_table_config: Optional[TableConfig] = None,
    backend: Any = None,
    adbc_connection: Any = None,
):
    main_table_config: TableConfig = tables[dataset.pipeline.get_main_table_name()[1]]
    tbl_to_header_map = dataset.pipeline.get_header_map(main_table_config.name)
    header_keys = [tbl_to_header_map.get(k, k) for k in main_table_config.primary_keys]
    is_xtdb = getattr(backend, "name", None) == "xtdb"

    with engine.connect() as connection:
        with _pipeline_transaction_context(connection, is_xtdb):
            staging = (
                backend.staging(
                    connection,
                    adbc_connection=adbc_connection,
                )
                if is_xtdb
                else None
            )
            if delta_table_config is not None and not is_xtdb:
                _ensure_delta_table(
                    connection,
                    delta_table_config,
                    dataset.delta_config.is_temporary_table,
                )
            modified_tables: Set[Tuple[str, str]] = set()
            for i, (ts, partition_df) in enumerate(partitions):
                stage_run_id = None
                assert isinstance(ts, datetime), (
                    f"timestamp is not a datetime [{type(ts)}]"
                )
                LOGGER.info(
                    "-- (%d/%d) time_partition[%s] %d rows",
                    i + 1,
                    len(partitions),
                    ts.isoformat(),
                    len(partition_df),
                )

                if not is_xtdb:
                    DataframeOps(connection).table_insert(
                        partition_df,
                        dataset.delta_table_schema,
                        dataset.name,
                        uniqueness_col_set=header_keys,
                        prefill_nulls_with_default=True,
                        clear_table_first=True,
                    )
                else:
                    if delta_table_config is None:
                        raise ValueError("XTDB ingest requires delta table config")
                    if staging is None:
                        raise ValueError("XTDB ingest requires staging ops")
                    stage_run_id = f"{dataset.name}:{uuid4()}"
                    staging.insert_partition(
                        partition_df,
                        delta_table_config,
                        stage_run_id,
                        ts,
                        uniqueness_col_set=header_keys,
                        prefill_nulls_with_default=True,
                    )

                try:
                    for pipeline_id, (
                        target_schema,
                        target_table,
                    ) in dataset.pipeline.get_pipeline_items().items():
                        did_modify = _scrape_pipeline_item(
                            pipeline_id,
                            dataset,
                            target_schema,
                            target_table,
                            tables,
                            ts,
                            connection,
                            partition_df=partition_df,
                            stage_run_id=stage_run_id,
                            staging=staging,
                            backend=backend,
                        )

                        if did_modify:
                            modified_item = (target_schema, target_table)
                            modified_tables.add(modified_item)
                finally:
                    if is_xtdb and stage_run_id is not None and staging is not None:
                        staging.cleanup_run(stage_run_id)

            if delta_table_config is not None:
                backend.finalize_ingest_run(connection, delta_table_config)

            if not finalizer.write_audit_before_commit(
                connection, sorted(modified_tables)
            ):
                raise NonRetryableException("Failed to update audit log")


async def try_run_pipeline_as_transaction(
    partitions: List[Tuple[datetime, pl.DataFrame]],
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    finalizer: BatchFinalizer,
    num_retries: int = 3,
    seconds_between_retries: float = 60,
    max_retry_delay: float = 300,
    retry_jitter: float = 0.1,
    delta_table_config: Optional[TableConfig] = None,
    backend: Any = None,
    adbc_connection: Any = None,
):
    if num_retries < 1:
        raise ValueError("num_retries must be at least 1")
    if not 0 <= retry_jitter <= 1:
        raise ValueError("retry_jitter must be between 0 and 1")
    for attempt in range(num_retries):
        try:
            await asyncio.to_thread(
                _run_pipeline_as_transaction,
                partitions,
                dataset,
                tables,
                engine,
                finalizer,
                delta_table_config,
                backend,
                adbc_connection,
            )
            break
        except NonRetryableException:
            raise
        except Exception as e:
            LOGGER.error("error in scrape_pipeline_as_transaction", exc_info=e)
            if attempt + 1 == num_retries:
                raise
            delay = min(seconds_between_retries * 2**attempt, max_retry_delay)
            delay *= uniform(1 - retry_jitter, 1 + retry_jitter)
            LOGGER.info("retries remaining: %d", num_retries - attempt - 1)
            await asyncio.sleep(delay)
    await finalizer.ack_after_commit()
