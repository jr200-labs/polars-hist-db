from datetime import datetime
import logging
import time
from typing import List, Optional, Tuple

from nats.js.client import JetStreamContext
import polars as pl
from sqlalchemy import Engine

from ..backends import backend_from_config
from ..loaders.input_source_factory import InputSourceFactory
from ..utils.clock import Clock

from ..config import PolarsHistDbConfig, DatasetConfig, TableConfig, TableConfigs
from ..config.input.input_source import InputConfig
from .scrape import try_run_pipeline_as_transaction

LOGGER = logging.getLogger(__name__)


async def run_datasets(
    config: PolarsHistDbConfig,
    engine: Optional[Engine] = None,
    dataset_name: Optional[str] = None,
    debug_capture_output: Optional[List[Tuple[datetime, pl.DataFrame]]] = None,
    js: Optional[JetStreamContext] = None,
    raise_on_error: bool = False,
):
    backend = backend_from_config(config.db_config)
    if engine is None:
        engine = backend.create_engine(config.db_config)

    num_datasets_processed = 0
    adbc_connection = None
    try:
        for dataset in config.datasets.datasets:
            if dataset_name is None or dataset.name == dataset_name:
                if getattr(backend, "name", None) == "xtdb" and adbc_connection is None:
                    adbc_connection = backend.create_adbc_connection(config.db_config)

                num_datasets_processed += 1
                LOGGER.info("scraping dataset %s", dataset.name)
                await _run_dataset(
                    dataset.input_config,
                    dataset,
                    config.tables,
                    engine,
                    debug_capture_output,
                    backend,
                    adbc_connection=adbc_connection,
                    js=js,
                    raise_on_error=raise_on_error,
                )
    finally:
        if adbc_connection is not None:
            adbc_connection.close()

    if num_datasets_processed == 0:
        LOGGER.error("no datasets processed for %s", dataset_name)


def _create_config_tables(engine: Engine, tables: TableConfigs, backend):
    """Create permanent config tables (idempotent)."""
    if getattr(backend, "name", None) == "xtdb":
        with engine.connect() as connection:
            backend.table_configs(connection).create_all(tables)
            connection.commit()
        return

    with engine.begin() as connection:
        backend.table_configs(connection).create_all(tables)


def _build_delta_table_config(
    tables: TableConfigs, dataset: DatasetConfig
) -> TableConfig:
    """Build the delta table config from dataset pipeline definitions.

    This only builds the config metadata — no database connection is needed.
    The actual table creation happens inside each connection context in
    try_run_pipeline_as_transaction, ensuring temporary tables are visible
    to the same session that uses them.
    """
    col_defs = dataset.pipeline.build_delta_table_column_configs(tables, dataset.name)
    return TableConfig(dataset.name, dataset.delta_table_schema, col_defs)


async def _run_dataset(
    input_config: InputConfig,
    dataset: DatasetConfig,
    tables: TableConfigs,
    engine: Engine,
    debug_capture_output: Optional[List[Tuple[datetime, pl.DataFrame]]],
    backend,
    adbc_connection=None,
    js: Optional[JetStreamContext] = None,
    raise_on_error: bool = False,
):
    LOGGER.info("starting %s ingest for %s", input_config.type, dataset.name)

    _create_config_tables(engine, tables, backend)
    delta_table_config = _build_delta_table_config(tables, dataset)

    start_time = time.perf_counter()

    input_source = InputSourceFactory.create_input_source(
        tables, dataset, input_config, js=js
    )
    try:
        async for partitions, commit_fn in await input_source.next_df(engine):
            if debug_capture_output is not None:
                debug_capture_output.extend(partitions)

            await try_run_pipeline_as_transaction(
                partitions,
                dataset,
                tables,
                engine,
                commit_fn,
                delta_table_config=delta_table_config,
                backend=backend,
                adbc_connection=adbc_connection,
            )

    except Exception as e:
        LOGGER.error("error while processing InputSource: %s", e, exc_info=e)
        if raise_on_error:
            raise
    finally:
        await input_source.cleanup()

    Clock().add_timing("dataset", time.perf_counter() - start_time)

    LOGGER.info("stopped scrape - %s", dataset.name)
