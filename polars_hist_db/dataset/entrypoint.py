import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
import time
import warnings
from typing import List, Optional, Tuple

from nats.js.client import JetStreamContext
import polars as pl
from sqlalchemy import Engine

from ..backends import backend_from_config
from ..loaders.input_source_factory import InputSourceFactory
from ..utils.clock import Clock

from ..config import PolarsHistDbConfig, DatasetConfig, TableConfig, TableConfigs
from ..config.config import IngestionConfig
from ..config.input.input_source import InputConfig
from .scrape import try_run_pipeline_as_transaction

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    datasets_processed: int
    datasets_failed: int
    errors: tuple[Exception, ...]


async def run_datasets(
    config: PolarsHistDbConfig,
    engine: Optional[Engine] = None,
    dataset_name: Optional[str] = None,
    debug_capture_output: Optional[List[Tuple[datetime, pl.DataFrame]]] = None,
    js: Optional[JetStreamContext] = None,
    raise_on_error: bool = False,
):
    if not raise_on_error:
        warnings.warn(
            "raise_on_error=False is deprecated; inspect the returned RunResult",
            DeprecationWarning,
            stacklevel=2,
        )
    backend = backend_from_config(config.db_config)
    owns_engine = engine is None
    if engine is None:
        engine = backend.create_engine(config.db_config)

    selected_datasets = [
        dataset
        for dataset in config.datasets.datasets
        if dataset_name is None or dataset.name == dataset_name
    ]
    if not selected_datasets:
        LOGGER.error("no datasets processed for %s", dataset_name)
        if owns_engine:
            await asyncio.to_thread(engine.dispose)
        return RunResult(0, 0, ())

    ingestion = getattr(config, "ingestion", IngestionConfig())
    errors: list[Optional[Exception]] = [None] * len(selected_datasets)
    try:
        await asyncio.to_thread(_create_config_tables, engine, config.tables, backend)
        await _run_dataset_workers(
            selected_datasets,
            config,
            engine,
            backend,
            ingestion,
            errors,
            debug_capture_output,
            js,
            raise_on_error,
        )
    finally:
        if owns_engine:
            await asyncio.to_thread(engine.dispose)

    failures = tuple(error for error in errors if error is not None)
    return RunResult(len(selected_datasets), len(failures), failures)


def _dataset_table_keys(dataset: DatasetConfig) -> tuple[tuple[str, str], ...]:
    pipeline = getattr(dataset, "pipeline", None)
    if pipeline is None:
        return ()
    return tuple(sorted(set(pipeline.get_pipeline_items().values())))


async def _run_dataset_workers(
    datasets: list[DatasetConfig],
    config: PolarsHistDbConfig,
    engine: Engine,
    backend,
    ingestion: IngestionConfig,
    errors: list[Optional[Exception]],
    debug_capture_output: Optional[List[Tuple[datetime, pl.DataFrame]]],
    js: Optional[JetStreamContext],
    raise_on_error: bool,
) -> None:
    worker_count = min(ingestion.max_workers, len(datasets))
    queue: asyncio.Queue[Optional[tuple[int, DatasetConfig]]] = asyncio.Queue(
        maxsize=ingestion.queue_size
    )
    table_locks = {
        key: asyncio.Lock()
        for dataset in datasets
        for key in _dataset_table_keys(dataset)
    }

    async def producer() -> None:
        for item in enumerate(datasets):
            await queue.put(item)
        for _ in range(worker_count):
            await queue.put(None)

    async def worker() -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                index, dataset = item
                locks = [table_locks[key] for key in _dataset_table_keys(dataset)]
                acquired_locks: list[asyncio.Lock] = []
                adbc_connection = None
                try:
                    for lock in locks:
                        await lock.acquire()
                        acquired_locks.append(lock)
                    if getattr(backend, "name", None) == "xtdb":
                        adbc_connection = await asyncio.to_thread(
                            backend.create_adbc_connection, config.db_config
                        )
                    LOGGER.info("scraping dataset %s", dataset.name)
                    errors[index] = await _run_dataset(
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
                        await asyncio.to_thread(adbc_connection.close)
                    for lock in reversed(acquired_locks):
                        lock.release()
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(producer())] + [
        asyncio.create_task(worker()) for _ in range(worker_count)
    ]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


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

    delta_table_config = _build_delta_table_config(tables, dataset)

    start_time = time.perf_counter()

    input_source = InputSourceFactory.create_input_source(
        tables, dataset, input_config, js=js
    )
    error = None
    try:
        async for partitions, finalizer in await input_source.next_df(engine):
            if debug_capture_output is not None:
                debug_capture_output.extend(partitions)

            await try_run_pipeline_as_transaction(
                partitions,
                dataset,
                tables,
                engine,
                finalizer,
                delta_table_config=delta_table_config,
                backend=backend,
                adbc_connection=adbc_connection,
            )

    except Exception as e:
        LOGGER.error("error while processing InputSource: %s", e, exc_info=e)
        if raise_on_error:
            raise
        error = e
    finally:
        await input_source.cleanup()

    Clock().add_timing("dataset", time.perf_counter() - start_time)

    LOGGER.info("stopped scrape - %s", dataset.name)
    return error
