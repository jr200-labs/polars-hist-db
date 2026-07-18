"""Benchmark XTDB delta and foreign-key scaling.

Examples:
    uv run python benchmarks/xtdb_delta.py
    uv run python benchmarks/xtdb_delta.py --target-rows 50000,5000000
    XTDB_BENCHMARK_DSN=postgresql://... uv run python benchmarks/xtdb_delta.py \
        --remote-table spire.vessel_info --remote-limit 50000
"""

from argparse import ArgumentParser
from dataclasses import dataclass
import gc
import json
import os
from pathlib import Path
import re
from statistics import median
from time import perf_counter
from typing import Any

import polars as pl

from polars_hist_db.backends.xtdb import (
    XtdbStagingOps,
    _filter_xtdb_unchanged_rows,
)
from polars_hist_db.config import DatasetConfig, TableColumnConfig, TableConfig


@dataclass
class CurrentRows:
    frame: pl.DataFrame

    def from_raw_sql(self, sql: str, *_args: Any) -> pl.DataFrame:
        if "__xtdb_minimum_id" in sql:
            return pl.DataFrame(
                {
                    "id": [None],
                    "__xtdb_minimum_id": [self.frame["id"].min()],
                },
                schema={"id": pl.Int64, "__xtdb_minimum_id": pl.Int64},
            )
        return self.frame

    def from_table(self, _schema: str, _table: str) -> pl.DataFrame:
        return self.frame

    def table_query(
        self,
        _schema: str,
        _table: str,
        query_df: pl.DataFrame,
        column_selection: list[str],
        **_kwargs: Any,
    ) -> pl.DataFrame:
        if query_df.is_empty():
            return self.frame.select(column_selection).head(0)
        frame = self.frame
        if "id" in column_selection and "id" not in frame.columns:
            frame = frame.with_columns(pl.col("_id").alias("id"))
        return frame.join(query_df, on=query_df.columns, how="inner").select(
            column_selection
        )


class ForeignKeyStaging(XtdbStagingOps):
    def __init__(self, parent: pl.DataFrame):
        super().__init__(object())
        self.rows = CurrentRows(parent)
        self.inserted_rows = 0

    def _dataframes(self) -> Any:
        return self.rows

    def _bulk_table_insert(self, df: pl.DataFrame, *args, **kwargs) -> int:
        self.inserted_rows += len(df)
        return len(df)


def network_floor_seconds(data_gb: float, bandwidth_gbps: float) -> float:
    return data_gb * 8 / bandwidth_gbps


def synthetic_frames(
    target_rows: int, upload_rows: int, value_columns: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if upload_rows > target_rows:
        raise ValueError("upload rows cannot exceed target rows")
    current = pl.DataFrame({"_id": range(target_rows)})
    incoming = pl.DataFrame({"id": range(upload_rows)})
    for column in range(value_columns):
        name = f"value_{column}"
        current = current.with_columns((pl.col("_id") + column).alias(name))
        incoming = incoming.with_columns((pl.col("id") + column).alias(name))
    current = current.sample(fraction=1, shuffle=True, seed=42)
    if upload_rows:
        incoming = incoming.with_columns(
            pl.when(pl.col("id") == upload_rows - 1)
            .then(pl.col("value_0") + 1)
            .otherwise(pl.col("value_0"))
            .alias("value_0")
        )
    return current, incoming


def table_config(value_columns: int) -> TableConfig:
    return TableConfig(
        schema="benchmark",
        name="records",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("records", "id", "BIGINT", nullable=False),
            *[
                TableColumnConfig("records", f"value_{column}", "BIGINT")
                for column in range(value_columns)
            ],
        ],
    )


def foreign_key_config() -> tuple[DatasetConfig, TableConfig]:
    dataset = DatasetConfig(
        name="records",
        delta_table_schema="benchmark",
        input_config={"type": "dsv", "search_paths": []},  # type: ignore[arg-type]
        pipeline=[  # type: ignore[arg-type]
            {
                "schema": "benchmark",
                "table": "parents",
                "type": "primary",
                "columns": [
                    {
                        "source": "parent_id",
                        "target": "id",
                        "deduce_foreign_key": True,
                    },
                    {"source": "parent_key", "target": "natural_key"},
                ],
            }
        ],
    )
    config = TableConfig(
        schema="benchmark",
        name="parents",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("parents", "id", "BIGINT", nullable=False),
            TableColumnConfig("parents", "natural_key", "BIGINT"),
        ],
    )
    return dataset, config


def benchmark_delta(
    target_rows: int,
    upload_rows: int,
    value_columns: int,
    repeats: int,
) -> tuple[float, float, float, int]:
    current, incoming = synthetic_frames(target_rows, upload_rows, value_columns)
    ops = CurrentRows(current)
    config = table_config(value_columns)
    timings = []
    changed_rows = 0
    for _ in range(repeats):
        gc.collect()
        started = perf_counter()
        changed = _filter_xtdb_unchanged_rows(
            incoming, "benchmark", "records", config, ops, None
        )
        timings.append(perf_counter() - started)
        changed_rows = len(changed)
    return (
        median(timings),
        current.estimated_size("mb"),
        incoming.estimated_size("mb"),
        changed_rows,
    )


def benchmark_foreign_keys(
    parent_rows: int, upload_rows: int, match_fraction: float, repeats: int
) -> tuple[float, float, float, int, int, int, bool]:
    if upload_rows > parent_rows:
        raise ValueError("upload rows cannot exceed parent rows")
    if not 0 <= match_fraction <= 1:
        raise ValueError("match fraction must be between zero and one")
    matched_rows = round(upload_rows * match_fraction)
    unmatched_rows = upload_rows - matched_rows
    parent = pl.DataFrame(
        {"id": range(parent_rows), "natural_key": range(parent_rows)}
    ).sample(fraction=1, shuffle=True, seed=42)
    incoming_keys = [
        *range(matched_rows),
        *range(parent_rows, parent_rows + unmatched_rows),
    ]
    incoming = pl.DataFrame(
        {
            "parent_id": pl.Series([None] * upload_rows, dtype=pl.Int64),
            "parent_key": incoming_keys,
        }
    )
    dataset, config = foreign_key_config()
    timings = []
    inserted_rows = 0
    generated_id_collisions = 0
    stage_updated = False
    for _ in range(repeats):
        gc.collect()
        staging = ForeignKeyStaging(parent)
        staging._stage_run_cache["benchmark"] = incoming
        started = perf_counter()
        result = staging.prepare_pipeline_item_dataframe(
            "benchmark", dataset, 0, config, valid_time=None
        )
        timings.append(perf_counter() - started)
        assert len(result) == upload_rows
        inserted_rows = staging.inserted_rows
        staged = staging._stage_run_cache["benchmark"]
        stage_updated = staged["parent_id"].null_count() == 0
        generated_ids = staged.filter(pl.col("parent_key") >= parent_rows).get_column(
            "parent_id"
        )
        generated_id_collisions = len(generated_ids) - generated_ids.n_unique()
        assert inserted_rows == unmatched_rows
        assert stage_updated
    return (
        median(timings),
        parent.estimated_size("mb"),
        incoming.estimated_size("mb"),
        matched_rows,
        inserted_rows,
        generated_id_collisions,
        stage_updated,
    )


def benchmark_remote_sample(dsn: str, table: str, limit: int) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("remote table must be an unquoted schema.table identifier")
    import psycopg

    with psycopg.connect(dsn) as connection:
        started = perf_counter()
        frame = pl.read_database(f"SELECT * FROM {table} LIMIT {limit}", connection)
        elapsed = perf_counter() - started
    print("remote_table,rows,decoded_mb,seconds,rows_per_second,decoded_mb_per_second")
    decoded_mb = frame.estimated_size("mb")
    print(
        f"{table},{len(frame)},{decoded_mb:.2f},{elapsed:.3f},"
        f"{len(frame) / elapsed:.0f},{decoded_mb / elapsed:.2f}"
    )


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--target-rows", default="50000,500000,5000000,10000000")
    parser.add_argument("--upload-rows", type=int, default=50_000)
    parser.add_argument("--value-columns", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--target-gb", type=float, default=100)
    parser.add_argument("--bandwidth-gbps", default="1,10,100")
    parser.add_argument("--fk-match-fractions", default="0,0.5,1")
    parser.add_argument("--remote-table")
    parser.add_argument("--remote-limit", type=int, default=50_000)
    parser.add_argument("--json-output")
    args = parser.parse_args()
    results = []

    print(
        "target_rows,upload_rows,target_mb,upload_mb,"
        "synthetic_selective_compare_seconds,changed_rows"
    )
    for target_rows in (int(value) for value in args.target_rows.split(",")):
        elapsed, target_mb, upload_mb, changed = benchmark_delta(
            target_rows,
            args.upload_rows,
            args.value_columns,
            args.repeats,
        )
        print(
            f"{target_rows},{args.upload_rows},{target_mb:.2f},{upload_mb:.2f},"
            f"{elapsed:.3f},{changed}"
        )
        results.append(
            {
                "name": f"delta {target_rows} stored / {args.upload_rows} uploaded",
                "unit": "seconds",
                "value": elapsed,
            }
        )

    print("target_gb,bandwidth_gbps,ideal_transfer_floor_seconds")
    for bandwidth in (float(value) for value in args.bandwidth_gbps.split(",")):
        print(
            f"{args.target_gb:g},{bandwidth:g},"
            f"{network_floor_seconds(args.target_gb, bandwidth):.1f}"
        )

    print(
        "parent_rows,upload_rows,match_fraction,matched_rows,created_rows,"
        "generated_id_collisions,stage_updated,parent_mb,upload_fk_mb,"
        "synthetic_selective_fk_seconds_excluding_network_and_insert"
    )
    for parent_rows in (int(value) for value in args.target_rows.split(",")):
        for match_fraction in (
            float(value) for value in args.fk_match_fractions.split(",")
        ):
            elapsed, parent_mb, upload_mb, matched, created, collisions, updated = (
                benchmark_foreign_keys(
                    parent_rows, args.upload_rows, match_fraction, args.repeats
                )
            )
            print(
                f"{parent_rows},{args.upload_rows},{match_fraction:g},{matched},"
                f"{created},{collisions},{str(updated).lower()},{parent_mb:.2f},"
                f"{upload_mb:.2f},{elapsed:.3f}"
            )
            results.append(
                {
                    "name": (
                        f"foreign keys {parent_rows} stored / "
                        f"{args.upload_rows} uploaded / {match_fraction:g} matched"
                    ),
                    "unit": "seconds",
                    "value": elapsed,
                }
            )

    if args.remote_table:
        dsn = os.environ.get("XTDB_BENCHMARK_DSN")
        if not dsn:
            parser.error("XTDB_BENCHMARK_DSN is required with --remote-table")
        benchmark_remote_sample(dsn, args.remote_table, args.remote_limit)

    if args.json_output:
        Path(args.json_output).write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
