"""Benchmark XTDB's current client-side delta calculation.

Examples:
    uv run python benchmarks/xtdb_delta.py
    uv run python benchmarks/xtdb_delta.py --target-rows 50000,5000000
    XTDB_BENCHMARK_DSN=postgresql://... uv run python benchmarks/xtdb_delta.py \
        --remote-table spire.vessel_info --remote-limit 50000
"""

from argparse import ArgumentParser
from dataclasses import dataclass
import gc
import os
import re
from statistics import median
from time import perf_counter

import polars as pl

from polars_hist_db.backends.xtdb import _filter_xtdb_unchanged_rows
from polars_hist_db.config import TableColumnConfig, TableConfig


@dataclass
class CurrentRows:
    frame: pl.DataFrame

    def from_raw_sql(self, _sql: str) -> pl.DataFrame:
        return self.frame


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
    parser.add_argument("--remote-table")
    parser.add_argument("--remote-limit", type=int, default=50_000)
    args = parser.parse_args()

    print(
        "target_rows,upload_rows,target_mb,upload_mb,"
        "polars_compare_seconds_excluding_target_read,changed_rows"
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

    print("target_gb,bandwidth_gbps,ideal_transfer_floor_seconds")
    for bandwidth in (float(value) for value in args.bandwidth_gbps.split(",")):
        print(
            f"{args.target_gb:g},{bandwidth:g},"
            f"{network_floor_seconds(args.target_gb, bandwidth):.1f}"
        )

    if args.remote_table:
        dsn = os.environ.get("XTDB_BENCHMARK_DSN")
        if not dsn:
            parser.error("XTDB_BENCHMARK_DSN is required with --remote-table")
        benchmark_remote_sample(dsn, args.remote_table, args.remote_limit)


if __name__ == "__main__":
    main()
