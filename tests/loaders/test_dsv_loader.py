from datetime import date
from pathlib import Path

import polars as pl
import pytest

from polars_hist_db.config.parser_config import IngestionColumnConfig
from polars_hist_db.loaders.dsv.dsv_loader import load_typed_dsv


@pytest.mark.parametrize("from_file", [False, True], ids=["bytes", "file"])
@pytest.mark.parametrize("delimiter", [None, ";"], ids=["inferred", "explicit"])
def test_load_typed_dsv_preserves_rows_and_types(
    tmp_path: Path, from_file: bool, delimiter: str | None
) -> None:
    payload = (
        b"id;label;event_date\n"
        b"2;second;2026-10-13\n"
        b"1;;2026-10-12\n"
        b"2;second;2026-10-13\n"
    )
    source: Path | bytes = payload
    if from_file:
        source = tmp_path / "events.csv"
        source.write_bytes(payload)
    columns = [
        IngestionColumnConfig(
            column_type="data",
            schema="sample",
            table="events",
            source=name,
            target=name,
            ingestion_data_type=dtype,
            target_data_type=dtype,
        )
        for name, dtype in [
            ("id", "INT"),
            ("label", "VARCHAR(32)"),
            ("event_date", "DATE"),
        ]
    ]

    result = load_typed_dsv(source, columns, delimiter=delimiter)

    assert dict(result.schema) == {
        "id": pl.Int64,
        "label": pl.String,
        "event_date": pl.Date,
    }
    assert result.select("id", "label", "event_date").rows() == [
        (2, "second", date(2026, 10, 13)),
        (1, None, date(2026, 10, 12)),
    ]
