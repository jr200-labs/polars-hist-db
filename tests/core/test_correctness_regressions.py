from datetime import datetime, timezone

import polars as pl
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from polars_hist_db.config import DeltaConfig
from polars_hist_db.config.parser_config import IngestionColumnConfig
from polars_hist_db.core.dataframe import DataframeOps
from polars_hist_db.core.delta_table import (
    DeltaTableOps,
    _prevalidate_upsert_from_table,
)
from polars_hist_db.loaders.dsv.dsv_loader import _validate_expected_columns


def test_boolean_string_defaults_are_parsed():
    df = pl.DataFrame({"enabled": [None]}, schema={"enabled": pl.Boolean})

    result = DataframeOps.fill_nulls_with_defaults(df, {"enabled": "false"})

    assert result["enabled"].to_list() == [False]


def test_missing_dsv_columns_are_added_to_rows():
    config = IngestionColumnConfig(
        column_type="data",
        schema="sample",
        table="items",
        ingestion_data_type="VARCHAR(10)",
        target_data_type="VARCHAR(10)",
        source="missing",
        target="missing",
    )

    result = _validate_expected_columns(pl.DataFrame({"present": [1, 2]}), [config])

    assert result.schema == {"missing": pl.String}
    assert result["missing"].to_list() == [None, None]


def test_last_delta_column_type_mismatch_raises():
    metadata = MetaData()
    source = Table("source", metadata, Column("id", String(10)))
    target = Table("target", metadata, Column("id", Integer))

    with pytest.raises(ValueError, match="column type mismatches"):
        _prevalidate_upsert_from_table(source, target, ["id"], {}, False)


def test_temporal_delete_resets_session_timestamp_after_error(monkeypatch):
    timestamps = []
    ops = DataframeOps(object())
    monkeypatch.setattr(
        "polars_hist_db.core.dataframe.DbOps.set_system_versioning_time",
        lambda self, value: timestamps.append(value),
    )
    monkeypatch.setattr(
        ops,
        "table_delete_rows",
        lambda *args: (_ for _ in ()).throw(RuntimeError("delete failed")),
    )
    update_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="delete failed"):
        ops.table_delete_rows_temporal(
            pl.DataFrame({"id": [1]}), "sample", "items", update_time
        )

    assert timestamps == [update_time, None]


def test_temporal_upsert_resets_session_timestamp_after_error(monkeypatch):
    timestamps = []
    ops = DeltaTableOps("sample", "delta", DeltaConfig(), object())
    monkeypatch.setattr(
        "polars_hist_db.core.delta_table.DbOps.set_system_versioning_time",
        lambda self, value: timestamps.append(value),
    )
    monkeypatch.setattr(
        "polars_hist_db.core.delta_table.TableOps.get_table_metadata",
        lambda self: (_ for _ in ()).throw(RuntimeError("upsert failed")),
    )
    update_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="upsert failed"):
        ops.upsert("items", update_time)

    assert timestamps == [update_time, None]
