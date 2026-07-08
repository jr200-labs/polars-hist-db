from datetime import datetime, timezone

import polars as pl
import pytest

from polars_hist_db.config import TableColumnConfig, TableConfig, ValidTimeConfig
from polars_hist_db.pipeline_projection import project_staged_pipeline_item_dataframe


class _Pipeline:
    def __init__(self, items: pl.DataFrame):
        self._items = items

    def extract_items(self, pipeline_id: int) -> pl.DataFrame:
        return self._items.filter(pl.col("id") == pipeline_id)


class _Dataset:
    def __init__(self, items: pl.DataFrame):
        self.pipeline = _Pipeline(items)


def _table_config() -> TableConfig:
    return TableConfig(
        schema="source",
        name="events",
        primary_keys=["id"],
        columns=[
            TableColumnConfig("events", "id", "INT", nullable=False),
            TableColumnConfig("events", "event_value", "VARCHAR(64)"),
        ],
    )


def _dataset() -> _Dataset:
    return _Dataset(
        pl.DataFrame(
            {
                "id": [0, 0],
                "schema": ["source", "source"],
                "table": ["events", "events"],
                "source": ["id", "event_value"],
                "target": ["id", "event_value"],
            }
        )
    )


def test_valid_time_source_columns_are_projected_from_staging_data():
    event_time = datetime(2030, 1, 1, tzinfo=timezone.utc)

    result = project_staged_pipeline_item_dataframe(
        pl.DataFrame(
            {
                "id": [1],
                "event_value": ["Alpha"],
                "event_timestamp": [event_time],
            }
        ),
        _dataset(),
        0,
        _table_config(),
        ValidTimeConfig(table="events", from_column="event_timestamp"),
    )

    assert result.to_dict(as_series=False) == {
        "id": [1],
        "event_value": ["Alpha"],
        "event_timestamp": [event_time],
    }


@pytest.mark.parametrize("event_timestamp", [None])
def test_valid_time_source_columns_are_required_and_non_null(event_timestamp):
    with pytest.raises(ValueError, match="null source value.*event_timestamp"):
        project_staged_pipeline_item_dataframe(
            pl.DataFrame(
                {
                    "id": [1],
                    "event_value": ["Alpha"],
                    "event_timestamp": [event_timestamp],
                }
            ),
            _dataset(),
            0,
            _table_config(),
            ValidTimeConfig(table="events", from_column="event_timestamp"),
        )


def test_missing_valid_time_source_column_is_rejected_before_backend_write():
    with pytest.raises(ValueError, match="missing source column.*event_timestamp"):
        project_staged_pipeline_item_dataframe(
            pl.DataFrame(
                {
                    "id": [1],
                    "event_value": ["Alpha"],
                }
            ),
            _dataset(),
            0,
            _table_config(),
            ValidTimeConfig(table="events", from_column="event_timestamp"),
        )
