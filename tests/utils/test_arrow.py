from typing import cast

import pyarrow as pa
import pytest

from polars_hist_db.utils import (
    ArrowSchemaContractError,
    require_unique_arrow_field_names,
)


def test_rejects_duplicate_arrow_field_names() -> None:
    schema = pa.schema(
        [
            cast(pa.Field, pa.field("id", pa.int64())),
            cast(pa.Field, pa.field("id", pa.string())),
        ]
    )

    with pytest.raises(
        ArrowSchemaContractError,
        match="Arrow response schema contains duplicate field names: id",
    ):
        require_unique_arrow_field_names(schema, context="Arrow response schema")


def test_accepts_unique_arrow_field_names() -> None:
    schema = pa.schema(
        [
            cast(pa.Field, pa.field("id", pa.int64())),
            cast(pa.Field, pa.field("name", pa.string())),
        ]
    )

    require_unique_arrow_field_names(schema)
