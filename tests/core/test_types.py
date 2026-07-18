from decimal import Decimal

import polars as pl
import pytest
from sqlalchemy.types import TIMESTAMP

from polars_hist_db.types import PolarsType, SQLAlchemyType, SQLType


def test_apply_schema_casts_scaled_decimal() -> None:
    result = PolarsType.apply_schema_to_dataframe(
        pl.DataFrame({"amount": [1.2345]}),
        amount=pl.Decimal(precision=10, scale=4),
    )

    assert result.schema["amount"] == pl.Decimal(precision=10, scale=4)
    assert result[0, "amount"] == Decimal("1.2345")


def test_apply_schema_rejects_invalid_value_instead_of_leaving_wrong_type() -> None:
    with pytest.raises(Exception, match="conversion"):
        PolarsType.apply_schema_to_dataframe(
            pl.DataFrame({"amount": ["not-a-number"]}),
            amount=pl.Decimal(precision=10, scale=4),
        )


def test_timezone_aware_timestamp_type_roundtrip() -> None:
    utc_timestamp = pl.Datetime("us", "UTC")

    assert PolarsType.from_sql("TIMESTAMP WITH TIME ZONE") == utc_timestamp
    assert PolarsType.from_sql("TIMESTAMPTZ") == utc_timestamp
    assert SQLType.from_polars(utc_timestamp) == "TIMESTAMP WITH TIME ZONE"

    sqlalchemy_type = SQLAlchemyType.from_sql("TIMESTAMP WITH TIME ZONE")
    assert isinstance(sqlalchemy_type, TIMESTAMP)
    assert sqlalchemy_type.timezone is True
