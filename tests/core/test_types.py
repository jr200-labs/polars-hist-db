from decimal import Decimal

import polars as pl
import pytest

from polars_hist_db.types import PolarsType


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
