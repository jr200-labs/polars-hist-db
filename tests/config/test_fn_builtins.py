import warnings

import polars as pl

from polars_hist_db.config.fn_builtins import apply_type_casts


def test_apply_type_casts_parses_string_dates_without_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        result = apply_type_casts(
            pl.DataFrame({"date": ["2026-07-18"]}), "date", ["Date"]
        )

    assert result.schema["date"] == pl.Date
