from datetime import datetime, timedelta

from polars_hist_db.backends import MariaDbBackend, XtdbBackend
from polars_hist_db.core import TimeHint


def test_mariadb_backend_builds_existing_time_hint_clauses():
    asof = datetime.fromisoformat("2026-01-02T03:04:05+00:00")

    assert MariaDbBackend().time_hint_clause(TimeHint(mode="none")) is None
    assert (
        MariaDbBackend().time_hint_clause(TimeHint(mode="all")) == "FOR SYSTEM_TIME ALL"
    )
    assert (
        MariaDbBackend().time_hint_clause(TimeHint(mode="asof", asof_utc=asof))
        == "FOR SYSTEM_TIME AS OF '2026-01-02T03:04:05+00:00'"
    )
    assert (
        MariaDbBackend().time_hint_clause(
            TimeHint(mode="span", asof_utc=asof, history_span=timedelta(hours=2))
        )
        == "FOR SYSTEM_TIME BETWEEN '2026-01-02T01:04:05+00:00' AND '2026-01-02T03:04:05+00:00'"
    )


def test_xtdb_backend_builds_matching_system_time_hint_clauses():
    asof = datetime.fromisoformat("2026-01-02T03:04:05+00:00")

    assert XtdbBackend().time_hint_clause(TimeHint(mode="none")) is None
    assert XtdbBackend().time_hint_clause(TimeHint(mode="all")) == "FOR SYSTEM_TIME ALL"
    assert (
        XtdbBackend().time_hint_clause(TimeHint(mode="asof", asof_utc=asof))
        == "FOR SYSTEM_TIME AS OF '2026-01-02T03:04:05+00:00'"
    )
    assert (
        XtdbBackend().time_hint_clause(
            TimeHint(mode="span", asof_utc=asof, history_span=timedelta(hours=2))
        )
        == "FOR SYSTEM_TIME BETWEEN '2026-01-02T01:04:05+00:00' AND '2026-01-02T03:04:05+00:00'"
    )
