from unittest.mock import Mock

import pytest

from polars_hist_db.backends import MariaDbBackend, XtdbBackend


@pytest.mark.parametrize(
    ("backend", "expected_table"),
    [
        (MariaDbBackend(), "`app`.`events`"),
        (XtdbBackend(), '"app"."events"'),
    ],
)
def test_backend_health_query_is_dialect_correct_and_bounded(backend, expected_table):
    query = backend.table_health_query("app", "events", minimum_rows=3)

    assert query == f"SELECT 1 FROM {expected_table} LIMIT 3"
    assert "COUNT" not in query


@pytest.mark.parametrize("backend", [MariaDbBackend(), XtdbBackend()])
def test_backend_health_check_enforces_minimum_rows(backend):
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [(1,), (1,)]

    result = backend.check_table_resource(connection, "app", "events", minimum_rows=3)

    assert result.ready is False
    assert result.sampled_rows == 2
    assert result.detail == "need ≥3 rows, found 2"
    assert connection.execute.call_args.args[0].text.endswith("LIMIT 3")


@pytest.mark.parametrize("backend", [MariaDbBackend(), XtdbBackend()])
def test_backend_health_check_accepts_empty_table_when_no_minimum_is_configured(
    backend,
):
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = []

    result = backend.check_table_resource(connection, "app", "events")

    assert result.ready is True
    assert result.detail == "readable"
    assert connection.execute.call_args.args[0].text.endswith("LIMIT 0")


@pytest.mark.parametrize("backend", [MariaDbBackend(), XtdbBackend()])
def test_backend_health_check_reports_singular_minimum(backend):
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [(1,)]

    result = backend.check_table_resource(connection, "app", "events", minimum_rows=1)

    assert result.detail == "at least 1 row"


@pytest.mark.parametrize("backend", [MariaDbBackend(), XtdbBackend()])
@pytest.mark.parametrize(
    ("schema", "table", "minimum_rows"),
    [("bad-schema", "events", 0), ("app", "bad.table", 0), ("app", "events", -1)],
)
def test_backend_health_query_rejects_invalid_input(
    backend, schema, table, minimum_rows
):
    with pytest.raises(ValueError):
        backend.table_health_query(schema, table, minimum_rows)
