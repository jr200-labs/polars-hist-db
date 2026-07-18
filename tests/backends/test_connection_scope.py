from unittest.mock import MagicMock

import pytest

from polars_hist_db.backends import MariaDbBackend, XtdbBackend


def test_mariadb_connection_scope_uses_engine_transaction():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value

    with MariaDbBackend().connection_scope(engine) as active:
        assert active is connection

    engine.begin.assert_called_once_with()
    engine.connect.assert_not_called()


def test_xtdb_connection_scope_commits_successful_connection():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value

    with XtdbBackend().connection_scope(engine) as active:
        assert active is connection

    engine.connect.assert_called_once_with()
    engine.begin.assert_not_called()
    connection.commit.assert_called_once_with()


def test_xtdb_connection_scope_does_not_commit_failure():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value

    with pytest.raises(RuntimeError), XtdbBackend().connection_scope(engine):
        raise RuntimeError("failed operation")

    connection.commit.assert_not_called()
