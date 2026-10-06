from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from polars_hist_db.core.db import DbOps
from polars_hist_db.loaders.dsv import file_search


def test_system_variables_read_real_result_rows(monkeypatch):
    with create_engine("sqlite:///:memory:").connect() as connection:
        ops = DbOps(connection)
        monkeypatch.setattr(
            ops,
            "execute_sqlalchemy",
            lambda *args: connection.execute(
                text("SELECT 'timestamp' AS Variable_name, '123.5' AS Value")
            ),
        )

        assert ops.get_all_variables("timestamp").to_dict(as_series=False) == {
            "timestamp": ["123.5"]
        }


def test_file_search_falls_back_to_stat_when_scanner_omits_mtime(tmp_path, monkeypatch):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")
    monkeypatch.setattr(
        file_search,
        "Scandir",
        lambda **kwargs: [
            SimpleNamespace(is_file=True, path="data.csv", st_mtime=None)
        ],
    )

    result = file_search._find_files_with_timestamps(
        str(tmp_path), ["*.csv"], {"method": "mtime"}, True
    )

    assert result["__path"].to_list() == [str(path)]
    assert result["__created_at"].item() == datetime.fromtimestamp(
        path.stat().st_mtime, tz=UTC
    )
