from polars_hist_db.config import PolarsHistDbConfig


def test_config_can_create_engine_from_db_backend():
    config = PolarsHistDbConfig.from_yaml("tests/_data/config/simple.yaml")

    engine = config.create_engine()

    assert engine.url.drivername == "mariadb+pymysql"
    assert engine.url.host == "127.0.0.1"
