from polars_hist_db.backends import DbEngineConfig
from polars_hist_db.config import PolarsHistDbConfig


def test_config_parses_db_section():
    config = PolarsHistDbConfig.from_yaml("tests/_data/config/simple.yaml")

    assert isinstance(config.db_config, DbEngineConfig)
    assert config.db_config.backend == "mariadb"
    assert config.db_config.hostname == "127.0.0.1"
    assert config.db_config.username == "root"
    assert config.db_config.password == "admin"


def test_config_defaults_to_mariadb_when_db_section_missing():
    config = PolarsHistDbConfig(
        {"table_configs": [], "datasets": []},
        table_configs_path=["table_configs"],
        datasets_path=["datasets"],
    )

    assert config.db_config.backend == "mariadb"
    assert config.db_config.port == 3306


def test_db_engine_config_parses_database_name():
    config = DbEngineConfig.from_mapping(
        {
            "backend": "xtdb",
            "hostname": "127.0.0.1",
            "port": 15432,
            "database": "analytics",
        }
    )

    assert config.backend == "xtdb"
    assert config.database == "analytics"


def test_db_engine_config_parses_adbc_port():
    config = DbEngineConfig.from_mapping(
        {
            "backend": "xtdb",
            "adbc_port": "19832",
        }
    )

    assert config.adbc_port == 19832


def test_db_engine_config_defaults_xtdb_adbc_port():
    config = DbEngineConfig.from_mapping({"backend": "xtdb"})

    assert config.adbc_port == 9832
