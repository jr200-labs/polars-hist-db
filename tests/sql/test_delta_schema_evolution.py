import pytest

from polars_hist_db.config import TableColumnConfig, TableConfig
from polars_hist_db.core import TableConfigOps, TableOps

from ..utils.dsv_helper import mariadb_engine_test

pytestmark = pytest.mark.integration


def test_delta_table_create_adds_missing_columns():
    engine = mariadb_engine_test()
    initial_config = TableConfig(
        name="delta_schema_evolution",
        schema="test",
        columns=[
            TableColumnConfig(
                table="delta_schema_evolution",
                name="id",
                data_type="INT",
            ),
        ],
    )
    evolved_config = TableConfig(
        name="delta_schema_evolution",
        schema="test",
        columns=[
            TableColumnConfig(
                table="delta_schema_evolution",
                name="id",
                data_type="INT",
            ),
            TableColumnConfig(
                table="delta_schema_evolution",
                name="image_url",
                data_type="VARCHAR(255)",
            ),
        ],
    )

    with engine.begin() as connection:
        table_ops = TableOps("test", "delta_schema_evolution", connection)
        TableConfigOps(connection).drop(initial_config)
        try:
            TableConfigOps(connection).create(initial_config, is_delta_table=True)

            assert "image_url" not in table_ops.get_table_metadata().columns

            TableConfigOps(connection).create(evolved_config, is_delta_table=True)

            assert "image_url" in table_ops.get_table_metadata().columns
        finally:
            TableConfigOps(connection).drop(evolved_config)


def test_nontemporal_table_create_adds_missing_columns():
    engine = mariadb_engine_test()
    initial_config = TableConfig(
        name="nontemporal_schema_evolution",
        schema="test",
        columns=[
            TableColumnConfig(
                table="nontemporal_schema_evolution",
                name="id",
                data_type="INT",
            ),
        ],
    )
    evolved_config = TableConfig(
        name="nontemporal_schema_evolution",
        schema="test",
        columns=[
            TableColumnConfig(
                table="nontemporal_schema_evolution",
                name="id",
                data_type="INT",
            ),
            TableColumnConfig(
                table="nontemporal_schema_evolution",
                name="image_file",
                data_type="VARCHAR(255)",
            ),
        ],
    )

    with engine.begin() as connection:
        table_ops = TableOps("test", "nontemporal_schema_evolution", connection)
        TableConfigOps(connection).drop(initial_config)
        try:
            TableConfigOps(connection).create(initial_config)

            assert "image_file" not in table_ops.get_table_metadata().columns

            TableConfigOps(connection).create(evolved_config)

            assert "image_file" in table_ops.get_table_metadata().columns
        finally:
            TableConfigOps(connection).drop(evolved_config)
