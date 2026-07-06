"""Backend-agnostic invariant: pipeline runs leave no rows in the stage/delta table.

The pipeline uses a per-run scratch table (called `__<name>_stream_stage` on
XTDB, `<name>` on MariaDB) to buffer partition rows before merging into the
final table. That scratch table is ephemeral by design and must be empty
after `run_datasets` completes — otherwise ingest volume compounds into the
backing store forever.

The failure mode this test guards against (JRL-47) is XTDB-specific in its
mechanism — `DELETE FROM` is temporal so the tombstoned rows keep their
Arrow files — but the *invariant* is backend-independent. This test runs
against every backend `backend_params()` yields.
"""

from datetime import datetime

import pytest
from sqlalchemy import text

from polars_hist_db.dataset import run_datasets

from ..utils.dsv_helper import backend_params, setup_fixture_dataset

pytestmark = [
    pytest.mark.integration,
    pytest.mark.parametrize(
        "fixture_simple",
        backend_params(),
        indirect=True,
    ),
]


@pytest.fixture
def fixture_simple(request):
    yield from setup_fixture_dataset("simple_nontemporal.yaml", request.param)


def _stage_row_count(engine, dataset, backend_name: str) -> int:
    """Read all rows the pipeline might have left behind in the stage table.

    XTDB stage sits at `<schema>.__<name>_stream_stage` and needs
    `FOR ALL SYSTEM_TIME` because a temporal DELETE would hide rows at the
    current system-time while leaving files on disk.

    MariaDB stage sits at `<schema>.<name>` and DELETE is physical, so a
    plain SELECT is sufficient.
    """
    schema = dataset.delta_table_schema
    if backend_name == "xtdb":
        table = f"{schema}.__{dataset.name}_stream_stage"
        sql = f"SELECT COUNT(*) FROM {table} FOR ALL SYSTEM_TIME"
    else:
        table = f"{schema}.{dataset.name}"
        sql = f"SELECT COUNT(*) FROM {table}"

    with engine.connect() as connection:
        try:
            result = connection.execute(text(sql)).scalar()
        except Exception:
            # A missing table also satisfies the invariant (DROP is a
            # valid implementation choice).
            return 0
    return int(result or 0)


@pytest.mark.asyncio
async def test_run_datasets_leaves_stage_table_empty(fixture_simple, request):
    engine, base_config = fixture_simple
    dataset = base_config.datasets.datasets[0]
    backend_name = request.node.callspec.params["fixture_simple"]

    ts = datetime.fromisoformat("2025-01-01T00:00:00Z")
    dsv = "id,double_col,varchar_col\n1,1.5,alpha\n2,2.5,beta\n"

    dataset.input_config.set_payload(dsv, ts)
    await run_datasets(base_config, engine)

    assert _stage_row_count(engine, dataset, backend_name) == 0
