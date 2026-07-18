from types import SimpleNamespace

import pytest

from polars_hist_db.loaders.input_source import BatchFinalizer
from polars_hist_db.dataset.scrape import try_run_pipeline_as_transaction
from polars_hist_db.types import TypeContractError


@pytest.mark.asyncio
async def test_pipeline_acks_only_after_transaction_finishes(monkeypatch):
    events = []

    def run_batch(*args):
        events.append("committed")

    async def ack():
        events.append("acked")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", run_batch
    )

    await try_run_pipeline_as_transaction(
        [], object(), object(), object(), BatchFinalizer(ack_after_commit=ack)
    )

    assert events == ["committed", "acked"]


@pytest.mark.asyncio
async def test_pipeline_raises_final_retry_error(monkeypatch):
    attempts = 0

    def fail(*args):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("still broken")

    async def no_wait(delay):
        pass

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", fail
    )
    monkeypatch.setattr("polars_hist_db.dataset.scrape.asyncio.sleep", no_wait)

    with pytest.raises(RuntimeError, match="still broken"):
        await try_run_pipeline_as_transaction(
            [],
            SimpleNamespace(),
            object(),
            object(),
            BatchFinalizer(),
            num_retries=3,
        )

    assert attempts == 3


@pytest.mark.asyncio
async def test_pipeline_does_not_retry_type_contract_errors(monkeypatch):
    attempts = 0

    def fail(*args):
        nonlocal attempts
        attempts += 1
        raise TypeContractError("invalid schema")

    monkeypatch.setattr(
        "polars_hist_db.dataset.scrape._run_pipeline_as_transaction", fail
    )

    with pytest.raises(TypeContractError, match="invalid schema"):
        await try_run_pipeline_as_transaction(
            [],
            SimpleNamespace(),
            object(),
            object(),
            BatchFinalizer(),
            num_retries=3,
        )

    assert attempts == 1
