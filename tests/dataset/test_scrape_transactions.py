from types import SimpleNamespace

import pytest

from polars_hist_db.loaders.input_source import BatchFinalizer
from polars_hist_db.dataset.scrape import _pipeline_transaction_context
from polars_hist_db.dataset.scrape import try_run_pipeline_as_transaction


class _FakeTransaction:
    def __init__(self):
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False


class _FakeConnection:
    def __init__(self):
        self.transaction = _FakeTransaction()
        self.begin_called = False

    def begin(self):
        self.begin_called = True
        return self.transaction


def test_pipeline_transaction_context_uses_sqlalchemy_transaction_for_mariadb():
    connection = _FakeConnection()

    with _pipeline_transaction_context(connection, is_xtdb=False):
        pass

    assert connection.begin_called is True
    assert connection.transaction.entered is True
    assert connection.transaction.exited is True


def test_pipeline_transaction_context_uses_plain_context_for_xtdb():
    connection = _FakeConnection()

    with _pipeline_transaction_context(connection, is_xtdb=True):
        pass

    assert connection.begin_called is False
    assert connection.transaction.entered is False
    assert connection.transaction.exited is False


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
