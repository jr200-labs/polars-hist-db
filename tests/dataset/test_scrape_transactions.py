from polars_hist_db.dataset.scrape import _pipeline_transaction_context


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
