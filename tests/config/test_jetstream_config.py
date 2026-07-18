import pytest

from polars_hist_db.config.input.jetstream_config import JetStreamFetchConfig


def test_jetstream_heartbeat_interval_is_configurable():
    assert JetStreamFetchConfig().heartbeat_interval == 30.0
    assert JetStreamFetchConfig(heartbeat_interval=5).heartbeat_interval == 5
    assert JetStreamFetchConfig(heartbeat_interval=0).heartbeat_interval == 0

    with pytest.raises(ValueError, match="cannot be negative"):
        JetStreamFetchConfig(heartbeat_interval=-1)
