from benchmarks.xtdb_delta import (
    benchmark_foreign_keys,
    network_floor_seconds,
    synthetic_frames,
)


def test_xtdb_delta_benchmark_models_target_and_upload_independently():
    current, incoming = synthetic_frames(100, 10, 2)

    assert current.shape == (100, 3)
    assert incoming.shape == (10, 3)
    assert network_floor_seconds(100, 10) == 80

    _, parent_mb, upload_mb = benchmark_foreign_keys(100, 10, 1)
    assert parent_mb > upload_mb
