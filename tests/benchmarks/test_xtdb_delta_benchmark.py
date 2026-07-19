from benchmarks.xtdb_delta import (
    benchmark_arrow_override_crdt,
    benchmark_foreign_keys,
    benchmark_time_partitions,
    network_floor_seconds,
    synthetic_arrow_override_operations,
    synthetic_frames,
)


def test_xtdb_delta_benchmark_models_target_and_upload_independently():
    current, incoming = synthetic_frames(100, 10, 2)

    assert current.shape == (100, 3)
    assert incoming.shape == (10, 3)
    assert network_floor_seconds(100, 10) == 80

    _, parent_mb, upload_mb, matched, created, collisions, updated = (
        benchmark_foreign_keys(100, 10, 0.5, 1)
    )
    assert parent_mb > upload_mb
    assert (matched, created, collisions, updated) == (5, 5, 0, True)

    _, input_mb, partition_count = benchmark_time_partitions(100, 10, 1)
    assert input_mb > 0
    assert partition_count == 10


def test_arrow_override_benchmark_covers_clean_and_conflicting_frontiers():
    proposed = synthetic_arrow_override_operations(10, 5)
    assert proposed.num_rows == 10
    assert proposed.schema.field("valid_from").type.tz == "UTC"

    ipc, sync, ipc_mb, projected, conflicts = benchmark_arrow_override_crdt(10, 5, 1)

    assert ipc >= 0
    assert sync >= 0
    assert ipc_mb > 0
    assert (projected, conflicts) == (5, 5)
