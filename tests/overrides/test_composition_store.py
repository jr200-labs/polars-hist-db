from datetime import datetime, timezone
from uuid import uuid4

from polars_hist_db.overrides import (
    CompositionRevision,
    InMemoryLayerCompositionStore,
    LayerCompositionStoreConfig,
    OverridePurgeStoreConfig,
    build_layer_composition_table_config,
    build_override_purge_table_config,
)


def test_configurable_composition_and_purge_tables() -> None:
    composition = build_layer_composition_table_config(
        LayerCompositionStoreConfig("custom", "compositions")
    )
    purge = build_override_purge_table_config(
        OverridePurgeStoreConfig("custom", "purges")
    )

    assert (composition.schema, composition.name) == ("custom", "compositions")
    assert {column.name for column in composition.columns} >= {
        "child_layer_ids_json",
        "valid_from",
        "valid_to",
        "recorded_at",
        "supersedes_revision_id",
    }
    assert (purge.schema, purge.name) == ("custom", "purges")
    assert "value_json" not in {column.name for column in purge.columns}
    assert "comment" not in {column.name for column in purge.columns}


def test_in_memory_composition_store_preserves_revision_order() -> None:
    store = InMemoryLayerCompositionStore()
    first = CompositionRevision(
        str(uuid4()),
        "root",
        ("left", "right"),
        datetime(2026, 7, 13, tzinfo=timezone.utc),
        None,
        datetime(2026, 7, 12, tzinfo=timezone.utc),
    )
    store.append(first, "editor")

    assert store.revisions("root") == (first,)
