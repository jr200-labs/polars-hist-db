from datetime import datetime, timezone

from polars_hist_db.overrides import (
    InMemoryOverrideLedgerStore,
    OverrideLedger,
    OverrideTypedValue,
)


def _utc(hour: int) -> datetime:
    return datetime(2026, 7, 9, hour, tzinfo=timezone.utc)


def test_cargo_classification_set_replace_and_system_close_timeline():
    store = InMemoryOverrideLedgerStore()
    ledger = OverrideLedger(store)

    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "probable"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(13),
        comment="first assessment",
    )
    ledger.set_field(
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        value=OverrideTypedValue("enum", {"value": "possible"}),
        observed_canonical_value_json={"value": "expected"},
        valid_from=_utc(15),
    )
    ledger.close_field(
        owner_user_id="user-1",
        actor_user_id="system",
        feed_id="cargos",
        entity_id="cargo-1",
        field_path="classification",
        valid_to=_utc(20),
        reason="source_match",
        system=True,
    )

    raw_history = ledger.history_for_entity("user-1", "cargos", "cargo-1")
    history = ledger.projected_history_for_entity("user-1", "cargos", "cargo-1")

    assert [operation.operation_type for operation in raw_history] == [
        "set",
        "close",
        "set",
        "system_close",
    ]
    assert raw_history[0].valid_to is None
    assert raw_history[2].valid_to is None
    assert history[0].valid_from == _utc(13)
    assert history[0].valid_to == _utc(15)
    assert history[1].valid_from == _utc(15)
    assert history[1].valid_to == _utc(15)
    assert history[1].reason == "replaced"
    assert history[2].value == OverrideTypedValue("enum", {"value": "possible"})
    assert history[2].valid_from == _utc(15)
    assert history[2].valid_to == _utc(20)
    assert history[3].valid_from == _utc(20)
    assert history[3].valid_to == _utc(20)
    assert history[3].reason == "source_match"
    assert ledger.active_for_entity("user-1", "cargos", "cargo-1") == []
