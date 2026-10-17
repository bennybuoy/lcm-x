"""Synthetic safety contracts for the default-off one-leaf cache."""

import json
import copy
import sqlite3

import pytest

from hermes_lcm import leaf_prep
from tests.test_787_slice1_background_leaf_prep import (  # noqa: F401
    _isolated as _isolated,
    provider as provider,
    engine as engine,
    _crossing_view,
    _turn_end,
)


def test_one_private_leaf_survives_worker_restart_without_prompt_churn(
    engine, provider, monkeypatch
):
    view = _crossing_view(engine)
    before = copy.deepcopy(view)
    _turn_end(engine, view)
    leaves = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)
    assert len(leaves) == 1
    assert provider.on("lcm-leaf-prep") == 1
    assert engine._dag.get_session_nodes("S") == []
    assert engine._lifecycle.get_by_conversation("conv").current_frontier_store_id == 0
    assert view == before
    monkeypatch.setattr(leaf_prep, "_LEAF_PREP", leaf_prep._LeafPrep())
    assert leaf_prep.ready_chain(engine._store.db_path, "conv", 0) == leaves


def _publish(engine, leaf):
    from hermes_lcm.dag import SummaryNode

    node = SummaryNode(
        session_id=leaf.session_id,
        depth=0,
        summary=leaf.summary,
        token_count=leaf.summary_tokens,
        source_token_count=leaf.source_tokens,
        source_ids=list(leaf.covered_store_ids),
        source_type="messages",
    )
    engine._store_complete_copy_exclusion_proofs = {}
    engine._publish_leaf_node(
        node,
        level=leaf.level,
        model=leaf.model,
        expected_frontier=leaf.start_frontier,
        consumed_store_ids=list(leaf.covered_store_ids),
        publication_excluded_store_ids=list(leaf.excluded_store_ids),
        filter_exclusion_proofs={},
        carried_ranges=[],
        before_publication=engine._prepared_leaf_revalidation(leaf),
    )
    return node


@pytest.mark.parametrize("change", ["profile", "window", "frontier"])
def test_promotion_revalidates_binding_policy_and_frontier(engine, change):
    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    if change == "profile":
        engine._hermes_home += "/other"
    elif change == "window":
        engine.context_length += 1000
    else:
        engine._store.connection.execute(
            "UPDATE lcm_lifecycle_state SET current_frontier_store_id=1 WHERE conversation_id='conv'"
        )
        engine._store.connection.commit()
    with pytest.raises(leaf_prep.PromotionRejected):
        engine._prepared_leaf_revalidation(leaf)(engine._store.connection)


def test_rescue_publishes_only_the_chunk_actually_summarized(
    engine, provider, monkeypatch
):
    import hermes_lcm.engine as engine_module
    from tests.test_787_slice1_background_leaf_prep import SUMMARY

    engine._config.leaf_chunk_tokens = 8000
    seen = []

    def answer(prompt):
        seen.append(len(prompt))
        if len(prompt) > seen[0] * 0.7:
            raise RuntimeError(
                "context_length_exceeded: maximum context length exceeded"
            )
        return SUMMARY

    provider.result = answer
    # Force the rescue selector to a complete user/assistant unit. The real
    # summarizer/escalation chain still handles the transport refusal.
    monkeypatch.setattr(
        engine_module,
        "_next_leaf_rescue_chunk_for",
        lambda c, select, chunk, tokens, final=False: chunk[:2],
    )
    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    assert len(seen) > 1 and min(seen) < max(seen)
    assert leaf.covered_store_ids == (2, 3)
    node = _publish(engine, leaf)
    assert node.source_ids == [2, 3]
    assert engine._lifecycle.get_by_conversation("conv").current_frontier_store_id == 3
    assert engine._store.get_session_count("S") == len(view)


def test_foreground_call_prevents_new_speculative_transport(engine, provider):
    import threading

    view = _crossing_view(engine)

    def during_call():
        if threading.current_thread().name == "MainThread":
            engine.last_prompt_tokens = engine.threshold_tokens
            engine._prepare_leaves_after_turn(view)
            assert leaf_prep._LEAF_PREP.wait_idle(5)

    provider.hook = during_call
    engine.compress(view, current_tokens=engine.threshold_tokens + 1)
    assert provider.on("MainThread") > 0
    assert provider.on("lcm-leaf-prep") == 0


def test_explicit_focus_does_not_publish_cached_auto_focus(engine, provider):
    view = _crossing_view(engine)
    _turn_end(engine, view)
    engine.compress(
        view,
        current_tokens=engine.threshold_tokens + 1,
        focus_topic="new explicit topic",
    )
    assert leaf_prep._LEAF_PREP.counters["promoted"] == 0
    assert provider.on("MainThread") > 0


def test_pressure_admission_is_configurable(engine, provider, monkeypatch):
    from hermes_lcm.config import LCMConfig

    monkeypatch.setenv("LCM_BACKGROUND_LEAF_PREP_PRESSURE_RATIO", "0.9")
    assert (
        getattr(LCMConfig.from_env(), "background_leaf_prep_pressure_ratio", None)
        == 0.9
    )
    engine._config.background_leaf_prep_pressure_ratio = 0.9
    view = _crossing_view(engine)
    _turn_end(engine, view, tokens_now=int(engine.threshold_tokens * 0.85))
    assert provider.calls == []
    _turn_end(engine, view, tokens_now=int(engine.threshold_tokens * 0.95))
    assert provider.on("lcm-leaf-prep") == 1


def test_source_byte_cap_refuses_before_payload_decode(engine, provider, monkeypatch):
    monkeypatch.setattr(leaf_prep, "_SCAN_BYTES", 16, raising=False)
    decoded = []
    original = leaf_prep._decode_row
    monkeypatch.setattr(
        leaf_prep, "_decode_row", lambda row: (decoded.append(True), original(row))[1]
    )
    _turn_end(engine, _crossing_view(engine))
    assert not decoded and not provider.calls


def test_failed_claim_releases_the_process_call_slot(engine, provider, monkeypatch):
    from hermes_lcm import leaf_prep_store

    claim = leaf_prep_store.claim
    monkeypatch.setattr(
        leaf_prep_store,
        "claim",
        lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("busy")),
    )
    view = _crossing_view(engine)
    _turn_end(engine, view)
    monkeypatch.setattr(leaf_prep_store, "claim", claim)
    _turn_end(engine, view)
    assert provider.on("lcm-leaf-prep") == 1


def test_queue_bounds_distinct_conversations():
    from types import SimpleNamespace

    state = leaf_prep._LeafPrep()
    state._worker = SimpleNamespace(is_alive=lambda: True)
    for i in range(100):
        state.enqueue(
            SimpleNamespace(
                generation=0, db_path="synthetic", key=("synthetic", str(i))
            )
        )
    assert len(state._jobs) <= 64
    assert state.counters["jobs_dropped"] == 36


def test_shutdown_of_another_clone_does_not_cancel_live_preparer(
    engine, provider, tmp_path
):
    import threading
    from tests.test_787_slice1_background_leaf_prep import _engine

    entered, release = threading.Event(), threading.Event()
    provider.hook = lambda: (entered.set(), release.wait(5))
    view = _crossing_view(engine)
    engine.ingest(view)
    engine.last_prompt_tokens = engine.threshold_tokens
    engine._prepare_leaves_after_turn(view)
    assert entered.wait(5)
    other = _engine(tmp_path)
    other.shutdown()
    release.set()
    assert leaf_prep._LEAF_PREP.wait_idle(10)
    assert leaf_prep.ready_chain(engine._store.db_path, "conv", 0)


def test_budget_is_not_reset_by_a_new_scheduler(engine, provider, monkeypatch):
    from hermes_lcm import leaf_prep_store

    engine._config.background_leaf_prep_max_calls = 1
    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    leaf_prep_store.drop(engine._store.db_path, leaf.claim_token)
    monkeypatch.setattr(leaf_prep, "_LEAF_PREP", leaf_prep._LeafPrep())
    _turn_end(engine, view)
    assert provider.on("lcm-leaf-prep") == 1


def test_policy_mutation_rejects_prepared_leaf(engine):
    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    engine._config.fresh_tail_count += 5
    with pytest.raises(leaf_prep.PromotionRejected, match="route_fingerprint_mismatch"):
        engine._prepared_leaf_revalidation(leaf)(engine._store.connection)


def test_promotion_settles_ready_in_canonical_transaction_without_a_call(
    engine, provider
):
    from hermes_lcm import leaf_prep_store

    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    calls = list(provider.calls)
    node = _publish(engine, leaf)
    assert provider.calls == calls
    assert node.source_ids == list(leaf.covered_store_ids)
    assert (
        engine._lifecycle.get_by_conversation("conv").current_frontier_store_id
        == leaf.covered_end
    )
    assert leaf_prep_store.load(engine._store.db_path, "conv", 0) is None


@pytest.mark.parametrize("phase", ["settlement", "frontier", "commit"])
def test_process_death_is_wholly_old_or_new(engine, tmp_path, phase):
    import os
    import subprocess
    import sys
    from hermes_lcm import leaf_prep_store

    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    script = """
import os, sys
from pathlib import Path
import tests.conftest
from tests.test_787_slice1_background_leaf_prep import _engine
from tests.test_background_leaf_safety import _publish
from hermes_lcm import leaf_prep
e = _engine(Path(sys.argv[1]))
leaf = leaf_prep.ready_chain(e._store.db_path, "conv", 0)[0]
phase = sys.argv[2]
original = e._lifecycle.stage_compaction_publication
def stage(*a, **kw):
    if phase == "settlement": os._exit(73)
    original(*a, **kw)
    if phase == "frontier": os._exit(73)
e._lifecycle.stage_compaction_publication = stage
_publish(e, leaf)
os._exit(73)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), phase],
        env={**os.environ, "HERMES_HOME": str(tmp_path)},
        timeout=30,
    )
    assert result.returncode == 73
    with sqlite3.connect(engine._store.db_path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        count = conn.execute("SELECT count(*) FROM summary_nodes").fetchone()[0]
        searchable = conn.execute(
            "SELECT count(*) FROM nodes_fts WHERE nodes_fts MATCH 'Earlier'"
        ).fetchone()[0]
        frontier = conn.execute(
            "SELECT current_frontier_store_id FROM lcm_lifecycle_state WHERE conversation_id='conv'"
        ).fetchone()[0]
    if phase == "commit":
        assert count == searchable == 1 and frontier == leaf.covered_end
        assert leaf_prep_store.load(engine._store.db_path, "conv", 0) is None
        with pytest.raises(Exception):
            _publish(engine, leaf)
    else:
        assert count == searchable == frontier == 0
        assert leaf_prep_store.load(engine._store.db_path, "conv", 0)
        _publish(engine, leaf)  # retry creates exactly one owner
    assert len(engine._dag.get_session_nodes("S")) == 1
    assert engine._store.get_session_count("S") == len(view)


def test_claim_lease_has_one_winner_and_late_worker_cannot_resurrect(engine):
    import concurrent.futures
    from hermes_lcm import leaf_prep_store as store

    path = engine._store.db_path
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        claims = list(
            pool.map(lambda _: store.claim(path, "conv", "S", 0, now=1000), range(2))
        )
    winner = next(token for token in claims if token)
    assert sum(token is not None for token in claims) == 1
    assert store.claim(path, "conv", "S", 0, now=1001) is None
    new = store.claim(path, "conv", "S", 0, now=1000 + store.LEASE_SECONDS)
    assert new and new != winner
    assert not store.finish(path, winner, {"synthetic": "late"}, now=1200)
    assert store.finish(path, new, {"synthetic": "ready"}, now=1200)


def test_new_tail_and_reload_preserve_exact_occurrences(engine, provider, tmp_path):
    from tests.test_787_slice1_background_leaf_prep import _engine

    engine._config.threshold_full_sweep_enabled = False
    view = _crossing_view(engine)
    _turn_end(engine, view)
    tail = [
        {"role": "user", "content": "new tail occurrence", "timestamp": 999.0},
        {"role": "assistant", "content": "new tail reply", "timestamp": 1000.0},
    ]
    view += tail
    result = engine.compress(view, current_tokens=engine.threshold_tokens + 1)
    assert provider.on("MainThread") == 0
    assert all(m in result for m in tail)
    before = engine._store.get_session_count("S")
    engine.shutdown()
    fresh = _engine(tmp_path, threshold_full_sweep_enabled=False)
    try:
        fresh.ingest(result)
        assert fresh._store.get_session_count("S") == before
        fresh.ingest(
            result
            + [
                {
                    "role": "user",
                    "content": "first subsequent occurrence",
                    "timestamp": 1001.0,
                }
            ]
        )
        assert fresh._store.get_session_count("S") == before + 1
    finally:
        fresh.shutdown()


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "tool"),
        ("tool_call_id", "changed-call"),
        (
            "tool_calls",
            json.dumps(
                [{"id": "changed", "function": {"name": "inspect", "arguments": "{}"}}]
            ),
        ),
        ("tool_name", "changed-tool"),
        ("timestamp", 12345.0),
        ("content", '[{"type":"text","text":"changed"}]'),
        ("session_id", "other-session"),
        ("conversation_id", "other-conversation"),
        ("source", "other-source"),
    ],
)
def test_transaction_rejects_any_consumed_source_change(engine, field, value):
    view = _crossing_view(engine)
    _turn_end(engine, view)
    leaf = leaf_prep.ready_chain(engine._store.db_path, "conv", 0)[0]
    conn = engine._store.connection
    conn.execute(
        f"UPDATE messages SET {field} = ? WHERE store_id = ?",
        (value, leaf.covered_store_ids[0]),
    )
    conn.commit()
    with pytest.raises(leaf_prep.PromotionRejected, match="coverage_digest_mismatch"):
        engine._prepared_leaf_revalidation(leaf)(conn)
