"""Tool result names survive storage and host reload without weakening identity."""

import copy

import pytest

from hermes_lcm import tokens
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.store import MessageStore


@pytest.fixture(autouse=True)
def deterministic_tokens(monkeypatch):
    monkeypatch.setattr(tokens, "_encoder_ready", True)
    monkeypatch.setattr(tokens, "_encoder", None)


def _engine(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(database_path=str(tmp_path / "lcm.db"),
                         embeddings_enabled=False, temporal_rollups_enabled=False),
        hermes_home=str(tmp_path / "home"),
    )
    engine.on_session_start("S", platform="cli", context_length=200_000, conversation_id="conv")
    return engine


def _messages():
    return [
        {"role": "user", "content": "Run the probe."},
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": "call-1", "type": "function", "function": {"name": "probe", "arguments": "{}"}},
        ]},
        {"role": "tool", "tool_call_id": "call-1", "name": "probe", "content": "error: " + "x" * 3000},
    ]


def _reload(messages):
    reloaded = copy.deepcopy(messages)
    for message in reloaded:
        if message["role"] == "tool":
            message["tool_name"] = message.get("tool_name") or message.get("name")
            message.pop("name", None)
    return reloaded


@pytest.mark.parametrize("batch", [False, True], ids=["append", "append_batch"])
@pytest.mark.parametrize("role", ["tool", "assistant", "user", "system"])
def test_explicit_tool_name_wins_and_non_tool_names_stay_unstored(tmp_path, batch, role):
    store = MessageStore(tmp_path / "lcm.db")
    messages = [
        {"role": role, "content": "explicit", "tool_name": "explicit", "name": "other"},
        {"role": role, "content": "unnamed"},
    ]
    if role != "tool":
        messages.append({"role": role, "content": "name only", "name": "speaker"})
    try:
        if batch:
            ids = store.append_batch("S", messages)
        else:
            ids = [store.append("S", message) for message in messages]
        assert [store.get(store_id)["tool_name"] for store_id in ids] == ["explicit"] + [None] * (len(ids) - 1)
        assert [store.get(store_id)["content"] for store_id in ids] == [m["content"] for m in messages]
    finally:
        store.close()


def test_normal_hermes_result_cold_replay_is_unchanged(tmp_path):
    messages = _messages()
    messages[-1]["tool_name"] = "probe"
    engine = _engine(tmp_path)
    try:
        engine.ingest(messages)
        before = engine._store.get_session_messages("S")
        assert len(before) == len(messages)
        assert before[-1]["tool_name"] == "probe"
    finally:
        engine.shutdown()
    engine = _engine(tmp_path)
    try:
        engine.ingest(_reload(messages))
        engine.ingest(_reload(messages))
        assert engine._store.get_session_messages("S") == before
    finally:
        engine.shutdown()
