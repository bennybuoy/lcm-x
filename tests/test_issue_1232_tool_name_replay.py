"""#1232: preserve name-only results across reload, upgrade and rollback."""

import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from hermes_lcm import survival_fit
from hermes_lcm.store import MessageStore
from tests.test_host_uid_shadow import ROLLBACK_READERS, _skip_or_fail_in_ci
from tests.test_issue_1232_tool_name import _engine, _messages, _reload, deterministic_tokens  # noqa: F401


_BASE = "3c9b34bc1165905869369c7d93f914aec95fdf5c"
_ARCHIVED_ENGINE = r'''
import importlib.util, json, sys
from pathlib import Path
old, db, home, view_file, mode, projected = sys.argv[1:]
spec = importlib.util.spec_from_file_location("hermes_lcm", old + "/__init__.py", submodule_search_locations=[old])
sys.modules["hermes_lcm"] = importlib.util.module_from_spec(spec)
from hermes_lcm import store, tokens, survival_fit
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
assert Path(store.__file__).resolve().parent == Path(old).resolve()
assert Path(sys.modules[LCMEngine.__module__].__file__).resolve().parent == Path(old).resolve()
tokens._encoder, tokens._encoder_ready = None, True
survival_fit._host_estimate = lambda messages: None
engine = LCMEngine(config=LCMConfig(database_path=db, embeddings_enabled=False,
                                  temporal_rollups_enabled=False), hermes_home=home)
try:
    engine.on_session_start("S", platform="cli", context_length=200_000, conversation_id="conv")
    view = json.loads(Path(view_file).read_text())
    before = engine._store.get_session_messages("S")
    engine.ingest(view)
    first = engine._store.get_session_messages("S")
    if mode == "write" and projected == "yes":
        view = engine._survival_projection(view, {id(m): r["store_id"] for m, r in zip(view, first)}, 0)
    if mode == "read":
        engine.ingest(view)
    after = engine._store.get_session_messages("S")
    assert engine._store.connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    print(json.dumps({"before": before, "first": first, "after": after, "view": view}))
finally:
    engine.shutdown()
'''


@pytest.fixture(scope="module")
def archives(tmp_path_factory):
    root = Path(__file__).resolve().parents[1]
    directory = tmp_path_factory.mktemp("tool-name-archives")
    result = {}
    for label, commit in (("base", _BASE), ("old", ROLLBACK_READERS["v0.26.2"])):
        if subprocess.run(["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=root,
                          capture_output=True).returncode:
            _skip_or_fail_in_ci(f"archived reader commit {commit} is unavailable")
        archive = subprocess.run(["git", "archive", commit], cwd=root, check=True, capture_output=True).stdout
        result[label] = directory / label
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(result[label], filter="data")
    return result


def _archived(archive, tmp_path, view, mode="read", projected=False):
    view_file = tmp_path / "view.json"
    view_file.write_text(json.dumps(view))
    result = subprocess.run(
        [sys.executable, "-B", "-c", _ARCHIVED_ENGINE, str(archive), str(tmp_path / "lcm.db"),
         str(tmp_path / "home"), str(view_file), mode, "yes" if projected else "no"],
        cwd=tmp_path, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout)


@pytest.mark.parametrize("batch", [False, True], ids=["append", "append_batch"])
@pytest.mark.parametrize("explicit", [None, ""], ids=["missing", "empty"])
def test_name_only_result_is_persisted(tmp_path, batch, explicit):
    message = _messages()[-1]
    if explicit is not None:
        message["tool_name"] = explicit
    store = MessageStore(tmp_path / "lcm.db")
    try:
        ids = store.append_batch("S", [message]) if batch else [store.append("S", message)]
        assert store.get(ids[0])["tool_name"] == "probe"
    finally:
        store.close()


@pytest.mark.parametrize("projected", [False, True], ids=["plain", "survival"])
def test_name_only_result_cold_replay(tmp_path, monkeypatch, projected):
    monkeypatch.setattr(survival_fit, "_host_estimate", lambda messages: None)
    view = _messages()
    engine = _engine(tmp_path)
    try:
        engine.ingest(view)
        before = engine._store.get_session_messages("S")
        if projected:
            view = engine._survival_projection(view, {id(m): r["store_id"] for m, r in zip(view, before)}, 0)
            assert survival_fit._PROJECTED_PREFIX in view[-1]["content"]
    finally:
        engine.shutdown()
    engine = _engine(tmp_path)
    try:
        engine.ingest(_reload(view))
        assert engine._store.get_session_messages("S") == before
        if projected:
            tool = _reload(view)[-1]
            assert engine._survival_projection_source(tool, "tool", tool["content"])["store_id"] == before[-1]["store_id"]
            tool["tool_name"] = "different"
            assert engine._survival_projection_source(tool, "tool", tool["content"]) is None
    finally:
        engine.shutdown()


@pytest.mark.parametrize("projected", [False, True], ids=["plain", "survival"])
def test_base_store_upgrade_and_v0262_rollback_replay(tmp_path, archives, projected):
    seeded = _archived(archives["base"], tmp_path, _messages(), mode="write", projected=projected)
    assert len(seeded["first"]) == 3
    assert seeded["first"][-1]["tool_name"] is None  # The actual base writer dropped it.
    # The seed process must not cold-replay: keep this a clean pre-fix store.
    assert seeded["after"] == seeded["first"]
    view = _reload(seeded["view"])
    if projected:
        assert survival_fit._PROJECTED_PREFIX in view[-1]["content"]
    engine = _engine(tmp_path)
    try:
        engine.ingest(view)
        upgraded = engine._store.get_session_messages("S")
        assert len(upgraded) == len(seeded["first"])
        expected = seeded["first"]
        expected[-1]["tool_name"] = "probe"
        assert upgraded == expected
        engine.ingest(view)
        assert engine._store.get_session_messages("S") == upgraded
    finally:
        engine.shutdown()
    rollback = _archived(archives["old"], tmp_path, view)
    assert rollback["before"] == rollback["first"] == rollback["after"] == upgraded
