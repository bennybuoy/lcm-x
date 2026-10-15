"""The tool-name data repair is atomic, scoped and one-time."""

import json
import sqlite3

import pytest

from hermes_lcm import db_bootstrap as boot


STEP = "messages_tool_name_backfill_v1"


def _connection():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE messages (
            store_id INTEGER PRIMARY KEY, session_id TEXT, source TEXT, conversation_id TEXT,
            role TEXT, tool_call_id TEXT, tool_calls TEXT, tool_name TEXT, content TEXT);
        CREATE INDEX idx_msg_session ON messages(session_id, store_id);
        CREATE TABLE lcm_migration_state (step_name TEXT PRIMARY KEY, completed_at REAL);
    """)
    return conn


def _call(name):
    return json.dumps([{"id": "reused", "function": {"name": name}}])


def test_backfill_uses_nearest_preceding_call_within_origin_and_preserves_rows():
    with _connection() as conn:
        payloads = [
            ("tool", "reused", None, None),  # No preceding call.
            ("assistant", None, _call("first"), None),
            ("tool", "reused", None, None),
            ("tool", "reused", None, "explicit"),
            ("assistant", None, _call("second"), None),
            ("tool", "reused", None, ""),
            ("user", "reused", None, None),
            ("tool", "unknown", None, None),
            ("assistant", None, _call(""), None),  # Do not use an older named call.
            ("tool", "reused", None, None),
            ("assistant", None, "not json", None),
            ("assistant", None, json.dumps({"unexpected": True}), None),
            ("assistant", None, json.dumps([None, 1]), None),
            ("assistant", None, _call("last"), None),
        ]
        for index, payload in enumerate(payloads, 1):
            conn.execute("INSERT INTO messages VALUES (?, 'S', 'cli', 'conv', ?, ?, ?, ?, ?)",
                         (index, *payload, f"content-{index}"))
        for index, origin in enumerate((("other", "cli", "conv"), ("S", "other", "conv"),
                                        ("S", "cli", "other")), len(payloads) + 1):
            conn.execute("INSERT INTO messages VALUES (?, ?, ?, ?, 'tool', 'reused', NULL, NULL, ?)",
                         (index, *origin, f"content-{index}"))
        before = conn.execute("SELECT * FROM messages ORDER BY store_id").fetchall()
        boot.backfill_tool_names(conn)
        expected = [list(row) for row in before]
        expected[2][7] = "first"
        expected[5][7] = "second"
        assert conn.execute("SELECT * FROM messages ORDER BY store_id").fetchall() == [tuple(row) for row in expected]
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        marker = conn.execute("SELECT * FROM lcm_migration_state").fetchall()
        assert marker[0][0] == STEP
        # A completed migration must not scan the message table on reopen.
        def no_message_reads(action, table, *_):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "messages" else sqlite3.SQLITE_OK
        conn.set_authorizer(no_message_reads)
        boot.backfill_tool_names(conn)
        assert conn.execute("SELECT * FROM lcm_migration_state").fetchall() == marker
        conn.set_authorizer(None)


def test_backfill_rolls_back_names_and_marker_together():
    with _connection() as conn:
        conn.execute("INSERT INTO messages VALUES (1, 'S', '', '', 'assistant', NULL, ?, NULL, 'call')", (_call("probe"),))
        conn.executemany("INSERT INTO messages VALUES (?, 'S', '', '', 'tool', 'reused', NULL, NULL, 'error')", [(2,), (3,)])
        conn.commit()
        conn.execute("""CREATE TRIGGER fail_second BEFORE UPDATE OF tool_name ON messages
                        WHEN old.store_id = 3 BEGIN SELECT RAISE(ABORT, 'synthetic interruption'); END""")
        before = conn.execute("SELECT * FROM messages").fetchall()
        with pytest.raises(sqlite3.IntegrityError, match="synthetic interruption"):
            boot.backfill_tool_names(conn)
        assert conn.execute("SELECT * FROM messages").fetchall() == before
        assert conn.execute("SELECT * FROM lcm_migration_state").fetchall() == []
        conn.execute("DROP TRIGGER fail_second")
        boot.backfill_tool_names(conn)
        assert conn.execute("SELECT tool_name FROM messages WHERE role='tool'").fetchall() == [("probe",), ("probe",)]
        assert conn.execute("SELECT step_name FROM lcm_migration_state").fetchall() == [(STEP,)]
