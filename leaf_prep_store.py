"""Private one-leaf cache. No canonical frontier or second publication protocol.

Only opt-in preparation creates these additive tables. Each helper owns a short
connection; publication settlement MUST use the DAG's existing transaction.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

LEASE_SECONDS = 180.0
READY_SECONDS = 3600.0
MAX_PAYLOAD_BYTES = 256_000
MAX_BATCHES = 64


@contextmanager
def connection(db_path):
    conn = sqlite3.connect(
        Path(db_path).resolve().as_uri() + "?mode=rw", uri=True, timeout=1.0
    )
    try:
        yield conn
    finally:
        conn.close()


def exists(conn):
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='lcm_leaf_preparation'"
        ).fetchone()
        is not None
    )


def claim(db_path, conversation_id, session_id, frontier, *, now=None):
    """One actionable lease per conversation; never steal a live worker's claim."""
    now = time.time() if now is None else now
    token = uuid.uuid4().hex
    with connection(db_path) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS lcm_leaf_preparation (
            conversation_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
            frontier INTEGER NOT NULL, claim_token TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('preparing','ready')),
            expires REAL NOT NULL, payload TEXT)
        """)
        conn.execute(
            "INSERT OR IGNORE INTO lcm_migration_state(step_name, completed_at) VALUES (?, ?)",
            ("background_leaf_cache_v1", now),
        )
        conn.execute("DELETE FROM lcm_leaf_preparation WHERE expires <= ?", (now,))
        state = conn.execute(
            "SELECT current_session_id, current_frontier_store_id FROM lcm_lifecycle_state "
            "WHERE conversation_id=?",
            (conversation_id,),
        ).fetchone()
        if state != (session_id, frontier):
            return None
        conn.execute(
            "DELETE FROM lcm_leaf_preparation WHERE conversation_id=? "
            "AND (session_id!=? OR frontier!=?)",
            (conversation_id, session_id, frontier),
        )
        if (
            conn.execute("SELECT COUNT(*) FROM lcm_leaf_preparation").fetchone()[0]
            >= MAX_BATCHES
        ):
            return None
        written = conn.execute(
            "INSERT OR IGNORE INTO lcm_leaf_preparation VALUES (?, ?, ?, ?, 'preparing', ?, NULL)",
            (conversation_id, session_id, frontier, token, now + LEASE_SECONDS),
        ).rowcount
        return token if written else None


def finish(db_path, token, payload, *, now=None):
    now = time.time() if now is None else now
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode()) > MAX_PAYLOAD_BYTES:
        return False
    with connection(db_path) as conn, conn:
        return (
            conn.execute(
                "UPDATE lcm_leaf_preparation SET state='ready', payload=?, expires=? "
                "WHERE claim_token=? AND state='preparing' AND expires>?",
                (encoded, now + READY_SECONDS, token, now),
            ).rowcount
            == 1
        )


def load(db_path, conversation_id, frontier):
    with connection(db_path) as conn:
        if not exists(conn):
            return None
        row = conn.execute(
            "SELECT substr(payload, 1, ?) FROM lcm_leaf_preparation "
            "WHERE conversation_id=? AND frontier=? AND state='ready' AND expires>? "
            "AND length(CAST(payload AS BLOB))<=?",
            (
                MAX_PAYLOAD_BYTES,
                conversation_id,
                frontier,
                time.time(),
                MAX_PAYLOAD_BYTES,
            ),
        ).fetchone()
        return json.loads(row[0]) if row and row[0] else None


def drop(db_path, token):
    with connection(db_path) as conn, conn:
        if exists(conn):
            conn.execute(
                "DELETE FROM lcm_leaf_preparation WHERE claim_token=?", (token,)
            )


def reserve_call(db_path, token, max_calls, window_s, *, now=None):
    """Conservative fixed-window spend shared by all preparers of this database.

    Charge every attempt before transport. Changing route or restarting cannot
    reset the window. This is not an account-wide provider quota.
    """
    now = time.time() if now is None else now
    if not 0 < max_calls <= 64 or not 0 < window_s <= 86400:
        return False
    with connection(db_path) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        if not conn.execute(
            "SELECT 1 FROM lcm_leaf_preparation WHERE claim_token=? "
            "AND state='preparing' AND expires>?",
            (token, now),
        ).fetchone():
            return False
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lcm_leaf_prep_spend "
            "(id INTEGER PRIMARY KEY CHECK(id=1), ends REAL NOT NULL, calls INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO lcm_leaf_prep_spend VALUES (1, ?, 0)",
            (now + window_s,),
        )
        conn.execute(
            "UPDATE lcm_leaf_prep_spend SET ends=?, calls=0 WHERE id=1 AND ends<=?",
            (now + window_s, now),
        )
        return (
            conn.execute(
                "UPDATE lcm_leaf_prep_spend SET calls=calls+1 WHERE id=1 AND calls<?",
                (max_calls,),
            ).rowcount
            == 1
        )


def stage_settlement(conn, token, conversation_id, session_id, frontier):
    """CAS and settlement under the canonical DAG writer lock; never commit here."""
    return (
        conn.execute(
            "DELETE FROM lcm_leaf_preparation WHERE claim_token=? AND conversation_id=? "
            "AND session_id=? AND frontier=? AND state='ready' AND expires>?",
            (token, conversation_id, session_id, frontier, time.time()),
        ).rowcount
        == 1
    )
