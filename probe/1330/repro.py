"""R1 engine probe plus source-snapshotted Hermes one-shot exit sequence."""
import argparse
import atexit
import faulthandler
import importlib.util
import json
import os
from pathlib import Path
import socket
import signal
import sys
import threading
import time
import types

parser = argparse.ArgumentParser()
parser.add_argument('--worktree', required=True)
parser.add_argument('--run-dir', required=True)
parser.add_argument('--variant', choices=['normal', 'shutdown', 'off', 'completed', 'stub'], required=True)
args = parser.parse_args()
root = Path(args.run_dir)
root.mkdir(parents=True, exist_ok=True)
home = root / 'home'
home.mkdir()
os.environ['HERMES_HOME'] = str(home)
os.environ['LCM_EMBEDDINGS_ENABLED'] = 'false' if args.variant == 'off' else 'true'
os.environ['LCM_EMBEDDING_PROVIDER'] = 'fastembed'
os.environ['LCM_EMBEDDING_MODEL'] = 'BAAI/bge-small-en-v1.5'
os.environ['LCM_PREANSWER_EVIDENCE_ENABLED'] = 'false' if args.variant == 'off' else 'true'

def dump(label):
    print(label + ' ' + json.dumps([{'name': t.name, 'daemon': t.daemon,
          'ident': t.ident, 'alive': t.is_alive()} for t in threading.enumerate()]), file=sys.stderr, flush=True)
    faulthandler.dump_traceback(all_threads=True)

atexit.register(dump, 'ATEXIT')  # registered before third-party imports
faulthandler.enable()
faulthandler.register(signal.SIGUSR1, all_threads=True)
# Observe registrations before fastembed/concurrent.futures import, without joining.
_original_register = threading._register_atexit
def observed_register(func, *a, **kw):
    print('THREADING_ATEXIT ' + func.__module__ + '.' + func.__qualname__, flush=True)
    return _original_register(func, *a, **kw)
threading._register_atexit = observed_register
faulthandler.dump_traceback_later(10, repeat=True)
sys.excepthook = lambda typ, exc, tb: (__import__('traceback').print_exception(typ, exc, tb), dump('UNCAUGHT'))
threading.excepthook = lambda arg: (__import__('traceback').print_exception(arg.exc_type, arg.exc_value, arg.exc_traceback), dump('THREAD_ERROR'))

def deny_network(*_a, **_kw):
    raise RuntimeError('repro has no network authority')

socket.socket.connect = deny_network
socket.create_connection = deny_network
worktree = Path(args.worktree)
sys.path.insert(0, str(worktree))
from benchmarking.standalone import ensure_agent_context_engine_importable
ensure_agent_context_engine_importable()
pkg = types.ModuleType('hermes_lcm')
pkg.__path__ = [str(worktree)]
sys.modules['hermes_lcm'] = pkg
from hermes_lcm import tokens
tokens._encoder_ready = True  # offline estimator; no tiktoken download
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.dag import SummaryNode
from hermes_lcm import embedding_provider as ep

started = threading.Event()
original = ep.FastembedProvider._encode_local
def observed(self, *a, **kw):
    # Synchronize exit with a real backfill batch, rather than a short profile
    # registration probe that may have finished before the main thread resumes.
    if len(a[0]) >= 32 and not kw.get('query'):
        self._ensure_local()  # target active inference, not model construction
        started.set()
    return original(self, *a, **kw)
ep.FastembedProvider._encode_local = observed
if args.variant == 'stub':
    class BlockingModel:
        def query_embed(self, texts):
            started.set()
            threading.Event().wait(180)
            return [[1.0, 0.0] for _ in texts]
        embed = query_embed
    ep.FastembedProvider._construct = lambda self, **kw: BlockingModel()

config = LCMConfig.from_env()
config.database_path = str(root / 'lcm.db')
engine = LCMEngine(config=config, hermes_home=str(home))
if args.variant == 'stub':
    from hermes_lcm.vector_store import VectorStore
    store = VectorStore(config.database_path, config=config)
    store.register_profile(config.embedding_model, 'fastembed', 2)
    store.register_profile(config.embedding_model, 'fastembed', 2, task='chunk')
    store.close()
# Seed synthetic rows before binding so the real session-bind pass has work.
for i in range(32):
    engine._store.append('exit-repro', {'role': 'user', 'content': f'Synthetic ledger item {i}: the blue crate ships on Tuesday. ' * 30})
    engine._dag.add_node(SummaryNode(session_id='exit-repro', depth=0,
        summary=f'Synthetic ledger summary {i}: blue crate ships Tuesday.', created_at=1.0, latest_at=1.0))
engine.on_session_start('exit-repro', platform='cli', conversation_id='exit-repro-conversation', context_length=200000)
if args.variant != 'off':
    assert started.wait(20), 'session-bind provider did not start'
node_id = engine._dag.add_node(SummaryNode(session_id='exit-repro', depth=0,
    summary='Post-leaf synthetic ledger summary.', created_at=2.0, latest_at=2.0))
engine._invalidate_rollups_for_published_node(engine._dag.get_node(node_id))
if args.variant == 'completed':
    assert engine.drain_embedding_maintenance(timeout=40)
    import sqlite3
    with sqlite3.connect(config.database_path) as db:
        print('COUNTS', db.execute('SELECT count(*) FROM lcm_embedding_meta').fetchone(),
              db.execute('SELECT count(*) FROM lcm_chunk_meta').fetchone(), flush=True)
turn_end = time.monotonic()
(root / 'turn-end.json').write_text(json.dumps({'monotonic': turn_end}))
print('TURN_END ' + str(turn_end), flush=True)
from host_replay import replay
replay(engine)
if args.variant == 'shutdown':
    engine.shutdown()
dump('SCRIPT_END')
# Deliberately no join, os._exit, or host CLI: normal interpreter finalization.
