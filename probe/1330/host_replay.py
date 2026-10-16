"""Lifecycle bodies from Hermes d923a5eb487a17c0a68801bba9ef0f56c81040a8.
AST-unparsed with docstrings removed. Dependencies absent from the engine probe
are explicit no-op fixtures below; this is not an installed-Hermes CLI run.
Upstream source: https://github.com/NousResearch/hermes-agent (MIT).
"""
from __future__ import annotations
import logging
import os
import sys
import threading
import time
from contextlib import suppress
from types import SimpleNamespace
logger = logging.getLogger(__name__)
def _cli():
    return sys.modules[__name__]
def _quietly(fn):
    with suppress(Exception):
        return fn()
def event(name):
    print('HOST_STAGE ' + name, flush=True)

# Host hermes_cli/cli_shutdown.py:43
def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default

# Host hermes_cli/cli_shutdown.py:51
def _exit_watchdog_timeout() -> float:
    from cli import _float_env
    return _float_env('HERMES_EXIT_WATCHDOG_S', 30.0)

# Host hermes_cli/cli_shutdown.py:34
def _flush_logging_and_stdio() -> None:
    with suppress(Exception):
        logging.shutdown()
    for _stream in (sys.stdout, sys.stderr):
        with suppress(Exception):
            _stream.flush()

# Host hermes_cli/cli_shutdown.py:57
def _arm_exit_watchdog(timeout_s: float | None=None, *, from_signal: bool=False) -> None:
    from cli import _exit_watchdog_timeout, _flush_logging_and_stdio
    if timeout_s is None:
        timeout_s = _exit_watchdog_timeout()
    if timeout_s <= 0:
        return
    if os.environ.get('PYTEST_CURRENT_TEST'):
        return

    def _watchdog():
        time.sleep(timeout_s)
        if from_signal and _cli()._cleanup_in_progress:
            return
        try:
            logger.warning('Exit watchdog fired after %.0fs — forcing process exit (a cleanup step or non-daemon thread is wedged).', timeout_s)
        except Exception:
            pass
        _flush_logging_and_stdio()
        with suppress(Exception):
            from tools.environments.base import kill_live_foreground_processes
            kill_live_foreground_processes(now=True)
        os._exit(0)
    with suppress(Exception):
        threading.Thread(target=_watchdog, daemon=True, name='exit-watchdog').start()

# Host hermes_cli/cli_shutdown.py:102
def _shutdown_agent_memory_provider(agent) -> None:
    if not (agent and hasattr(agent, 'shutdown_memory_provider')):
        return
    _mm = getattr(agent, '_memory_manager', None)
    if _mm is not None and hasattr(_mm, 'flush_pending'):
        with suppress(Exception):
            _mm.flush_pending(timeout=10)
    _session_msgs = getattr(agent, '_session_messages', None)
    _sid = getattr(agent, 'session_id', None) or '<unknown>'
    if isinstance(_session_msgs, list):
        logger.info('CLI cleanup calling memory shutdown for session %s with %d message(s)', _sid, len(_session_msgs))
        agent.shutdown_memory_provider(_session_msgs)
    else:
        logger.info('CLI cleanup calling memory shutdown for session %s without session message list', _sid)
        agent.shutdown_memory_provider()

# Host hermes_cli/cli_shutdown.py:309
def _finalize_single_query(cli) -> None:
    import cli as cli_module
    from cli import _flush_one_shot_session_store, _notify_single_query_session_finalize, _run_cleanup, _shutdown_agent_memory_provider, _wait_for_oneshot_background_completions
    try:
        try:
            _flush_one_shot_session_store(cli)
        except Exception:
            logger.debug('one-shot session store flush failed', exc_info=True)
        _notify_single_query_session_finalize(cli)
        try:
            _shutdown_agent_memory_provider(getattr(cli, 'agent', None) or cli_module._active_agent_ref)
        except Exception:
            logger.debug('one-shot memory provider shutdown failed', exc_info=True)
    finally:
        cli._release_active_session()
    try:
        _wait_for_oneshot_background_completions(cli)
    except Exception:
        logger.debug('one-shot background completion wait failed', exc_info=True)
    _run_cleanup(notify_session_finalize=False)

# Host cli.py:505
def _run_cleanup(*, notify_session_finalize: bool=True):
    global _cleanup_done, _cleanup_in_progress
    if _cleanup_done:
        return
    _cleanup_done = True
    _cleanup_in_progress = True
    try:
        _arm_exit_watchdog()
        _reset_terminal_input_modes_on_exit()
        for (step, swallow) in _CLEANUP_STEPS:
            with suppress(swallow):
                globals()[step]()
        if notify_session_finalize:
            cleanup_session_id = _active_agent_ref.session_id if _active_agent_ref else None
            if _should_emit_cleanup_session_finalize(cleanup_session_id):
                _session_end_messages.extend(_notify_session_finalize(session_id=cleanup_session_id, platform='cli', reason='shutdown'))
        try:
            _shutdown_agent_memory_provider(_active_agent_ref)
        except Exception as e:
            logger.warning('CLI cleanup memory shutdown failed: %s', e, exc_info=True)
    finally:
        _cleanup_in_progress = False

# Host run_agent.py:182
def _notify_context_engine_session_end(agent: Any, messages: Optional[list]) -> None:
    engine = getattr(agent, 'context_compressor', None)
    if engine:
        _quietly(lambda : engine.on_session_end(agent.session_id or '', messages or []))

# Host run_agent.py:871
def shutdown_memory_provider(self, messages: list | None=None) -> None:
    if getattr(self, '_memory_provider_shutdown', False):
        return
    self._memory_provider_shutdown = True
    if self._memory_manager:
        try:
            self._memory_manager.on_session_end(messages or [])
        except Exception as e:
            logger.warning('Memory provider on_session_end failed during shutdown: %s', e, exc_info=True)
        _quietly(lambda : self._memory_manager.shutdown_all())
    _notify_context_engine_session_end(self, messages)


_CLEANUP_STEPS = tuple((name, Exception) for name in (
    '_stop_cli_wake_word', '_cleanup_all_terminals', '_interrupt_async_delegations',
    '_cleanup_all_browsers', '_shutdown_mcp_servers', '_shutdown_cached_aux_clients'))
for _name, _exc in _CLEANUP_STEPS:
    globals()[_name] = lambda name=_name: event(name + ': empty fixture')
_cleanup_done = False
_cleanup_in_progress = False
_session_end_messages = []
def _reset_terminal_input_modes_on_exit():
    event('terminal reset: non-TUI no-op')
def _flush_one_shot_session_store(cli):
    event('persist transcript -> flush token counts -> end_session: empty host DB fixture')
def _notify_single_query_session_finalize(cli):
    event('on_session_finalize: empty observer/plugin fixture')
def _wait_for_oneshot_background_completions(cli):
    event('linger: skipped; quiet loop already consumed empty process registry')
class EmptyMemory:
    def flush_pending(self, timeout):
        event('memory.flush_pending timeout=' + str(timeout) + ': empty FIFO')
    def on_session_end(self, messages):
        event('memory.on_session_end: zero providers')
    def shutdown_all(self):
        event('memory.shutdown_all: empty FIFO, zero providers')
def replay(engine):
    global _active_agent_ref
    sys.modules['cli'] = sys.modules[__name__]
    agent = SimpleNamespace(context_compressor=engine, session_id='exit-repro',
        _memory_manager=EmptyMemory(), _session_messages=[])
    agent.shutdown_memory_provider = lambda messages=None: shutdown_memory_provider(agent, messages)
    cli = SimpleNamespace(agent=agent, _release_active_session=lambda: event('release active session lease: fixture'))
    _active_agent_ref = agent
    import atexit
    atexit.register(_run_cleanup)
    # turn_finalizer.py:797 per-turn plugin on_session_end; LCM-X registers none.
    event('per-turn plugin on_session_end: zero matching LCM-X hooks')
    event('quiet notify completion loop: empty registry')
    event('reply printed; sys.exit(0) unwinds finally')
    _finalize_single_query(cli)
