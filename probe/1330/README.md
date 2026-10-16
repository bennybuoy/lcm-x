# Synthetic #1330 exit probe

R1 engine-level workload plus the successful one-shot exit order inspected at
Hermes commit `d923a5eb487a17c0a68801bba9ef0f56c81040a8`.
`host_replay.py` snapshots selected lifecycle bodies (AST-unparsed, docstrings
removed), with explicit empty fixtures for memory providers, observers/plugins,
host transcript DB, process registry, terminals, browsers, MCP, and HTTP clients.
The real LCM-X `on_session_end` runs in the original memory-shutdown order.
The source host does not call the engine's `shutdown` here. The source host's
30-second exit watchdog is preserved; any watchdog exit is visible in stderr
and is not a clean-exit proof. Normal Python/threading/third-party finalization
runs. This is not a complete Hermes CLI execution or a fleet reproduction.

Four embedding worker slots and two ONNX threads are unchanged source defaults.
Each run seeds 32 synthetic summaries and messages, starts actual cached ONNX
backfill, schedules post-leaf work and exits without draining it. Keys-off turns
off embeddings and preanswer evidence together. No API requests or real data.
The driver captures delayed native/Python stacks at turn end +10 seconds;
SIGABRT cores get gdb all-thread traces even when exit precedes that point.
