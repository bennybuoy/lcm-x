# Experimental one-leaf background cache

**Default off; draft integration, not an activation recommendation.** This builds
on the native worker and publication refactor in [#1142](https://github.com/electricsheephq/lcm-x/pull/1142)
(at `96a0aa7a05fcedba087bcb0f47b89f438f92aad3`), rather than adding a competing
worker or context engine. Related: [#610](https://github.com/electricsheephq/lcm-x/issues/610),
[#787](https://github.com/electricsheephq/lcm-x/issues/787),
[#1145](https://github.com/electricsheephq/lcm-x/issues/1145).

## Native, not an extension plugin

Hermes exposes completed-turn hooks and context-engine registration. LCM-X does
not expose a public external prepare/atomic-promote API. Chunk selection,
occurrence mapping, rescue, and the canonical publication callback are internal
LCM-X contracts. A separate plugin would have to patch them or replace the
context engine. This implementation instead reuses `SummaryDAG.add_node` with
`before_commit` and `LifecycleStateStore.stage_compaction_publication`.
No monkeypatching, new canonical frontier system, content deduplication, or
historical transcript cleanup is involved.

## Contract

- `LCM_BACKGROUND_LEAF_PREP_ENABLED=false` is inert: no scheduler job, pending
  schema, or promotion lookup. Existing foreground behavior remains available.
- After successfully ingesting a completed, non-interrupted ordinary turn,
  enqueue a snapshot with a copied config, profile context, session,
  conversation, database identity, effective policy and durable frontier.
- Prepare at `LCM_BACKGROUND_LEAF_PREP_PRESSURE_RATIO` (default `0.8`) times the
  effective compression threshold. This does not lower the cutover. The existing
  small-window, fresh-tail, ownership, tool-group and ignore-pattern exclusions
  remain conservative. At most **one leaf per conversation** is prepared.
- Preparation changes only private cache/migration/spend metadata. It inserts no
  canonical DAG/FTS node, advances no lifecycle frontier, rewrites no prompt and
  does no transcript GC. A ready row is not an active summary.
- At normal compression, the existing host occurrence mapper must prove the same
  lineage **and consumption**. The prepared result replaces only the summary
  call. Fresh-tail selection, canonical assembly, provenance and post-publication
  bookkeeping remain the foreground loop's responsibility.
- Under the DAG writer transaction, revalidate the complete decoded source
  fields (including structured tools, chronology and ownership), config/policy,
  profile/database binding and current lifecycle frontier. Settle the private
  ready row in that same transaction. Rejection creates no searchable orphan.
- Rescue stores only the helper's returned chunk, not its requested extent.
  A rescued chunk ending inside a host unit is not cached.
- An explicit focus request uses foreground summarization. Auto-focus drift is
  still accepted, matching the upstream design's steering-hint policy.

A matched leaf costs **zero additional summarizer calls on promotion**. A full
sweep can still call for unprepared remainder or condensation; one cached leaf
is not a promise of an entirely call-free crossing or a latency percentile.

## Resources and lifetime

`lcm_leaf_preparation` is an additive private table with one actionable row per
conversation. `background_leaf_cache_v1` is a named migration, without a numeric
schema bump. `lcm_leaf_prep_spend` is a private fixed-window admission counter.
Old readers ignore both tables. This intentionally keeps one payload in the
claim row rather than introducing the design's multi-node batch schema.

Limits: 64 queued jobs per process; 64 private rows per database; 2,000 candidate
rows and 4 MB of source field bytes checked before payload materialization;
256 KB ready payload; 180-second claim lease; 120-second summary-chain deadline;
one-hour ready TTL. Source sizing and reading share a read snapshot released
before network work. SQLite helpers use private, short-lived connections.

Claim tokens prevent a duplicate worker or expired owner from making late work
ready. Starting a clone does not steal another live claim. Ready data survives a
worker/process restart and is always revalidated. Expired/stale private rows are
reclaimed only during opt-in admission, not by a historical cleanup sweep.

Every transport attempt, including fallback/rescue attempts, is charged before
the call. `LCM_BACKGROUND_LEAF_PREP_MAX_CALLS` (default 8, admitted range 1–64) and
`LCM_BACKGROUND_LEAF_PREP_WINDOW_S` (default 600, admitted range >0–86400) bound
preparers sharing a database. An in-process guard additionally retains its
backoff. This is **not an account-wide quota across independent databases**.

The worker does not start transport during same-process foreground compression
of the same store, skips a single host auxiliary slot, and leaves foreground
spend untouched. Cancellation revokes only the scheduling engine's jobs. It
cannot cancel an already-running remote request or refund spend; the worker
keeps its own connection alive until return, and rejects late acceptance.

## Verification and open activation gates

Synthetic tests exercise the real worker, serializer/escalation, private SQLite
cache and canonical publication path, with a fake transport. They cover:

- no canonical publication or prompt churn during prepare;
- zero-call single-leaf crossing and ordinary foreground remainder;
- source/policy/binding mutation rejection;
- claim races, lease expiry, late completion, queue bounds, persistent budget;
- shutdown of an unrelated clone and a blocked network call;
- rescue consumption, new tail preservation and fresh-engine exact-once ingest;
- subprocess death after settlement, after lifecycle staging and after commit:
  DAG/FTS/frontier/cache are wholly old or wholly new, with a safe retry.

**Do not enable in production yet.** Remaining gates are deliberately separate:

- [ ] Reconcile with the latest #1142 integration train before merge; the draft
      does not import its unrelated replay/maintenance changes.
- [ ] Review/cache-fence the externally resolved auxiliary route and credentials
      under live multiplexed profile contexts (#987). The current fingerprint
      covers explicit LCM configuration, not every host-side routing input.
- [ ] Resolve process-wide status counters and the unavailable-route promotion
      opportunity from #1145. Neither affects default-off operation.
- [ ] Independently verify host replay identity, trusted summary carriers, and
      generation-fenced deferred usage updates before activation. The synthetic
      timestamped replay test here is not proof for every timestamp-free tool,
      merged carrier, detached hygiene and reload sequence. Keep those fixes in
      separate changes; see also #1402.
- [ ] Real-route disposable sidecar, exact-candidate review, cost/latency/waste
      measurements and maintainer CI approval. No live configuration, model,
      release, deployment or default-on change is part of this draft.

The broader `docs/async-background-compaction.md` remains a design for later
multi-leaf and off-turn condensation work, not a claim this draft implements it.
