# Current implementation handoff

Last reviewed: 2026-07-27

This is the shortest reliable entry point for a new coding session. It records
the decisions behind the current PoV; use `ARCHITECTURE.md` for endpoint detail
and the Graphify commands at the end for targeted code traversal.

## Product position

The PoV proves that MongoDB Atlas can be a trustworthy data and event platform
for a PIX-shaped workload. It is deliberately **not** a benchmark, sizing
exercise or production topology recommendation.

- The workload and values are synthetic; Atlas, Change Streams, Kafka,
  checkpoints, Stream Processing and the DLQ are real.
- TPS and latency describe only the current laptop-to-Atlas execution.
- One Kafka source connector and one Change Stream cursor are enough for the
  default demonstration. Extra filtered cursors/connectors remain an experiment,
  not native partitioning.
- Reliability is demonstrated by finite-run reconciliation, resumability,
  idempotency, observable backlog and explicit failure states—not by a large
  throughput number.

## Changes and rationale

### Streaming backend: evidence instead of claims

Primary files: `backend/routers/streaming.py`,
`backend/tests/test_streaming.py`, `scripts/setup-asp.js`.

| Change | Why |
|---|---|
| Every generator start creates a `run_id`; transactions also carry a sequence and stable `endToEndId`. | A finite run can be counted independently of previous demonstrations. |
| `RunTracker` records unique IDs seen by Change Streams and Kafka; reconciliation also reads the source, ASP windows, DLQ and audit collections. | “Nothing was lost” is now an accounting result, not an animation or counter comparison. |
| Change Stream workers persist resume tokens in `pix.consumer_checkpoints`. | API restarts and transient cursor failures can resume from durable state. |
| A checkpoint is discarded only when MongoDB confirms `ChangeStreamHistoryLost`; other errors retain it. | Retaining an older token may redeliver, while silently starting at “now” could lose events. |
| Duplicate deliveries are measured by `endToEndId`. | Change Streams and Kafka are at-least-once paths; idempotency is explicit. |
| Kafka health is downgraded from connector state to task state, and connector/consumer restart endpoints were added. | A connector can report `RUNNING` while its only task is `FAILED`; the task moves the data. |
| ASP status includes processor runtime stats when available; restart uses controlled stop/start and preserves the managed checkpoint. | The UI shows operational state and recovery without recreating the processor. |
| The ASP pipeline validates `run_id`, PIX type and numeric value, uses event-time tumbling windows with allowed lateness, and merges with a deterministic execution/window/UF/type `_id`. | Bad input is auditable in the DLQ, late events have a defined policy, and replay replaces rather than double-counts a window. |
| DLQ summary, injection and idempotent reprocessing preserve the business key. | Error handling becomes a demonstrable recovery path rather than a dead-end counter. |
| Oplog window, network RTT and point-read latency are measured separately. | Resume-token retention, network distance and operational reads answer different questions and must not be conflated. |
| Moderate presets and a laptop-safe generator ceiling replaced “impressive” capacity claims. | The PoV proves mechanics on a low-cost environment; cluster sizing is explicitly out of scope. |

### Streaming cost posture: the PoV must fit on M20

Primary files: `backend/routers/streaming.py`, `scripts/ambiente.sh`,
`frontend/src/pages/Streaming.jsx`.

The cluster was auto-scaling to M30 during every streaming demo. The dominant
cause was not the write load: `/streaming/reconciliacao` counted the source with
`count_documents({"run_id": ...})` against a collection that had no `run_id`
index, and the UI polled it every 2 s. That collection scan pulled the whole
live set through the WiredTiger cache in a loop.

| Change | Why |
|---|---|
| `_ensure_indexes()` creates a `run_id` index. | Turns the reconciliation count from a repeated COLLSCAN into an index scan. Largest single win. |
| Reconciliation poll 2 s → 5 s, and the loop stops once the run is final. | It kept querying Atlas after the answer could no longer change. |
| `TPS_MAX` = 1,000; `STREAMING_CONCEPT_TPS` default 500 → 200. | Sustained write rate is a real but secondary contributor; the ceiling is now a stated cost decision. |
| `STREAMING_TTL_SEGUNDOS` 1800 → 600. | The window sets the live-set size, not the delete rate. At 1800 s the live set exceeded an M20's cache on its own. |
| `scripts/ambiente.sh up` normalizes the current tier to `ATLAS_TIER_INICIAL` (default `M20`). The auto-scaling ceiling is **not** touched. | A run that scaled up yesterday must not start today on the larger tier. Scale-up stays available for genuine need; staying on M20 is the job of the four fixes above. |
| `/preflight`'s `cluster_tier` check was inverted. It used to fail on M20 and tell the operator to "run load for a few minutes to scale up before the demo"; it now passes on the entry tier and fails when the cluster has scaled **above** it. `_cluster_info_sync()` exposes `escalou` in place of `aquecido`, and the header badge turns yellow on scale-up. | Scaling up was encoded in the product as a demo prerequisite. That assumption is what made M30 feel normal; the check now states the opposite expectation. |
| `scripts/setup-kafka-connector.sh` stops each stale connector, deletes its offsets, then deletes the connector. | Deleting a connector does not delete its offsets — Connect keeps the resume token in `connect-offsets` under the connector name, and `startup.mode: latest` only applies when no offset is stored. Since `up` drops `pix.transacoes`, the stored token pointed at a vanished oplog position and the task died with `ChangeStreamHistoryLost`, leaving the connector `RUNNING` with its only task `FAILED`. |

**Measured against the live M20 cluster (2026-07-27).** A 25-minute run at 200
TPS, generator writing continuously, one Change Stream connector and the ASP
processor active. Phase A ran the fixed code; phase B reverted only the `run_id`
index and set the poll back to 2 s, reproducing the regression in place.
Per-minute figures from the Atlas Admin API, primary node:

| Metric | A — indexed, 5 s poll | B — COLLSCAN, 2 s poll |
|---|---|---|
| `QUERY_EXECUTOR_SCANNED_OBJECTS` (docs/s) | 751 | 50,176 (**67×**) |
| `QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED` | 1.68 | 89.23 (**53×**) |
| `PROCESS_CPU_USER` (avg) | 8.24% | 12.55% |

`QUERY_EXECUTOR_SCANNED` moves the *other* way (8,785 → 3,035) because it counts
index keys, and a collection scan reads none — the pair of metrics together is
what identifies the plan change.

The cluster stayed on M20 throughout, peaking at 16.4% CPU and 2.27 GB of 4 GB.
Two honest limits on this evidence: 25 minutes is far too short to trigger Atlas
compute auto-scaling (which needs sustained utilisation over roughly an hour),
so "it did not scale" proves less than the headroom does; and phase B is a
*weakened* reproduction — it reverted the index and the poll interval but kept
TTL at 600 s and 200 TPS, so its live set was ~131k documents against the
~900k the original 1800 s / 500 TPS configuration produced.

Client-side latency barely separated the phases (p50 579 ms vs 613 ms) because
it is dominated by laptop-to-Atlas round trip; only the server-side metrics
resolve the difference.

**End-to-end UI run (browser, same day).** Generator started from the page at
200 TPS and stopped from the page: source 14,420 = Change Streams 14,420 =
Kafka 14,420 = ASP 14,420, zero duplicates, DLQ 0, `final: reconciliado`.
Reconciliation poll measured at the browser: 5.0 s average interval. Only
console message is the pre-existing `favicon.ico` 404.

One behaviour to know before demoing: the ASP window uses
`boundary: "eventTime"`, so the **final window closes only when a newer event
advances the watermark**. Stopping the generator is exactly what stops that from
happening — the last ~10 s of a run sits as "pending" indefinitely until another
burst arrives. In the run above, the final window closed only after a second
burst was emitted. A consequence for the frontend: the poll-stop added here
("stop polling once the run is final") therefore fires less often than expected,
because the run the page displays is always the newest one and its last window
is still open. The 5 s interval and the indexed count carry the cost saving; the
poll-stop is a bonus, not the mechanism.

Normalization runs only after the cluster reaches `IDLE` — Atlas rejects spec
changes on a paused or transitioning cluster. A rejected PATCH warns and
continues rather than blocking the demo.

Two Atlas constraints were found the hard way, both HTTP 400, and both are now
encoded in the script:

- `minInstanceSize` must be **strictly** less than `maxInstanceSize` when
  compute auto-scaling is enabled. "Pin the cluster to M20 with auto-scaling on"
  is therefore inexpressible: the range would collapse to one tier.
- Each tier has a maximum disk size. This cluster has 150 GB, and M10 tops out
  at 128 GB, so an M10 floor is rejected. `ATLAS_MIN_TIER` defaults to empty
  (leave the floor alone) for that reason.

### Environment lifecycle: one command, clean boundaries

Primary files: `bin/overview`, `scripts/ambiente.sh`,
`scripts/cleanup-streaming-data.py`, `scripts/kafka-local.sh`,
`scripts/setup-kafka-connector.sh`, `scripts/setup-asp.js`.

`./bin/overview` now treats the whole demo as one lifecycle:

1. Resume Atlas and wait until it is ready.
2. Stop the previous processor, remove scoped PIX residue and recreate a clean
   ASP processor definition.
3. Start local Kafka/Connect and register the single default source connector.
4. Start backend and frontend only after the environment is consistent.

`./bin/overview down` performs a two-layer shutdown:

1. The API stops the generator and ASP, waits for `STOPPED`, removes the source,
   windows, DLQ, audit and application checkpoints.
2. The environment script repeats a direct scoped cleanup, removes the demo
   connector/topic/consumer group, stops Kafka, and pauses Atlas.

The second layer handles an unavailable or interrupted API. Cleanup is limited
to the known `pix` demo collections and Kafka resources; it must never become a
database-wide delete. The 30-minute TTL on transaction timestamps is only a
safety net for an abandoned run.

### Frontend: proof-first narrative

Primary files: `frontend/src/pages/Streaming.jsx`,
`Aggregations.jsx`, `Reindexacao.jsx`, `SchemaValidation.jsx`,
`frontend/src/hooks/useApi.js`, `frontend/src/App.jsx`,
`frontend/src/index.css`.

| Area | Current decision and reason |
|---|---|
| Streaming | The scenario, observed environment, compact generator and start of all three capability columns fit in the first 1440×900 viewport. The detailed comparison table is collapsed. This keeps the reliability proof above supporting reference material. |
| Aggregations | Tabs describe outcomes, while the operator is secondary. A permanent `Source → Pipeline → Result` flow and honest pre-execution state make the business narrative visible before a query runs. |
| Reindexing | Cards show a one-line command and collapse the commented version. The explain panel highlights `COLLSCAN → IXSCAN`; the index list defaults to demo-relevant indexes. This emphasizes measured plan change over code volume. |
| Schema Validation | The guided write/reject sequence remains primary; the full JSON Schema is collapsed under details. |
| API errors | Expected request aborts caused by module unmount are ignored, and identical global errors are deduplicated for eight seconds. Navigation no longer produces false failure toasts; genuine timeouts and API failures remain visible. |
| Other modules | Hot/Cold, Change Streams and Transactions retained their structure because their one-screen narratives were already strong. |

The visual direction remains the existing MongoDB dark system: Outfit,
JetBrains Mono, `#001E2B` and `#00ED64`. The review intentionally improved
hierarchy and progressive disclosure instead of introducing a second design
language.

## Operational truth and limitations

- Reconciliation turns green only after the generator is stopped and the final
  ASP window has closed. While data is flowing, “pending” is observable backlog,
  not evidence of loss.
- Change Stream and Kafka unique counters belong to the current backend process.
  Source, ASP and DLQ counts are read from Atlas.
- Resume works only while the saved token remains inside the oplog window.
- Local Kafka is single-node and intentionally has no TLS/SASL, ACLs or Schema
  Registry. These are production concerns, not hidden claims.
- A single connector per collection is the default. Filtered multi-connector
  fan-out is educational and can add oplog pressure.
- The ASP processor must be stopped after the PoV because it bills while idle.
  Atlas storage can remain billable while the cluster is paused.
- Never point destructive demo endpoints or cleanup scripts at a non-disposable
  database.

## Validation baseline

Validated after the current implementation:

```bash
backend/venv/bin/python -m pytest -q backend/tests  # 91 passed
npm --prefix frontend run build                    # Vite build passed
git diff --check                                   # passed
```

Browser validation used a 1440×900 viewport. Streaming exposed all three
capability columns above the fold, the revised Aggregations/Reindexing screens
rendered correctly, Schema code was collapsed by default, and rapid module
navigation produced zero API-error toasts and zero console errors.

## Fast reading order for a new session

1. Read this file.
2. Read `CLAUDE.md` for commands, environment variables and repository rules.
3. Read only the relevant section of `ARCHITECTURE.md`.
4. Query Graphify before opening a large implementation file.
5. For Streaming implementation work, inspect
   `backend/routers/streaming.py`, its matching tests and the relevant frontend
   column together.

Useful targeted graph queries:

```bash
graphify query "How does one PIX run reconcile source, Change Streams, Kafka, ASP and DLQ?" --budget 1600
graphify query "How do overview up and down create and clean the streaming environment?" --budget 1400
graphify query "How are Change Stream resume tokens persisted and invalidated?" --budget 1200
graphify query "How does the ASP pipeline validate, window and merge PIX events?" --budget 1400
graphify affected "useApi()" --depth 2
graphify affected "reset()" --depth 2
```

