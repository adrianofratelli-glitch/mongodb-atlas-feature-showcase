# Current implementation handoff

Last reviewed: 2026-07-26

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
backend/venv/bin/python -m pytest -q backend/tests  # 71 passed
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

