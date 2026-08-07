# Current implementation handoff

Last reviewed: 2026-08-07

## Region move: done and measured (2026-08-07)

The cluster and the Stream Processing workspace both run in **sa-east-1 (São
Paulo)**. Measured before and after, same laptop, same harness:

| Measure | us-east-1 | **sa-east-1** | Gain |
|---|---:|---:|---:|
| Pure RTT (`ping`, no write) | 148.10 ms | **7.39 ms** | 20× |
| One PIX, client round-trip (p50) | 141.70 ms | **11.54 ms** | 12× |
| One PIX, inside mongod (`opLatencies`) | 3.06 ms | 4.19 ms | — |
| Commit → change stream event | 0.10 ms | 0.09 ms | — |
| Individual-insert ceiling | 260 TPS | **2,376 TPS** | 9× |

Three conclusions that should shape how the PoV is presented:

1. **A PIX round trip is now 11.5 ms end-to-end**, well under the 100 ms the
   customer conversation needs. The two numbers that did *not* change are the
   ones that were never about distance: time inside mongod, and commit → CDC
   propagation. Atlas was never the latency; geography was.
2. **`1 insert = 1 PIX` is now viable.** 2,376 TPS with 50 threads clears the
   1,000 TPS Inter mark with headroom, so the ~800-document micro-batch is no
   longer required to reach demo volume. The batch existed only to amortise a
   148 ms round trip.
3. **50 threads is the sweet spot, and more is worse**: 50 → 2,376 TPS at p95
   33.9 ms, while 600 → 2,041 TPS at p95 2,038 ms. Past ~50 the bottleneck is
   CPython (GIL plus BSON encoding), not Atlas. Do not "tune" this by raising
   thread count.

**Beware the egress path when reading any latency number.** These numbers only
appeared after disabling a Cloudflare WARP-style proxy on the presenting laptop.
With it on, traffic egressed from **New York**, making São Paulo *farther* than
Virginia (s3 connect: sa-east-1 308 ms vs us-east-1 226 ms) and the post-move RTT
read 254 ms — worse than before the move. Confirm egress with
`curl -s https://ipinfo.io/json` before trusting or debugging a latency figure.
The customer sits in Brazil on a direct route; the demo laptop must too, or it
cannot show the latency the customer would actually get.

Reproduce with the four measures: pure RTT, per-PIX client round-trip, per-PIX
`serverStatus().opLatencies.writes`, and the individual-insert ceiling at
50/150/300/600 threads. The harness lived in the session scratchpad and is not
committed.

### Individual mode is now the default (2026-08-07)

`STREAMING_MODO_ESCRITA=individual` makes the generator write **one `insert_one`
per PIX**. This is what a bank's flow actually looks like, and it is what makes
`opLatencies` measure a transaction instead of an 800-document micro-batch — the
objection that motivated the change.

The write path uses **`AsyncMongoClient`**. With `asyncio.to_thread(insert_one)`
each PIX held a pool thread, and since the same process also runs four change
stream cursors, the Kafka consumer and the UI polls, the GIL capped throughput at
~1,000 TPS. Async doubled it with identical semantics.

Measured in sa-east-1 with all three consumers active (target → measured):

| Target | Measured | Atlas p50 | ACK p50 | CS p50 | Kafka p50 |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,018 | 3.07 ms | 14.6 ms | 20.6 ms | 28.2 ms |
| **2,000** | **2,037** | **3.07 ms** | **17.6 ms** | **21.9 ms** | **32.1 ms** |
| 2,500 | 2,277 | 8.19 ms | 21.2 ms | 23.7 ms | 99.8 ms |
| 3,000 | 2,544 | 8.19 ms | 23.4 ms | 32.8 ms | 262.9 ms |
| 4,000 | 3,015 | 8.19 ms | 25.7 ms | 40.4 ms | 1,003 ms |

Every row reconciled across all three paths with DLQ 0 and zero pending.

**2,000 TPS is the default** — the last target that is actually delivered and
keeps the whole post-commit path under 35 ms. The three stage presets are 363
(Inter average), 1,000 (Inter peak, the customer's mark) and 2,000 (2× the mark).

**What saturates first is the local Kafka consumer, not Atlas.** During a
1,000 TPS run the server ingested each PIX in 3.07 ms using 157 of 3,000
connections, with WiredTiger cache at 67%. Raising the target past 2,000 only
degrades the laptop's consumer — do not read it as an Atlas limit, and do not
"fix" it by raising thread counts.

`modo: "lote"` remains available on `/streaming/generator/start` for the 8,000
TPS volume story; the batch exists solely to amortise client-side cost.

**ASP workspace region cannot be changed in place.** `PATCH /streams/{name}` and
`atlas streams instances update --region` both return
`400 INVALID_JSON_ATTRIBUTE`. The workspace must be deleted and recreated in the
new region, which also destroys its connections and processors.

**The São Paulo region identifier is `SAOPAULO_BRA`** — no underscore between
SAO and PAULO. `SA_EAST_1` (what the docs table lists), `SAO_PAULO_BRA` and
`sa-east-1` are all rejected with `400 INVALID_JSON_ATTRIBUTE`, which reads like
"region unsupported" and is really just a wrong identifier. Verify a new value by
POSTing a throwaway workspace before concluding a region is unavailable.

Rebuild recipe, done on 2026-08-06 and verified (`activeRegion: sa-east-1`):

```bash
# 1. workspace (tier goes in streamConfig; the CLI has no --tier flag)
POST /api/atlas/v2/groups/{proj}/streams
  {"name":"spi-inter-pix",
   "dataProcessRegion":{"cloudProvider":"AWS","region":"SAOPAULO_BRA"},
   "streamConfig":{"tier":"SP10"}}

# 2. connection
POST /api/atlas/v2/groups/{proj}/streams/spi-inter-pix/connections
  {"name":"atlasCluster","type":"Cluster","clusterName":"inter",
   "dbRoleToExecute":{"role":"readWriteAnyDatabase","type":"BUILT_IN"}}

# 3. processor — replace only the host in ASP_CONNECTION_STRING first
ASP_RECREATE=true ASP_TIER=SP10 mongosh "$ASP_CONNECTION_STRING" \
  --file scripts/setup-asp.js
```

**The new workspace gets a different hostname, so `ASP_CONNECTION_STRING` in
`backend/.env` must be updated** — leaving the old `virginia-usa` host there is
the failure mode that makes column 3 silently read a cluster across the
continent. A backup of the pre-change file is at `backend/.env.bak-regiao`.

Keep cluster and ASP workspace in the **same region**. With them split, the ASP
column measures a transcontinental hop and looks like a product weakness when it
is topology.

This is the shortest reliable entry point when picking the project back up. It
records the decisions behind the current PoV; use `ARCHITECTURE.md` for endpoint
detail.

## Product position

The PoV proves that MongoDB Atlas can be a trustworthy data and event platform
for a PIX-shaped workload. It is deliberately **not** a benchmark, sizing
exercise or production topology recommendation.

- The workload and values are synthetic; Atlas, Change Streams, Kafka,
  checkpoints, Stream Processing and the DLQ are real.
- TPS and latency describe only the current laptop-to-Atlas execution.
- One Kafka source connector and four filtered Change Stream cursors are used in
  the default demonstration. The cursors expose consumer parallelism in this
  PoV; they are not native Kafka partitions or a production sizing prescription.
- Reliability is demonstrated by finite-run reconciliation, resumability,
  idempotency, observable backlog and explicit failure states—not by a large
  throughput number.

## Changes and rationale

### Module 08 reframed: card-present risk, not "geo" (2026-08-07)

The tab was a showcase of geospatial features; it is now a **risk** tab. Renamed
"Risco geográfico" in the nav, and the three demos were reordered by what a bank
actually asks:

| Before | After |
|---|---|
| Demo A: index plan comparison (first) | moved into a `<details>` — it answers "is the index right?", not "what problem does this solve?" |
| Demo B: impossible travel | **01 · Sinal de risco** — now the opening demo |
| Demo C: geo + Atlas Search | **02 · Contexto para investigação**, anchored on dispute/alert triage |

**The dataset now models card-present purchases (`VERSAO_DATASET = 4`).** The old
seed was internally inconsistent: it had a physical `estabelecimento` but sourced
the coordinate from `APP_MOBILE` / `GPS_APP_SIMULADO`. An attentive analyst asks
why a purchase at a bakery is located by the customer's phone.

Why this matters for the argument, not just for tidiness:

- **PIX carries no coordinate.** The BACEN arrangement has no geolocation, so any
  "geo of PIX" framing invites a correction from the room and costs credibility.
  PIX is also the *weakest* geo case — it is online, with no terminal.
- **Card-present fixes exactly that.** The coordinate is the acquirer's terminal:
  fixed and registered independently from handset telemetry. It is harder for
  the customer to manipulate, but the acquirer registry can still be stale or
  incorrect. Impossible
  travel over card-present transactions is the canonical industry case.
- **It gives the narrative continuity without lying.** Module 07 is PIX
  (transfer, online); module 08 is card (purchase, present). Two transactional
  fronts of a digital bank, one cluster. Relevant because the customer here is a
  digital bank with **no branches** — so branch-network geo cases do not apply.

Document changes: `dispositivo.canal` → `POS_PRESENCIAL`, `localizacaoMeta.origem`
→ `TERMINAL_ADQUIRENTE`, `qualidade` → `CADASTRAL`, `tipo` → `CARTAO_DEBITO`/
`CARTAO_CREDITO`. Establishments and terminals are stable catalog entities: the
same terminal keeps the same registered coordinate across purchases. Re-seed with
`python scripts/seed_geo.py --drop`.

The GPS caveat on screen was rewritten accordingly: terminal capture is far more
trustworthy than handset GPS, but the signal still does not decide alone —
additional cards, authorised third-party use and capture delay all produce false
positives.

### Streaming live for the PIX team (2026-08-05; defaults updated 2026-08-07)

- Module 07 defaults to a real live session again; the recorded run remains a
  visibly labelled contingency selected with `overview --replay`.
- The UI opens its three SSE observers and Atlas-facing polls only after the
  operator starts a live session, and closes them after reconciliation. This
  preserves the relative-CPU lesson that originally motivated replay-only mode.
- `Parar e reconciliar` waits one window plus allowed lateness, then inserts a
  technical event under `__demo_watermark__`. It advances event time without
  contaminating the demonstrated `run_id`, so the final ASP window can close.
- The business comparison is now explicit and sourced from BCB: 313,339,828 PIX
  on the 2025-12-05 record day is 3,627 TPS average, or ~363 TPS under the
  customer's 10% share premise. The stage-impact target is 1,000 TPS, equal to
  10% of the BCB's planned 10k sustained peak. These are comparison marks, not
  a production capacity or sizing claim.
- `overview` now starts ASP and Kafka by default and `overview --replay` is the
  no-write path. `overview down` is mandatory because ASP bills per second.
- The original live stage default was 8,000 TPS in batch mode. After the region
  move and async-driver work, the customer-facing default became **2,000 TPS in
  individual mode**: one acknowledged `insert_one` per PIX. Batch mode keeps
  8,000 TPS for the separate volume story. Atlas write ACK is the persistence
  metric; Change Streams, Kafka and ASP are post-commit paths.
- Every live Play now performs a scoped PIX reset before opening the observers,
  then starts a fresh `run_id`. In-flight status/reconciliation responses are
  guarded by that `run_id`, so a completed prior run cannot close the new SSE
  session or overwrite its reconciliation. The UI also names the exact Data
  Explorer namespace (`pix.transacoes`) and detects an old backend contract.
- M20 and M30 are both healthy states inside the configured M20→M30 compute
  auto-scaling range. The header still exposes the real tier, but preflight no
  longer fails merely because Atlas legitimately moved to M30.

### PIX presentation hardening (2026-08-04)

- Module 05 now delivers its UI feed over SSE and no longer implies that its
  introductory animation proves durable resume. Module 07 remains the evidence
  for persisted tokens, re-delivery, idempotency and oplog-bounded recovery.
- The module 07 recording badge is permanent again. Spoken disclosure is not a
  substitute for visible provenance of recorded measurements.
- Kafka now makes its current document key/JSON contract explicit and separates
  observed offsets from production decisions about ordering, partition key,
  schema compatibility, HA and security.
- The ASP snippet matches the deployed 5 s / 2 s event-time policy and exposes
  watermark idleness, late-event DLQ behavior and the single terminal-sink
  constraint. Stopping input does not force an event-time window closed.
- **Superseded by the card-present model above:** Geo originally labelled
  coordinates as synthetic app telemetry. Dataset v4 now uses registered
  acquirer-terminal coordinates and stable terminal/merchant identities.
  Impossible travel remains a retrospective risk signal, not a fraud verdict.

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
| **Individual Play** = 2,000 TPS for 30 s; batch mode = 8,000 TPS; `TPS_MAX` = 15,000; 4 Change Stream partitions; ASP stage tier SP10. | Individual mode is the customer-facing default after the region move. The 8,000 TPS batch run remains a measured volume story, not production sizing. |
| `STREAMING_TTL_SEGUNDOS` 1800 → 300. | Reset is the primary cleanup. A 60 s TTL would begin deleting near the ingest rate during repeated runs, add oplog pressure and risk racing reconciliation. |
| Cluster normalization was removed from `scripts/ambiente.sh`; tier/state are operator-owned. | Demo startup must not pause, resume or resize the user's Atlas cluster. |
| `/preflight`'s `cluster_tier` check was inverted. It used to fail on M20 and tell the operator to "run load for a few minutes to scale up before the demo"; it now passes on the entry tier and fails when the cluster has scaled **above** it. `_cluster_info_sync()` exposes `escalou` in place of `aquecido`, and the header badge turns yellow on scale-up. | Scaling up was encoded in the product as a demo prerequisite. That assumption is what made M30 feel normal; the check now states the opposite expectation. |
| `scripts/kafka-local.sh down` reads the connector list into a variable and parses it defensively, and now also stops each connector and deletes its offsets before removing it. | The teardown printed a raw `json.load` traceback (`Extra data: line 1 column 7`) when Connect returned something other than the expected JSON array while shutting down. In a cleanup path that noise is indistinguishable from a genuine failure. It now warns in one line and prints the first 200 bytes of the body, which is what a future diagnosis needs. Resetting offsets on the way down complements the same fix on the way up. |
| `scripts/setup-kafka-connector.sh` stops each stale connector, deletes its offsets, then deletes the connector. | Deleting a connector does not delete its offsets — Connect keeps the resume token in `connect-offsets` under the connector name, and `startup.mode: latest` only applies when no offset is stored. Since `up` drops `pix.transacoes`, the stored token pointed at a vanished oplog position and the task died with `ChangeStreamHistoryLost`, leaving the connector `RUNNING` with its only task `FAILED`. |

**Live stage calibration, M20 + SP10 (2026-08-06).** Each row is a finite
20-second run, reconciled across source, Change Streams, Kafka and ASP with zero
duplicates, DLQ 0 and zero pending on all three paths. These are PoV
observations, not production sizing:

| Target | Documents | ACK p99 | Change Stream p99 | Kafka p99 | Reconciled in | Result |
|---:|---:|---:|---:|---:|---:|---|
| 4,000 TPS | 80,400 | 0.61 s | 0.51 s | 0.48 s | 4.1 s | Comfortable. |
| 6,000 TPS | 120,600 | 0.95 s | 0.60 s | 0.59 s | 4.1 s | Comfortable. |
| 8,000 TPS | 160,800 | 0.67 s | 0.76 s | 3.61 s | 4.2 s | Comfortable; stage default. |
| 10,000 TPS | 201,000 | 0.99 s | 2.67 s | 12.5 s | 4.3 s | Inflection: post-commit latency climbs sharply. |
| 12,000 TPS | 241,200 | 0.83 s | 11.6 s | 16.0 s | 12.0 s | Reconciled, but the observer ran a visible backlog; stress boundary. |

**The ASP tier is SP10, not SP30.** The earlier version of this table claimed
SP30. `sp.pixJanelas5s.stats()` reports `tier` and `effectiveTier` both SP10,
and the 2026-08-05 row for 4,000 TPS records the same 80,400 documents measured
here — that calibration was almost certainly already running on SP10 and only
the label was wrong. Do not re-provision SP30 on the strength of the old note.

Why SP10 is enough for this pipeline, measured rather than assumed:

- **Window state is trivial.** The `$group` key is `(run_id, uf, tipo)` over 10
  UFs, so a window holds tens of keys. `stateSize` is 0 and memory sat between
  186 and 221 MB of the tier's 2 GB — about 11% of the 80% ceiling that causes
  OOM. Large-window-state is the usual reason to demand SP30 and it does not
  apply here.
- **Bandwidth is not close.** 605 bytes/event measured (1.42 GB over 2.35 M
  events). At 8,000 TPS that is ~39 Mbps against the tier's 200 Mbps.
- **Parallelism is 0.** Every stage runs at the default 1, which is included in
  the tier. Nothing in the pipeline needs SP30's higher parallelism ceiling.

The bottleneck at 10,000+ is not the processor. ASP p99 stays flat (9.4 s → 11.5
s, and it measures window close, not per-event lag) while the **local Kafka
observer** degrades first — 3.6 s at 8,000 and 12.5 s at 10,000. Above 8,000 the
number that moves is the consumer's, not Atlas's.

The cluster stayed **M20 through the entire ramp**, including 12,000 TPS; it did
not auto-scale to M30. That supports a short demo claim only; it does not
establish sustained M20 capacity. The processor bills per second and must be
stopped after the run.

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
**Do not read that 16.4% as headroom** — see "Auto-scaling fires on RELATIVE
CPU" below, which corrects it. The threshold is relative to a burstable
instance's baseline, and ~17% absolute is ~88% relative. The cluster scaled to
M30 later the same day.

Two further limits on this evidence: 25 minutes is far too short to trigger
Atlas compute auto-scaling, which averages over roughly an hour, so "it did not
scale" proves little on its own; and phase B is a
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
`boundary: "eventTime"`, so the final window closes only when a newer event
advances the watermark. The stop path now waits for the 5 s window + 2 s
lateness and writes a technical marker under the reserved
`__demo_watermark__` run id. The marker is excluded from the demonstrated
run's accounting, closes its last window, and lets reconciliation become final
without requiring a second business burst.

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

### Auto-scaling fires on RELATIVE CPU — and replay mode

The fixes above were not enough. The cluster scaled to M30 again the same day,
at 15:36Z, **with the generator stopped** since 14:45Z. The Atlas event payload
is unambiguous:

```
computeAutoScalingTriggers: "CPU_ABOVE"
threshold: NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75  (mode: AVERAGE)
absoluteCpuMetric: 0.176   cpuThresholdType: "RELATIVE"   relativeCpuMetric: 0.881
```

M20/M30 are burstable instances. The threshold applies to CPU **relative** to
the instance's baseline entitlement, not absolute CPU. 17.6% absolute registered
as 88% relative. An earlier scale event (2026-07-26, before the fixes) shows
27.0% absolute at 100% relative — so the fixes did cut CPU by about a third, but
not below the line.

This corrects an earlier conclusion recorded here: "16.4% CPU is far below the
75% threshold" compared the right metric against the wrong ruler.

The consequence that matters operationally: with the generator stopped, what
sustained that CPU was **the dashboard itself** — three change-stream cursors
plus polling (generator status 1 s, oplog/read-probe/DLQ 4 s, reconciliation and
ASP 5 s, Kafka 4 s). `/streaming/oplog` alone does a `$natural` sort over
`local.oplog.rs` every 4 s. Leaving the page open costs cluster.

**Historical decision, superseded on 2026-08-05:** replay was temporarily the
only mode because an always-observing dashboard stressed the cluster even after
the generator stopped. The current live mode addresses that root cause by
opening observers only for an explicit session and closing them after
reconciliation; replay remains the zero-write contingency.

The word "replay" was dropped from the UI, but the disclosure was not. The
badge is permanent and unconditional, the Change Streams column reads
"reproduzindo" rather than "ao vivo", and environment-acting buttons stay
disabled. Renaming the control is cosmetic; removing the badge would not be.

The honesty constraint is part of the design, not decoration. The PoV's whole
claim is "evidence instead of claims", so a mode that *looks* live while nothing
happens would invert it — and what would become fake is exactly what the module
sells (change streams, Kafka fan-out, ASP windows). Therefore: the recorder
synthesises nothing, every replayed payload carries `replay: true`,
`/replay/manifest` states the origin and the recorded `run_id`, the page shows a
permanent badge, actions that touch the real environment are disabled, and a
test asserts `replay.py` never imports or calls into Mongo. A replay must never
be presented as a live run.

**Capture ordering:** the Change Stream and Kafka consumers start lazily on the
first SSE subscription. The observer now uses `auto.offset.reset=earliest`; the
source connector retains `startup.mode=latest` when no connector offset exists.
Open the live capture page first and wait for the Kafka column to report
`consumindo` before starting the generator. `scripts/capture_replay.py` encodes
that ordering so consumer startup is not measured as application backlog.

**Historical replay-only period:** `scripts/ambiente.sh` and `bin/overview`
temporarily did not provision ASP or Kafka unless `STREAMING_AO_VIVO=1` /
`overview --ao-vivo` was used. That decision is superseded: `overview` now
defaults to the live rig and `overview --replay` is the explicit no-write path.
The `down` path still stops both unconditionally, since a processor left running
bills per second.

**Two bugs found while wiring this up, both worth knowing:**

- The replay SSE shipped without a keepalive (the live one has always had one).
  An idle stream — replay paused, or simply between events — is dropped by the
  browser and the Vite proxy; `useSse` reconnects every 2 s, and the abandoned
  server-side generator never notices, because a generator that never writes
  never sees the disconnect. Those leaked streams exhaust the browser's ~6
  connections-per-host budget, and ordinary fetches queue until the 30 s
  timeout while the backend answers in ~2 ms. Fixed with a 10 s keepalive; a
  test covers it.
- `porta_ativa()` in `bin/overview` and `scripts/kafka-local.sh` used
  `lsof -ti:PORT` with no state filter, so ESTABLISHED connections (the Vite
  proxy, the browser) counted as "service is up". With the backend dead, `up`
  concluded "já estava de pé" and printed **✅ Pronto with no backend**; `down`
  used the same list to pick PIDs to kill, so it could kill a client instead of
  the server. Both now filter `-sTCP:LISTEN`.

**Idle polling is now gone.** `frontend/src/hooks/usePolling.js` adds
`useVisivel()` and `useIntervaloVisivel()`; every interval on the Streaming page
and the shell's cluster poll go through it, and `useSse` closes its
EventSource when the tab is hidden. Two rules: nothing polls while the tab is
hidden, and nothing polls for data that cannot change — the recorded snapshots
only move while the playback clock runs.

Measured in the browser, replay stopped: **48 requests per 20 s → 1**. Tab
hidden: **0 requests in 15 s**, resuming immediately on return. While playing it
is back to the normal cadence, which is the point.

Worth stating plainly, because an earlier note here implied otherwise: this is
*not* what stopped the auto-scaling. The Atlas-facing load disappeared when
module 07 became a replay (its polls now read a file, ~2 ms, no Mongo) and when
ASP and Kafka stopped being provisioned, which removed three change-stream
cursors. The only recurring remote call left is `/streaming/cluster`, and that
is the Admin API — control plane, not cluster CPU. Killing the idle polling buys
laptop and backend CPU, and it removes the held connections that caused the
30 s fetch timeouts; it does not change the tier. Pausing polls on
`document.visibilityState !== 'visible'` and when the generator is stopped, plus
revisiting the `/streaming/oplog` probe, is the remaining work for live mode.

### Environment lifecycle: one command, clean boundaries

Primary files: `bin/overview`, `scripts/ambiente.sh`,
`scripts/cleanup-streaming-data.py`, `scripts/kafka-local.sh`,
`scripts/setup-kafka-connector.sh`, `scripts/setup-asp.js`.

`./bin/overview` now treats the whole demo as one lifecycle:

1. Run `scripts/prepare-demo.sh` ahead of time to materialize the dedicated Geo
   dataset, MongoDB indexes and a queryable Atlas Search index.
2. At startup, perform only a fast read-only readiness check; never alter cluster
   state, tier or auto-scaling.
3. Stop any processor left running and remove scoped PIX residue.
4. In the default live mode, recreate the ASP definition/checkpoint and start
   Kafka/Connect with a clean connector. `overview --replay` keeps both off.
5. Start backend and frontend only after the selected environment is consistent.

If readiness fails, `overview` aborts and directs the operator to
`scripts/prepare-demo.sh`; it never begins a slow rebuild during the demo.

`./bin/overview down` performs a two-layer shutdown:

1. The API stops the generator and ASP, waits for `STOPPED`, removes the source,
   windows, DLQ, audit and application checkpoints.
2. The environment script repeats a direct scoped cleanup, removes the demo
   connector/topic/consumer group and stops Kafka. Atlas is not altered.

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

- Reconciliation turns green only after every path accounts for the finite run.
  Stopping input does not close an event-time window by itself; the watermark
  must advance. “Pending” is observable backlog or open-window state, not
  evidence of loss.
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

### Streaming presentation hardening (2026-08-07)

- The ambiguous yellow `Verificar` state was traced to
  `cleanup-streaming-data.py`: it recreated the unique and TTL indexes but not
  `run_id_reconciliacao`. The cleanup now materializes all three contracts, so
  the normal state is the green `Pronto`; a real failure reads `Pré-voo
  pendente` instead of asking the audience to "verify" something.
- `/streaming/reset` no longer creates one new SRV `MongoClient` per collection.
  It reuses the already connected application topology and purges independent
  collections in parallel. A DNS outage can therefore no longer leave Play in
  preparation for 20 seconds while the healthy existing connection is ignored.
- Kafka Connect is restarted only after a collection drop, the operation that
  actually invalidates its source cursor. A routine clean run no longer
  perturbs a healthy connector.
- The controlled-drop threshold is now 25k documents (configurable with
  `STREAMING_DROP_ACIMA_DE`). Deleting the 59,896-document acceptance run took
  9.92 s even after the DNS fix; above the threshold, reset stops ASP, drops the
  dedicated source, recreates its indexes and resumes ASP/Kafka instead.
  Live measurement after the change: **6.43 s** to prepare after a 59k-document
  run, versus **9.92 s** with `delete_many`; an initially clean run remains
  **1.33 s**.
- The presenter-only architecture decision panel was removed from the customer
  UI. Its react/distribute/transform talk track and trade-offs now live in the
  editable `docs/roteiro-apresentacao-streaming.md`; regenerate the PDF with
  `scripts/generate-streaming-guide.py`.
- Live acceptance after the fix: preparation **1.33 s**, 30-second run,
  **59,896** source documents reconciled across Atlas, Change Streams, Kafka
  and ASP + DLQ, zero lost, HTTP/console errors zero, final state in **41.09 s**.

Validated after the current implementation:

```bash
backend/venv/bin/python -m pytest -q backend/tests  # 127 passed
npm --prefix frontend run build                    # Vite build passed
git diff --check                                   # passed
```

Browser validation used a 1440×900 viewport. Streaming exposed all three
capability columns above the fold, the revised Aggregations/Reindexing screens
rendered correctly, Schema code was collapsed by default, and rapid module
navigation produced zero API-error toasts and zero console errors.

## 2026-08-07 — modules 07 and 08 joined; failure injection; density pass

Driven by a critical read of the PoV from the seat of a PIX-squad architect at a
bank that already runs Kafka and Elastic. Four objections, four changes.

1. **"Fan-out sem ETL argues for Kafka, not MongoDB."** The real, unarguable
   gain is removing the application's dual-write/outbox. Reconciliation proves
   it; the copy now leads with it.
2. **"It is all the happy path."** `POST /streaming/falha/connector` stops the
   connector mid-flow and resumes it from the stored offset;
   `POST /streaming/falha/evento-invalido` writes a document with a string
   `valor` that the ASP diverts to the DLQ while the processor keeps running.
   Measured with both injected: source 742, Change Streams 742, Kafka 742,
   ASP 742, duplicates 0, DLQ 1, final `reconciliado`.
3. **"Module 08 is not my problem, and it is retrospective."** The stream now
   carries two channels (`PIX` without coordinate, `CARTAO_PRESENCIAL` with the
   terminal's), and a second processor, `geoSinais30s`, computes the risk signal
   inside a 30 s hopping window into `geo.sinais_ao_vivo`. Module 08 opens with
   that panel; the on-demand panels stay, labelled as retrospective
   investigation.
4. **"Your only findings are the ones you planted."** Signals carry
   `origem: plantado | emergente` and the page counts them apart. A 1,000 TPS
   run produced 5 planted and 4 emergent.

Two traps found while building, both worth remembering:

- Back-dating `ts` to model acquirer capture delay put the TTL field in the
  past, so the older half of a pair expired before reconciliation ran and the
  source counted 610 against 652 in all three consumers. Arrival is `ts`;
  the purchase instant is `compradaEm`. Never conflate them.
- A km/h threshold alone is a false-positive factory: two purchases 20 km apart
  captured seconds apart read as 1,343 km/h. The signal needs a minimum
  distance (200 km) and a minimum interval (1 min) as well.

Also in this pass: the Kafka connector no longer resolves DNS on task start
(`scripts/lib/expand_srv.py` rewrites the SRV URI to its standard form at setup
— the `Failed looking up TXT record` failure that killed the task under load was
a flaky resolver, not throughput), the 12,000 TPS preset is labelled
`Volume em lote` with its bottleneck stated, delivery semantics
(at-least-once + unique `endToEndId` + per-partition ordering) are on screen
instead of in a footnote, and module 07's narrative prose moved into `<details>`
so the three columns and reconciliation own the fold.

Network caveat that bit again mid-session: RTT to the cluster measured
**243.6 ms** and throughput collapsed to ~68 TPS with `write_ack` p50 at 324 ms.
That is the WARP/US-egress route, not the code. Check `/streaming/rede` before
trusting any latency number.

## Fast reading order

1. Read this file.
2. Read only the relevant section of `ARCHITECTURE.md`.
3. For Streaming work, inspect `backend/routers/streaming.py`, its matching
   tests and the relevant frontend column together.
