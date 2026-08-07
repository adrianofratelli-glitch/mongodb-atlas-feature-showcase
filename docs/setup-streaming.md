# Streaming module — setup

Everything the Streaming module (`/#streaming`) needs before a live run: the
local Kafka broker, the MongoDB source connector and the two Atlas Stream
Processing jobs. The recorded fallback (`overview --replay`) needs none of it.

Back to the [README](../README.md).

## 1. Start Kafka locally

Two ways to do it, pick one.

*Native (no Docker required, recommended on a laptop):*

```bash
brew install kafka          # once
./scripts/kafka-local.sh up # broker (KRaft) + Kafka Connect + MongoDB plugin
./scripts/kafka-local.sh status
./scripts/kafka-local.sh down
```

*Docker:*

```bash
docker compose -f docker-compose.streaming.yml up -d
```

Either way the `mongodb-kafka-connect` plugin is downloaded on the first run
only and cached locally, so later runs work offline. The native path uses
`localhost:9092` and the Docker path uses `localhost:19092`, so set
`KAFKA_BROKERS` in `backend/.env` accordingly.

## 2. Register the source connectors

Reads `MONGO_URI` from `backend/.env`:

```bash
./scripts/setup-kafka-connector.sh      # one connector (default)
./scripts/setup-kafka-connector.sh 2    # optional experiment with disjoint filters
```

It PUTs a `MongoSourceConnector` config on the Connect REST API
(`http://localhost:8083`) with `database=pix`, `collection=transacoes`,
`publish.full.document.only=true`, `startup.mode=latest`, heartbeats and
`topic.prefix=atlas`, producing the topic `atlas.pix.transacoes`.
On the Docker path a console at http://localhost:8085 lets you inspect the topic
live; the native path has no UI, so use `./scripts/kafka-local.sh status`.

Before registering, the script stops each stale connector, deletes its offsets
and only then deletes the connector. Deleting a connector does **not** delete
its offsets — Connect keeps the resume token in `connect-offsets` keyed by
connector name, and `startup.mode=latest` applies only when no offset is stored.
Because `overview up` drops `pix.transacoes`, that stored token points at an
oplog position that no longer exists, and the task fails with
`ChangeStreamHistoryLost` while the connector still reports `RUNNING`. If you
ever see `RUNNING · FAILED`, that is the cause; the offsets endpoint needs
Kafka Connect 3.6+.

## 3. Create the Stream Processing Instance

In the Atlas UI, under Stream Processing, create an SPI in the same region
as the cluster. The processor reads the cluster's change stream, so keeping them together saves a cross-region hop on
every window. Add an *Atlas Database* connection named `atlasCluster`, then:

```bash
# in backend/.env: ASP_ENABLED=true and ASP_CONNECTION_STRING=<SPI connection string>
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp-geo.js
```

There are two processors because a deployed pipeline has exactly one terminal
sink. `pixJanelas5s` ends in `pix.metricas_janela`; `geoSinais30s` ends in
`geo.sinais_ao_vivo`. They are two independent consumers of the same change
stream, which happens to be the module's own argument. `scripts/ambiente.sh`
provisions and stops both.

The script preserves an existing processor and its checkpoint by default. To
replace the definition intentionally, run it once with `ASP_RECREATE=true`;
that destructive choice is printed explicitly.

The processor reads the change stream of `pix.transacoes`, sends malformed
documents to a DLQ, aggregates 5-second event-time windows by `run_id`, `uf`
and `tipo` (count, volume, ticket and a simple high-value signal), and `$merge`s each closed window into
`pix.metricas_janela`. The backend surfaces those windows by watching that
collection with a change stream, so the stream processing result reaches the
screen through the same mechanism as column 1.

Tear everything down with `./scripts/teardown-streaming.sh` (add `--volumes` to
drop the cached plugin).

**Cleaning up between runs.** `POST /streaming/reset` (the **Reset** button)
clears the current source, windows, DLQ and audit while keeping the environment
ready for another run. `POST /streaming/reset?finalizar=true`, used by
`overview down`, first stops the processor and also removes application
checkpoints. `scripts/ambiente.sh down` performs a second direct, scoped cleanup
before stopping the local integration services, so an interrupted API call does
not leave demo data behind. It never pauses or resizes the Atlas cluster.

Reset preserves the three source contracts: unique `endToEndId`, TTL on `ts`
and `run_id_reconciliacao`. It reuses the application's connected MongoDB
topology and clears independent collections concurrently. Above
`STREAMING_DROP_ACIMA_DE` (25,000 documents by default), it uses a controlled
drop/recreate and then recovers ASP/Kafka instead of spending the stage pause on
`delete_many`; measured preparation was 6.43 s after a 59k-document run and
1.33 s on an initially clean run. A routine cleanup does not restart Kafka.

The 5-minute TTL index on `ts` (`STREAMING_TTL_SEGUNDOS`) is the safety net for
when you forget to reset, not the main mechanism. In steady state the TTL
deleter removes at the same rate you insert, whatever the window; what the
window actually decides is the size of the **live set**. At 1800 s the
collection settled near a million documents — data plus indexes larger than an
M20's WiredTiger cache, which on its own sustained the memory pressure that
triggers auto-scaling. A 60-second TTL was deliberately rejected for the stage
run: after the first minute it would delete at roughly the ingest rate, add
oplog pressure and could remove source documents before reconciliation. The
30-second finite run plus **Reset** is the cleanup path; 300 seconds is only the
fallback window.

**The Streaming page defaults to live.** It opens the three observer paths only
when the operator starts a session, measures the requested and achieved TPS,
and stops Atlas-facing polling once a finite run reconciles. The alternate
**Replay de segurança** mode reads `backend/data/replay_streaming.json` through
`/replay/*`; it never writes to MongoDB and remains permanently labelled.

The shell's normal state is green `Pronto`. `Pré-voo pendente` means an actual
required check failed; it replaced the ambiguous `Verificar` badge. The
presenter-oriented React/Distribute/Transform decision panel was intentionally
removed from the customer screen and moved to the presentation guide,
`docs/roteiro-apresentacao-streaming.md`.

The window moved from 10 s to 5 s. The semantics are unchanged — tumbling, no
overlap — but at 10 s column 3 went mute for ten seconds at a time, and an
audience watching twenty seconds of the demo saw at most two bursts. The
recording also keeps rolling for 25 s after the run reconciles: stopping at the
moment of reconciliation left the final green state alive for only the last few
seconds of a ~106 s loop, so the payoff vanished into the rewind.

The live page keeps the earlier relative-CPU lesson: do not leave it observing
after the run, and always finish with `overview down`.

**Open the live capture page before starting the generator.** The Change Stream
and Kafka consumers start lazily on the first SSE subscription. The observer
uses `auto.offset.reset=earliest`, while the source connector itself uses
`startup.mode=latest` when it has no stored offset. Waiting for the Kafka column
to report `consumindo` before writing keeps the capture boundary explicit and
avoids measuring consumer startup as backlog.

![Replay mode with the run reconciled](screenshots/07c-streaming-replay.png)

The on-screen origin badge is permanent and names the recorded `run_id` and
timestamp. The generator card also says that playback does not touch the
database, and a separate warning appears when the recording file is absent. The
figures are real measurements, not a live execution. In the shot the four paths
agree at 12,200 with zero observed duplicates and an empty DLQ.

Actions that would act on a real environment (connector restart, DLQ injection,
checkpoint restart) stay visible but disabled — the capability is part of the
story, but there is nothing to act on during a replay.

**ASP and Kafka are provisioned by default for the live page.** `overview`
verifies the materialized assets, starts ASP/Kafka/backend/frontend and does not
resize the cluster. The processor bills per second, so `overview down` is part
of the demo runbook. To capture or refresh the fallback recording:

```bash
overview                      # preflight + ASP + Kafka + backend + frontend
python scripts/capture_replay.py
```

Record a run with the environment up:

```bash
python scripts/capture_replay.py                    # 60 s of real writes at 200 TPS
python scripts/capture_replay.py --segundos 90 --tps 200
```

The capture subscribes to the same SSE streams and polls the same endpoints the
page consumes live, storing each payload with its timestamp. **The replayed
numbers are measurements, not simulation** — the recorder does not synthesise
anything. Because that distinction only holds if the audience can see it, the
page shows a permanent badge naming the recorded `run_id` and its date, every
replay payload carries `replay: true`, and actions that act on the real
environment (connector restart, DLQ injection and checkpoint restart) are
disabled. Do not present a replay as a live run.

Why it exists: M20/M30 are burstable instances, and Atlas compute auto-scaling
fires on **relative** CPU (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`), not
absolute. Measured on this project, 17.6% absolute CPU registered as 88%
relative and scaled the cluster to M30 — with the generator already stopped, on
dashboard polling alone. Replay removes that cost entirely for the parts of the
demo that only need to show the mechanics.

**Stage calibration.** The live path is calibrated for a finite 30-second run;
it is not a production benchmark or a sustained-capacity statement:

- `run_id` is indexed. The reconciliation panel counts the source every few
  seconds; without that index the count is a full collection scan repeated in a
  loop, and it was by far the largest consumer of cluster CPU and cache.
- The reconciliation poll runs every 5 s and **stops** once the run is final —
  it no longer re-queries Atlas for an answer that cannot change.
- **Play** defaults to individual mode at 2,000 TPS for 30 seconds. The async
  driver issues one acknowledged `insert_one` per PIX, which matches the bank
  path and makes ACK latency interpretable per transaction. At 2,000 TPS the
  measured run delivered 2,037 TPS with Atlas p50 3.07 ms, client ACK p50
  17.6 ms, Change Streams p50 21.9 ms and Kafka p50 32.1 ms.
- Batch mode remains available for the volume story. The generator ceiling is
  15,000 TPS and four disjoint Change Stream observers expose consumer
  headroom; neither is a production capacity claim. The full 4k→12k batch ramp
  reconciled on M20 + **SP10** with DLQ 0; the local Kafka observer degraded
  before Atlas Stream Processing.
- The 2026-08-07 live acceptance run produced 59,896 documents and reconciled
  Atlas, Change Streams, Kafka and ASP/DLQ with zero loss. Final state arrived
  in 41.09 s including the 30-second generation window.
- `/preflight`'s `cluster_tier` check now **passes** on the entry tier and fails
  when the cluster has scaled above it. It previously did the opposite: it
  failed on M20 and told the operator to run load until the cluster scaled up,
  which encoded "scale up before demoing" as a prerequisite.
- Cluster state and tier are operator-owned. `overview` reports application
  readiness but never pauses, resumes, resizes or changes auto-scaling.

Two Atlas rules are worth knowing before touching this, because both fail with
HTTP 400: compute auto-scaling requires `minInstanceSize` **strictly** less than
`maxInstanceSize` — so "pin the cluster to M20 while keeping auto-scaling on" is
not expressible — and every tier has a maximum disk size, so a 150 GB cluster
cannot take an M10 floor (M10 tops out at 128 GB).

Relevant environment variables: `STREAMING_DB`, `KAFKA_BROKERS`, `CONNECT_URL`,
`CONNECT_CONNECTOR_NAME`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`,
`ASP_CONNECTION_NAME`, `ASP_PROCESSOR_NAME`, `ASP_GEO_PROCESSOR_NAME`
(`geoSinais30s`), `ASP_TIER` (stage default `SP10`),
`STREAMING_CARTAO_PCT` (18 — share of the stream on the card channel; at 0 the
stream is PIX-only and module 08's event-time panel stays empty),
`STREAMING_SINAL_KMH` (900), `STREAMING_SINAL_MIN_KM` (200),
`STREAMING_SINAL_MIN_MIN` (1),
`STREAMING_MODO_ESCRITA` (`individual`), `STREAMING_DEMO_TPS_INDIVIDUAL`
(2,000), `STREAMING_DEMO_TPS` (8,000 for batch mode),
`STREAMING_CS_PARTICOES` (default 4), `STREAMING_TTL_SEGUNDOS` (default 300),
`STREAMING_DROP_ACIMA_DE` (default 25,000), `STREAMING_DEMO_DURATION_S` (30),
`STREAMING_CONCEPT_TPS` (200; API ceiling 15,000),
`ATLAS_TIER_INICIAL` (preflight expectation only), `ATLAS_MIN_TIER`
(documentation only) and `KAFKA_CONSUMER_GROUP`.

## Transaction values

Real payment traffic is lopsided: lots of small transfers and a few big ones that
carry most of the money. Drawing values uniformly loses that, and the average
ticket comes out wrong. `PERFIS_VALORES` declares weighted value bands per
transaction type instead. Pick one with `STREAMING_PERFIL_VALORES`:

| Profile | Median | Mean | Mean ÷ median | Top 1% of volume |
|---|---|---|---|---|
| `varejo` (default) | R$ 91 | R$ 559 | 6.2× | 36% |
| `corpo_medio` | R$ 500 | R$ 1,252 | 2.5× | 18% |

Both keep the long tail. We tried a flat draw between R$ 100 and R$ 2,000: the
mean lands almost on the median (1.4×) and the top 1% ends up carrying 3% of the
volume, which is not what a payments flow looks like.

`GET /streaming/perfil-valores` returns the declared bands next to percentiles
measured with `$percentile` on the live collection — a nice way to see an
aggregation operator answer a question about data that is being written as you
ask.

## Reading the numbers

The throughput and latency on screen describe this run, on this cluster, from
wherever you are sitting. They are not a capacity figure for MongoDB, and the
default presets stay modest on purpose so the whole thing runs on a small,
cheap tier.

Two things worth knowing before you read a latency number:

- It includes the round trip to the cluster, printed above the columns. Running
  from Brazil against a US cluster adds roughly 200 ms of pure distance to
  everything.
- The per-event feeds redraw once every 120 ms, because no browser tab draws
  thousands of rows per second. The counters and percentiles behind them still
  count every single event.

The number to actually trust is the reconciliation described above. Throughput
varies with your laptop, your region and your tier; whether the events all
arrived does not.

