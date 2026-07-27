# MongoDB Atlas Feature Showcase

Eight MongoDB Atlas capabilities, each with a page you can click through while
they run against a real cluster. Build an index and watch reads keep flowing.
Break a change stream and watch it resume from its token. Roll back a
transaction and see the documents go back to where they were.

Nothing here is mocked: every screen talks to Atlas, and if something is not
configured the page says so instead of showing a number it made up.

Built with FastAPI and React 18. The UI is in Portuguese (pt-BR).

![Online Reindexing module](docs/screenshots/01-reindex.png)

## Features

| Module | What MongoDB does here |
|---|---|
| Online Reindexing | Builds indexes as a rolling operation — no downtime, no collection lock. `live_monitor.py` keeps printing read and write latency while it happens. |
| Hot / Cold Tiering | Online Archive moves aged documents to cheaper storage and keeps querying them through a single federated namespace. Driven by the Atlas Admin API. |
| Aggregation Pipeline | One query language for joins and analytics: `$lookup` with a sub-pipeline, `$facet`, `$unionWith`, `$setWindowFields`, `$bucketAuto`. |
| Schema Validation | JSON Schema enforced by the database itself — enums, regex, ranges, required fields — so a bad document is rejected at the source, not by application code. |
| Change Streams | An ordered feed of inserts, updates and deletes, with `fullDocumentBeforeChange` for the previous version and resume tokens to pick up where a consumer left off. |
| ACID Transactions | Multi-document, multi-collection transactions through the `with_transaction` callback API, stepped through on screen, including a rollback. |
| Streaming | Three ways to react to a change, side by side on the same writes: Change Streams, the MongoDB Kafka Connector, and Atlas Stream Processing doing windowed aggregation in the cloud. |
| Geo | `2dsphere` inside a compound index next to business fields, impossible-travel detection with `$setWindowFields` + haversine in pure MQL, and one `$search` combining text relevance, a geo filter and facets. |

Every module is deep-linkable through the URL hash (`/#agg`, `/#streams`, `/#tx`, and so on).

For the current implementation decisions, trade-offs and validation baseline,
see [`docs/SESSION_HANDOFF.md`](docs/SESSION_HANDOFF.md).

## The Streaming module

A question that comes up a lot: if something changes in MongoDB, how does the
rest of my system hear about it? There is more than one answer, so this module
shows three of them running at once on the same data.

One generator writes PIX-shaped transactions into `pix.transacoes`. Three
consumers read those same writes simultaneously: Change Streams open inside the
app, the MongoDB Kafka Connector publishing to a real broker, and an Atlas
Stream Processing job aggregating 10-second windows in the cloud and merging
them back into a collection.

The transactions are synthetic. Everything else — Atlas, the Kafka broker, the
checkpoints, the dead-letter queue — is real, and if a piece is not configured
its column says so while the other two keep going.

![Streaming module](docs/screenshots/07-streaming.png)

### The three columns

Same data, three routes out of it. Each column reports its own events per second
and p50/p95/p99 latency, plus the state of the thing actually doing the work.

![The three columns side by side](docs/screenshots/07b-streaming-colunas.png)

The button on the left column is the interesting one: it kills the change stream
cursor for 3 seconds while writes keep landing, then reopens it from the saved
resume token. The events from the gap arrive marked as recovered — nothing was
lost, it just arrived late. Beside it sits the oplog window, which tells you how
long a consumer could have stayed down before that trick stops working.

### Did anything get lost?

Each generator run gets a `run_id`, which makes the run countable. The panel
compares that one run across the source collection, Change Streams, Kafka, and
Atlas Stream Processing plus its DLQ. Stop the generator, wait for the last
window to close, and the four numbers should agree. Re-deliveries stay visible
rather than being quietly dropped, because at-least-once delivery is the honest
description of what these pipelines do.

## Screenshots

| Hot / Cold Tiering | Aggregation Pipeline |
|---|---|
| ![Hot/Cold Tiering](docs/screenshots/02-hotcold.png) | ![Aggregations](docs/screenshots/03-aggregations.png) |

| Schema Validation | Change Streams |
|---|---|
| ![Schema Validation](docs/screenshots/04-schema.png) | ![Change Streams](docs/screenshots/05-changestreams.png) |

| ACID Transactions | Geo |
|---|---|
| ![Transactions](docs/screenshots/06-transactions.png) | ![Geo](docs/screenshots/08-geo.png) |

The Geo shot is a live run: the explain comparison reads 62 index keys against
38,044 for the same `$geoWithin`, the impossible-travel table plots the selected
trip, and the search panel returns its facets — all from one cluster.

## Stack

- Backend: Python 3.11, FastAPI, PyMongo, Uvicorn
- Frontend: React 18, Vite, plain CSS
- Database: MongoDB Atlas, with `produtos` and `avaliacoes` collections
- Optional for the Streaming module: Kafka (local, via Homebrew or Docker) and an
  Atlas Stream Processing instance

## Getting Started

### Prerequisites

- Python 3.11+
- Node.js 18+
- A MongoDB Atlas cluster (transactions and change streams work on any replica set, including free/Flex tiers; Online Archive requires M10+)

### 1. Clone

```bash
git clone https://github.com/adrianofratelli-glitch/mongodb-atlas-feature-showcase.git
cd mongodb-atlas-feature-showcase
```

### Quick start, once configured

After the setup below has been done once, a whole demo environment is one
command. `bin/overview` resumes the Atlas cluster, removes residue from a
previous interrupted run, creates a fresh Atlas Stream Processing processor,
starts local Kafka, registers the MongoDB source connector, starts the backend
and frontend, and opens the browser:

```bash
./bin/overview          # up
./bin/overview down     # stop, clean PIX data/Kafka, then pause the environment
./bin/overview status
./bin/overview logs     # tail the backend log
```

Symlink it for a one-word command: `ln -sf "$(pwd)/bin/overview" /opt/homebrew/bin/overview`.

**Run `overview down` when you are finished.** It stops the generator and ASP
before removing `pix.transacoes`, windows, DLQ, audit, application checkpoints,
the connector, the demo Kafka topic and its consumer group. It then pauses the
cluster and stops Kafka. A running stream processor bills per second whether or
not anyone is watching. The cloud half is
`scripts/ambiente.sh {up,down,status}`, which reads credentials from
`backend/.env` and can be called on its own.

### 2. Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy the environment template and fill in your own values:

```bash
cp .env.example .env
```

```env
MONGO_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/
MONGO_DB=POC
ATLAS_PUBLIC_KEY=your_atlas_public_key
ATLAS_PRIVATE_KEY=your_atlas_private_key
ATLAS_PROJECT_ID=your_atlas_project_id
ATLAS_CLUSTER=your_cluster_name
```

Seed the database with synthetic data (required on a fresh cluster):

```bash
python seed_data.py            # 100k products + 20k reviews, enough for every module
python seed_data.py --full     # 5M products + 1M reviews, full-scale dataset
```

Start the API:

```bash
uvicorn main:app --reload --port 8002
```

Before presenting, verify that MongoDB, the required collections, and the
mutation guard are ready:

```bash
curl http://localhost:8002/preflight
```

### 3. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5174.

Or start both processes with readiness checks, without touching the cloud
environment:

```bash
./start.sh --foreground
```

## Streaming — setup

The Streaming module (`/#streaming`) shows the three ways to react to a change in
Atlas side by side, all fed by the same live write generator against
`pix.transacoes`. Column 1 (Change Streams) works with nothing but `MONGO_URI`.
Columns 2 and 3 need the setup below; without it they render as
**"não configurado"** with these instructions, and the rest of the module keeps
working.

**1. Start Kafka locally.** Two ways to do it, pick one.

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

**2. Register the source connectors** (reads `MONGO_URI` from `backend/.env`):

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

**3. Create the Stream Processing Instance** (Atlas UI → Stream Processing):
create an SPI in the same region as the cluster. The processor reads the
cluster's change stream, so keeping them together saves a cross-region hop on
every window. Add an *Atlas Database* connection named `atlasCluster`, then:

```bash
# in backend/.env: ASP_ENABLED=true and ASP_CONNECTION_STRING=<SPI connection string>
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js
```

The script preserves an existing processor and its checkpoint by default. To
replace the definition intentionally, run it once with `ASP_RECREATE=true`;
that destructive choice is printed explicitly.

The processor reads the change stream of `pix.transacoes`, sends malformed
documents to a DLQ, aggregates 10-second event-time windows by `run_id`, `uf`
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
before pausing the cluster, so an interrupted API call does not leave demo data
behind.

The 10-minute TTL index on `ts` (`STREAMING_TTL_SEGUNDOS`) is the safety net for
when you forget to reset, not the main mechanism. In steady state the TTL
deleter removes at the same rate you insert, whatever the window; what the
window actually decides is the size of the **live set**. At 1800 s the
collection settled near a million documents — data plus indexes larger than an
M20's WiredTiger cache, which on its own sustained the memory pressure that
triggers auto-scaling. At 600 s and the current moderate TPS the live set stays
in the tens of thousands: it fits in cache, and the deletion rate is far too low
to compete with the peak or to flood the oplog the resume-token demo depends on.
The window is still longer than any demo burst.

**The Streaming page replays a recorded run — it does not write.** There is no
live mode on this page: a single **▶ Play** button replays one *recorded real
run* from `backend/data/replay_streaming.json` through `/replay/*`, which
mirrors the `/streaming/*` paths. Nothing is written to MongoDB, so the page
works with the cluster **paused** and with no Kafka or ASP running — only the
backend and the frontend. All three columns are in the recording (Change Streams
456 events, Kafka 531, ASP 80 closed windows), which is what lets the three
approaches be compared side by side.

Live writing was removed because it stressed the cluster for no demonstrative
gain — see the relative-CPU note below.

**Open the page before starting the generator.** The Change Stream and Kafka
consumers start lazily, on the first SSE subscription, and the Kafka observer
joins its consumer group with `auto.offset.reset=latest`. If the generator is
already running when the page opens, the connector will have published thousands
of messages while the group was still joining, the consumer starts at the tail,
and the Kafka column reads zero for that run — which also blocks reconciliation.
Wait for the Kafka column to report `consumindo`, then start the generator.

![Replay mode with the run reconciled](docs/screenshots/07c-streaming-replay.png)

The badge is permanent and names the recorded run and the date it was measured.
It is not optional decoration: the figures are real measurements, and without
that line an audience reasonably reads them as happening now. In the shot the
four paths agree at 12,040 with zero duplicates and an empty DLQ.

Actions that would act on a real environment (connector restart, DLQ injection,
checkpoint restart) stay visible but disabled — the capability is part of the
story, but there is nothing to act on during a replay.

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
environment (connector restart, DLQ injection, checkpoint restart, Reset) are
disabled. Do not present a replay as a live run.

Why it exists: M20/M30 are burstable instances, and Atlas compute auto-scaling
fires on **relative** CPU (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`), not
absolute. Measured on this project, 17.6% absolute CPU registered as 88%
relative and scaled the cluster to M30 — with the generator already stopped, on
dashboard polling alone. Replay removes that cost entirely for the parts of the
demo that only need to show the mechanics.

**Staying on M20.** The module is calibrated so the whole PoV runs on the entry
tier without the cluster scaling up:

- `run_id` is indexed. The reconciliation panel counts the source every few
  seconds; without that index the count is a full collection scan repeated in a
  loop, and it was by far the largest consumer of cluster CPU and cache.
- The reconciliation poll runs every 5 s and **stops** once the run is final —
  it no longer re-queries Atlas for an answer that cannot change.
- The generator is capped at 1,000 TPS (`TPS_MAX`) and defaults to 200.
- `/preflight`'s `cluster_tier` check now **passes** on the entry tier and fails
  when the cluster has scaled above it. It previously did the opposite: it
  failed on M20 and told the operator to run load until the cluster scaled up,
  which encoded "scale up before demoing" as a prerequisite.
- `scripts/ambiente.sh up` normalizes the cluster to `ATLAS_TIER_INICIAL`
  (default `M20`) so a run that scaled up yesterday does not start today on the
  larger tier. The auto-scaling **ceiling is deliberately left alone** —
  scale-up stays available for when it is genuinely needed. Staying on M20 is
  the job of the four fixes above, not of a ceiling.

Two Atlas rules are worth knowing before touching this, because both fail with
HTTP 400: compute auto-scaling requires `minInstanceSize` **strictly** less than
`maxInstanceSize` — so "pin the cluster to M20 while keeping auto-scaling on" is
not expressible — and every tier has a maximum disk size, so a 150 GB cluster
cannot take an M10 floor (M10 tops out at 128 GB).

Relevant environment variables: `STREAMING_DB`, `KAFKA_BROKERS`, `CONNECT_URL`,
`CONNECT_CONNECTOR_NAME`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`,
`ASP_CONNECTION_NAME`, `ASP_PROCESSOR_NAME`, `STREAMING_CS_PARTICOES` (demonstration cursors, default 1),
`STREAMING_TTL_SEGUNDOS`, `STREAMING_CONCEPT_TPS` (capped at 1,000 by the API),
`ATLAS_TIER_INICIAL`, `ATLAS_MIN_TIER` and `KAFKA_CONSUMER_GROUP`.

### Transaction values

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

### Reading the numbers

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

## Geo — setup

The Geo module runs against its own database (`geo`, override with `GEO_DB`), so
it never touches the `POC` or `pix` collections. Seed it once:

```bash
python scripts/seed_geo.py            # 2,000 clients × 75 transactions = 150k docs
python scripts/seed_geo.py --drop     # recreate from scratch
```

The generator is seeded with a fixed value, so every `endToEndId` is stable and
the unique index rejects re-inserts: running the seed twice leaves 150k
documents, not 300k. Points are gaussian clusters around 40 real Brazilian
municipalities weighted by population — uniformly random coordinates inside the
country's bounding box look obviously fake on a projector.

Forty clients get a deliberately impossible pair: two transactions roughly five
minutes and 700+ km apart. Their IDs are written to
`backend/data/fraud_seeds.json` so the impossible-travel panel has a guaranteed
result on stage.

Indexes created by the seed:

| Index | Used by |
|---|---|
| `cliente_status_local_idx` — `{clienteId: 1, status: 1, local: "2dsphere"}` | Demo A, the compound plan |
| `local_2dsphere_idx` — `{local: "2dsphere"}` | Demo A, the geo-only plan it is compared against |
| `cliente_ts_idx` — `{clienteId: 1, ts: 1}` | Demo B, `$setWindowFields` partition + sort |
| `categoria_local_idx` — `{"estabelecimento.categoria": 1, local: "2dsphere"}` | category-scoped geo queries |
| `uf_ts_idx` — `{uf: 1, ts: -1}` | regional slicing |
| `e2e_unq_idx` — unique `{endToEndId: 1}` | seed idempotency |

### The Atlas Search index

Demo C needs one Atlas Search index. Create it with:

```bash
./scripts/create_search_index_geo.sh
```

Until it reports `READY`, the search panel renders a "não configurado" notice
rather than inventing results. The definition it applies:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "estabelecimento": {
        "type": "document",
        "fields": {
          "nome": { "type": "string", "analyzer": "lucene.portuguese" },
          "categoria": [{ "type": "token" }, { "type": "stringFacet" }]
        }
      },
      "uf": [{ "type": "token" }, { "type": "stringFacet" }],
      "local": { "type": "geo" }
    }
  }
}
```

`categoria` and `uf` are indexed twice on purpose: `token` serves the exact
filter, `stringFacet` serves the `$searchMeta` facet.

### What the module does not claim

The page says this out loud, and so does this README: MongoDB answers
geospatial *predicates* — is this inside, does it cross, what is nearby. It has
no geometry algebra (no buffer, union, intersection or area), only WGS84 with
no reprojection, and no raster, topology or routing. `$geoNear` must be the
first pipeline stage, and `$vectorSearch`'s `filter` does not accept geospatial
operators at all. For heavy GIS analysis, PostGIS is the better tool and the
demo says so.

## Security model

Some of these demos genuinely destroy things — they drop indexes, change
validation rules with `collMod`, and create and delete Online Archives. That is
the point, but it means the default setup keeps the blast radius local:

- binds the launcher to `127.0.0.1`;
- accepts browser mutations only from the configured local origins;
- rejects remote mutations unless `DEMO_ADMIN_TOKEN` is configured;
- limits request bodies and validates user-controlled query parameters;
- returns request IDs instead of exposing internal exception details.

For a shared network, set a long random `DEMO_ADMIN_TOKEN` in `backend/.env`
and the same value as `VITE_DEMO_API_TOKEN` in `frontend/.env`. This protects
the PoV control surface but is not a replacement for production user
authentication or a reverse proxy.

The bundled Kafka stack is a single-node local lab: no TLS, SASL, ACLs or Schema
Registry. It shows the connector working, nothing more. A real deployment needs
encrypted transport, authorization, HA brokers, secrets through a
ConfigProvider, and a schema-compatibility policy.

## Dataset

The demos run against two collections, both generated by `backend/seed_data.py`:

| Collection | Documents (full) | Description |
|---|---|---|
| `produtos` | ~5,000,000 | E-commerce products: price, category, stock, ratings |
| `avaliacoes` | ~1,000,000 | Product reviews linked by `produto_id` |

The seed script also creates the indexes the demos depend on. The default run
(100k/20k) takes a couple of minutes and is enough to exercise every module.
Use `--full` to reproduce the large-scale dataset.

## Project Structure

`ARCHITECTURE.md` has the full request-path diagram and a per-file
responsibility table. The short version:

```
.
├── bin/overview                 # One command: cloud env + backend + frontend
├── scripts/
│   ├── ambiente.sh              # Atlas cluster + ASP processor on/off
│   ├── cleanup-streaming-data.py # Scoped removal of collections generated by PIX
│   ├── kafka-local.sh           # Native Kafka (KRaft) + Connect + Mongo plugin
│   ├── setup-kafka-connector.sh # Registers the source connector
│   ├── setup-asp.js             # Creates the stream processor (mongosh)
│   ├── seed_geo.py              # Geo dataset: 150k georeferenced transactions
│   ├── create_search_index_geo.sh # Atlas Search index for the Geo module
│   └── teardown-streaming.sh
├── backend/
│   ├── main.py                  # FastAPI app, CORS, health, /preflight, /stats
│   ├── database.py              # MongoClient, timeouts and readiness
│   ├── security.py              # Mutation guard and defensive headers
│   ├── settings.py              # Centralized environment configuration
│   ├── requirements.txt
│   ├── .env.example             # Environment template (copy to .env)
│   ├── routers/
│   │   ├── reindexacao.py       # Online index management
│   │   ├── hot_cold.py          # Online Archive (Atlas Admin API)
│   │   ├── aggregations.py      # Aggregation pipeline demos
│   │   ├── schema_validation.py # JSON Schema collMod demo
│   │   ├── change_streams.py    # Change stream watcher
│   │   ├── transactions.py      # ACID multi-document transactions
│   │   ├── streaming.py         # Generator + Change Streams / Kafka / ASP (SSE)
│   │   └── geo.py               # explain compare, impossible travel, geo + $search
│   ├── data/                    # uf-independent module data (fraud_seeds.json)
│   └── tests/                   # pytest; no live cluster required
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Shell, sidebar, hash routing
│   │   ├── index.css            # Design tokens and base styles
│   │   ├── hooks/useApi.js      # Fetch wrapper
│   │   ├── components/          # DemoFlow, QueryBlock
│   │   └── pages/               # One component per module
│   └── vite.config.js           # Proxies /api to :8002
├── docker-compose.streaming.yml # Redpanda + Connect + console (Docker path)
├── live_monitor.py              # Terminal latency monitor
└── docs/screenshots/
```

## Live Monitor (optional)

`live_monitor.py` prints read and write latency against the cluster, live, in a
terminal. Run it in a second window while the Online Reindexing module builds an
index: the latency line just keeps going, which is the whole claim about rolling
index builds, visible instead of asserted.

```bash
python live_monitor.py
```

## Notes

- Change Streams require a replica set or sharded cluster (every Atlas cluster qualifies, including free/Flex tiers).
- ACID Transactions require MongoDB 4.0 or later on a replica set.
- Online Archive (Hot/Cold Tiering) requires a dedicated cluster (M10+).
- The Hot/Cold Tiering module calls the Atlas Admin API, so `ATLAS_PUBLIC_KEY`,
  `ATLAS_PRIVATE_KEY`, `ATLAS_PROJECT_ID`, and `ATLAS_CLUSTER` must be set.
- Several endpoints are deliberately destructive. Point this at a disposable demo
  cluster only.
- `backend/.env` is gitignored. Never commit real credentials.
- Tests: `pip install -r backend/requirements-dev.txt && pytest` (85 tests, all
  unit — Mongo is stubbed, so no cluster is needed). Lint with `ruff check backend`.
- GitHub Actions builds both applications, runs tests/lint, and audits dependencies.

## License

MIT
