# MongoDB Atlas Feature Showcase

Seven MongoDB Atlas capabilities, each with a page you can click through while
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

Every module is deep-linkable through the URL hash (`/#agg`, `/#streams`, `/#tx`, and so on).

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

| ACID Transactions |
|---|
| ![Transactions](docs/screenshots/06-transactions.png) |

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
command. `bin/overview` resumes the Atlas cluster, starts the Atlas Stream
Processing processor, brings up local Kafka, starts the backend and frontend,
and opens the browser:

```bash
./bin/overview          # up
./bin/overview down     # pause cluster + stop processor: no compute cost
./bin/overview status
./bin/overview logs     # tail the backend log
```

Symlink it for a one-word command: `ln -sf "$(pwd)/bin/overview" /opt/homebrew/bin/overview`.

**Run `overview down` when you are finished.** A running stream processor bills
per second whether or not anyone is watching. The cloud half of this is
`scripts/ambiente.sh {up,down,status}`, which reads its credentials from
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

**Cleaning up between runs.** `POST /streaming/reset` (the **Reset** button) is
the real cleanup: it clears `transacoes`, `metricas_janela` and `dlq` and zeroes
every counter. Above 300k documents it drops and recreates the collection, so
half a million documents take about 10 seconds instead of timing out.

The 30-minute TTL index on `ts` (`STREAMING_TTL_SEGUNDOS`) is the safety net for
when you forget to reset, not the main mechanism. The window is deliberately
longer than any demo burst. In steady state the TTL deleter removes at the same
rate you insert: 10k/s in is 10k/s deleted, whether the TTL is 2 minutes or 30.
A short window only guarantees that deletion competes with your peak while the
audience is watching, and it floods the oplog that the resume-token demo depends
on. A long window pushes the cleanup to after the presentation, on an idle
cluster.

Relevant environment variables: `STREAMING_DB`, `KAFKA_BROKERS`, `CONNECT_URL`,
`CONNECT_CONNECTOR_NAME`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`,
`ASP_CONNECTION_NAME`, `ASP_PROCESSOR_NAME`, `STREAMING_CS_PARTICOES` (demonstration cursors, default 1),
`STREAMING_TTL_SEGUNDOS`, `STREAMING_CONCEPT_TPS` (capped at 2,000 by the API)
and `KAFKA_CONSUMER_GROUP`.

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
│   ├── kafka-local.sh           # Native Kafka (KRaft) + Connect + Mongo plugin
│   ├── setup-kafka-connector.sh # Registers the partitioned source connectors
│   ├── setup-asp.js             # Creates the stream processor (mongosh)
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
│   │   └── streaming.py         # Generator + Change Streams / Kafka / ASP (SSE)
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
- Tests: `pip install -r backend/requirements-dev.txt && pytest` (63 tests, all
  unit — Mongo is stubbed, so no cluster is needed). Lint with `ruff check backend`.
- GitHub Actions builds both applications, runs tests/lint, and audits dependencies.

## License

MIT
