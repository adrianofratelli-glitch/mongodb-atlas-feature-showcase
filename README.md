# MongoDB Atlas Feature Showcase

An interactive demo application that exercises seven core MongoDB Atlas capabilities
against a live cluster. Built with FastAPI and React 18.

> The application UI is in Portuguese (pt-BR).

![Online Reindexing module](docs/screenshots/01-reindex.png)

## Features

| Module | Description |
|---|---|
| Online Reindexing | Rolling index builds with no downtime and no collection locks |
| Hot / Cold Tiering | Online Archive for automatic tiering of historical data through the Atlas Admin API |
| Aggregation Pipeline | `$lookup` with sub-pipeline, `$facet`, `$unionWith`, `$setWindowFields`, `$bucketAuto` |
| Schema Validation | JSON Schema enforcement at the database layer (enum, regex, ranges, required fields) |
| Change Streams | Real-time event feed (insert, update, delete) with `fullDocumentBeforeChange` and resume tokens |
| ACID Transactions | Multi-document, multi-collection transactions (`with_transaction` callback API) with step-by-step visualization and a rollback demo |
| Streaming | The three ways to react to change in Atlas, side by side, fed by one live write stream: Change Streams, MongoDB Kafka Connector and Atlas Stream Processing |

Every module is deep-linkable through the URL hash (`/#agg`, `/#streams`, `/#tx`, and so on).

## Screenshots

| Hot / Cold Tiering | Aggregation Pipeline |
|---|---|
| ![Hot/Cold Tiering](docs/screenshots/02-hotcold.png) | ![Aggregations](docs/screenshots/03-aggregations.png) |

| Schema Validation | Change Streams |
|---|---|
| ![Schema Validation](docs/screenshots/04-schema.png) | ![Change Streams](docs/screenshots/05-changestreams.png) |

| ACID Transactions | Streaming |
|---|---|
| ![Transactions](docs/screenshots/06-transactions.png) | ![Streaming](docs/screenshots/07-streaming.png) |

## Stack

- Backend: Python 3.11, FastAPI, PyMongo, Uvicorn
- Frontend: React 18, Vite, plain CSS
- Database: MongoDB Atlas (M20), with `produtos` and `avaliacoes` collections

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

Or start both processes with readiness checks:

```bash
./start.sh --foreground
```

## Streaming — setup

The Streaming module (`/#streaming`) shows the three ways to react to a change in
Atlas side by side, all fed by the same live write generator against
`pix.transacoes`. Column 1 (Change Streams) works with nothing but `MONGO_URI`.
Columns 2 and 3 need the setup below; without it they render as
**"não configurado"** with these instructions, and the rest of the module keeps
working. No panel ever shows synthetic data.

**1. Start the local Kafka stack** (Redpanda + Kafka Connect + Redpanda Console):

```bash
docker compose -f docker-compose.streaming.yml up -d
```

The `mongodb-kafka-connect` plugin is downloaded on the first start only and
cached in a named volume, so later runs work offline.

**2. Register the source connector** (reads `MONGO_URI` from `backend/.env`):

```bash
./scripts/setup-kafka-connector.sh
```

It PUTs a `MongoSourceConnector` config on the Connect REST API
(`http://localhost:8083`) with `database=pix`, `collection=transacoes`,
`publish.full.document.only=true`, `startup.mode=copy_existing` and
`topic.prefix=atlas`, producing the topic `atlas.pix.transacoes`.
Inspect it live at http://localhost:8085.

**3. Create the Stream Processing Instance** (Atlas UI → Stream Processing):
create an SPI **in the same region as the cluster** — the processor reads the
cluster's change stream, so co-locating removes a cross-region hop from every
window — add an *Atlas Database* connection named `atlasCluster`, then:

```bash
# in backend/.env: ASP_ENABLED=true and ASP_CONNECTION_STRING=<SPI connection string>
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js
```

The processor reads the change stream of `pix.transacoes`, sends malformed
documents to a DLQ, aggregates 10-second tumbling windows by `uf` and `tipo`
(count, summed volume, average ticket) and `$merge`s each closed window into
`pix.metricas_janela`. The backend surfaces those windows by watching that
collection with a change stream — the ASP result reaches the screen through the
mechanics of column 1.

Tear everything down with `./scripts/teardown-streaming.sh` (add `--volumes` to
drop the cached plugin). `POST /streaming/reset` (the **Reset** button) clears
`transacoes`, `metricas_janela` and `dlq` and zeroes every counter. A 2-hour TTL
index on `ts` keeps the collection from growing between demos.

Relevant environment variables: `STREAMING_DB`, `KAFKA_BROKERS`, `CONNECT_URL`,
`CONNECT_CONNECTOR_NAME`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`.

### Reading the numbers

The module is built for a PIX-scale audience, so it is explicit about what is
measured and what is assumed:

- **Measured:** sustained TPS (5 s sliding window, never the requested value),
  events/s per column, p50/p95/p99 latency over 100% of the events, the volume
  the processor aggregated, and the app↔cluster RTT.
- **Premises (labelled as such in the UI):** the daily PIX volume, Inter's 10%
  share and the 3× peak factor. They only provide a ruler — the presets
  (347 / 1.041 / 3.472 TPS) are derived from them.
- **Sampled:** the per-event feeds, which show one frame every 120 ms because a
  browser tab cannot render thousands of rows per second. Counters and
  percentiles still cover every event.

The latency shown includes the app↔cluster round trip, printed above the
columns. Presenting from Brazil against a US cluster puts ~200 ms of pure
distance in every number.

## Security model

The application contains intentionally destructive demonstrations (index and
collection removal, schema changes, and Online Archive administration). The
default local setup therefore:

- binds the launcher to `127.0.0.1`;
- accepts browser mutations only from the configured local origins;
- rejects remote mutations unless `DEMO_ADMIN_TOKEN` is configured;
- limits request bodies and validates user-controlled query parameters;
- returns request IDs instead of exposing internal exception details.

For a shared network, set a long random `DEMO_ADMIN_TOKEN` in `backend/.env`
and the same value as `VITE_DEMO_API_TOKEN` in `frontend/.env`. This protects
the PoV control surface but is not a replacement for production user
authentication or a reverse proxy.

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

```
.
├── backend/
│   ├── main.py                  # FastAPI app and CORS
│   ├── database.py              # MongoClient, timeouts and readiness
│   ├── security.py              # Mutation guard and defensive headers
│   ├── settings.py              # Centralized environment configuration
│   ├── requirements.txt
│   ├── .env.example             # Environment template (copy to .env)
│   └── routers/
│       ├── reindexacao.py       # Online index management
│       ├── hot_cold.py          # Online Archive (Atlas Admin API)
│       ├── aggregations.py      # Aggregation pipeline demos
│       ├── schema_validation.py # JSON Schema collMod demo
│       ├── change_streams.py    # Change stream watcher
│       ├── transactions.py      # ACID multi-document transactions
│       └── streaming.py         # Generator + Change Streams / Kafka / ASP (SSE)
└── frontend/
    ├── src/
    │   ├── App.jsx              # Shell, sidebar, navigation
    │   ├── index.css            # Design tokens and base styles
    │   ├── hooks/useApi.js      # Fetch wrapper
    │   └── pages/               # One component per module
    └── vite.config.js
```

## Live Monitor (optional)

`live_monitor.py` is a terminal monitor that prints live read and write latency
against the cluster. It is useful during a demo to show that the cluster stays
fully available while an index builds.

```bash
python live_monitor.py
```

## Notes

- Change Streams require a replica set or sharded cluster (every Atlas cluster qualifies, including free/Flex tiers).
- ACID Transactions require MongoDB 4.0 or later on a replica set.
- Online Archive (Hot/Cold Tiering) requires a dedicated cluster (M10+).
- The Hot/Cold Tiering module calls the Atlas Admin API, so `ATLAS_PUBLIC_KEY`,
  `ATLAS_PRIVATE_KEY`, `ATLAS_PROJECT_ID`, and `ATLAS_CLUSTER` must be set.
- `backend/.env` is gitignored. Never commit real credentials.
- Run `pip install -r backend/requirements-dev.txt && pytest` for backend tests.
- GitHub Actions builds both applications, runs tests/lint, and audits dependencies.

## License

MIT
