# Architecture

```
React 18 + Vite (frontend/, :5174)
   │  fetch /api/*        (JSON)
   │  EventSource /api/streaming/* (SSE)
   ▼
FastAPI (backend/main.py, :8002)
   ├─ PyMongo ─────────────► MongoDB Atlas   (POC.*, pix.* and geo.*)
   ├─ requests ────────────► Atlas Admin API v2      (Online Archive only)
   ├─ requests ────────────► Kafka Connect REST      (:8083, Streaming column 2)
   └─ aiokafka (optional) ─► Redpanda / Kafka broker (:19092, Streaming column 2)
```

The Vite dev server proxies `/api` to `http://localhost:8002`, stripping the
prefix. Every browser call — including SSE — goes through that proxy, so no
external host is contacted to render a page.

## Backend layout

| File | Responsibility |
|---|---|
| `main.py` | App, CORS, request-id middleware, exception handlers, router wiring, `/`, `/health/live`, `/health/ready`, `/preflight`, `/stats` |
| `settings.py` | Frozen dataclass reading env vars once; `settings.atlas_configured` gates Online Archive |
| `database.py` | Single `MongoClient` (`connect=False`, explicit timeouts) plus `readiness()` |
| `security.py` | `MutationGuardMiddleware` (loopback/token/Origin) and `ApiHardeningMiddleware` (body cap + headers) |
| `routers/*.py` | One module per demo |

`MutationGuardMiddleware` skips safe methods, so the SSE endpoints (all `GET`)
are reachable from `EventSource`, which cannot send the `X-Demo-Token` header.

## Endpoints

### Ops

`GET /` · `GET /health/live` · `GET /health/ready` · `GET /preflight` · `GET /stats`

### Modules

| Prefix | Endpoints |
|---|---|
| `/reindexacao` | `GET /indexes`, `POST /create`, `GET /build-status`, `DELETE /drop/{index_name}`, `GET /read-probe`, `GET /explain`, `GET /demo-scenarios` |
| `/hot-cold` | `GET /distribution`, `GET /archive-simulation`, `GET /query-transparent`, `GET /online-archive/list`, `POST /online-archive/create`, `DELETE /online-archive/{archive_id}` |
| `/aggregations` | `GET /lookup`, `GET /facet`, `GET /union-with`, `GET /group-advanced`, `GET /window-functions`, `GET /bucket-auto` |
| `/schema` | `GET /status`, `POST /step1-create-collection`, `POST /step2-insert-without-schema`, `POST /step3-activate-schema`, `POST /step4-insert-invalid`, `POST /insert-valid`, `GET /documents`, `DELETE /reset` |
| `/change-streams` | `POST /start`, `POST /trigger`, `GET /events`, `GET /collection`, `POST /stop`, `DELETE /clear` |
| `/transactions` | `GET /status`, `POST /executar`, `POST /reset` |
| `/streaming` | see below |
| `/geo` | `GET /status`, `GET /municipios`, `POST /explain-compare`, `GET /impossible-travel`, `POST /search` |

### `/streaming` (module 07)

One write generator feeds three consumers of the same change. Data lives in
`pix.transacoes`, `pix.metricas_janela`, `pix.dlq`, `pix.dlq_audit` and
`pix.consumer_checkpoints` (`STREAMING_DB` overrides the database name).

**Negócio e operação**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/oplog` | Oplog retention window in minutes, read from `local.oplog.rs`, plus the configured minimum retention. This is the **operational limit of the resume-token guarantee** — recovery works only while the resume point is still in the oplog. |
| `GET` | `/streaming/leitura` | Latency of a point lookup by `endToEndId` sampled every 250 ms **while the generator writes**, with p50/p95/p99. Answers the daily operational question the throughput numbers do not. |
| `GET` | `/streaming/asp/dlq/resumo` | DLQ grouped by rejection reason, with first/last occurrence. |
| `POST` | `/streaming/asp/dlq/reprocessar` | Fixes the known defect and re-inserts, preserving the original `endToEndId` — running it twice does not duplicate, the unique index blocks it. Idempotency by business key. |
| `GET` | `/streaming/reconciliacao` | Reconciles one finite `run_id`: source documents, unique Change Stream events, unique Kafka messages, ASP aggregates and DLQ/audit. It only reports `reconciliado` after input stops and every path accounts for the same run. The source count relies on the `run_id` index created by `_ensure_indexes()`; the UI polls this every 5 s and stops once the result is final. |

**Cenário e rede**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/cenario` | Moderate concept presets. Explicitly states that the run is synthetic and not a sizing or product-capacity result. |
| `GET` | `/streaming/rede` | Median RTT app ↔ cluster, measured with `ping`. Without it the columns' latency reads as change-stream cost when a large part is distance. |

**Generator**

| Method | Path | Description |
|---|---|---|
| `POST` | `/streaming/generator/start` | Body `{"tps": 1..TPS_MAX}` (`TPS_MAX` = 1,000). Creates a `run_id` and sequence, then starts (or retunes) an asyncio task inserting micro-batches every 100 ms with `insert_many`. The ceiling keeps the PoV reproducible on an M20 without triggering auto-scaling; it is not a product limit. |
| `POST` | `/streaming/generator/stop` | Cancels the task. |
| `GET` | `/streaming/generator/status` | `run_id`, `running`, `tps_alvo`, **`tps_medido`**, `inseridos` and collection state. TPS describes this run only. |
| `POST` | `/streaming/reset` | Stops the generator, clears source, windows, DLQ and DLQ audit, resets in-memory evidence and broadcasts `reset`. With `?finalizar=true`, it first waits for ASP to reach `STOPPED`, also removes application checkpoints and does not restart ASP/Kafka. `overview down` follows with a direct scoped cleanup before pausing the cluster. |

**Column 1 — Change Streams**

The PoV can open multiple `watch()` cursors with disjoint filters. This is a
demonstration technique, not native partitioning or a sizing recommendation.
Each cursor persists its resume token in `pix.consumer_checkpoints`. Transient
errors retain the checkpoint; only a confirmed `ChangeStreamHistoryLost`
condition discards it.

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/changestream` | **SSE.** One `collection.watch()` cursor per demonstration partition (`[{$match: {operationType: "insert"}}]`) broadcasts to all subscribers. Because the pipeline only accepts inserts, it reads `fullDocument` from the event without `updateLookup`. Event types: `hello`, `aberto`, `evento`, `derrubado`, `erro`, `reset`. Each `evento` carries end-to-end `latency_ms`, the truncated resume token and the `recuperado` flag. |
| `POST` | `/streaming/changestream/drop-resume` | Closes the cursor, waits 3 s while the generator keeps writing, and reopens with `resume_after(<persisted resume token>)`. Events whose `ts` precedes the reopen are marked `recuperado`; the reconciliation endpoint then verifies the accounting instead of inferring “zero loss” from animation alone. |
| `GET` | `/streaming/changestream/status` | `aberto`, `eventos`, `recuperados`, **`duplicados`**, `token`, plus `eventos_s` and the p50/p95/p99 latency percentiles. Delivery is at-least-once, so duplicates are *measured* over a bounded window of recent `endToEndId`s rather than asserted away. |

**Column 2 — MongoDB Kafka Connector**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/kafka` | **SSE** of messages consumed from `atlas.pix.transacoes`, with partition, offset and the Atlas-insert → topic-arrival `latency_ms`. `aiokafka` is imported lazily: without the dependency or the broker, the stream emits `{"type": "status", "estado": "nao_configurado"}` and the UI renders the setup instructions. |
| `GET` | `/streaming/kafka/status` | Aggregated state of every `atlas-pix-source*` connector, **downgraded by task health**: a connector reporting `RUNNING` with every task `FAILED` is reported as `FAILED` (`DEGRADADO` when only some failed), because the task is what moves data. Plus message count and current offset. |
| `POST` | `/streaming/kafka/restart` | Restarts connector and tasks. A task killed by a network blip or a cluster restart never recovers on its own while the connector keeps claiming `RUNNING`. |
| `POST` | `/streaming/kafka/consumer/restart` | Restarts the UI observer with the same `group.id`, demonstrating recovery from committed offsets and exposing re-deliveries to reconciliation. |

**Column 3 — Atlas Stream Processing**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/asp` | **SSE** of closed windows and DLQ documents. The backend does not query the SPI: it watches `pix.metricas_janela` and `pix.dlq` with change streams, so the ASP result reaches the screen through the mechanics of column 1. |
| `GET` | `/streaming/asp/status` | Real state plus `getStreamProcessorStats`: input/output/DLQ, oplog lag, watermark, state size, latency and maximum operator memory when available. |
| `POST` | `/streaming/asp/restart-checkpoint` | Stops the named processor, waits for `STOPPED`, then starts it normally so ASP resumes from its managed checkpoint. It never drops/recreates the processor. |
| `POST` | `/streaming/asp/inject-invalid` | `?quantidade=N` (up to 5,000) inserts documents violating the expected schema in four different ways, so the DLQ shows distinct reasons; `$validate` routes them to the DLQ instead of failing the processor. Partial failures are tolerated and reported. Returns 409 when ASP is not configured. |
| `GET` | `/streaming/asp/dlq` | Last DLQ documents. |
| `GET` | `/streaming/asp/janelas` | Last closed windows straight from the collection the processor writes. |

**Sampling.** A browser should not render every streaming event. The SSE feed is therefore a
*sample* (one frame every 120 ms), labelled as such in the UI, while counters
and percentiles are computed in the worker over **100% of the events**. A
recovered event is never sampled out: it is the proof the drop/resume works.

### `/geo` (module 08)

Its own database (`geo`, override with `GEO_DB`), a single collection
`geo.transacoes`, seeded by `scripts/seed_geo.py`. No other module reads or
writes it.

| Method | Path | Description |
|---|---|---|
| `GET` | `/geo/status` | Document count, the index list read from the collection, and whether the Atlas Search index exists. Nothing is hard-coded in the UI. |
| `GET` | `/geo/municipios` | Municipalities present in the dataset with a representative point, so the UI can centre a query without shipping a coordinate table to the browser. Cached in memory; the list only changes when the seed runs again. |
| `POST` | `/geo/explain-compare` | The same `$geoWithin` (`$centerSphere`) query explained twice: hinted at `cliente_status_local_idx` (equality fields first, geo last) and at `local_2dsphere_idx`. Returns winning stage, index used, `totalKeysExamined`, `totalDocsExamined`, `nReturned` and `executionTimeMillis` for each. If the measurement contradicts the didactic note, the measurement is what the screen shows. |
| `GET` | `/geo/impossible-travel` | `$setWindowFields` partitioned by `clienteId`, sorted by `ts`, `$shift` pulling the previous timestamp and coordinates, haversine expressed in `$degreesToRadians`/`$sin`/`$cos`/`$asin`/`$sqrt`. No `$function`, and no document leaves the cluster for the calculation. Returns the executed pipeline alongside the pairs. |
| `POST` | `/geo/search` | One `$search` with `compound`: `must` fuzzy text on `estabelecimento.nome`, `filter` with `geoWithin` (circle, metres) and an optional category `in`. `$searchMeta` produces the category/UF facets. Distance from the centre is recomputed in the pipeline so the requested radius is verifiable without trusting the operator. Without the index the endpoint returns `estado: "nao_configurado"` rather than empty results. |

The geo checks join `/preflight` but never fail it: the module is optional, the
same way Kafka and ASP are.

The map is rendered as inline SVG with a hand-written linear projection over the
Brazilian bounding box — no Leaflet, no Mapbox, no tiles, no new frontend
dependency. With the external network blocked the module still renders and every
number still comes from the cluster; the one external request in the app is the
Google Fonts link in `frontend/index.html`, which is app-wide and pre-existing,
and typography falls back to system fonts when it fails.

### SSE conventions

All streaming endpoints share `_sse_stream`: a per-subscriber `asyncio.Queue`
fed by a broadcast `Hub`, a `hello` frame on connect, a `: keepalive` comment
every 15 s, and disconnect detection via `request.is_disconnected()`. Producers
running in threads (PyMongo cursors) publish through
`loop.call_soon_threadsafe`. A slow subscriber has its oldest frame dropped
rather than blocking the producer.

## Frontend

- `src/App.jsx` — shell, sidebar, hash routing (`/#agg`, `/#streams`, `/#tx`, `/#streaming`).
- `src/pages/` — one component per module; `src/components/` — `DemoFlow`, `QueryBlock`.
- `src/hooks/useApi.js` — fetch wrapper adding `X-Demo-Token`. Expected aborts
  caused by module unmount are silent; timeouts and real failures still dispatch
  a global error. `App.jsx` deduplicates identical error toasts for eight
  seconds. SSE uses `EventSource` directly (`useSse` in
  `pages/Streaming.jsx`).
- State lives only in React state — no `localStorage`/`sessionStorage`.

The UI follows a proof-first hierarchy documented in
`docs/SESSION_HANDOFF.md`: Streaming exposes the three paths in the first
laptop viewport, Aggregations uses `Source → Pipeline → Result`, and large code
definitions are progressively disclosed.

## External infrastructure

`docker-compose.streaming.yml` runs Redpanda (`:19092`), Kafka Connect
(`:8083`) with the `mongodb-kafka-connect` plugin cached in a named volume, and
Redpanda Console (`:8085`). See the *Streaming — setup* section of the README.
