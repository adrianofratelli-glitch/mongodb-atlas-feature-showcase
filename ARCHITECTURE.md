# Architecture

```
React 18 + Vite (frontend/, :5174)
   │  fetch /api/*        (JSON)
   │  EventSource /api/streaming/* (SSE)
   ▼
FastAPI (backend/main.py, :8002)
   ├─ PyMongo ─────────────► MongoDB Atlas   (POC.* and pix.*)
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

### `/streaming` (module 07)

One write generator feeds three consumers of the same change. Data lives in
`pix.transacoes`, `pix.metricas_janela` and `pix.dlq` (`STREAMING_DB` overrides
the database name).

**Cenário e rede**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/cenario` | PIX scale premises (daily volume, Inter's share, peak factor) and the TPS presets derived from them, split into `premissas` and `derivados` so the UI can label them. Nothing here is a measurement. |
| `GET` | `/streaming/rede` | Median RTT app ↔ cluster, measured with `ping`. Without it the columns' latency reads as change-stream cost when a large part is distance. |

**Generator**

| Method | Path | Description |
|---|---|---|
| `POST` | `/streaming/generator/start` | Body `{"tps": 1..5000}`. Starts (or retunes) an asyncio task inserting micro-batches every 100 ms with `insert_many`. Ensures the unique index on `endToEndId` and the 2-hour TTL index on `ts`. |
| `POST` | `/streaming/generator/stop` | Cancels the task. |
| `GET` | `/streaming/generator/status` | `running`, `tps_alvo`, **`tps_medido`** (measured over a 5 s sliding window, never the requested value), `inseridos`, `docs_na_colecao`, plus the arithmetic projection `projecao_dia` and `pct_dia_inter` / `pct_dia_brasil`. |
| `POST` | `/streaming/reset` | Stops the generator, empties the three collections (dedicated client with a long socket timeout, retried on election), zeroes all counters and broadcasts a `reset` event. Returns `restantes` if the purge could not finish. The change-stream worker is deliberately left running. |

**Column 1 — Change Streams**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/changestream` | **SSE.** A single `collection.watch()` cursor (`[{$match: {operationType: "insert"}}]`, `full_document="updateLookup"`) broadcast to all subscribers. Event types: `hello`, `aberto`, `evento`, `derrubado`, `erro`, `reset`. Each `evento` carries the end-to-end `latency_ms` (document `ts` vs receipt), the truncated resume token and the `recuperado` flag. |
| `POST` | `/streaming/changestream/drop-resume` | Closes the cursor, waits 3 s while the generator keeps writing, reopens with `resume_after(<resume token>)`. Events whose `ts` precedes the reopen are flagged `recuperado` — the proof that nothing was lost. |
| `GET` | `/streaming/changestream/status` | `aberto`, `eventos`, `recuperados`, `token`, plus `eventos_s` and the p50/p95/p99 latency percentiles. |

**Column 2 — MongoDB Kafka Connector**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/kafka` | **SSE** of messages consumed from `atlas.pix.transacoes`, with partition, offset and the Atlas-insert → topic-arrival `latency_ms`. `aiokafka` is imported lazily: without the dependency or the broker, the stream emits `{"type": "status", "estado": "nao_configurado"}` and the UI renders the setup instructions. |
| `GET` | `/streaming/kafka/status` | Connector state read from the Kafka Connect REST API (`RUNNING` / `FAILED` / `nao_configurado`) plus the consumer's message count and current offset. |

**Column 3 — Atlas Stream Processing**

| Method | Path | Description |
|---|---|---|
| `GET` | `/streaming/asp` | **SSE** of closed windows and DLQ documents. The backend does not query the SPI: it watches `pix.metricas_janela` and `pix.dlq` with change streams, so the ASP result reaches the screen through the mechanics of column 1. |
| `GET` | `/streaming/asp/status` | `configurado` / `nao_configurado` — the real processor state read with `listStreamProcessors` on the SPI, not just connectivity — plus window/DLQ counts, the totals the processor has aggregated (`transacoes_agregadas`, `volume_agregado`) and the pipeline as a code block for the slide. |
| `POST` | `/streaming/asp/inject-invalid` | Inserts a document violating the expected schema; the processor's `$validate` stage routes it to the DLQ instead of failing. Returns 409 when ASP is not configured. |
| `GET` | `/streaming/asp/dlq` | Last DLQ documents. |
| `GET` | `/streaming/asp/janelas` | Last closed windows straight from the collection the processor writes. |

**Sampling.** At Inter-peak load the columns produce thousands of events per
second — more than a browser tab can render. The SSE feed is therefore a
*sample* (one frame every 120 ms), labelled as such in the UI, while counters
and percentiles are computed in the worker over **100% of the events**. A
recovered event is never sampled out: it is the proof the drop/resume works.

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
- `src/hooks/useApi.js` — fetch wrapper adding `X-Demo-Token`; SSE uses `EventSource` directly (`useSse` in `pages/Streaming.jsx`).
- State lives only in React state — no `localStorage`/`sessionStorage`.

## External infrastructure

`docker-compose.streaming.yml` runs Redpanda (`:19092`), Kafka Connect
(`:8083`) with the `mongodb-kafka-connect` plugin cached in a named volume, and
Redpanda Console (`:8085`). See the *Streaming — setup* section of the README.
