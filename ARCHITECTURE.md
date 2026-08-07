# Architecture

```
React 18 + Vite (frontend/, :5174)
   │  fetch /api/*        (JSON)
   │  EventSource /api/streaming/*        (SSE — live session, primary mode)
   │  EventSource /api/replay/streaming/* (SSE — recorded fallback)
   ▼
FastAPI (backend/main.py, :8002)
   ├─ PyMongo ─────────────► MongoDB Atlas   (POC.*, pix.* and geo.*)
   ├─ requests ────────────► Atlas Admin API v2      (Online Archive only)
   ├─ requests ────────────► Kafka Connect REST      (:8083, live mode)
   ├─ aiokafka (optional) ─► Redpanda / Kafka broker (:19092, live mode)
   └─ file ────────────────► backend/data/replay_streaming.json (module 07 playback)
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
| `/change-streams` | `POST /start`, `POST /trigger`, `GET /feed` (SSE), `GET /events`, `GET /collection`, `POST /stop`, `DELETE /clear` |
| `/transactions` | `GET /status`, `POST /executar`, `POST /reset` |
| `/streaming` | see below |
| `/geo` | `GET /status`, `GET /municipios`, `GET /sinais-ao-vivo`, `POST /explain-compare`, `GET /impossible-travel`, `POST /search` |

### `/streaming` (module 07)

One write generator feeds four consumers of the same change. Data lives in
`pix.transacoes`, `pix.metricas_janela`, `pix.dlq`, `pix.dlq_audit` and
`pix.consumer_checkpoints` (`STREAMING_DB` overrides the database name), plus
`geo.sinais_ao_vivo` for the fourth one.

**Two channels in one stream.** `canal: "PIX"` carries no coordinate — a PIX
transfer genuinely does not have one. `canal: "CARTAO_PRESENCIAL"`
(`STREAMING_CARTAO_PCT`, 18% by default) carries `local` as the acquirer
terminal's registered point, the same modelling as the module 08 dataset, from
the same `backend/data/municipios.json`. That is what lets `geoSinais30s`
compute geographic risk in event time instead of module 08 scanning history on
demand.

The card channel has two distinct instants and conflating them is a real bug:
`ts` is arrival into the stream and the TTL field; `compradaEm` is the purchase
at the terminal, which can be minutes earlier because acquirer capture lags.
Speed is computed from `compradaEm`. Back-dating `ts` made the TTL delete the
older half of a pair before reconciliation ran, so the source counted fewer than
the consumers — expiry indistinguishable from loss.

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
| `GET` | `/streaming/cenario` | Returns presets for the active write mode. The default individual path exposes 1,000 TPS as the customer reference, 2,000 TPS as the sustained stage target, and 12,000 TPS in batch mode as tier headroom. The endpoint also returns `modo_escrita`, `default_tps` and the individual ceiling. These are comparison and stage evidence, not sizing or certified capacity. |
| `GET` | `/streaming/rede` | Median RTT app ↔ cluster, measured with `ping`. Without it the columns' latency reads as change-stream cost when a large part is distance. |

**Generator**

| Method | Path | Description |
|---|---|---|
| `POST` | `/streaming/generator/start` | Body `{"tps": 1..TPS_MAX, "duration_s": 10..120, "modo": "individual|lote"}` (`TPS_MAX` = 15,000; defaults to individual mode at 2,000 TPS/30 s). Individual mode uses the async driver and one acknowledged `insert_one` per PIX; batch mode uses `insert_many` micro-batches for the higher-volume story. Creates a `run_id` and sequence and stops automatically. The ceiling is a guardrail, not an M20 guarantee or product limit. |
| `POST` | `/streaming/generator/stop` | Cancels the task, waits 7.2 s (5 s window + 2 s lateness) and writes one technical marker under a reserved `run_id` to advance the event-time watermark. The marker is outside the demonstrated run's reconciliation and lets its final window close. |
| `GET` | `/streaming/generator/status` | `run_id`, `running`, `stopping`, `duration_s`, `ends_at`, `tps_alvo`, **`tps_medido`**, `inseridos`, write mode, `write_ack` p50/p95/p99 and collection state. In individual mode `write_ack` is the end-to-end ACK for one PIX; in batch mode it describes one acknowledged micro-batch. The three consumer columns measure post-commit propagation. |
| `POST` | `/streaming/reset` | Stops the generator, ensures the unique business-key, TTL and `run_id_reconciliacao` indexes, then clears source, windows, DLQ and audit concurrently using the application's connected MongoDB topology. Above `STREAMING_DROP_ACIMA_DE` (25k by default), it stops ASP, drops/recreates the dedicated source and indexes, then recovers ASP and Kafka; a routine delete does not restart Kafka. Residual data returns 503 instead of starting a mixed run. With `?finalizar=true`, it also removes application checkpoints and leaves ASP/Kafka stopped. |

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

**Injected failure**

| Method | Path | Description |
|---|---|---|
| `POST` | `/streaming/falha/connector` | Stops every showcase connector, waits `segundos` (1–30, default 8) and resumes them. Stopping does not discard the offset: the resume token stays in `connect-offsets`, so everything written during the outage is delivered afterwards, and reconciliation has to close anyway. |
| `POST` | `/streaming/falha/evento-invalido` | Writes one transaction whose `valor` is a string. It is a valid document for the collection — it passes the unique index and counts at the source — but the processor's `$validate` diverts it to the DLQ while the pipeline keeps running. |

Both exist because a run where nothing fails proves nothing failed. They are the
counterpart to reconciliation: the number only means something once the path
that produced it has been broken and recovered on stage.

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

### `/replay` (module 07 playback)

Replay is the no-write contingency, selected explicitly in the page or with
`bin/overview --replay`. It does not provision ASP or Kafka and talks to
`/replay/*`, which mirrors the `/streaming/*` paths. Everything is served
from `backend/data/replay_streaming.json`, recorded by
`scripts/capture_replay.py` against the real cluster.

This router never touches MongoDB — a test asserts it — so the page works with
the cluster paused. The reason it exists is cost: M20/M30 are burstable, and
Atlas compute auto-scaling fires on **relative** CPU
(`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`). Measured here, 17.6% absolute read
as 88% relative and scaled the cluster with the generator already stopped, on
dashboard polling alone.

Every payload carries `replay: true`, and the page shows a permanent one-line
badge naming the recorded `run_id` and its date. The numbers are real
measurements from that run — presenting them as live would be the one thing
this mode must not do.

| Method | Path | Description |
|---|---|---|
| `GET` | `/replay/manifest` | `run_id`, `gravado_em`, duration, event count and the clock state. Answers **200 with `disponivel: false`** when there is no recording — the page probes this on load, and a 5xx would raise a global error toast on every install that never recorded. |
| `POST` | `/replay/play` | Starts the playback clock at zero (`retomar=true` resumes from the paused position). This is what the single **▶ Play** button calls. |
| `POST` | `/replay/pause` / `/replay/stop` | Freeze at the current position / rewind and stop. Position is derived from a monotonic clock on read, so a stopped replay costs nothing. |
| `GET` | `/replay/estado` | `rodando`, `posicao_s`, `duracao_s`, `repetir`. |
| `GET` | `/replay/streaming/{cenario,rede,cluster}` | Static context captured with the run: it describes the environment the run was *measured* in, not the current one. |
| `GET` | `/replay/streaming/{generator/status,kafka/status,asp/status,oplog,leitura,asp/dlq/resumo,reconciliacao}` | The recorded snapshot whose timestamp is the last one at or before the current playback position. |
| `GET` | `/replay/streaming/{changestream,kafka,asp}` | **SSE.** Re-emits the recorded events as the clock advances, plus a `reset` when the recording loops. Sends `: keepalive` every 10 s — without it an idle stream is dropped by the browser and the Vite proxy, the client reconnects, and the server-side generator never learns the client is gone (a generator that never writes never sees the disconnect). Those leaked streams exhaust the browser's ~6-connection-per-host budget and ordinary fetches start timing out at 30 s while the backend answers in milliseconds. |

### `/geo` (module 08)

Its own database (`geo`, override with `GEO_DB`). Two collections:
`geo.transacoes`, the versioned dataset seeded by `scripts/seed_geo.py`, and
`geo.sinais_ao_vivo`, which is run data written by the ASP processor and cleared
by `/streaming/reset` and `cleanup-streaming-data.py`. The dataset collection is
never touched by either cleanup path.

| Method | Path | Description |
|---|---|---|
| `GET` | `/geo/sinais-ao-vivo` | Reads `geo.sinais_ao_vivo`, materialized by the `geoSinais30s` stream processor while module 07 runs. Nothing is computed here — the window already did it. Returns the recent pairs plus separate `plantados` and `emergentes` counts, because merging them would turn the demo's guaranteed signal into evidence. |
| `GET` | `/geo/status` | Document count, the index list read from the collection, and whether the Atlas Search index exists. Nothing is hard-coded in the UI. |
| `GET` | `/geo/municipios` | Municipalities present in the dataset with a representative point, so the UI can centre a query without shipping a coordinate table to the browser. Cached in memory; the list only changes when the seed runs again. |
| `POST` | `/geo/explain-compare` | The same `$geoWithin` (`$centerSphere`) query explained twice: hinted at `cliente_status_local_idx` (equality fields first, geo last) and at `local_2dsphere_idx`. Returns winning stage, index used, `totalKeysExamined`, `totalDocsExamined`, `nReturned` and `executionTimeMillis` for each. If the measurement contradicts the didactic note, the measurement is what the screen shows. |
| `GET` | `/geo/impossible-travel` | Retrospective risk signal: `$setWindowFields` partitioned by `clienteId`, sorted by `ts`, `$shift` pulling the previous timestamp, coordinates, device and location provenance, then haversine in pure MQL. It explicitly returns `decisao_fraude: false`; no document leaves the cluster for the calculation. |
| `POST` | `/geo/search` | Contextual receiver/merchant discovery: one `$search` with fuzzy text, `geoWithin`, optional category filter and `$searchMeta` facets. It is not part of PIX settlement or cadastral validation. Without the index the endpoint returns `estado: "nao_configurado"` rather than empty results. |

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
Redpanda Console (`:8085`). Step-by-step instructions live in
`docs/setup-streaming.md`.

Two Atlas Stream Processing jobs, not one, because a deployed pipeline has a
single terminal sink: `pixJanelas5s` (`scripts/setup-asp.js`) merges 5-second
windows into `pix.metricas_janela`, and `geoSinais30s`
(`scripts/setup-asp-geo.js`) merges geographic risk signals into
`geo.sinais_ao_vivo`. They are independent consumers of the same change stream.
`scripts/ambiente.sh` provisions and stops both.

`scripts/lib/expand_srv.py` rewrites the `mongodb+srv://` URI into its standard
three-host form before the connector is registered. The source connector
reparses `connection.uri` on every task start, so an SRV URI turns each restart
into a DNS SRV+TXT lookup; a flaky resolver left the connector `RUNNING` with
its only task `FAILED`.
