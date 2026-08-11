# MongoDB Atlas Feature Showcase

Eight Atlas capabilities, one page each, running against a real cluster. Build an index and watch reads keep flowing. Break a change stream and watch it resume from its token. Roll back a transaction and see the documents go back.

Nothing is mocked. If a piece isn't configured, the UI says so instead of inventing a number.

FastAPI + React 18. UI in pt-BR. No LLM.

## The eight modules

| Module | What Atlas does |
|---|---|
| **01** Online Reindexing | Rolling index build, no downtime. `live_monitor.py` prints read/write latency while it happens. |
| **02** Hot / Cold Tiering | Online Archive moves aged docs to cheap storage, still queryable in one namespace. |
| **03** Aggregation Pipeline | `$lookup`, `$facet`, `$unionWith`, `$setWindowFields`, `$bucketAuto`. |
| **04** Schema Validation | JSON Schema enforced by the database, not the app. |
| **05** Change Streams | Ordered feed of inserts/updates/deletes with pre/post-images. |
| **06** ACID Transactions | Multi-document transactions stepped through on screen, rollback included. |
| **07** Streaming | Change Streams, Kafka Connector and Atlas Stream Processing on the same writes. |
| **08** Geo risk | Impossible travel in event time + `$search` with text, geo filter and facets. |

Every module is deep-linkable: `/#agg`, `/#streams`, `/#tx`, …

**Module 01 — index builds while reads keep flowing:**

![Online Reindexing module during a rolling index build](docs/screenshots/01-reindex.png)

## Quick start

```bash
cd backend && python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # MONGO_URI, optionally Atlas API keys
python seed_data.py           # 100k products + 20k reviews
uvicorn main:app --reload --port 8002

cd ../frontend && npm install && npm run dev   # http://localhost:5174
```

`curl http://localhost:8002/preflight` before presenting — checks URI, cluster, collections, Atlas keys and the mutation guard.

Once configured, `bin/overview` replaces all of it:

```bash
./scripts/prepare-demo.sh   # ahead of time: Geo dataset + Search index
./bin/overview              # preflight, backend, frontend, Kafka, ASP
./bin/overview --replay     # recorded fallback, nothing written to the cluster
./bin/overview down         # stops everything; cluster untouched
```

**Run `overview down` when you finish** — a stream processor bills per second.

Detail: [Streaming setup](docs/setup-streaming.md) · [Geo setup](docs/setup-geo.md) · [reference](docs/reference.md).

## Module 07 — three consumers, one write

Change Streams in the app, the Kafka Connector publishing to a real broker, and Atlas Stream Processing aggregating 5s windows — all on the same writes. The stream has two channels: `PIX` (no coordinate, because a PIX transfer has none) and `CARTAO_PRESENCIAL` (acquirer-terminal coordinate, which is what makes module 08 defensible).

![Streaming module: three consumers counting the same live run](docs/screenshots/07-streaming.png)

**Break it on purpose.** Four buttons sit next to the generator: *drop the connector* (stops it mid-flow, resumes from its stored offset), *inject an invalid event* (a string `valor`, diverted to the DLQ by the processor's `$validate`), *publish an incompatible schema version* (the required `valor` renamed to `amount` — the change a Schema Registry would refuse) and *force a primary failover* (the Atlas test failover, on the cluster, under load).

Then watch reconciliation close anyway — and it checks three things, not one: **count** (nothing missing), **value** summed in integer cents (nothing transformed in transit) and an XOR **digest** of the `endToEndId` set (the paths saw the same documents, not merely the same quantity). Measured through a real election: 332,568 documents, R$ 104,486,759.65 identical on all three paths, 0 writes rejected after driver retry, 0 duplicates.

![Reconciliation closing after a connector outage and a poisoned event](docs/screenshots/07e-reconciliacao.png)

Delivery is at-least-once, stated above the numbers, not in a footnote: after a resume the same event can arrive twice, and the unique index on `endToEndId` is what makes that safe. Ordering holds inside a partition, not across them.

**What did it cost?** A throughput number alone invites the wrong reading — *so that's all Atlas does?* — because it never says whose ceiling was hit. The page reads the primary's CPU from the Atlas Admin API, cut to the run's own window, and puts it next to the TPS that produced it: **~1,600 TPS sustained over two minutes on an M20, at 29–53% primary CPU across runs** (the panel always shows the run in front of you, never a stored number). Two traps live here, and both were found by measuring rather than assuming. Atlas publishes process metrics one to two minutes late, so a panel queried right after a 30s run describes the cluster *before* the load — it now says `metricas_pendentes` instead of concluding. And a run shorter than the one-minute publish interval is averaged with the idle remainder of that minute: the same workload read 43% when it landed inside a bucket and 15% when it straddled two, so short runs are labelled a floor.

The last panel answers the question that follows *does it work?* — **what leaves the design**. It compares the components each path requires, without inventing a saving: the Kafka row stays right whenever the event must reach systems outside Atlas, and that is why it is in the demo, working.

## Module 08 — the signal, while it happens

A second processor reads the same change stream, groups the card channel by cardholder in a 30s hopping window and runs haversine in MQL. Impossible travel surfaces in event time, not from a scan someone remembers to run.

![Impossible travel detected in event time, plotted on the inline SVG map](docs/screenshots/08-geo.png)

Planted pairs and emergent ones are counted separately — the guarantee must not become the evidence. The map is inline SVG with a hand-written projection: no tiles, no runtime request, works with the venue's network down.

The retrospective panel answers the two questions an operations team asks before anything else: **how many alerts does this put in the queue** (pairs evaluated, flagged, rate, alerts per day) and **what does the query cost** — measured in both scopes, full scan against a per-client cut. Investigation starts at the contested purchase, not at a place name: pick a flagged case and one `$search` returns what exists around *that terminal*, with fuzzy name matching kept as a refinement for the cloned-merchant case.

Output is a **risk signal** for policy, never an automatic decision — and explicitly not a fraud engine, which an issuer already has.

## More screenshots

| Hot / Cold Tiering | Aggregation Pipeline |
|---|---|
| ![Hot/Cold Tiering: archived documents still queryable](docs/screenshots/02-hotcold.png) | ![Aggregation pipeline stages and results](docs/screenshots/03-aggregations.png) |

| Schema Validation | Change Streams |
|---|---|
| ![Schema validation rejecting a bad document](docs/screenshots/04-schema.png) | ![Change stream feed with pre/post images](docs/screenshots/05-changestreams.png) |

| ACID Transactions | Module 07, three columns |
|---|---|
| ![Transaction stepped through with rollback](docs/screenshots/06-transactions.png) | ![Change Streams, Kafka and ASP side by side](docs/screenshots/07b-streaming-colunas.png) |

## Security

Some of these demos genuinely destroy things — they drop indexes, `collMod` validation rules, create and delete Online Archives. So the blast radius stays local: the launcher binds to `127.0.0.1`, browser mutations are accepted only from configured origins, remote mutations need `DEMO_ADMIN_TOKEN`, bodies are capped, errors return a request id instead of a trace.

**Never point this at anything but a disposable demo cluster.**

On a shared network set a long random `DEMO_ADMIN_TOKEN` in `backend/.env` and mirror it as `VITE_DEMO_API_TOKEN`. The bundled Kafka stack is a single-node lab — no TLS, SASL, ACLs or Schema Registry.

## Stack

Python 3.11 · FastAPI · PyMongo · React 18 · Vite · MongoDB Atlas. Module 07 optionally needs a local Kafka broker and an ASP instance.

```bash
pip install -r backend/requirements-dev.txt
pytest             # 127 unit tests, Mongo stubbed, no cluster needed
ruff check backend
```

[`ARCHITECTURE.md`](ARCHITECTURE.md) · [`docs/SESSION_HANDOFF.md`](docs/SESSION_HANDOFF.md)

## License

MIT
