# MongoDB Atlas Feature Showcase

Eight Atlas capabilities, each with a page you click through while it runs
against a real cluster. Build an index and watch reads keep flowing. Break a
change stream and watch it resume from its token. Roll back a transaction and
see the documents go back where they were.

Nothing here invents results. If a piece is not configured, the UI says so
instead of fabricating a number.

FastAPI + React 18. The interface is in Portuguese (pt-BR).

![Online Reindexing module](docs/screenshots/01-reindex.png)

## The eight modules

| Module | What Atlas does here |
|---|---|
| **01** Online Reindexing | Rolling index build — no downtime, no collection lock. `live_monitor.py` keeps printing read/write latency while it happens. |
| **02** Hot / Cold Tiering | Online Archive moves aged documents to cheaper storage, still queryable through one federated namespace. |
| **03** Aggregation Pipeline | `$lookup` with a sub-pipeline, `$facet`, `$unionWith`, `$setWindowFields`, `$bucketAuto` — joins and analytics in one language. |
| **04** Schema Validation | JSON Schema enforced by the database: a bad document is rejected at the source, not by application code. |
| **05** Change Streams | An ordered feed of inserts, updates and deletes, with pre/post-images. |
| **06** ACID Transactions | Multi-document, multi-collection transactions through `with_transaction`, stepped through on screen, rollback included. |
| **07** Streaming | Change Streams, the Kafka Connector and Atlas Stream Processing reacting to the same writes, side by side. |
| **08** Geographic risk | Impossible travel computed in event time, plus `$search` combining text relevance, a geo filter and facets. |

Every module is deep-linkable through the URL hash: `/#agg`, `/#streams`, `/#tx`
and so on.

## Quick start

```bash
git clone https://github.com/adrianofratelli-glitch/mongodb-atlas-feature-showcase.git
cd mongodb-atlas-feature-showcase

cd backend && python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in MONGO_URI and, optionally, the Atlas keys
python seed_data.py           # 100k products + 20k reviews
uvicorn main:app --reload --port 8002

cd ../frontend && npm install && npm run dev   # http://localhost:5174
```

Check `curl http://localhost:8002/preflight` before presenting: it verifies the
URI, the cluster, the collections, the Atlas credentials and the mutation guard.

Once the environment has been configured, `bin/overview` replaces all of it:

```bash
./scripts/prepare-demo.sh   # ahead of the presentation: Geo dataset + Search index
./bin/overview              # preflight, backend, frontend, Kafka and ASP
./bin/overview --replay     # recorded fallback, nothing written to the cluster
./bin/overview down         # stops everything and cleans PIX; cluster untouched
```

**Run `overview down` when you finish.** A stream processor bills per second
whether or not anyone is watching. The launcher never pauses, resumes or resizes
the cluster — that stays with you.

Full instructions: [Streaming setup](docs/setup-streaming.md) ·
[Geo setup](docs/setup-geo.md) · [reference and layout](docs/reference.md).

## Module 07 — three consumers, one write

If something changes in MongoDB, how does the rest of the system hear about it?
There is more than one answer, so this module runs several at once on the same
writes: Change Streams inside the application, the MongoDB Kafka Connector
publishing to a real broker, and Atlas Stream Processing aggregating 5-second
windows in the cloud.

The stream carries two channels. `PIX` has no coordinate — a PIX transfer
genuinely does not carry one. `CARTAO_PRESENCIAL` does: a card-present purchase
is captured at an acquirer terminal, whose position is registered data, not the
customer's phone GPS. That is what makes module 08's signal defensible, and why
the two channels share one stream instead of getting one demo each.

![The Streaming module during a live run](docs/screenshots/07-streaming.png)

The transactions are synthetic. Everything else — Atlas, the broker, the
checkpoints, the dead-letter queue — is real.

### Break it on purpose

A run where nothing fails does not prove recovery; it proves nothing failed. Two
buttons sit next to the generator for that reason.

**Drop the Kafka Connector** stops it mid-flow and brings it back eight seconds
later. Stopping does not discard the offset, so everything written during the
outage is delivered afterwards. **Inject an invalid event** writes a transaction
whose `valor` is a string: a valid document for the collection, so it counts at
the source, but the processor's `$validate` diverts it to the DLQ and keeps
running.

Then you watch reconciliation close anyway.

![Reconciliation after a connector outage and a poisoned event](docs/screenshots/07e-reconciliacao.png)

Each run carries a `run_id`, which makes it countable across the source
collection, Change Streams, Kafka and Atlas Stream Processing plus its DLQ. The
run above had both failures injected halfway through and still finished with the
same count on every path.

Delivery is at-least-once, and the page says so above the numbers rather than in
a footnote: after a resume the same event can arrive again, and the unique index
on `endToEndId` is what makes reprocessing safe. Ordering holds inside a
partition — `particao`, derived from the payer — not across partitions.

## Module 08 — the signal, while it happens

A second stream processor reads the same change stream, groups the card channel
by cardholder in a 30-second hopping window and runs haversine in MQL right
there. Impossible travel therefore surfaces in event time, not from a scan
somebody remembers to run.

![Impossible travel detected in event time](docs/screenshots/08-geo.png)

Two counters, kept deliberately apart: the generator plants a pair every six
seconds so the stage always has a signal, and everything else came out of
ordinary traffic. One combined total would turn the guarantee into the evidence.

The map is inline SVG with a hand-written projection — no Leaflet, no tiles, no
request at runtime — so the module still works with the venue's network down.

The retrospective panels remain, answering a different question: the same
calculation over 90 days of history, an index-plan comparison, and one `$search`
mixing merchant text, a geo filter and facets. What comes out is a **risk
signal** to feed policy, never an automatic decision.

## More screenshots

| Hot / Cold Tiering | Aggregation Pipeline |
|---|---|
| ![Hot/Cold Tiering](docs/screenshots/02-hotcold.png) | ![Aggregations](docs/screenshots/03-aggregations.png) |

| Schema Validation | Change Streams |
|---|---|
| ![Schema Validation](docs/screenshots/04-schema.png) | ![Change Streams](docs/screenshots/05-changestreams.png) |

| ACID Transactions | The three columns of module 07 |
|---|---|
| ![Transactions](docs/screenshots/06-transactions.png) | ![Three columns side by side](docs/screenshots/07b-streaming-colunas.png) |

## Security model

Some of these demos genuinely destroy things: they drop indexes, change
validation rules with `collMod`, create and delete Online Archives. That is the
point, which is why the default setup keeps the blast radius local — the
launcher binds to `127.0.0.1`, browser mutations are accepted only from the
configured origins, remote mutations require `DEMO_ADMIN_TOKEN`, request bodies
are capped, and errors return a request id instead of an internal trace.

**Never point this at anything other than a disposable demo cluster.**

On a shared network, set a long random `DEMO_ADMIN_TOKEN` in `backend/.env` and
mirror it as `VITE_DEMO_API_TOKEN` in `frontend/.env`. That protects the control
surface; it does not replace real authentication or a reverse proxy. The bundled
Kafka stack is a single-node lab with no TLS, SASL, ACLs or Schema Registry — it
shows the connector working, nothing more.

## Stack

Python 3.11 · FastAPI · PyMongo · React 18 · Vite · MongoDB Atlas.
Optional for module 07: a local Kafka broker and an Atlas Stream Processing
instance. No LLM anywhere in this PoV.

Tests and lint, from the repository root:

```bash
pip install -r backend/requirements-dev.txt
pytest                  # 127 tests, all unit — Mongo is stubbed, no cluster needed
ruff check backend
```

Architecture and per-file responsibilities live in
[`ARCHITECTURE.md`](ARCHITECTURE.md); the current decisions, trade-offs and
validation baseline are in
[`docs/SESSION_HANDOFF.md`](docs/SESSION_HANDOFF.md).

## License

MIT
