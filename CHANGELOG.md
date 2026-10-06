# Changelog

## 1.1.0 (2026-10-06)

- Thesis page measures one real operation per capability through the same client (`POST /tese/medir`, p50/max with evidence) and no longer claims eight modules.
- `seed_data.py` is idempotent (deterministic upserts) and refuses non-`_test` databases without `ALLOW_DEMO_DB_WRITE=1`; new single reset `scripts/reset_demo.py` (`--check` for the launcher).
- `prepare-demo.sh` and `ambiente.sh up` no longer call the removed geo seed (they aborted `overview up`).
- Module 01 only drops indexes it created (`demo01_` prefix); module 02 only removes Online Archive rules of the demo database. Both previously reached data of other PoVs sharing the cluster.
- Empty or unreachable database is explained in the UI; the transaction demo no longer reports a rollback when there is simply no data; the archive list no longer hangs on "Carregando...".
- Benchmark and thesis measurements reject concurrent runs (409); `run_id` is validated.
- Adversarial test suite (`backend/tests/test_endpoints_adversarial.py`).
- Removed unused `DemoFlow` component and leftover geo CSS/env entries.
- Dependencies: pymongo 4.18.2 (4 advisories), source-map-js (npm high).
- UI: layout MongoDB 2026 "Dark Stage v4" (tokens mais escuros, Special Gothic / Source Code Pro locais, motivos de escada e grade, movimento escalonado).

## 1.0.0 (2026-09-30)

First public release.

- Repository rebuilt with a clean, single-commit history.
- English README and repository description, with screenshots captured against a real Atlas cluster.
- MIT license.
- Internal notes, presentation decks, test-output snapshots, and tooling configuration removed from the repository.
