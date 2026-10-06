#!/usr/bin/env python3
"""Reset único e idempotente da demo.

Recria o estado inicial de TODOS os módulos sem tocar em dado de outra PoV:

  1. dropa as coleções que só os módulos criam (`schema_demo`,
     `transacoes_cs_demo`, `pedidos_demo`/`pagamentos_demo`/`estoque_demo`,
     `bench_*`, `tese_probe*`, `_monitor_heartbeat`);
  2. remove os índices criados pelo módulo 01 (prefixo `demo01_`) em `produtos`;
  3. garante `produtos`/`avaliacoes` (upsert determinístico do `seed_data.py`;
     se a coleção já tem ao menos o volume pedido, não grava nada, porque no
     cluster de demo ela é compartilhada com o marketplace de busca) e os
     índices B-tree que os módulos usam;
  4. limpa os dados da rodada de streaming (`scripts/cleanup-streaming-data.py`).

Nunca dropa `produtos`/`avaliacoes` (perderia o índice Atlas Search de outra
PoV) e nunca mexe em Online Archive, tier ou stream processors.

Uso:
    MONGO_DB=POC_test STREAMING_DB=pix_test GEO_DB=geo_test python scripts/reset_demo.py
    ALLOW_DEMO_DB_WRITE=1 python scripts/reset_demo.py     # banco da demo (backend/.env)
    python scripts/reset_demo.py --check                    # só verifica, não escreve

Guarda: recusa qualquer banco que não termine em `_test` sem ALLOW_DEMO_DB_WRITE=1.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from dotenv import load_dotenv  # noqa: E402

# Variáveis já exportadas têm precedência sobre backend/.env.
load_dotenv(BACKEND / ".env", override=False)

from pymongo import MongoClient  # noqa: E402

import seed_data  # noqa: E402

DEMO_COLLECTIONS = (
    "schema_demo",
    "transacoes_cs_demo",
    "pedidos_demo",
    "pagamentos_demo",
    "estoque_demo",
    "bench_pedidos",
    "bench_estoque",
    "bench_pagamentos",
    "bench_ordens",
    "tese_probe",
    "tese_probe_b",
    "tese_probe_validado",
    "_monitor_heartbeat",
)
DEMO_INDEX_PREFIX = "demo01_"


def _load_cleanup():
    spec = importlib.util.spec_from_file_location("cleanup_streaming", ROOT / "scripts" / "cleanup-streaming-data.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(db) -> list[str]:
    """Problemas que impedem a demo; lista vazia = pronta."""
    problemas = []
    names = set(db.list_collection_names())
    for col in ("produtos", "avaliacoes"):
        if col not in names or db[col].estimated_document_count() == 0:
            problemas.append(f"{db.name}.{col} vazia ou ausente — rode scripts/reset_demo.py")
    if "produtos" in names:
        existentes = {i["name"] for i in db["produtos"].list_indexes()}
        for nome in ("produto_id_1", "categoria_1", "cat_total_av_idx"):
            if nome not in existentes:
                problemas.append(f"índice {nome} ausente em produtos")
    return problemas


def reset(db, streaming_db: str, geo_db: str, ttl: int, n_produtos: int, n_avaliacoes: int) -> dict:
    relatorio: dict = {"banco": db.name}
    names = set(db.list_collection_names())
    relatorio["colecoes_dropadas"] = [c for c in DEMO_COLLECTIONS if c in names]
    for col in relatorio["colecoes_dropadas"]:
        db[col].drop()

    relatorio["indices_removidos"] = []
    if "produtos" in names:
        for idx in db["produtos"].list_indexes():
            if idx["name"].startswith(DEMO_INDEX_PREFIX):
                db["produtos"].drop_index(idx["name"])
                relatorio["indices_removidos"].append(idx["name"])

    atual_p = db["produtos"].estimated_document_count() if "produtos" in names else 0
    atual_a = db["avaliacoes"].estimated_document_count() if "avaliacoes" in names else 0
    if atual_p >= n_produtos and atual_a >= n_avaliacoes:
        seed_data.ensure_indexes(db)
        relatorio["seed"] = f"mantido ({atual_p:,} produtos, {atual_a:,} avaliações já presentes); índices garantidos"
    else:
        seed_data.seed(n_produtos, n_avaliacoes, db=db, verbose=False)
        relatorio["seed"] = f"upsert de {n_produtos:,} produtos e {n_avaliacoes:,} avaliações"

    cleanup = _load_cleanup()
    relatorio["streaming"] = cleanup.cleanup(db.client, streaming_db, ttl, geo_db)
    return relatorio


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="somente verifica; não escreve")
    parser.add_argument("--produtos", type=int, default=100_000)
    parser.add_argument("--avaliacoes", type=int, default=20_000)
    args = parser.parse_args(argv)
    if args.produtos < 1 or args.avaliacoes < 0:
        parser.error("--produtos deve ser >= 1 e --avaliacoes >= 0.")

    uri = (os.getenv("MONGO_URI") or "").strip()
    if not uri:
        print("❌ MONGO_URI ausente (backend/.env).", file=sys.stderr)
        return 1
    db_name = os.getenv("MONGO_DB", "POC").strip() or "POC"
    streaming_db = os.getenv("STREAMING_DB", "pix").strip() or "pix"
    geo_db = os.getenv("GEO_DB", "geo").strip() or "geo"
    ttl = int(os.getenv("STREAMING_TTL_SEGUNDOS", "300") or "300")

    client = MongoClient(uri, appname="mongodb-atlas-feature-showcase-reset", serverSelectionTimeoutMS=15_000)
    try:
        db = client[db_name]
        if args.check:
            problemas = check(db)
            for p in problemas:
                print(f"❌ {p}")
            if not problemas:
                print(f"✅ {db_name}: produtos/avaliacoes e índices da demo presentes.")
            return 1 if problemas else 0

        for name in (db_name, streaming_db, geo_db):
            seed_data.assert_writable_db(name)
        inicio = time.perf_counter()
        rel = reset(db, streaming_db, geo_db, ttl, args.produtos, args.avaliacoes)
        dur = time.perf_counter() - inicio
        print(f"✅ Reset concluído em {dur:.1f} s no banco {rel['banco']}")
        print(f"   coleções dropadas : {', '.join(rel['colecoes_dropadas']) or 'nenhuma'}")
        print(f"   índices demo01_   : {', '.join(rel['indices_removidos']) or 'nenhum'}")
        print(f"   seed              : {rel['seed']}")
        print(f"   streaming ({streaming_db}): {', '.join(rel['streaming']) or 'nenhuma coleção anterior'}")
        problemas = check(db)
        for p in problemas:
            print(f"❌ {p}")
        return 1 if problemas else 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
