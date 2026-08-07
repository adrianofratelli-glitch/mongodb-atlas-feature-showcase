#!/usr/bin/env python3
"""Remove somente os artefatos de dados gerados pelo módulo PIX."""

from __future__ import annotations

import sys
from pathlib import Path

from pymongo import MongoClient


COLLECTIONS = (
    "transacoes",
    "metricas_janela",
    "dlq",
    "dlq_audit",
    "consumer_checkpoints",
)
# No database `geo`, e por isso fora da tupla acima.
COLLECTION_SINAIS = "sinais_ao_vivo"


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    env_file = root / "backend" / ".env"
    if not env_file.exists():
        print("❌ backend/.env não encontrado.", file=sys.stderr)
        return 1

    env = read_env(env_file)
    uri = env.get("MONGO_URI", "")
    database = env.get("STREAMING_DB", "pix") or "pix"
    ttl_seconds = int(env.get("STREAMING_TTL_SEGUNDOS", "300") or "300")
    if not uri:
        print("❌ MONGO_URI ausente em backend/.env.", file=sys.stderr)
        return 1

    client = MongoClient(
        uri,
        appname="mongodb-atlas-feature-showcase-cleanup",
        serverSelectionTimeoutMS=15_000,
        socketTimeoutMS=180_000,
    )
    try:
        db = client[database]
        existing = set(db.list_collection_names())
        removed: list[str] = []
        for name in COLLECTIONS:
            if name in existing:
                db[name].drop()
                removed.append(name)

        # Sinal materializado pelo processor de geo. Vive no database `geo`,
        # mas é dado DA RODADA, não do dataset versionado: cai na limpeza, e
        # `geo.transacoes` nunca é tocada aqui.
        geo_db = client[env.get("GEO_DB", "geo") or "geo"]
        if COLLECTION_SINAIS in set(geo_db.list_collection_names()):
            geo_db[COLLECTION_SINAIS].drop()
            removed.append(f"{geo_db.name}.{COLLECTION_SINAIS}")

        # A próxima execução já encontra os contratos mínimos da fonte prontos.
        db["transacoes"].create_index(
            "endToEndId",
            unique=True,
            name="endToEndId_unique",
        )
        db["transacoes"].create_index(
            "run_id",
            name="run_id_reconciliacao",
        )
        db["transacoes"].create_index(
            "ts",
            expireAfterSeconds=ttl_seconds,
            name="ts_ttl",
        )
    finally:
        client.close()

    summary = ", ".join(removed) if removed else "nenhuma coleção anterior"
    print(f"   dados PIX removidos de {database}: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
