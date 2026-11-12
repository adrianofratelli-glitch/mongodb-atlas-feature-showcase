#!/usr/bin/env python3
"""Remove somente os artefatos de dados gerados pelo módulo PIX.

Também é chamado por `scripts/reset_demo.py` (função `cleanup`). Variáveis do
ambiente têm precedência sobre `backend/.env`, o que permite apontar para um
banco próprio de teste (ex.: `STREAMING_DB=showcase_pix_test GEO_DB=showcase_geo_test`).

Guarda: sem `ALLOW_DEMO_DB_WRITE=1`, os dois bancos precisam seguir o formato
`showcase_*_test`. Um `*_test` genérico pode ser de outra PoV no mesmo cluster
(`geo_test` é o banco live do geo-showcase), e este script dropa coleções inteiras.
O banco da demo (`pix`/`geo` do backend/.env) só com a flag, como faz o `overview`.
"""

from __future__ import annotations

import os
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
ENV_KEYS = ("MONGO_URI", "STREAMING_DB", "GEO_DB", "STREAMING_TTL_SEGUNDOS")
ENV_FILE = Path(__file__).resolve().parents[1] / "backend" / ".env"
# Padrão do código quando o backend/.env não define a variável: é o banco da demo.
PADRAO_DEMO = {"STREAMING_DB": "pix", "GEO_DB": "geo"}


def banco_proprio(nome: str) -> bool:
    return bool(nome) and nome.startswith("showcase_") and nome.endswith("_test")


def banco_da_demo(variavel: str, env_file: Path | None = None) -> str:
    """Nome do banco da demo segundo o ARQUIVO backend/.env (ou o padrão do código).

    Lido do arquivo de propósito, sem o override do ambiente: a flag
    ALLOW_DEMO_DB_WRITE=1 autoriza limpar o banco que a PoV está configurada para
    usar, não qualquer nome exportado no shell (`GEO_DB=geo_test` é de outra PoV).
    """
    arquivo = ENV_FILE if env_file is None else env_file
    valores = read_env(arquivo) if arquivo.exists() else {}
    return valores.get(variavel) or PADRAO_DEMO[variavel]


def assert_banco_proprio(nome: str, variavel: str, env_file: Path | None = None) -> None:
    """Aceita `showcase_*_test`; com ALLOW_DEMO_DB_WRITE=1, também o banco da demo do backend/.env."""
    if banco_proprio(nome):
        return
    if os.getenv("ALLOW_DEMO_DB_WRITE") == "1":
        demo = banco_da_demo(variavel, env_file)
        if nome == demo:
            print(f"⚠  ALLOW_DEMO_DB_WRITE=1 — limpando dados de streaming em '{nome}' ({variavel}).")
            return
        raise SystemExit(
            f"❌ Recusado: {variavel}='{nome}' não é o banco da demo configurado em backend/.env "
            f"('{demo}') nem um banco próprio showcase_*_test. ALLOW_DEMO_DB_WRITE=1 só autoriza o "
            "banco do backend/.env; confira se há um override exportado no shell."
        )
    raise SystemExit(
        f"❌ Recusado: {variavel}='{nome}' não é um banco próprio desta PoV (showcase_*_test). "
        "Um *_test genérico pode ser de outra PoV. Para o banco da demo (o do backend/.env), rode com "
        "ALLOW_DEMO_DB_WRITE=1 (nunca durante uma demo ao vivo)."
    )


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def cleanup(client: MongoClient, database: str, ttl_seconds: int, geo_database: str) -> list[str]:
    """Dropa as coleções da rodada e recria os índices-contrato da fonte."""
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
    geo_db = client[geo_database]
    if COLLECTION_SINAIS in set(geo_db.list_collection_names()):
        geo_db[COLLECTION_SINAIS].drop()
        removed.append(f"{geo_db.name}.{COLLECTION_SINAIS}")

    # A próxima execução já encontra os contratos mínimos da fonte prontos.
    db["transacoes"].create_index("endToEndId", unique=True, name="endToEndId_unique")
    db["transacoes"].create_index("run_id", name="run_id_reconciliacao")
    db["transacoes"].create_index("ts", expireAfterSeconds=ttl_seconds, name="ts_ttl")
    return removed


def main() -> int:
    env_file = ENV_FILE
    # O ambiente tem precedência; sem backend/.env, basta o ambiente trazer MONGO_URI.
    base = read_env(env_file) if env_file.exists() else {}
    env = {**base, **{k: v for k, v in os.environ.items() if k in ENV_KEYS}}
    uri = env.get("MONGO_URI", "")
    database = env.get("STREAMING_DB", "pix") or "pix"
    geo_database = env.get("GEO_DB", "geo") or "geo"
    ttl_seconds = int(env.get("STREAMING_TTL_SEGUNDOS", "300") or "300")
    if not uri:
        print("❌ MONGO_URI ausente (ambiente e backend/.env).", file=sys.stderr)
        return 1
    assert_banco_proprio(database, "STREAMING_DB")
    assert_banco_proprio(geo_database, "GEO_DB")
    # Banco da demo autorizado pela flag: o cluster também é o do backend/.env, não
    # um MONGO_URI exportado no shell. Sem .env (CI), segue o do ambiente.
    if not (banco_proprio(database) and banco_proprio(geo_database)) and base.get("MONGO_URI"):
        uri = base["MONGO_URI"]

    client = MongoClient(
        uri,
        appname="mongodb-atlas-feature-showcase-cleanup",
        serverSelectionTimeoutMS=15_000,
        socketTimeoutMS=180_000,
    )
    try:
        removed = cleanup(client, database, ttl_seconds, geo_database)
    finally:
        client.close()

    summary = ", ".join(removed) if removed else "nenhuma coleção anterior"
    print(f"   dados PIX removidos de {database}: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
