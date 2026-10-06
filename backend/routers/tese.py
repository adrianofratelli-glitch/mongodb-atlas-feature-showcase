"""Tese: "uma plataforma, N capacidades", medida agora.

`POST /tese/medir` executa, pelo MESMO `MongoClient` e no MESMO cluster que os
módulos usam, uma operação real de cada capacidade que dá para provar em
segundos, repete cada uma algumas vezes e devolve p50/máx em milissegundos.
Nenhum número é fixo no código: sem cluster, a resposta diz o que falhou.

Escreve somente em coleções `tese_probe*` do banco configurado e as dropa ao
final (o `scripts/reset_demo.py` também as remove). Uma medição por vez.
"""

from __future__ import annotations

import statistics
import threading
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pymongo.errors import OperationFailure, PyMongoError, WriteError

from database import client, db

router = APIRouter(prefix="/tese", tags=["Tese"])

REPETICOES = 5
COL_A = "tese_probe"
COL_B = "tese_probe_b"
COL_VALIDADO = "tese_probe_validado"
_lock = threading.Lock()

# Desenho, não medição: o que cada capacidade exige fora do Atlas numa stack
# montada por peças. Fica separado das medições e rotulado como tal na UI.
COMPONENTES_STACK = [
    {"capacidade": "Consulta indexada + agregação", "atlas": "cluster", "stack": "banco operacional"},
    {"capacidade": "Validação de schema no banco", "atlas": "cluster", "stack": "validação na aplicação ou banco relacional"},
    {"capacidade": "Change Streams (CDC ordenado)", "atlas": "cluster", "stack": "conector CDC + broker"},
    {"capacidade": "Transação multi-documento", "atlas": "cluster", "stack": "banco transacional"},
    {"capacidade": "Hot/cold tiering consultável", "atlas": "Online Archive (mesmo namespace)",
     "stack": "object storage + motor de consulta federada"},
    {"capacidade": "Janelas sobre o fluxo", "atlas": "Atlas Stream Processing", "stack": "processador de stream dedicado"},
]


def _ms(inicio: float) -> float:
    return (time.perf_counter() - inicio) * 1000


def _resumo(amostras: list[float]) -> dict:
    return {
        "p50_ms": round(statistics.median(amostras), 1),
        "max_ms": round(max(amostras), 1),
        "n": len(amostras),
    }


def _medir_ping() -> dict:
    amostras = []
    for _ in range(REPETICOES):
        t = time.perf_counter()
        client.admin.command("ping")
        amostras.append(_ms(t))
    return {**_resumo(amostras), "evidencia": "ida e volta de rede até o primário: o piso de toda linha abaixo"}


def _medir_consulta_indexada() -> dict:
    filtro = {"categoria": "Eletrônicos"}
    amostras = []
    for _ in range(REPETICOES):
        t = time.perf_counter()
        list(db["produtos"].find(filtro, {"_id": 0, "nome": 1}).sort("total_avaliacoes", -1).limit(10))
        amostras.append(_ms(t))
    plano = db["produtos"].find(filtro).sort("total_avaliacoes", -1).limit(10).explain()
    winning = plano.get("queryPlanner", {}).get("winningPlan", {})
    indice = None
    stack = [winning]
    while stack:
        etapa = stack.pop()
        if isinstance(etapa, dict):
            indice = indice or etapa.get("indexName")
            stack.extend(v for k, v in etapa.items() if k in ("inputStage", "queryPlan"))
            stack.extend(etapa.get("inputStages", []))
    return {**_resumo(amostras), "evidencia": f"índice usado: {indice or 'nenhum (COLLSCAN)'}"}


def _medir_agregacao() -> dict:
    pipeline = [
        {"$match": {"categoria": "Eletrônicos"}},
        {"$group": {"_id": "$marca", "produtos": {"$sum": 1}, "preco_medio": {"$avg": "$preco"}}},
        {"$sort": {"produtos": -1}},
        {"$limit": 5},
    ]
    amostras, grupos = [], 0
    for _ in range(REPETICOES):
        t = time.perf_counter()
        grupos = len(list(db["produtos"].aggregate(pipeline)))
        amostras.append(_ms(t))
    return {**_resumo(amostras), "evidencia": f"$match + $group + $sort: {grupos} grupos devolvidos"}


def _medir_schema() -> dict:
    db.drop_collection(COL_VALIDADO)
    db.create_collection(COL_VALIDADO, validator={"$jsonSchema": {
        "bsonType": "object", "required": ["valor"],
        "properties": {"valor": {"bsonType": "number", "minimum": 0}},
    }}, validationAction="error")
    amostras, rejeitados = [], 0
    for _ in range(REPETICOES):
        t = time.perf_counter()
        try:
            db[COL_VALIDADO].insert_one({"valor": -1})
        except WriteError as exc:
            if exc.code == 121:
                rejeitados += 1
        amostras.append(_ms(t))
    return {**_resumo(amostras), "evidencia": f"{rejeitados}/{REPETICOES} inserts inválidos recusados pelo banco (code 121)"}


def _medir_change_stream() -> dict:
    db[COL_A].insert_one({"aquecimento": True})
    amostras, entregues = [], 0
    with db[COL_A].watch(full_document="updateLookup", max_await_time_ms=200) as stream:
        for _ in range(REPETICOES):
            marca = uuid.uuid4().hex
            t = time.perf_counter()
            db[COL_A].insert_one({"marca": marca, "ts": datetime.now(timezone.utc)})
            prazo = time.monotonic() + 5
            while time.monotonic() < prazo:
                evento = stream.try_next()
                if evento and evento.get("fullDocument", {}).get("marca") == marca:
                    entregues += 1
                    break
            amostras.append(_ms(t))
    return {**_resumo(amostras), "evidencia": f"{entregues}/{REPETICOES} eventos entregues (insert → evento no cursor)"}


def _medir_transacao() -> dict:
    amostras, commits = [], 0
    for _ in range(REPETICOES):
        t = time.perf_counter()
        with client.start_session() as session:
            def _tx(s):
                pid = uuid.uuid4().hex
                db[COL_A].insert_one({"pedido": pid}, session=s)
                db[COL_B].insert_one({"pedido": pid}, session=s)
            session.with_transaction(_tx)
        commits += 1
        amostras.append(_ms(t))
    return {**_resumo(amostras), "evidencia": f"{commits}/{REPETICOES} commits em 2 coleções (with_transaction)"}


MEDICOES = (
    ("ping", "Ida e volta (ping)", _medir_ping),
    ("consulta_indexada", "Consulta indexada", _medir_consulta_indexada),
    ("agregacao", "Aggregation Pipeline", _medir_agregacao),
    ("schema", "Schema Validation", _medir_schema),
    ("change_stream", "Change Streams", _medir_change_stream),
    ("transacao", "Transação ACID", _medir_transacao),
)


def _executar() -> dict:
    resultados = []
    for chave, nome, fn in MEDICOES:
        try:
            resultados.append({"chave": chave, "capacidade": nome, "ok": True, **fn()})
        except (PyMongoError, OperationFailure) as exc:
            resultados.append({"chave": chave, "capacidade": nome, "ok": False, "erro": type(exc).__name__})
    for col in (COL_A, COL_B, COL_VALIDADO):
        try:
            db.drop_collection(col)
        except PyMongoError:
            pass
    nos = sorted(f"{h}:{p}" for h, p in client.nodes)
    return {
        "medido_em": datetime.now(timezone.utc).isoformat(),
        "banco": db.name,
        "repeticoes": REPETICOES,
        "conexoes": {"clientes_mongo": 1, "nos_do_replica_set": len(nos)},
        "resultados": resultados,
        "nao_medido_aqui": [
            {"capacidade": "Hot/Cold (Online Archive)", "onde": "módulo 02 (Atlas Admin API)"},
            {"capacidade": "Streaming (Kafka + ASP)", "onde": "módulo 07 (reconciliação medida)"},
            {"capacidade": "Reindexação online", "onde": "módulo 01 (leituras durante o build)"},
        ],
        "componentes": COMPONENTES_STACK,
        "aviso": "Latência inclui ida e volta de rede até o cluster. Prova funcional, não benchmark.",
    }


@router.post("/medir")
def medir():
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Já há uma medição em andamento. Aguarde alguns segundos.")
    try:
        return _executar()
    finally:
        _lock.release()


@router.get("/componentes")
def componentes():
    return {"componentes": COMPONENTES_STACK}
