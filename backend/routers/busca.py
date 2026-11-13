"""Módulo 08 · Busca — MongoDB Search (antes "Atlas Search") no mesmo cluster.

O que prova: busca textual com tolerância a erro de digitação e facetas,
servida pelo próprio cluster, sobre a coleção operacional, sem pipeline de
sincronização para um motor de busca à parte. O índice é alimentado pelos
change streams da coleção (consistência eventual — ver `Limites` na tela).

Regras de desenho:
- Índice próprio (`showcase_busca`). `produtos` pode ser compartilhada com
  outra PoV no mesmo banco (no cluster de demo, `POC.produtos` já tem
  `produtos_search`, do marketplace). Este módulo só cria o seu índice e
  nunca remove nem reaproveita o de outra demo.
- `$search` com índice inexistente devolve vazio SEM erro (doc "Query
  Reference › Empty Result Set"). Por isso a busca só roda depois que
  `list_search_indexes` confirma o índice `queryable`; antes disso a API diz
  "ausente"/"construindo" e não executa nada.
- O `pipeline` devolvido é exatamente o que foi executado (a UI mostra
  `body.pipeline`). Quando nada roda, a chave é `pipeline_nao_executado`.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, Query
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.operations import SearchIndexModel

from database import db

router = APIRouter(prefix="/busca", tags=["Busca"])

COLLECTION = "produtos"
INDICE = "showcase_busca"
LIMITE_RESULTADOS = 10
PROJECAO = {"_id": 0, "nome": 1, "categoria": 1, "marca": 1, "preco": 1, "em_estoque": 1}

# Mapeamento estático: só o que a tela consulta. Dinâmico indexaria todos os
# campos (custo de disco/memória do mongot sem uso aqui).
DEFINICAO = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "nome": {"type": "string", "analyzer": "lucene.portuguese"},
            "categoria": {"type": "token"},
            "marca": {"type": "token"},
            "em_estoque": {"type": "boolean"},
        },
    }
}


def _info_indice() -> dict:
    """Estado do índice próprio segundo `$listSearchIndexes`.

    estado: pronto | construindo | ausente | falhou | indisponivel
    (indisponivel = o servidor não tem MongoDB Search, p.ex. mongod local).
    """
    try:
        lista = list(db[COLLECTION].list_search_indexes(INDICE))
    except (OperationFailure, PyMongoError) as exc:
        return {"estado": "indisponivel", "nome": INDICE, "motivo": type(exc).__name__}
    if not lista:
        return {"estado": "ausente", "nome": INDICE}
    idx = lista[0]
    status = idx.get("status")
    queryable = bool(idx.get("queryable"))
    if queryable:
        estado = "pronto"
    elif status == "FAILED":
        estado = "falhou"
    else:
        estado = "construindo"
    return {"estado": estado, "nome": INDICE, "status": status, "queryable": queryable}


def comando_criacao() -> dict:
    """O comando que `POST /busca/indice` executa (mesma forma do mongosh)."""
    return {"createSearchIndexes": COLLECTION, "indexes": [{"name": INDICE, "definition": DEFINICAO}]}


@router.get("/indice")
def estado_indice():
    info = _info_indice()
    return {**info, "colecao": COLLECTION, "definicao": DEFINICAO}


@router.post("/indice")
def criar_indice():
    """Cria o índice próprio se faltar. Idempotente; nunca toca outro índice."""
    info = _info_indice()
    if info["estado"] == "indisponivel":
        return {**info, "criado": False}
    if info["estado"] != "ausente":
        return {**info, "criado": False, "query": comando_criacao()}
    try:
        db[COLLECTION].create_search_index(SearchIndexModel(definition=DEFINICAO, name=INDICE))
    except OperationFailure as exc:
        # Corrida com outra chamada: o índice já existe. Qualquer outra falha sobe.
        if "already exists" not in str(exc).lower():
            raise
    return {**_info_indice(), "criado": True, "query": comando_criacao()}


def montar_pipelines(q: str, fuzzy: bool, so_em_estoque: bool) -> dict:
    """Pipelines de resultado e de facetas para a mesma consulta."""
    texto: dict = {"query": q, "path": "nome"}
    if fuzzy:
        texto["fuzzy"] = {"maxEdits": 1}
    compound: dict = {"must": [{"text": texto}]}
    if so_em_estoque:
        compound["filter"] = [{"equals": {"path": "em_estoque", "value": True}}]
    resultados = [
        {"$search": {"index": INDICE, "compound": compound}},
        {"$limit": LIMITE_RESULTADOS},
        {"$project": {**PROJECAO, "score": {"$meta": "searchScore"}}},
    ]
    facetas = [
        {"$searchMeta": {
            "index": INDICE,
            "facet": {
                "operator": {"compound": compound},
                "facets": {
                    "categoria": {"type": "string", "path": "categoria"},
                    "marca": {"type": "string", "path": "marca"},
                },
            },
            "count": {"type": "total"},
        }},
    ]
    return {"resultados": resultados, "facetas": facetas}


@router.get("")
def buscar(
    q: str = Query(..., min_length=1, max_length=80),
    fuzzy: bool = True,
    so_em_estoque: bool = False,
):
    q = q.strip()
    pipelines = montar_pipelines(q, fuzzy, so_em_estoque)
    info = _info_indice()
    if info["estado"] != "pronto":
        return {
            "indice": info,
            "executado": False,
            "resultados": [],
            "facetas": None,
            "total": None,
            "pipeline_nao_executado": pipelines,
            "aviso": (
                "Busca não executada: o índice não está pronto. Um $search com índice "
                "inexistente devolve vazio sem erro, por isso a API confere o índice antes."
            ),
        }

    col = db[COLLECTION]
    t0 = time.perf_counter()
    docs = list(col.aggregate(pipelines["resultados"]))
    t_resultados = (time.perf_counter() - t0) * 1000
    t1 = time.perf_counter()
    meta = next(iter(col.aggregate(pipelines["facetas"])), {})
    t_facetas = (time.perf_counter() - t1) * 1000

    buckets = meta.get("facet", {})
    for d in docs:
        if "score" in d:
            d["score"] = round(d["score"], 3)
    return {
        "indice": info,
        "executado": True,
        "q": q,
        "fuzzy": fuzzy,
        "so_em_estoque": so_em_estoque,
        "resultados": docs,
        "total": meta.get("count", {}).get("total"),
        "facetas": {
            nome: [{"valor": b["_id"], "n": b["count"]} for b in buckets.get(nome, {}).get("buckets", [])]
            for nome in ("categoria", "marca")
        },
        "tempos_ms": {"resultados": round(t_resultados, 1), "facetas": round(t_facetas, 1)},
        "pipeline": pipelines,
    }
