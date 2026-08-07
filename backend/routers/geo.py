"""Módulo Geo — três demonstrações geoespaciais sobre `geo.transacoes`.

O objetivo é provar comportamento, não desenhar mapa bonito:

1. `POST /geo/explain-compare` — mesma query `$geoWithin`, dois `hint`: o índice
   composto `{clienteId, status, local}` e o `2dsphere` puro. O que aparece na
   tela é o `executionStats` medido, não a narrativa esperada.
2. `GET /geo/impossible-travel` — `$setWindowFields` + `$shift` + haversine em
   operadores MQL puros. Nenhum documento sai do cluster para o cálculo.
3. `POST /geo/search` — um único `$search` com relevância textual, filtro
   geográfico e categoria, mais `$searchMeta` para as facetas.

Todos os endpoints devolvem o pipeline executado: nada na tela vem de mock.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo.errors import OperationFailure

from database import client

router = APIRouter(prefix="/geo", tags=["Geo"])

GEO_DB = os.getenv("GEO_DB", "geo").strip() or "geo"
GEO_COLECAO = "transacoes"
GEO_SEARCH_INDEX = os.getenv("GEO_SEARCH_INDEX", "idx_geo_estabelecimento").strip() or "idx_geo_estabelecimento"

INDICE_COMPOSTO = "cliente_status_local_idx"
INDICE_GEO_PURO = "local_2dsphere_idx"

RAIO_TERRA_KM = 6371.0088
ARQUIVO_FRAUDES = Path(__file__).resolve().parent.parent / "data" / "fraud_seeds.json"

banco = client[GEO_DB]
colecao = banco[GEO_COLECAO]

# Cache do resumo de municípios: a lista muda apenas quando o seed roda de novo.
_municipios_cache: list[dict[str, Any]] | None = None


# ─────────────────────────────────────────────────────────────── helpers ────
def _haversine_stages(destino: str, lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> list[dict]:
    """Haversine em MQL puro — sem `$function` e sem trazer dado à aplicação.

    a  = sin²(Δφ/2) + cos φ₁ · cos φ₂ · sin²(Δλ/2)
    km = 2R · asin(√a)

    Tudo num único `$addFields` com `$let` aninhado: a versão em quatro stages
    encadeadas custava quatro passadas completas sobre o resultado da janela —
    medido em `executionStats`, era a maior fatia do tempo do pipeline.
    """
    return [{"$addFields": {destino: {"$let": {
        "vars": {
            "phi1": {"$degreesToRadians": lat1},
            "phi2": {"$degreesToRadians": lat2},
            "dphi": {"$degreesToRadians": {"$subtract": [lat2, lat1]}},
            "dlmb": {"$degreesToRadians": {"$subtract": [lng2, lng1]}},
        },
        "in": {"$let": {
            "vars": {"a": {"$add": [
                {"$pow": [{"$sin": {"$divide": ["$$dphi", 2]}}, 2]},
                {"$multiply": [
                    {"$cos": "$$phi1"},
                    {"$cos": "$$phi2"},
                    {"$pow": [{"$sin": {"$divide": ["$$dlmb", 2]}}, 2]},
                ]},
            ]}},
            # $min contra 1 protege o $asin de erro de arredondamento em pares
            # praticamente antipodais.
            "in": {"$multiply": [
                2 * RAIO_TERRA_KM,
                {"$asin": {"$sqrt": {"$min": ["$$a", 1]}}},
            ]},
        }},
    }}}}]


def _resumo_plano(explain: dict) -> dict[str, Any]:
    """Extrai do explain só o que a tela precisa comparar."""
    planner = explain.get("queryPlanner", {})
    vencedor = planner.get("winningPlan", {})
    # MongoDB 7+ aninha o plano clássico em `queryPlan` quando o SBE está ativo.
    raiz = vencedor.get("queryPlan", vencedor)

    estagios: list[str] = []
    indice = None
    nodo: dict | None = raiz
    while isinstance(nodo, dict):
        if nodo.get("stage"):
            estagios.append(nodo["stage"])
        if nodo.get("indexName"):
            indice = nodo["indexName"]
        proximo = nodo.get("inputStage")
        if proximo is None and nodo.get("inputStages"):
            proximo = nodo["inputStages"][0]
        nodo = proximo

    stats = explain.get("executionStats", {})
    return {
        "estagios": estagios,
        "estagio_vencedor": estagios[0] if estagios else None,
        "indice_usado": indice,
        "nReturned": stats.get("nReturned"),
        "totalKeysExamined": stats.get("totalKeysExamined"),
        "totalDocsExamined": stats.get("totalDocsExamined"),
        "executionTimeMillis": stats.get("executionTimeMillis"),
    }


def _explain_find(filtro: dict, hint: str) -> dict[str, Any]:
    comando = {
        "explain": {"find": GEO_COLECAO, "filter": filtro, "hint": hint},
        "verbosity": "executionStats",
    }
    return _resumo_plano(banco.command(comando))


def _search_disponivel() -> tuple[bool, str]:
    try:
        nomes = {indice.get("name") for indice in colecao.list_search_indexes()}
    except Exception as exc:  # driver antigo, permissão ou cluster sem Search
        return False, f"não foi possível listar os search indexes ({type(exc).__name__})"
    if GEO_SEARCH_INDEX in nomes:
        return True, "disponível"
    return False, f"crie o índice {GEO_SEARCH_INDEX} com scripts/create_search_index_geo.sh"


def _municipios() -> list[dict[str, Any]]:
    global _municipios_cache
    if _municipios_cache is None:
        _municipios_cache = list(colecao.aggregate([
            {"$group": {
                "_id": {"municipio": "$municipio", "uf": "$uf"},
                "transacoes": {"$sum": 1},
                "centro": {"$first": "$local.coordinates"},
            }},
            {"$sort": {"transacoes": -1}},
            {"$project": {
                "_id": 0,
                "municipio": "$_id.municipio",
                "uf": "$_id.uf",
                "transacoes": 1,
                "centro": 1,
            }},
        ], allowDiskUse=True))
    return _municipios_cache


def preflight_checks() -> dict[str, dict[str, Any]]:
    """Entra no `/preflight` do `main.py`; o Search é opcional e não reprova."""
    try:
        total = colecao.estimated_document_count()
    except Exception as exc:
        return {"geo_dataset": {"ok": False, "message": f"indisponível ({type(exc).__name__})"}}
    search_ok, search_msg = _search_disponivel()
    return {
        "geo_dataset": {
            "ok": total > 0,
            "message": f"{total} transações em {GEO_DB}.{GEO_COLECAO}" if total else "execute scripts/seed_geo.py",
        },
        "geo_search": {"ok": search_ok, "message": search_msg},
    }


# ──────────────────────────────────────────── sinal em event time (ASP) ────
COL_SINAIS = "sinais_ao_vivo"
sinais = banco[COL_SINAIS]


@router.get("/sinais-ao-vivo")
def sinais_ao_vivo(limite: int = Query(default=12, ge=1, le=50)):
    """
    Sinais de impossible travel materializados pelo processor `geoSinais30s`.

    A diferença para `/geo/impossible-travel` é o *quando*: aqui o cálculo já
    aconteceu na janela, na passagem do evento, e esta rota apenas lê o
    resultado. O painel sob demanda continua existindo — ele responde a
    investigação retrospectiva, que é outra pergunta.

    `plantados` e `emergentes` vêm separados de propósito: o gerador injeta
    pares para a demo ter sinal garantido, e misturar os dois números
    transformaria a garantia em prova.
    """
    try:
        recentes = list(
            sinais.find({}, {"pontos": 0})
            .sort("detectadoEm", -1)
            .limit(limite)
        )
        plantados = sinais.count_documents({"origem": "plantado"})
        emergentes = sinais.count_documents({"origem": "emergente"})
    except Exception as exc:  # noqa: BLE001 - o painel é opcional
        return {
            "estado": "indisponivel",
            "mensagem": f"{GEO_DB}.{COL_SINAIS} inacessível ({type(exc).__name__})",
            "sinais": [], "plantados": 0, "emergentes": 0,
        }

    for s in recentes:
        s["_id"] = str(s.get("_id"))
        for extremo in ("de", "para"):
            ponto = s.get(extremo) or {}
            if isinstance(ponto.get("ts"), object) and hasattr(ponto.get("ts"), "isoformat"):
                ponto["ts"] = ponto["ts"].isoformat()
        if hasattr(s.get("detectadoEm"), "isoformat"):
            s["detectadoEm"] = s["detectadoEm"].isoformat()

    return {
        "estado": "ok" if recentes else "sem_sinais",
        "colecao": f"{GEO_DB}.{COL_SINAIS}",
        "sinais": recentes,
        "plantados": plantados,
        "emergentes": emergentes,
        "total": plantados + emergentes,
    }


# ────────────────────────────────────────────────────────────── endpoints ────
@router.get("/status")
def status():
    """Estado do dataset, dos índices e do Atlas Search — sem nada hard-coded."""
    try:
        total = colecao.estimated_document_count()
        indices = [
            {"nome": nome, "chave": [[campo, tipo] for campo, tipo in definicao.get("key", [])]}
            for nome, definicao in sorted(colecao.index_information().items())
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Coleção {GEO_DB}.{GEO_COLECAO} indisponível: {type(exc).__name__}")

    search_ok, search_msg = _search_disponivel()
    fraudes = {}
    if ARQUIVO_FRAUDES.exists():
        dados = json.loads(ARQUIVO_FRAUDES.read_text(encoding="utf-8"))
        fraudes = {"clientes": len(dados.get("clientes", [])), "limite_kmh": dados.get("limite_kmh")}

    return {
        "db": GEO_DB,
        "colecao": GEO_COLECAO,
        "transacoes": total,
        "indices": indices,
        "search": {"index": GEO_SEARCH_INDEX, "disponivel": search_ok, "mensagem": search_msg},
        "fraudes_plantadas": fraudes,
    }


@router.get("/municipios")
def municipios():
    """Municípios presentes no dataset, com um ponto representativo para centrar o mapa."""
    return {"municipios": _municipios()}


class ExplainRequest(BaseModel):
    clienteId: str = Field(..., min_length=1, max_length=64)
    status: str = Field("APROVADA", max_length=32)
    raioKm: float = Field(50.0, gt=0, le=5_000)
    centro: list[float] = Field(..., min_length=2, max_length=2, description="[lng, lat]")


@router.post("/explain-compare")
def explain_compare(pedido: ExplainRequest):
    """Demo A — o mesmo `$geoWithin` sob dois índices diferentes.

    Nota didática: campos de igualdade primeiro, geo por último; o campo geo não
    precisa ser prefixo do índice para `$geoWithin`/`$geoIntersects`.

    Divergência medida (registrar aqui se aparecer): se em algum cenário o
    2dsphere puro examinar menos chaves que o composto, o número medido é que
    vale — a nota acima descreve o caso geral de igualdade + geo, não uma
    garantia para toda seletividade. Nenhum ajuste de texto para caber na
    narrativa.
    """
    lng, lat = pedido.centro
    if not (-180 <= lng <= 180 and -90 <= lat <= 90):
        raise HTTPException(status_code=422, detail="centro fora do intervalo [lng, lat] válido.")

    filtro = {
        "clienteId": pedido.clienteId,
        "status": pedido.status,
        "local": {"$geoWithin": {"$centerSphere": [[lng, lat], pedido.raioKm / RAIO_TERRA_KM]}},
    }

    try:
        composto = _explain_find(filtro, INDICE_COMPOSTO)
        geo_puro = _explain_find(filtro, INDICE_GEO_PURO)
    except OperationFailure as erro:
        raise HTTPException(status_code=409, detail=f"Explain falhou: {erro.details.get('errmsg', str(erro))}")

    return {
        "filtro": filtro,
        "query": (
            f'db.{GEO_COLECAO}.find({json.dumps(filtro, ensure_ascii=False)})\n'
            f'  .hint("<índice>")\n'
            f'  .explain("executionStats")'
        ),
        "planos": [
            {"rotulo": "Índice composto (igualdade + geo)", "hint": INDICE_COMPOSTO, **composto},
            {"rotulo": "2dsphere puro", "hint": INDICE_GEO_PURO, **geo_puro},
        ],
    }


@router.get("/impossible-travel")
def impossible_travel(
    limiteKmh: float = Query(900.0, gt=0, le=100_000),
    clienteId: str | None = Query(None, max_length=64),
    limite: int = Query(50, ge=1, le=500),
):
    """Demo B — pares do mesmo cliente com velocidade implícita acima do limite.

    `$setWindowFields` particiona por cliente, `$shift` traz a transação
    anterior e a distância sai de haversine em operadores nativos. O cálculo
    inteiro roda no cluster.
    """
    pipeline: list[dict] = []
    if clienteId:
        pipeline.append({"$match": {"clienteId": clienteId}})

    pipeline += [
        {"$setWindowFields": {
            "partitionBy": "$clienteId",
            "sortBy": {"ts": 1},
            "output": {
                "ts_ant": {"$shift": {"output": "$ts", "by": -1}},
                "coord_ant": {"$shift": {"output": "$local.coordinates", "by": -1}},
                "municipio_ant": {"$shift": {"output": "$municipio", "by": -1}},
                "uf_ant": {"$shift": {"output": "$uf", "by": -1}},
                "dispositivo_ant": {"$shift": {"output": "$dispositivo", "by": -1}},
                "localizacao_meta_ant": {"$shift": {"output": "$localizacaoMeta", "by": -1}},
            },
        }},
        # A primeira transação de cada cliente não tem anterior.
        {"$match": {"ts_ant": {"$ne": None}}},
        {"$addFields": {"minutos": {"$divide": [{"$subtract": ["$ts", "$ts_ant"]}, 60_000]}}},
        # Corte geométrico antes do haversine: nenhum par de pontos na Terra
        # dista mais que meia circunferência, então um intervalo maior que
        # (π·R / limite) horas não pode violar o limite, seja qual for a
        # geografia. Descarta a maior parte dos documentos antes da parte cara
        # do pipeline sem depender de nada específico deste dataset.
        {"$match": {
            "minutos": {"$gt": 0, "$lt": (math.pi * RAIO_TERRA_KM / limiteKmh) * 60},
        }},
    ]
    pipeline += _haversine_stages(
        "km",
        {"$arrayElemAt": ["$coord_ant", 1]},
        {"$arrayElemAt": ["$coord_ant", 0]},
        {"$arrayElemAt": ["$local.coordinates", 1]},
        {"$arrayElemAt": ["$local.coordinates", 0]},
    )
    pipeline += [
        {"$addFields": {"kmh": {"$divide": ["$km", {"$divide": ["$minutos", 60]}]}}},
        {"$match": {"kmh": {"$gt": limiteKmh}}},
        {"$sort": {"kmh": -1}},
        {"$limit": limite},
        {"$project": {
            "_id": 0,
            "clienteId": 1,
            "endToEndId": 1,
            "km": {"$round": ["$km", 1]},
            "minutos": {"$round": ["$minutos", 1]},
            "kmh": {"$round": ["$kmh", 0]},
            "de": {
                "municipio": "$municipio_ant", "uf": "$uf_ant", "coordinates": "$coord_ant",
                "dispositivo": "$dispositivo_ant", "localizacaoMeta": "$localizacao_meta_ant",
            },
            "para": {
                "municipio": "$municipio", "uf": "$uf", "coordinates": "$local.coordinates",
                "dispositivo": "$dispositivo", "localizacaoMeta": "$localizacaoMeta",
            },
            "ts_ant": 1,
            "ts": 1,
        }},
    ]

    resultados = list(colecao.aggregate(pipeline, allowDiskUse=True))
    return {
        "natureza": "sinal_de_risco_retrospectivo",
        "decisao_fraude": False,
        "limite_kmh": limiteKmh,
        "encontrados": len(resultados),
        "truncado": len(resultados) == limite,
        "pipeline": pipeline,
        "resultados": resultados,
    }


class SearchRequest(BaseModel):
    termo: str = Field(..., min_length=1, max_length=120)
    centro: list[float] = Field(..., min_length=2, max_length=2, description="[lng, lat]")
    raioKm: float = Field(25.0, gt=0, le=2_000)
    categorias: list[str] = Field(default_factory=list, max_length=10)
    limite: int = Field(20, ge=1, le=100)


@router.post("/search")
def geo_search(pedido: SearchRequest):
    """Demo C — relevância textual, filtro geográfico e facetas em uma stage.

    O `filter` do `$vectorSearch` não aceita operadores geoespaciais; aqui o
    caminho é `$search`, onde `geoWithin` é um operador de primeira classe.
    """
    disponivel, mensagem = _search_disponivel()
    if not disponivel:
        return {"estado": "nao_configurado", "mensagem": mensagem, "index": GEO_SEARCH_INDEX}

    lng, lat = pedido.centro
    if not (-180 <= lng <= 180 and -90 <= lat <= 90):
        raise HTTPException(status_code=422, detail="centro fora do intervalo [lng, lat] válido.")

    filtros: list[dict] = [{
        "geoWithin": {
            "path": "local",
            "circle": {
                "center": {"type": "Point", "coordinates": [lng, lat]},
                "radius": pedido.raioKm * 1_000,  # o operador usa metros
            },
        },
    }]
    if pedido.categorias:
        filtros.append({"in": {"path": "estabelecimento.categoria", "value": pedido.categorias}})

    compound = {
        "must": [{"text": {
            "query": pedido.termo,
            "path": "estabelecimento.nome",
            "fuzzy": {"maxEdits": 1},
        }}],
        "filter": filtros,
    }

    pipeline: list[dict] = [
        {"$search": {
            "index": GEO_SEARCH_INDEX,
            "compound": compound,
            "highlight": {"path": "estabelecimento.nome"},
        }},
        {"$addFields": {"score": {"$meta": "searchScore"}, "highlights": {"$meta": "searchHighlights"}}},
    ]
    # A distância volta calculada para a UI e para o teste de aceite: o raio
    # pedido é verificável sem confiar na palavra do operador.
    pipeline += _haversine_stages(
        "km_do_centro",
        lat, lng,
        {"$arrayElemAt": ["$local.coordinates", 1]},
        {"$arrayElemAt": ["$local.coordinates", 0]},
    )
    # A coleção contém compras, mas a pergunta da investigação é "quais
    # estabelecimentos existem aqui?". Deduplicar pelo terminal no cluster
    # impede que vinte compras da mesma maquininha ocupem vinte resultados.
    pipeline += [
        {"$sort": {"score": -1, "endToEndId": 1}},
        {"$group": {"_id": "$dispositivo.id", "documento": {"$first": "$$ROOT"}}},
        {"$replaceWith": "$documento"},
        {"$limit": pedido.limite},
        {"$project": {
        "_id": 0,
        "endToEndId": 1,
        "terminalId": "$dispositivo.id",
        "estabelecimento": 1,
        "municipio": 1,
        "uf": 1,
        "valor": {"$toString": "$valor"},
        "local": 1,
        "score": {"$round": ["$score", 3]},
        "highlights": 1,
        "km_do_centro": {"$round": ["$km_do_centro", 2]},
    }}]

    pipeline_meta = [{"$searchMeta": {
        "index": GEO_SEARCH_INDEX,
        "facet": {
            "operator": {"compound": compound},
            "facets": {
                "categoria": {"type": "string", "path": "estabelecimento.categoria"},
                "uf": {"type": "string", "path": "uf"},
            },
        },
    }}]

    try:
        resultados = list(colecao.aggregate(pipeline))
        meta = list(colecao.aggregate(pipeline_meta))
    except OperationFailure as erro:
        raise HTTPException(status_code=409, detail=f"$search falhou: {erro.details.get('errmsg', str(erro))}")

    return {
        "estado": "ok",
        "index": GEO_SEARCH_INDEX,
        "resultados": resultados,
        "meta": meta[0] if meta else {},
        "pipeline": pipeline,
        "pipeline_meta": pipeline_meta,
    }
