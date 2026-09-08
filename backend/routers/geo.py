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
import time
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
# Janela coberta pelo dataset (mesmo valor de `DIAS` em scripts/seed_geo.py).
# Serve para converter a contagem de sinais em alertas por dia, que é a unidade
# em que uma operação de risco raciocina.
DIAS_DATASET = 90

banco = client[GEO_DB]
colecao = banco[GEO_COLECAO]
# $geoNear recusa rodar (mesmo com hint) quando o campo geo tem mais de um
# índice 2dsphere — e `transacoes` tem dois de propósito (o puro e o composto
# da Demo A de explain). Cópia mantida por scripts/seed_geo.py com um índice
# só, dedicada a esse operador.
COLECAO_GEONEAR = "transacoes_geonear"
colecao_geonear = banco[COLECAO_GEONEAR]

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


def _clientes_plantados() -> set[str]:
    """IDs cujo par de impossible travel foi plantado pelo seed.

    Lido do arquivo que o próprio seed grava, e não de uma lista no código: se o
    dataset for regerado com outro número de casos, a marcação acompanha.
    """
    if not ARQUIVO_FRAUDES.exists():
        return set()
    try:
        dados = json.loads(ARQUIVO_FRAUDES.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return set()
    return {c["clienteId"] if isinstance(c, dict) else c for c in dados.get("clientes", [])}


def _milhar(n: int) -> str:
    """150000 → "150.000" — a tela é pt-BR e o número aparece dentro da frase."""
    return f"{n:,}".replace(",", ".")


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
        # A lista vai inteira para a UI: os campos da aba de Geo são seleções,
        # não texto livre — um clienteId digitado errado devolve tela vazia e no
        # palco isso é lido como "a demo não achou nada".
        ids = sorted(c["clienteId"] if isinstance(c, dict) else c for c in dados.get("clientes", []))
        fraudes = {"clientes": len(ids), "limite_kmh": dados.get("limite_kmh"), "lista": ids}

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
    ]

    # Corte geométrico antes do haversine: nenhum par de pontos na Terra dista
    # mais que meia circunferência, então um intervalo maior que
    # (π·R / limite) horas não pode violar o limite, seja qual for a geografia.
    # Descarta a maior parte dos documentos antes da parte cara do pipeline sem
    # depender de nada específico deste dataset.
    ramo_sinais: list[dict] = [
        {"$match": {
            "minutos": {"$gt": 0, "$lt": (math.pi * RAIO_TERRA_KM / limiteKmh) * 60},
        }},
    ]
    ramo_sinais += _haversine_stages(
        "km",
        {"$arrayElemAt": ["$coord_ant", 1]},
        {"$arrayElemAt": ["$coord_ant", 0]},
        {"$arrayElemAt": ["$local.coordinates", 1]},
        {"$arrayElemAt": ["$local.coordinates", 0]},
    )
    ramo_sinais += [
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

    # Contraponto do ramo sinalizado: uma amostra de pares comuns, dentro do
    # limite, pra mostrar as duas faces da mesma decisão — não só a fila de
    # suspeitos. `$sample` entra logo após o corte de "tem par anterior" pra não
    # rodar haversine sobre a coleção inteira; o corte geométrico do ramo
    # sinalizado não se aplica aqui porque o alvo é o oposto (ficar dentro do
    # limite), então a amostra roda haversine sobre um recorte aleatório pequeno.
    ramo_aprovados: list[dict] = [{"$sample": {"size": 200}}]
    ramo_aprovados += _haversine_stages(
        "km",
        {"$arrayElemAt": ["$coord_ant", 1]},
        {"$arrayElemAt": ["$coord_ant", 0]},
        {"$arrayElemAt": ["$local.coordinates", 1]},
        {"$arrayElemAt": ["$local.coordinates", 0]},
    )
    ramo_aprovados += [
        {"$addFields": {"kmh": {"$divide": ["$km", {"$divide": ["$minutos", 60]}]}}},
        {"$match": {"kmh": {"$lte": limiteKmh}}},
        {"$sort": {"ts": -1}},
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

    # `$facet` sobre o MESMO fluxo já particionado: o `$setWindowFields`, que é a
    # parte cara, roda uma vez só. O ramo `avaliados` conta os pares antes do
    # corte geométrico, porque a pergunta de um time de risco não é "quantos
    # sinais saíram" e sim "de quantas oportunidades" — sem denominador,
    # "40 pares" não diz se a regra é seletiva ou se inunda a fila de alertas.
    pipeline.append({"$facet": {
        "avaliados": [{"$count": "pares"}],
        "sinais": ramo_sinais,
        "aprovados": ramo_aprovados,
    }})

    inicio = time.perf_counter()
    saida = list(colecao.aggregate(pipeline, allowDiskUse=True))
    decorrido_ms = round((time.perf_counter() - inicio) * 1000, 1)
    bloco = saida[0] if saida else {}
    sinalizados = bloco.get("sinais", [])
    aprovados = bloco.get("aprovados", [])
    avaliados = (bloco.get("avaliados") or [{}])[0].get("pares", 0)

    for r in sinalizados:
        r["classificacao"] = "sinalizada"
    for r in aprovados:
        r["classificacao"] = "aprovada"

    # Intercala as duas classes em vez de ordenar por ts: a amostra aprovada
    # tende a concentrar datas mais recentes que os pares sinalizados, e ordenar
    # só por tempo empurrava todas as sinalizadas para o fim da tabela — quem
    # não rolasse até lá via só aprovação, o oposto do que este painel existe
    # para mostrar.
    resultados = []
    for i in range(max(len(sinalizados), len(aprovados))):
        if i < len(sinalizados):
            resultados.append(sinalizados[i])
        if i < len(aprovados):
            resultados.append(aprovados[i])

    universo = colecao.estimated_document_count()

    return {
        "natureza": "sinal_de_risco_retrospectivo",
        "decisao_fraude": False,
        "limite_kmh": limiteKmh,
        "encontrados": len(sinalizados),
        "encontrados_aprovados": len(aprovados),
        "truncado": len(sinalizados) == limite,
        "pipeline": pipeline,
        "resultados": resultados,
        # O volume operacional é a pergunta de quem opera a fila de alertas, e
        # vem antes de qualquer discussão sobre a qualidade do sinal.
        "seletividade": {
            "pares_avaliados": avaliados,
            "sinalizados": len(sinalizados),
            "taxa_pct": round(len(sinalizados) / avaliados * 100, 4) if avaliados else None,
            "alertas_por_dia": (
                round(len(sinalizados) / DIAS_DATASET, 2) if not clienteId else None
            ),
            "janela_dias": DIAS_DATASET if not clienteId else None,
            "nota": (
                "Taxa de sinalização sobre pares consecutivos do mesmo cliente, não sobre "
                "transações. Mede volume operacional — quantos casos chegariam à fila —, "
                "não acurácia: sem rótulo de fraude confirmada não existe precisão nem recall, "
                "e esta PoV não tem esse rótulo."
            ),
            # A ressalva mais importante da aba, e a mais fácil de omitir:
            # comparar esta taxa com a de um emissor real é comparar um dataset
            # sintético com uma medição real.
            "aviso": (
                "Este percentual descreve o dataset sintético desta demo, não um portfólio real. "
                "O que se transfere para uma conversa de produção é o método de medir volume "
                "operacional, e o custo da consulta — nunca o número em si."
            ),
        },
        # O custo tem de estar na tela junto com o resultado. Sem isto, a
        # pergunta "e sobre 90 dias reais?" fica sem resposta e o painel parece
        # prometer uma varredura de bilhões de documentos no mesmo tempo.
        "custo": {
            "ms": decorrido_ms,
            "documentos_no_escopo": universo if not clienteId else None,
            # O separador de milhar é formatado no número, isolado: aplicar o
            # replace na frase inteira comia a vírgula da própria frase.
            "escopo": (
                f"um cliente ({clienteId})" if clienteId
                else f"a coleção inteira, {_milhar(universo)} documentos"
            ),
            "complexidade": (
                "$setWindowFields particiona por cliente e ordena dentro da partição: o custo "
                "cresce com o volume varrido, não com o número de sinais encontrados."
            ),
            # Pergunta obrigatória de quem já apanhou de agregação em produção, e
            # que a tela não pode esperar ser feita para responder.
            "memoria": (
                "Cada partição é ordenada em memória, com o teto de 100 MB por stage. Aqui a "
                "consulta roda com allowDiskUse, então uma partição grande transborda para disco "
                "em vez de falhar — ao custo de I/O. Em produção o que mantém a partição pequena é "
                "o recorte (um cliente, uma janela de datas), não o tamanho da máquina."
            ),
            "leitura": (
                "Varredura completa é o modo de investigação: roda sob demanda, sobre o recorte "
                "que o analista pedir. Para decisão no fluxo, o caminho é o sinal em event time do "
                "processor geoSinais30s do módulo 07, que calcula na passagem e não varre "
                "histórico. Em produção, o recorte "
                "por cliente ou por período é o que mantém este mesmo pipeline barato — filtrar "
                "antes da janela reduz o universo, e o índice de clienteId sustenta o filtro."
            ),
        },
    }


def _poligono_quadrado(centro: list[float], raio_km: float) -> dict:
    """Quadrado aproximado ao redor do centro — não um círculo geodésico exato.

    Serve de geometria de teste para `$geoWithin`/`$geoIntersects` com
    `$geometry`: o objetivo é mostrar o operador executando sobre uma
    geometria real, não desenhar limite administrativo. A correção por
    `cos(lat)` evita que o quadrado fique achatado longe do equador — sem
    ela, 1° de longitude vale menos km perto dos polos que perto da linha do
    Equador, e o quadrado ficaria retangular sem motivo.
    """
    lng, lat = centro
    delta_lat = raio_km / 111.32
    delta_lng = raio_km / (111.32 * max(math.cos(math.radians(lat)), 0.1))
    # Perto do polo a correção por cos(lat) faz delta_lng explodir, e perto da
    # borda um delta_lat comum já empurra a latitude para fora de [-90, 90] —
    # nos dois casos o GeoJSON resultante é inválido e o driver recusa a
    # consulta (BadValue). O grampeamento mantém o polígono sempre válido, ao
    # custo de ficar menor que o raio pedido nesses extremos — situação que o
    # dataset desta PoV (só municípios do Brasil) nunca produz pela UI, mas que
    # a API aceita se chamada direto.
    lat_min = max(lat - delta_lat, -89.9)
    lat_max = min(lat + delta_lat, 89.9)
    lng_min = max(lng - delta_lng, -179.9)
    lng_max = min(lng + delta_lng, 179.9)
    return {
        "type": "Polygon",
        "coordinates": [[
            [lng_min, lat_min],
            [lng_max, lat_min],
            [lng_max, lat_max],
            [lng_min, lat_max],
            [lng_min, lat_min],
        ]],
    }


class OperadoresRequest(BaseModel):
    centro: list[float] = Field(..., min_length=2, max_length=2, description="[lng, lat]")
    raioKm: float = Field(50.0, gt=0, le=2_000)
    limite: int = Field(5, ge=1, le=20)


PROJECAO_OPERADORES = {"_id": 0, "endToEndId": 1, "municipio": 1, "uf": 1}


def _amostra(cursor, limite: int) -> list[dict]:
    return list(cursor.limit(limite))


def _geo_near_resultado(ponto: dict, raio_km: float, limite: int) -> dict:
    """`$geoNear` — o estágio de agregação, não o operador de find().

    Resolve exatamente a lacuna de `$near`/`$nearSphere`: roda dentro de um
    pipeline (aqui combinado com `$facet`, mas aceita `$match`/`$group`/etc
    normalmente) e devolve a distância calculada por documento — nenhum dos
    dois operadores de find() faz isso. A troca é a posição fixa: `$geoNear`
    precisa ser o primeiro estágio do pipeline.
    """
    pipeline = [
        {"$geoNear": {
            "near": ponto,
            "distanceField": "distanciaMetros",
            "maxDistance": raio_km * 1000,
            "spherical": True,
        }},
        {"$facet": {
            "amostra": [
                {"$limit": limite},
                {"$project": {
                    "_id": 0, "endToEndId": 1, "municipio": 1, "uf": 1,
                    "distanciaMetros": {"$round": ["$distanciaMetros", 0]},
                }},
            ],
            "total": [{"$count": "n"}],
        }},
    ]
    # Roda em colecao_geonear (índice único), não em colecao (dois índices
    # 2dsphere) — testei hint no aggregate(), "key" no próprio estágio e hint
    # cru via db.command, e o servidor recusou os três com "There is more than
    # one 2dsphere index". Sem escape via hint, a saída é isolar o dado.
    saida = list(colecao_geonear.aggregate(pipeline))
    bloco = saida[0] if saida else {}
    total = (bloco.get("total") or [{}])[0].get("n", 0)
    return {
        "operador": "$geoNear",
        "descricao": (
            "Estágio de agregação: ordena por distância E devolve a distância calculada, ao "
            "contrário de $near/$nearSphere. Combina com outros estágios no mesmo pipeline "
            "(aqui, $facet) — a troca é ter que ser o primeiro estágio."
        ),
        "query": pipeline,
        "contagem": total,
        "amostra": bloco.get("amostra", []),
    }


@router.post("/operadores")
def operadores_geo(pedido: OperadoresRequest):
    """Demo B2 — os cinco operadores/estágios de consulta geoespacial do
    MongoDB lado a lado, sobre a mesma geometria: `$geoWithin`, `$geoIntersects`,
    `$near`, `$nearSphere` (todos com `$geometry`) e `$geoNear`.

    `$near`/`$nearSphere` são operadores de `find()`: não funcionam dentro de
    `$match` de um pipeline de agregação, então não têm `count_documents` aqui
    — a mesma restrição do MongoDB, não uma limitação desta demo. `$geoNear`
    resolve isso: é o caminho de agregação, primeiro estágio obrigatório.
    """
    lng, lat = pedido.centro
    if not (-180 <= lng <= 180 and -90 <= lat <= 90):
        raise HTTPException(status_code=422, detail="centro fora do intervalo [lng, lat] válido.")

    poligono = _poligono_quadrado(pedido.centro, pedido.raioKm)
    ponto = {"type": "Point", "coordinates": [lng, lat]}

    filtro_within = {"local": {"$geoWithin": {"$geometry": poligono}}}
    filtro_intersects = {"local": {"$geoIntersects": {"$geometry": poligono}}}
    filtro_near = {"local": {"$near": {"$geometry": ponto, "$maxDistance": pedido.raioKm * 1000}}}
    filtro_near_sphere = {"local": {"$nearSphere": {"$geometry": ponto, "$maxDistance": pedido.raioKm * 1000}}}

    return {
        "centro": pedido.centro,
        "raioKm": pedido.raioKm,
        "poligono": poligono,
        "resultados": [
            {
                "operador": "$geoWithin",
                "descricao": "Documentos cujo ponto está inteiramente dentro da geometria informada.",
                "query": filtro_within,
                "contagem": colecao.count_documents(filtro_within),
                "amostra": _amostra(colecao.find(filtro_within, PROJECAO_OPERADORES), pedido.limite),
            },
            {
                "operador": "$geoIntersects",
                "descricao": (
                    "Documentos cuja geometria cruza a geometria informada. Sobre dados do tipo "
                    "Point, coincide com $geoWithin — a diferença aparece com LineString/Polygon "
                    "armazenados, que este dataset não tem."
                ),
                "query": filtro_intersects,
                "contagem": colecao.count_documents(filtro_intersects),
                "amostra": _amostra(colecao.find(filtro_intersects, PROJECAO_OPERADORES), pedido.limite),
            },
            {
                "operador": "$near",
                "descricao": (
                    "Ordena por proximidade ao ponto; exige índice geoespacial. Não devolve a "
                    "distância calculada nem tem contagem — operador de find(), não de agregação."
                ),
                "query": filtro_near,
                "contagem": None,
                "amostra": _amostra(colecao.find(filtro_near, PROJECAO_OPERADORES), pedido.limite),
            },
            {
                "operador": "$nearSphere",
                "descricao": (
                    "Mesma ordenação de $near, mas sempre esférica. Sobre índice 2dsphere com dado "
                    "GeoJSON os dois convergem — a diferença só aparece com índice 2d legado."
                ),
                "query": filtro_near_sphere,
                "contagem": None,
                "amostra": _amostra(colecao.find(filtro_near_sphere, PROJECAO_OPERADORES), pedido.limite),
            },
            _geo_near_resultado(ponto, pedido.raioKm, pedido.limite),
        ],
    }


class SearchRequest(BaseModel):
    # O termo deixou de ser obrigatório: a pergunta da investigação é "o que
    # existe em volta deste terminal", e o nome é um refinamento opcional em
    # cima dela — não o ponto de partida.
    termo: str = Field("", max_length=120)
    centro: list[float] | None = Field(None, min_length=2, max_length=2, description="[lng, lat]")
    raioKm: float = Field(25.0, gt=0, le=2_000)
    categorias: list[str] = Field(default_factory=list, max_length=10)
    limite: int = Field(20, ge=1, le=100)
    # Âncora da investigação: a compra contestada. Quando vem preenchida, o
    # centro sai da coordenada do terminal dela, e não de um município escolhido
    # num select — que é o que tornava o painel uma busca de catálogo.
    endToEndId: str | None = Field(None, max_length=64)


def _ancora_da_contestacao(end_to_end_id: str) -> dict[str, Any]:
    """A compra sob disputa: o ponto de partida real de uma investigação."""
    doc = colecao.find_one(
        {"endToEndId": end_to_end_id},
        {
            "_id": 0, "endToEndId": 1, "clienteId": 1, "estabelecimento": 1,
            "municipio": 1, "uf": 1, "local": 1, "dispositivo": 1, "ts": 1,
            "valor": 1, "status": 1, "localizacaoMeta": 1,
        },
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Transação {end_to_end_id} não encontrada.")
    doc["valor"] = str(doc.get("valor"))
    if hasattr(doc.get("ts"), "isoformat"):
        doc["ts"] = doc["ts"].isoformat()
    return doc


@router.post("/search")
def geo_search(pedido: SearchRequest):
    """Demo C — relevância textual, filtro geográfico e facetas em uma stage.

    O `filter` do `$vectorSearch` não aceita operadores geoespaciais; aqui o
    caminho é `$search`, onde `geoWithin` é um operador de primeira classe.
    """
    disponivel, mensagem = _search_disponivel()
    if not disponivel:
        return {"estado": "nao_configurado", "mensagem": mensagem, "index": GEO_SEARCH_INDEX}

    ancora = _ancora_da_contestacao(pedido.endToEndId) if pedido.endToEndId else None
    centro = (ancora["local"]["coordinates"] if ancora else pedido.centro)
    if not centro:
        raise HTTPException(
            status_code=422,
            detail="informe `endToEndId` da compra contestada ou um `centro` [lng, lat].",
        )

    lng, lat = centro
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

    # Sem termo, a pergunta é "o que existe aqui" e o `must` vira a existência do
    # próprio campo: o compound precisa de ao menos uma cláusula pontuável, e um
    # compound só de `filter` devolveria tudo com score zero.
    termo = pedido.termo.strip()
    clausula = (
        {"text": {"query": termo, "path": "estabelecimento.nome", "fuzzy": {"maxEdits": 1}}}
        if termo
        else {"exists": {"path": "estabelecimento.nome"}}
    )
    compound = {"must": [clausula], "filter": filtros}

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
        # Sem termo todos empatam no score, e "os mais relevantes" viraria uma
        # ordem arbitrária: a pergunta, aí, é geográfica. O critério de
        # desempate entra ANTES da deduplicação por terminal, para que o
        # documento escolhido de cada terminal seja o certo.
        {"$sort": ({"score": -1, "endToEndId": 1} if termo
                   else {"km_do_centro": 1, "endToEndId": 1})},
        {"$group": {"_id": "$dispositivo.id", "documento": {"$first": "$$ROOT"}}},
        {"$replaceWith": "$documento"},
        {"$sort": ({"score": -1} if termo else {"km_do_centro": 1})},
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

    # O terminal da própria compra contestada aparece na vizinhança; marcá-lo
    # evita que o analista o confunda com um estabelecimento vizinho.
    terminal_ancora = (ancora or {}).get("dispositivo", {}).get("id")
    for r in resultados:
        r["e_a_ancora"] = bool(terminal_ancora) and r.get("terminalId") == terminal_ancora

    return {
        "estado": "ok",
        "index": GEO_SEARCH_INDEX,
        "ancora": ancora,
        "centro": [lng, lat],
        "ordenacao": "relevância textual" if termo else "distância do terminal",
        "resultados": resultados,
        "meta": meta[0] if meta else {},
        "pipeline": pipeline,
        "pipeline_meta": pipeline_meta,
    }
