from fastapi import APIRouter, Path, Query
from database import db
from datetime import datetime, timedelta, timezone
import json
import math
import requests
import logging
from requests.auth import HTTPDigestAuth
from settings import settings

router = APIRouter(prefix="/hot-cold", tags=["Hot/Cold"])
logger = logging.getLogger("showcase.hot_cold")

COLLECTION = "produtos"
ATLAS_PUBLIC_KEY  = settings.atlas_public_key
ATLAS_PRIVATE_KEY = settings.atlas_private_key
ATLAS_PROJECT_ID  = settings.atlas_project_id
ATLAS_CLUSTER     = settings.atlas_cluster
ATLAS_BASE        = "https://cloud.mongodb.com/api/atlas/v2"
ATLAS_ACCEPT      = "application/vnd.atlas.2025-03-12+json"


class AtlasUnavailable(Exception):
    """Erro amigável quando a Atlas Admin API não está acessível/configurada."""


def _atlas_friendly_error(resp=None, exc=None) -> str:
    """Converte falhas da Atlas API em uma mensagem clara e acionável."""
    if exc is not None:
        logger.warning("Atlas Admin API indisponível: %s", type(exc).__name__)
        return "Não foi possível alcançar a Atlas Admin API. Verifique conectividade e access list."
    try:
        body = resp.json()
    except Exception:
        body = {}
    code = body.get("errorCode", "")
    if code == "IP_ADDRESS_NOT_ON_ACCESS_LIST":
        parameters = body.get("parameters")
        if isinstance(parameters, list) and parameters:
            ip = parameters[0]
        elif isinstance(parameters, dict) and parameters:
            ip = next(iter(parameters.values()))
        else:
            ip = "seu IP"
        return (
            f"A API key do Atlas não autoriza o IP {ip}. "
            "Adicione-o em Access Manager → API Keys → Access List "
            "(ou em Project Settings) e tente novamente."
        )
    if resp.status_code == 401:
        return "Credenciais da Atlas API inválidas (ATLAS_PUBLIC_KEY / ATLAS_PRIVATE_KEY)."
    return body.get("detail") or f"Atlas API retornou HTTP {resp.status_code}."


def _atlas_request(method: str, url: str, **kwargs) -> dict:
    """Faz a chamada à Atlas API e levanta AtlasUnavailable com mensagem amigável."""
    if not (ATLAS_PUBLIC_KEY and ATLAS_PRIVATE_KEY and ATLAS_PROJECT_ID):
        raise AtlasUnavailable(
            "Credenciais da Atlas API não configuradas no backend "
            "(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID)."
        )
    try:
        headers = {"Accept": ATLAS_ACCEPT}
        headers.update(kwargs.pop("headers", {}))
        resp = requests.request(
            method, url, auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY),
            timeout=15, headers=headers, **kwargs,
        )
    except requests.RequestException as e:
        raise AtlasUnavailable(_atlas_friendly_error(exc=e))
    if not 200 <= resp.status_code < 300:
        raise AtlasUnavailable(_atlas_friendly_error(resp=resp))
    return resp.json() if resp.text else {}


SAMPLE_SIZE = 5_000

# Cutoff default de 365 dias, usado até que o usuário crie de fato uma regra
# de Online Archive com outro `expire_after_days` (30-3650). A partir daí,
# `/distribution` e `/archive-simulation` passam a usar o valor real
# configurado — sem isto a simulação continuava fixa em 365 dias mesmo depois
# do usuário escolher, digamos, 90 ou 730 em `POST /online-archive/create`,
# e a demo mostrava um corte que não é o que foi de fato provisionado. Mesmo
# padrão de estado em runtime já usado neste módulo/arquivo pelo Atlas
# (nenhum global novo de infraestrutura, só um dict simples).
_ultimo_archive_cfg = {"expire_after_days": 365}


def _cutoff_atual() -> tuple[datetime, int]:
    dias = _ultimo_archive_cfg["expire_after_days"]
    return datetime.now(timezone.utc) - timedelta(days=dias), dias


def _margem_erro_estimada(count_extrapolado: int, sampled_count: int, sampled_total: int, total_docs: int) -> float:
    """
    Margem de erro (1 desvio-padrão) do count extrapolado a partir de `$sample`.

    `count` já é `round(p_amostral * total_docs)` — uma extrapolação, não uma
    contagem exata. O erro padrão de uma proporção amostral p com tamanho de
    amostra n é `sqrt(p*(1-p)/n)`; convertido para a escala do count
    extrapolado (multiplicando por total_docs), isso vira a margem de erro
    absoluta que falta no payload hoje. Mesmo rigor que streaming.py e
    transactions.py já aplicam a métricas medidas — aqui a métrica é
    estimada, então a margem é a honestidade equivalente.
    """
    if sampled_total <= 0 or total_docs <= 0:
        return 0.0
    p = sampled_count / sampled_total
    erro_padrao_proporcao = math.sqrt(max(p * (1 - p), 0) / sampled_total)
    return round(erro_padrao_proporcao * total_docs, 1)


@router.get("/distribution")
def data_distribution():
    """
    Distribuição de documentos por ano. Para resposta instantânea na demo,
    roda sobre uma amostra aleatória ($sample) e extrapola as contagens para
    o total da coleção, em vez de varrer os 5M de documentos.

    Cada `count` é uma extrapolação, não uma contagem exata — por isso cada
    bucket carrega `margem_erro_estimada` (1 desvio-padrão do erro binomial
    da proporção amostral, na mesma escala do count extrapolado).
    """
    pipeline = [
        {"$sample": {"size": SAMPLE_SIZE}},
        {"$group": {"_id": {"$year": "$created_at"}, "count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}}},
        {"$sort": {"_id": -1}},
        {"$limit": 10},
    ]
    result = list(db[COLLECTION].aggregate(pipeline))

    total_docs   = db[COLLECTION].estimated_document_count()
    sampled      = sum(r["count"] for r in result) or 1
    factor       = total_docs / sampled  # extrapola amostra → total
    cutoff, cutoff_dias = _cutoff_atual()

    rows = []
    for r in result:
        year = r["_id"]
        count_extrapolado = round(r["count"] * factor)
        rows.append({
            "year": year,
            "count": count_extrapolado,
            "margem_erro_estimada": _margem_erro_estimada(count_extrapolado, r["count"], sampled, total_docs),
            "avg_preco": round(r["avg_preco"] or 0, 2),
            "tier": (
                "🔥 Hot (ativo)" if year and year > cutoff.year
                else "🌓 Misto (ano do corte)" if year == cutoff.year
                else "❄️  Cold (arquivo)"
            ),
        })
    return {
        "distribution": rows,
        "sampled": True,
        "sample_size": SAMPLE_SIZE,
        "total_docs": total_docs,
        "cutoff_dias": cutoff_dias,
        "note": (
            f"Estimativa a partir de uma amostra de {SAMPLE_SIZE:,} documentos "
            "(resposta instantânea); cada count carrega margem_erro_estimada "
            "(1 desvio-padrão). Documentos com mais de "
            f"{cutoff_dias} dias seriam movidos automaticamente para o Online Archive."
        ),
    }


@router.get("/archive-simulation")
def archive_simulation():
    cutoff, cutoff_dias = _cutoff_atual()

    # Amostra para split hot/cold instantâneo, extrapolado para o total.
    pipeline = [
        {"$sample": {"size": SAMPLE_SIZE}},
        {"$group": {
            "_id": None,
            "hot":  {"$sum": {"$cond": [{"$gte": ["$created_at", cutoff]}, 1, 0]}},
            "cold": {"$sum": {"$cond": [{"$lt":  ["$created_at", cutoff]}, 1, 0]}},
        }},
    ]
    agg = list(db[COLLECTION].aggregate(pipeline))
    sampled_hot  = agg[0]["hot"]  if agg else 0
    sampled_cold = agg[0]["cold"] if agg else 0
    sampled = (sampled_hot + sampled_cold) or 1
    total   = db[COLLECTION].estimated_document_count()
    hot_count  = round(total * sampled_hot  / sampled)
    cold_count = round(total * sampled_cold / sampled)
    return {
        "hot":  {
            "count": hot_count, "pct": round(hot_count / total * 100, 1) if total else 0,
            "margem_erro_estimada": _margem_erro_estimada(hot_count, sampled_hot, sampled, total),
            "tier": "Cluster Atlas", "latency": "latência depende do tier e da região",
        },
        "cold": {
            "count": cold_count, "pct": round(cold_count / total * 100, 1) if total else 0,
            "margem_erro_estimada": _margem_erro_estimada(cold_count, sampled_cold, sampled, total),
            "tier": "Online Archive (Object Storage)", "latency": "latência depende da consulta federada",
        },
        "cutoff_dias": cutoff_dias,
        "savings_estimate": "Potencial de redução de storage: valide com região, retenção, compressão e padrão de leitura",
        "transparencia": "Endpoint federado dedicado — uma única query lê hot + cold, sem mudar o código de leitura",
    }


@router.get("/query-transparent")
def query_transparent(categoria: str = Query("Eletrônicos", min_length=2, max_length=40)):
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)

    def fmt(docs):
        for d in docs:
            if "created_at" in d and d["created_at"]:
                d["created_at"] = d["created_at"].isoformat()
        return docs

    hot  = list(db[COLLECTION].find({"categoria": categoria, "created_at": {"$gte": cutoff}}, {"nome": 1, "preco": 1, "created_at": 1, "_id": 0}).limit(3))
    cold = list(db[COLLECTION].find({"categoria": categoria, "created_at": {"$lt":  cutoff}}, {"nome": 1, "preco": 1, "created_at": 1, "_id": 0}).limit(3))

    return {
        "query_used": (
            f"db.produtos.find({{ categoria: {json.dumps(categoria, ensure_ascii=False)} }}) "
            "// via endpoint federado (cluster + archive)"
        ),
        "explanation": (
            "Com o Online Archive ativo, o Atlas expõe um endpoint federado dedicado: "
            "nele, UMA query sem filtro de data retorna documentos de AMBAS as camadas "
            "(hot e cold) — a query é a mesma, sem mudança de código. Leituras analíticas "
            "apontam para esse endpoint; escritas seguem no endpoint do cluster. "
            "(Abaixo, uma simulação didática do resultado usando o corte de 1 ano.)"
        ),
        "hot_samples":  fmt(hot),
        "cold_samples": fmt(cold),
    }


@router.get("/online-archive/list")
def list_online_archives():
    """Lista as regras de Online Archive configuradas no cluster via Atlas API."""
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives"
    try:
        data = _atlas_request("GET", url)
    except AtlasUnavailable as e:
        # Não dispara erro no front: a tela mostra um aviso acionável.
        return {"archives": [], "atlas_error": str(e)}
    archives = data.get("results", [])
    return {
        "archives": [
            {
                "id": a.get("id") or a.get("_id"),
                "status": a.get("state"),
                "collection": a.get("collName"),
                "date_field": a.get("criteria", {}).get("dateField"),
                "expire_after_days": a.get("criteria", {}).get("expireAfterDays"),
            }
            for a in archives
        ]
    }


@router.post("/online-archive/create")
def create_online_archive(expire_after_days: int = Query(365, ge=30, le=3650)):
    """
    Cria uma regra de Online Archive via Atlas API.
    Documentos com mais de expire_after_days dias serão movidos automaticamente.
    """
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives"
    payload = {
        "collName": COLLECTION,
        "dbName": settings.mongo_db,
        "criteria": {
            "type": "DATE",
            "dateField": "created_at",
            "dateFormat": "ISODATE",
            "expireAfterDays": expire_after_days,
        },
        # Sem dataExpirationRule de propósito: expiração APAGA dados do archive —
        # perigoso demais para uma demo sobre a coleção que os outros módulos usam.
        "partitionFields": [
            {"fieldName": "categoria",   "order": 0},
            {"fieldName": "created_at",  "order": 1},
        ],
        "schedule": {"type": "DEFAULT"},
    }
    try:
        result = _atlas_request("POST", url, json=payload)
    except AtlasUnavailable as e:
        return {"atlas_error": str(e)}
    # A simulação (`/distribution`, `/archive-simulation`) passa a usar este
    # cutoff real em vez do default de 365 dias — ver `_cutoff_atual()`.
    _ultimo_archive_cfg["expire_after_days"] = expire_after_days
    return {
        "archive_id": result.get("id") or result.get("_id"),
        "status": result.get("state"),
        "message": f"Regra criada: documentos com mais de {expire_after_days} dias serão arquivados automaticamente.",
        "collection": COLLECTION,
        "date_field": "created_at",
        "expire_after_days": expire_after_days,
        "atlas_url": f"https://cloud.mongodb.com/v2/{ATLAS_PROJECT_ID}#/clusters/onlineArchive/{ATLAS_CLUSTER}",
    }


@router.delete("/online-archive/{archive_id}")
def delete_online_archive(
    archive_id: str = Path(..., min_length=8, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
):
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives/{archive_id}"
    try:
        _atlas_request("DELETE", url)
    except AtlasUnavailable as e:
        return {"atlas_error": str(e)}
    return {"deleted": archive_id}
