from fastapi import APIRouter, Path, Query
from database import db
from datetime import datetime, timezone
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
ATLAS_BASE        = "https://cloud.mongodb.com/api/atlas/v1.0"


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
        ip = (body.get("parameters") or ["seu IP"])[0]
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
        resp = requests.request(
            method, url, auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY),
            timeout=15, **kwargs,
        )
    except requests.RequestException as e:
        raise AtlasUnavailable(_atlas_friendly_error(exc=e))
    if resp.status_code not in (200, 201, 204):
        raise AtlasUnavailable(_atlas_friendly_error(resp=resp))
    return resp.json() if resp.text else {}


SAMPLE_SIZE = 5_000


@router.get("/distribution")
def data_distribution():
    """
    Distribuição de documentos por ano. Para resposta instantânea na demo,
    roda sobre uma amostra aleatória ($sample) e extrapola as contagens para
    o total da coleção, em vez de varrer os 5M de documentos.
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
    current_year = datetime.now(timezone.utc).year

    rows = []
    for r in result:
        year = r["_id"]
        rows.append({
            "year": year,
            "count": round(r["count"] * factor),
            "avg_preco": round(r["avg_preco"] or 0, 2),
            "tier": "🔥 Hot (ativo)" if year and year >= current_year - 1 else "❄️  Cold (arquivo)",
        })
    return {
        "distribution": rows,
        "sampled": True,
        "sample_size": SAMPLE_SIZE,
        "total_docs": total_docs,
        "note": (
            f"Estimativa a partir de uma amostra de {SAMPLE_SIZE:,} documentos "
            "(resposta instantânea). Documentos com mais de 1 ano seriam movidos "
            "automaticamente para o Online Archive."
        ),
    }


@router.get("/archive-simulation")
def archive_simulation():
    current_year = datetime.now(timezone.utc).year
    cutoff = datetime(current_year - 1, 1, 1, tzinfo=timezone.utc)

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
        "hot":  {"count": hot_count,  "pct": round(hot_count / total * 100, 1) if total else 0, "tier": "Cluster Atlas (NVMe SSD)", "latency": "< 5ms"},
        "cold": {"count": cold_count, "pct": round(cold_count / total * 100, 1) if total else 0, "tier": "Online Archive (Object Storage)", "latency": "~ 100-300ms"},
        "savings_estimate": "Redução de ~60-80% no custo de armazenamento para dados históricos",
        "transparencia": "Endpoint federado dedicado — uma única query lê hot + cold, sem mudar o código de leitura",
    }


@router.get("/query-transparent")
def query_transparent(categoria: str = Query("Eletrônicos", min_length=2, max_length=40)):
    current_year = datetime.now(timezone.utc).year
    cutoff = datetime(current_year - 1, 1, 1, tzinfo=timezone.utc)

    def fmt(docs):
        for d in docs:
            if "created_at" in d and d["created_at"]:
                d["created_at"] = d["created_at"].isoformat()
        return docs

    hot  = list(db[COLLECTION].find({"categoria": categoria, "created_at": {"$gte": cutoff}}, {"nome": 1, "preco": 1, "created_at": 1, "_id": 0}).limit(3))
    cold = list(db[COLLECTION].find({"categoria": categoria, "created_at": {"$lt":  cutoff}}, {"nome": 1, "preco": 1, "created_at": 1, "_id": 0}).limit(3))

    return {
        "query_used": f"db.produtos.find({{ categoria: '{categoria}' }}) // via endpoint federado (cluster + archive)",
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
                "id": a.get("_id"),
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
    Documentos com created_at > expire_after_days dias serão movidos automaticamente.
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
    return {
        "archive_id": result.get("_id"),
        "status": result.get("state"),
        "message": f"Regra criada: documentos com created_at > {expire_after_days} dias serão arquivados automaticamente.",
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
