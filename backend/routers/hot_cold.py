from fastapi import APIRouter, HTTPException
from database import db
from datetime import datetime, timezone
import os, requests
from requests.auth import HTTPDigestAuth

router = APIRouter(prefix="/hot-cold", tags=["Hot/Cold"])

COLLECTION = "produtos"
ATLAS_PUBLIC_KEY  = os.getenv("ATLAS_PUBLIC_KEY", "")
ATLAS_PRIVATE_KEY = os.getenv("ATLAS_PRIVATE_KEY", "")
ATLAS_PROJECT_ID  = os.getenv("ATLAS_PROJECT_ID", "")
ATLAS_CLUSTER     = os.getenv("ATLAS_CLUSTER", "myCluster")
ATLAS_BASE        = "https://cloud.mongodb.com/api/atlas/v1.0"


@router.get("/distribution")
def data_distribution():
    pipeline = [
        {"$group": {"_id": {"$year": "$created_at"}, "count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}}},
        {"$sort": {"_id": -1}},
        {"$limit": 10},
    ]
    result = list(db[COLLECTION].aggregate(pipeline))
    current_year = datetime.now(timezone.utc).year
    rows = []
    for r in result:
        year = r["_id"]
        rows.append({
            "year": year,
            "count": r["count"],
            "avg_preco": round(r["avg_preco"] or 0, 2),
            "tier": "🔥 Hot (ativo)" if year >= current_year - 1 else "❄️  Cold (arquivo)",
        })
    return {
        "distribution": rows,
        "note": "Documentos criados há mais de 1 ano seriam movidos automaticamente para o Online Archive.",
    }


@router.get("/archive-simulation")
def archive_simulation():
    current_year = datetime.now(timezone.utc).year
    cutoff = datetime(current_year - 1, 1, 1, tzinfo=timezone.utc)
    hot_count  = db[COLLECTION].count_documents({"created_at": {"$gte": cutoff}})
    cold_count = db[COLLECTION].count_documents({"created_at": {"$lt": cutoff}})
    total = hot_count + cold_count
    return {
        "hot":  {"count": hot_count,  "pct": round(hot_count / total * 100, 1) if total else 0, "tier": "Cluster Atlas (NVMe SSD)", "latency": "< 5ms"},
        "cold": {"count": cold_count, "pct": round(cold_count / total * 100, 1) if total else 0, "tier": "Online Archive (Object Storage)", "latency": "~ 100-300ms"},
        "savings_estimate": "Redução de ~60-80% no custo de armazenamento para dados históricos",
        "transparencia": "Mesma connection string — o Atlas roteia automaticamente",
    }


@router.get("/query-transparent")
def query_transparent(categoria: str = "Eletrônicos"):
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
        "query_used": f"db.produtos.find({{ categoria: '{categoria}' }}) // sem filtro de data",
        "explanation": (
            "Com o Online Archive ativo, uma única query sem filtro de data retorna "
            "documentos de AMBAS as camadas (hot e cold). O Atlas roteia internamente — "
            "a aplicação não sabe de onde veio cada documento."
        ),
        "hot_samples":  fmt(hot),
        "cold_samples": fmt(cold),
    }


@router.get("/online-archive/list")
def list_online_archives():
    """Lista as regras de Online Archive configuradas no cluster via Atlas API."""
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives"
    resp = requests.get(url, auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY))
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
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
def create_online_archive(expire_after_days: int = 365):
    """
    Cria uma regra de Online Archive via Atlas API.
    Documentos com created_at > expire_after_days dias serão movidos automaticamente.
    """
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives"
    payload = {
        "collName": COLLECTION,
        "dbName": "POC",
        "criteria": {
            "type": "DATE",
            "dateField": "created_at",
            "dateFormat": "ISODATE",
            "expireAfterDays": expire_after_days,
        },
        "dataExpirationRule": {"expireAfterDays": expire_after_days * 2},
        "partitionFields": [
            {"fieldName": "categoria",   "order": 0},
            {"fieldName": "created_at",  "order": 1},
        ],
        "schedule": {"type": "DEFAULT"},
    }
    resp = requests.post(
        url,
        json=payload,
        auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY),
    )
    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    result = resp.json()
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
def delete_online_archive(archive_id: str):
    url = f"{ATLAS_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{ATLAS_CLUSTER}/onlineArchives/{archive_id}"
    resp = requests.delete(url, auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY))
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"deleted": archive_id}
