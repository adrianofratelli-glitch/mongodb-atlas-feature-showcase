"""
Acesso ao MongoDB Atlas (REAL) — reaproveita o mesmo driver PyMongo já usado
pela POC. Escritas síncronas são executadas via asyncio.to_thread para não
bloquear o event loop das APIs.
"""
from datetime import datetime, timezone
from pymongo import MongoClient

import config

_client = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        # timeouts curtos: o cenário é device-facing (SLA 100ms), então falha rápido.
        _client = MongoClient(
            config.MONGO_URI,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
        )
    return _client


def get_db():
    return get_client()[config.MONGO_DB]


def now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Operações de escrita (síncronas — chamadas via asyncio.to_thread nos workers)
# ---------------------------------------------------------------------------

def insert_pending_job(cid: str, payload: dict) -> None:
    """Cria o job durável em estado 'pending'. Comum aos dois caminhos."""
    get_db()[config.COL_JOBS].insert_one({
        "correlationId": cid,
        "status": "pending",
        "payload": payload,
        "result": None,
        "createdAt": now(),
        "completedAt": None,
    })


def persist_job_done(cid: str, result: dict) -> None:
    """
    ÚNICO write do caminho Mongo e também a op (a) 'persistir' do dual-write Redis.
    Marca o job como concluído e durável.
    """
    get_db()[config.COL_JOBS].update_one(
        {"correlationId": cid},
        {"$set": {"status": "done", "result": result, "completedAt": now()}},
    )


def write_audit(cid: str, result: dict, caminho: str) -> None:
    """
    Trilha de auditoria imutável. No caminho Redis é uma escrita EXTRA (mais um
    passo do dual-write). No caminho Mongo NÃO é chamada: o próprio doc jobs +
    o change stream já são a trilha.
    """
    get_db()[config.COL_AUDIT].insert_one({
        "correlationId": cid,
        "result": result,
        "caminho": caminho,
        "auditedAt": now(),
    })


def get_job(cid: str) -> dict | None:
    return get_db()[config.COL_JOBS].find_one({"correlationId": cid}, {"_id": 0})


def count_audit(cid: str) -> int:
    return get_db()[config.COL_AUDIT].count_documents({"correlationId": cid})


# ---------------------------------------------------------------------------
# resumeToken persistido (durável) — usado pelo dispatcher do change stream
# ---------------------------------------------------------------------------
#
# CRÍTICO: o checkpoint do resumeToken precisa de read-your-own-writes. Sem isso,
# uma leitura roteada para um secundário com lag (ou um valor antigo em cache)
# pode devolver um token À FRENTE do último evento — e o resume_after PULARIA a
# conclusão perdida. Por isso o doc do token é gravado com w=majority e lido com
# readConcern=majority no primário. É o mesmo cuidado que se teria em produção
# para um checkpoint durável e confiável.

from pymongo import ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


def _resume_col():
    return get_db().get_collection(
        config.COL_RESUME,
        write_concern=WriteConcern(w="majority"),
        read_concern=ReadConcern("majority"),
        read_preference=ReadPreference.PRIMARY,
    )


def save_resume_token(token: dict) -> None:
    _resume_col().update_one(
        {"_id": "jobs_stream"},
        {"$set": {"token": token, "updatedAt": now()}},
        upsert=True,
    )


def load_resume_token() -> dict | None:
    doc = _resume_col().find_one({"_id": "jobs_stream"})
    return doc["token"] if doc else None


def clear_resume_token() -> None:
    _resume_col().delete_one({"_id": "jobs_stream"})


def limpar_colecoes_demo() -> None:
    """Zera as coleções da demo (não toca em nenhuma coleção da POC existente)."""
    db = get_db()
    for col in (config.COL_JOBS, config.COL_AUDIT, config.COL_RESUME):
        db[col].delete_many({})
