from fastapi import APIRouter, HTTPException, Query
from database import db
from datetime import datetime, timezone
import threading
import time
import uuid
import random
import logging

router = APIRouter(prefix="/change-streams", tags=["Change Streams"])

_state = {
    "active":     False,
    "events":     [],
    "started_at": None,
    "thread":     None,
    "generation": 0,
}
_state_lock = threading.RLock()
logger = logging.getLogger("showcase.change_streams")


def _append_event(event: dict, generation: int | None = None):
    with _state_lock:
        if generation is not None and generation != _state["generation"]:
            return
        _state["events"].append(event)
        _state["events"] = _state["events"][-250:]

# Dados financeiros realistas para a demo
_PAGADORES   = ["João Silva", "Maria Oliveira", "Carlos Santos", "Ana Lima",
                 "Pedro Costa", "Fernanda Rocha", "Lucas Mendes", "Beatriz Souza",
                 "Rafael Alves", "Camila Torres"]
_RECEBEDORES = ["Mercado Livre", "iFood", "Amazon BR", "Shopee", "Magazine Luiza",
                "Nubank", "PicPay", "Itaú", "Bradesco", "C6 Bank"]
_TIPOS       = ["pix", "ted", "doc", "cartão débito", "cartão crédito"]

def _gerar_transacao():
    valor = round(random.uniform(12.5, 18000), 2)
    return {
        "transacao_id": str(uuid.uuid4()),
        "pagador":      random.choice(_PAGADORES),
        "recebedor":    random.choice(_RECEBEDORES),
        "valor":        valor,
        "tipo":         random.choice(_TIPOS),
        "status":       "pendente",
        "suspeita":     valor > 8000,
        "created_at":   datetime.now(timezone.utc),
    }

def _resumo(op: str, doc: dict, prev: dict) -> dict:
    if op == "insert":
        return {
            "texto":   f"{doc.get('pagador','?')} → {doc.get('recebedor','?')}",
            "detalhe": f"R$ {doc.get('valor',0):,.2f} via {doc.get('tipo','?')}",
            "alerta":  doc.get("suspeita", False),
        }
    if op == "update":
        novo    = doc.get("status", "?")
        antigo  = prev.get("status")  # before-image (changeStreamPreAndPostImages)
        pagador = doc.get("pagador", doc.get("transacao_id", "?")[:8])
        return {
            "texto":   f"Status atualizado: {pagador}",
            "detalhe": f"status: {antigo} → {novo}" if antigo else f"novo status → {novo}",
            "alerta":  False,
        }
    if op == "delete":
        return {"texto": "Transação removida", "detalhe": "", "alerta": False}
    return {"texto": op, "detalhe": "", "alerta": False}


def _watch_worker(generation: int, ready: threading.Event):
    try:
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "delete"]}}}]
        with db["transacoes_cs_demo"].watch(
            pipeline,
            full_document="updateLookup",
            full_document_before_change="whenAvailable",
            max_await_time_ms=400,
        ) as stream:
            # O endpoint /start só retorna depois que o cursor abriu. Sem esse
            # handshake, a UI podia disparar o primeiro insert antes do watch()
            # existir e o evento inicial desaparecia da demonstração.
            ready.set()
            deadline = time.time() + 120
            while time.time() < deadline:
                with _state_lock:
                    if not _state["active"] or generation != _state["generation"]:
                        break
                change = stream.try_next()
                if change:
                    op   = change["operationType"]
                    doc  = change.get("fullDocument") or {}
                    prev = change.get("fullDocumentBeforeChange") or {}
                    info = _resumo(op, doc, prev)
                    _append_event({
                        "ts":        datetime.now().strftime("%H:%M:%S.%f")[:-3],
                        "operation": op,
                        "texto":     info["texto"],
                        "detalhe":   info["detalhe"],
                        "alerta":    info["alerta"],
                    }, generation)
                else:
                    time.sleep(0.15)
    except Exception as e:
        logger.exception("Change Stream worker falhou")
        _append_event({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "operation": "ERROR",
            "texto": "Change Stream indisponível", "detalhe": type(e).__name__, "alerta": False,
        }, generation)
    finally:
        with _state_lock:
            if generation == _state["generation"]:
                _state["active"] = False
        ready.set()


@router.post("/start")
def start_watch():
    with _state_lock:
        if _state["active"]:
            return {"status": "already_watching", "events_so_far": len(_state["events"])}
        _state["active"] = True
        _state["events"] = []
        _state["started_at"] = datetime.now(timezone.utc).isoformat()
        _state["generation"] += 1
        generation = _state["generation"]
    # Recria a coleção limpa: os docs persistidos correspondem a esta simulação.
    # Pre/post images habilitados para o stream entregar o before-image real
    # (fullDocumentBeforeChange) nos updates — base do audit trail.
    try:
        if "transacoes_cs_demo" in db.list_collection_names():
            db["transacoes_cs_demo"].drop()
        db.create_collection(
            "transacoes_cs_demo",
            changeStreamPreAndPostImages={"enabled": True},
        )
    except Exception:
        with _state_lock:
            _state["active"] = False
        raise
    ready = threading.Event()
    t = threading.Thread(target=_watch_worker, args=(generation, ready), daemon=True)
    with _state_lock:
        _state["thread"] = t
    t.start()
    if not ready.wait(timeout=10):
        with _state_lock:
            if generation == _state["generation"]:
                _state["active"] = False
                _state["generation"] += 1
        raise HTTPException(status_code=503, detail="Change stream não abriu em até 10 segundos.")
    with _state_lock:
        worker_ready = _state["active"] and generation == _state["generation"]
    if not worker_ready:
        raise HTTPException(status_code=503, detail="Change stream falhou durante a abertura.")
    return {"status": "watching", "colecao": "transacoes_cs_demo", "timeout_seconds": 120}


@router.post("/trigger")
def trigger_event(operacao: str = Query("insert", pattern=r"^(insert|update|delete)$")):
    if operacao == "insert":
        tx = _gerar_transacao()
        db["transacoes_cs_demo"].insert_one(tx)
        return {"triggered": "insert", "transacao_id": tx["transacao_id"][:8]}

    if operacao == "update":
        doc = db["transacoes_cs_demo"].find_one({"status": "pendente"})
        if not doc:
            # Insere e depois atualiza se não há pendentes
            tx = _gerar_transacao()
            db["transacoes_cs_demo"].insert_one(tx)
            doc = tx
        novo_status = random.choice(["aprovada", "aprovada", "aprovada", "recusada"])
        db["transacoes_cs_demo"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": novo_status, "updated_at": datetime.now(timezone.utc)}},
        )
        return {"triggered": "update", "novo_status": novo_status}

    if operacao == "delete":
        doc = db["transacoes_cs_demo"].find_one({"status": "recusada"})
        if not doc:
            doc = db["transacoes_cs_demo"].find_one()
        if not doc:
            raise HTTPException(status_code=409, detail="Nenhuma transação disponível para remover.")
        db["transacoes_cs_demo"].delete_one({"_id": doc["_id"]})
        return {"triggered": "delete"}

    raise HTTPException(status_code=422, detail="Operação inválida.")


@router.get("/events")
def get_events():
    with _state_lock:
        active = _state["active"]
        started_at = _state["started_at"]
        events = list(_state["events"])
    return {
        "active": active,
        "started_at": started_at,
        "total": len(events),
        "events": events,
    }


@router.get("/collection")
def get_collection():
    """
    Retorna os documentos realmente persistidos em transacoes_cs_demo.
    Prova que os eventos não estão só na UI — estão gravados no banco e
    podem ser conferidos no Atlas Data Explorer.
    """
    col = db["transacoes_cs_demo"]
    total = col.count_documents({})
    docs = list(col.find({}, {"_id": 0}).sort("created_at", -1).limit(50))
    for d in docs:
        for k in ("created_at", "updated_at"):
            if d.get(k):
                d[k] = d[k].isoformat()
    return {
        "colecao": "transacoes_cs_demo",
        "db": db.name,
        "total": total,
        "documentos": docs,
    }


@router.post("/stop")
def stop_watch():
    """Para o watcher mas MANTÉM a coleção, para inspeção no Atlas Data Explorer."""
    with _state_lock:
        _state["active"] = False
        _state["generation"] += 1
        total = len(_state["events"])
    return {"stopped": True, "total_events": total}


@router.delete("/clear")
def clear_collection():
    """Remove a coleção de demo (limpeza manual após a apresentação)."""
    with _state_lock:
        _state["active"] = False
        _state["generation"] += 1
    try:
        db["transacoes_cs_demo"].drop()
    except Exception:
        logger.exception("Falha ao remover coleção de Change Streams")
    return {"cleared": True}
