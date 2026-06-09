from fastapi import APIRouter
from database import db
from datetime import datetime
import threading
import time
import uuid
import random

router = APIRouter(prefix="/change-streams", tags=["Change Streams"])

_state = {
    "active":     False,
    "events":     [],
    "started_at": None,
    "thread":     None,
}

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
        "created_at":   datetime.now(),
    }

def _resumo(op: str, doc: dict, prev: dict) -> dict:
    if op == "insert":
        return {
            "texto":   f"{doc.get('pagador','?')} → {doc.get('recebedor','?')}",
            "detalhe": f"R$ {doc.get('valor',0):,.2f} via {doc.get('tipo','?')}",
            "alerta":  doc.get("suspeita", False),
        }
    if op == "update":
        novo   = doc.get("status", "?")
        pagador = doc.get("pagador", doc.get("transacao_id","?")[:8])
        return {
            "texto":   f"Status atualizado: {pagador}",
            "detalhe": f"novo status → {novo}",
            "alerta":  False,
        }
    if op == "delete":
        return {"texto": "Transação removida", "detalhe": "", "alerta": False}
    return {"texto": op, "detalhe": "", "alerta": False}


def _watch_worker():
    try:
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "delete"]}}}]
        with db["transacoes_cs_demo"].watch(
            pipeline,
            full_document="updateLookup",
            full_document_before_change="whenAvailable",
            max_await_time_ms=400,
        ) as stream:
            deadline = time.time() + 120
            while time.time() < deadline and _state["active"]:
                change = stream.try_next()
                if change:
                    op   = change["operationType"]
                    doc  = change.get("fullDocument") or {}
                    prev = change.get("fullDocumentBeforeChange") or {}
                    info = _resumo(op, doc, prev)
                    _state["events"].append({
                        "ts":        datetime.now().strftime("%H:%M:%S.%f")[:-3],
                        "operation": op,
                        "texto":     info["texto"],
                        "detalhe":   info["detalhe"],
                        "alerta":    info["alerta"],
                    })
                else:
                    time.sleep(0.15)
    except Exception as e:
        _state["events"].append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "operation": "ERROR",
            "texto": str(e), "detalhe": "", "alerta": False,
        })
    finally:
        _state["active"] = False


@router.post("/start")
def start_watch():
    if _state["active"]:
        return {"status": "already_watching", "events_so_far": len(_state["events"])}
    if "transacoes_cs_demo" not in db.list_collection_names():
        db.create_collection("transacoes_cs_demo")
    _state["active"]     = True
    _state["events"]     = []
    _state["started_at"] = datetime.now().isoformat()
    t = threading.Thread(target=_watch_worker, daemon=True)
    _state["thread"] = t
    t.start()
    return {"status": "watching", "colecao": "transacoes_cs_demo", "timeout_seconds": 120}


@router.post("/trigger")
def trigger_event(operacao: str = "insert"):
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
            {"$set": {"status": novo_status, "updated_at": datetime.now()}},
        )
        return {"triggered": "update", "novo_status": novo_status}

    if operacao == "delete":
        doc = db["transacoes_cs_demo"].find_one({"status": "recusada"})
        if not doc:
            doc = db["transacoes_cs_demo"].find_one()
        if not doc:
            return {"error": "Nenhuma transação para remover"}
        db["transacoes_cs_demo"].delete_one({"_id": doc["_id"]})
        return {"triggered": "delete"}

    return {"error": f"Operação desconhecida: {operacao}"}


@router.get("/events")
def get_events():
    return {
        "active":     _state["active"],
        "started_at": _state["started_at"],
        "total":      len(_state["events"]),
        "events":     _state["events"],
    }


@router.post("/stop")
def stop_watch():
    _state["active"] = False
    try:
        db["transacoes_cs_demo"].drop()
    except Exception:
        pass
    return {"stopped": True, "total_events": len(_state["events"])}
