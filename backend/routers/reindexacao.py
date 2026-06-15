from fastapi import APIRouter, Query
from database import db
from pymongo import ASCENDING, DESCENDING
import threading
import time

router = APIRouter(prefix="/reindexacao", tags=["Reindexação"])

COLLECTION = "produtos"

# Estado dos builds em andamento (em memória). name -> {...}
_builds: dict[str, dict] = {}


def _index_name(fields: list[str]) -> str:
    """Reproduz o nome padrão que o MongoDB gera para o índice."""
    parts = []
    for f in fields:
        name = f.lstrip("-")
        direction = -1 if f.startswith("-") else 1
        parts.append(f"{name}_{direction}")
    return "_".join(parts)


def _existing_index_names() -> set[str]:
    return {i["name"] for i in db[COLLECTION].list_indexes()}


@router.get("/indexes")
def list_indexes():
    indexes = list(db[COLLECTION].list_indexes())
    return {"indexes": [{"name": i["name"], "key": dict(i["key"])} for i in indexes]}


def _build_worker(name: str, key, kwargs):
    start = time.time()
    try:
        db[COLLECTION].create_index(key, **kwargs)
        _builds[name] = {"status": "done", "elapsed_seconds": round(time.time() - start, 1), "error": None}
    except Exception as e:  # noqa: BLE001
        _builds[name] = {"status": "error", "elapsed_seconds": round(time.time() - start, 1), "error": str(e)}


@router.post("/create")
def create_index(
    fields: list[str] = Query(...),
    sparse: bool = False,
    partial_filter: dict | None = None,
):
    """
    Inicia a criação de um índice na coleção produtos em background (rolling build).
    Retorna imediatamente — a UI acompanha o progresso via /build-status.
    """
    name = _index_name(fields)

    if name in _existing_index_names():
        return {"status": "exists", "index_name": name, "message": "Índice já existe na coleção."}

    key = [(f.lstrip("-"), DESCENDING if f.startswith("-") else ASCENDING) for f in fields]
    kwargs: dict = {"background": True, "sparse": sparse}
    if partial_filter:
        kwargs["partialFilterExpression"] = partial_filter

    _builds[name] = {"status": "building", "elapsed_seconds": 0, "error": None}
    threading.Thread(target=_build_worker, args=(name, key, kwargs), daemon=True).start()

    return {
        "status": "building",
        "index_name": name,
        "fields": fields,
        "note": (
            "Build iniciado em background (rolling build). A coleção continua "
            "atendendo leituras e escritas normalmente enquanto o índice é construído."
        ),
    }


@router.get("/build-status")
def build_status(name: str):
    """
    Estado de um build iniciado por /create. A fonte da verdade é o worker
    (índices em construção já aparecem em listIndexes, então não basta checar
    existência). Sem estado rastreado, infere pela existência.
    """
    state = _builds.get(name)
    if state is not None:
        return {"index_name": name, **state}
    try:
        exists = name in _existing_index_names()
    except Exception:
        # Cluster ocupado/instável: não falha, apenas reporta indefinido.
        return {"status": "building", "index_name": name}
    return {"status": "done" if exists else "unknown", "index_name": name}


@router.delete("/drop/{index_name}")
def drop_index(index_name: str):
    if index_name == "_id_":
        return {"error": "Não é possível remover o índice _id"}
    db[COLLECTION].drop_index(index_name)
    _builds.pop(index_name, None)
    return {"dropped": index_name}


@router.get("/demo-scenarios")
def demo_scenarios():
    return {
        "scenarios": [
            {"title": "Índice Simples", "fields": ["categoria"]},
            {"title": "Índice Composto", "fields": ["categoria", "-preco"]},
            {"title": "Índice Parcial", "fields": ["preco"], "partial_filter": {"em_estoque": True}},
        ]
    }
