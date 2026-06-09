from fastapi import APIRouter, Query
from database import db
from pymongo import ASCENDING, DESCENDING
import time

router = APIRouter(prefix="/reindexacao", tags=["Reindexação"])

COLLECTION = "produtos"


@router.get("/indexes")
def list_indexes():
    indexes = list(db[COLLECTION].list_indexes())
    return {"indexes": [{"name": i["name"], "key": dict(i["key"])} for i in indexes]}


@router.post("/create")
def create_index(
    fields: list[str] = Query(...),
    sparse: bool = False,
    partial_filter: dict | None = None,
):
    """Cria índice na coleção produtos. fields é passado como query param."""
    key = [(f.lstrip("-"), DESCENDING if f.startswith("-") else ASCENDING) for f in fields]
    kwargs = {"background": True, "sparse": sparse}
    if partial_filter:
        kwargs["partialFilterExpression"] = partial_filter

    start = time.time()
    name = db[COLLECTION].create_index(key, **kwargs)
    elapsed = round(time.time() - start, 3)

    return {
        "index_name": name,
        "fields": fields,
        "elapsed_seconds": elapsed,
        "note": "Criado sem bloquear leituras/escritas (rolling build). No DocumentDB, esse processo travaria a coleção.",
    }


@router.delete("/drop/{index_name}")
def drop_index(index_name: str):
    if index_name == "_id_":
        return {"error": "Não é possível remover o índice _id"}
    db[COLLECTION].drop_index(index_name)
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
