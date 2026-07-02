from fastapi import APIRouter, Query
from database import db
from pymongo import ASCENDING, DESCENDING
import threading
import time

# Queries representativas para o explain() de cada cenário. sort+limit usa
# top-K sort (memória limitada), então o COLLSCAN funciona mesmo sem índice.
EXPLAIN_SCENARIOS = {
    "simples":  {"filter": {"categoria": "Eletrônicos"}, "limit": 100},
    "composto": {"filter": {"categoria": "Eletrônicos"}, "sort": {"preco": -1}, "limit": 100},
    "parcial":  {"filter": {"em_estoque": True, "preco": {"$lt": 100}}, "limit": 100},
}

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
    # Desde o MongoDB 4.2 todo build de índice é "hybrid": não bloqueia leituras
    # nem escritas (a antiga opção background foi deprecada e é ignorada).
    kwargs: dict = {"sparse": sparse}
    if partial_filter:
        kwargs["partialFilterExpression"] = partial_filter

    _builds[name] = {"status": "building", "elapsed_seconds": 0, "error": None}
    threading.Thread(target=_build_worker, args=(name, key, kwargs), daemon=True).start()

    return {
        "status": "building",
        "index_name": name,
        "fields": fields,
        "note": (
            "Build iniciado (hybrid build, MongoDB 4.2+). A coleção continua "
            "atendendo leituras e escritas normalmente durante toda a construção."
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


@router.get("/read-probe")
def read_probe():
    """
    Leitura curta usada pela UI durante o build do índice, para provar que a
    coleção segue atendendo leituras normalmente (sem lock) enquanto constrói.
    """
    t0 = time.time()
    doc = db[COLLECTION].find_one(
        {"categoria": "Eletrônicos"}, {"nome": 1, "preco": 1, "_id": 0}
    )
    return {"ok": doc is not None, "latency_ms": round((time.time() - t0) * 1000, 1)}


def _walk_stages(plan: dict, acc: list):
    """Percorre o winningPlan coletando os stages (IXSCAN, COLLSCAN, SORT...)."""
    if not isinstance(plan, dict):
        return
    stage = plan.get("stage")
    if stage:
        acc.append({"stage": stage, "index_name": plan.get("indexName")})
    for child_key in ("inputStage", "innerStage", "outerStage"):
        if child_key in plan:
            _walk_stages(plan[child_key], acc)
    for child in plan.get("inputStages", []):
        _walk_stages(child, acc)


@router.get("/explain")
def explain(scenario: str = "simples"):
    """
    Roda a query representativa do cenário com explain(executionStats).
    Antes do índice: COLLSCAN varrendo a coleção inteira. Depois: IXSCAN
    examinando só as chaves necessárias — a prova objetiva do ganho.
    """
    spec = EXPLAIN_SCENARIOS.get(scenario, EXPLAIN_SCENARIOS["simples"])
    find_cmd = {"find": COLLECTION, "filter": spec["filter"], "limit": spec["limit"]}
    if "sort" in spec:
        find_cmd["sort"] = spec["sort"]
    result = db.command("explain", find_cmd, verbosity="executionStats")

    stats = result.get("executionStats", {})
    stages: list = []
    _walk_stages(result.get("queryPlanner", {}).get("winningPlan", {}), stages)
    index_name = next((s["index_name"] for s in stages if s.get("index_name")), None)
    scan = "IXSCAN" if any(s["stage"] == "IXSCAN" for s in stages) else "COLLSCAN"

    return {
        "scenario": scenario,
        "query": {k: v for k, v in spec.items()},
        "scan": scan,
        "index_name": index_name,
        "stages": [s["stage"] for s in stages],
        "execution_ms": stats.get("executionTimeMillis"),
        "docs_examined": stats.get("totalDocsExamined"),
        "keys_examined": stats.get("totalKeysExamined"),
        "n_returned": stats.get("nReturned"),
    }


@router.get("/demo-scenarios")
def demo_scenarios():
    return {
        "scenarios": [
            {"title": "Índice Simples", "fields": ["categoria"]},
            {"title": "Índice Composto", "fields": ["categoria", "-preco"]},
            {"title": "Índice Parcial", "fields": ["preco"], "partial_filter": {"em_estoque": True}},
        ]
    }
