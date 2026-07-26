from fastapi import APIRouter, HTTPException, Query
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
_builds_lock = threading.RLock()
MAX_TRACKED_BUILDS = 128
ALLOWED_INDEX_FIELDS = {
    "categoria", "preco", "em_estoque", "total_avaliacoes",
    "avaliacao_media", "marca", "created_at", "produto_id",
}


def _index_name(fields: list[str], *, sparse: bool = False, partial: bool = False) -> str:
    """Nome estável que também diferencia opções sobre o mesmo key pattern."""
    parts = []
    for f in fields:
        name = f.lstrip("-")
        direction = -1 if f.startswith("-") else 1
        parts.append(f"{name}_{direction}")
    suffix = "_partial" if partial else "_sparse" if sparse else ""
    return "_".join(parts) + suffix


def _existing_index_names() -> set[str]:
    return {i["name"] for i in db[COLLECTION].list_indexes()}


def _equivalent_index_name(key: list[tuple[str, int]], sparse: bool, partial_filter: dict | None) -> str | None:
    wanted_key = dict(key)
    for index in db[COLLECTION].list_indexes():
        if dict(index.get("key", {})) != wanted_key:
            continue
        if bool(index.get("sparse", False)) != sparse:
            continue
        if index.get("partialFilterExpression") != partial_filter:
            continue
        return index["name"]
    return None


@router.get("/indexes")
def list_indexes():
    indexes = list(db[COLLECTION].list_indexes())
    return {
        "indexes": [{
            "name": i["name"],
            "key": dict(i["key"]),
            "sparse": bool(i.get("sparse", False)),
            "partial_filter": i.get("partialFilterExpression"),
        } for i in indexes]
    }


def _build_worker(name: str, key, kwargs):
    start = time.time()
    try:
        db[COLLECTION].create_index(key, **kwargs)
        with _builds_lock:
            _builds[name] = {"status": "done", "elapsed_seconds": round(time.time() - start, 1), "error": None}
    except Exception as e:  # noqa: BLE001
        with _builds_lock:
            _builds[name] = {"status": "error", "elapsed_seconds": round(time.time() - start, 1), "error": type(e).__name__}


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
    if not fields or len(fields) > 4:
        raise HTTPException(status_code=422, detail="Informe entre 1 e 4 campos para o índice.")
    normalized = [field.lstrip("-") for field in fields]
    if len(normalized) != len(set(normalized)) or any(field not in ALLOWED_INDEX_FIELDS for field in normalized):
        raise HTTPException(status_code=422, detail="Campo de índice não permitido ou duplicado.")
    if partial_filter not in (None, {"em_estoque": True}):
        raise HTTPException(status_code=422, detail="Filtro parcial não permitido nesta demonstração.")
    if sparse and partial_filter:
        raise HTTPException(status_code=422, detail="Escolha índice sparse ou parcial, não ambos.")

    key = [(f.lstrip("-"), DESCENDING if f.startswith("-") else ASCENDING) for f in fields]
    equivalent = _equivalent_index_name(key, sparse, partial_filter)
    if equivalent:
        return {"status": "exists", "index_name": equivalent, "message": "Índice equivalente já existe na coleção."}

    name = _index_name(fields, sparse=sparse, partial=partial_filter is not None)
    # Desde o MongoDB 4.2 todo build de índice é "hybrid": não bloqueia leituras
    # e escritas durante a maior parte do processo; locks exclusivos curtos
    # ainda existem no início e no fim.
    kwargs: dict = {"name": name, "sparse": sparse}
    if partial_filter:
        kwargs["partialFilterExpression"] = partial_filter

    with _builds_lock:
        current = _builds.get(name)
        if current and current.get("status") == "building":
            return {"status": "building", "index_name": name, "message": "Build já está em andamento."}
        # Estado é só conveniência da UI. Evita crescimento indefinido se a PoV
        # ficar exposta e receber muitas combinações de índices ao longo do dia.
        for old_name in list(_builds):
            if len(_builds) < MAX_TRACKED_BUILDS:
                break
            if _builds[old_name].get("status") != "building":
                _builds.pop(old_name)
        if len(_builds) >= MAX_TRACKED_BUILDS:
            raise HTTPException(
                status_code=429,
                detail="Muitos builds simultâneos. Aguarde a conclusão dos atuais.",
            )
        _builds[name] = {"status": "building", "elapsed_seconds": 0, "error": None}
    threading.Thread(target=_build_worker, args=(name, key, kwargs), daemon=True).start()

    return {
        "status": "building",
        "index_name": name,
        "fields": fields,
        "note": (
            "Build iniciado (hybrid build, MongoDB 4.2+). Leituras e escritas "
            "continuam durante a maior parte da construção; há locks curtos nas transições."
        ),
    }


@router.get("/build-status")
def build_status(
    name: str = Query(..., min_length=3, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
):
    """
    Estado de um build iniciado por /create. A fonte da verdade é o worker
    (índices em construção já aparecem em listIndexes, então não basta checar
    existência). Sem estado rastreado, infere pela existência.
    """
    with _builds_lock:
        state = dict(_builds[name]) if name in _builds else None
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
        raise HTTPException(status_code=403, detail="Não é possível remover o índice _id.")
    with _builds_lock:
        if _builds.get(index_name, {}).get("status") == "building":
            raise HTTPException(status_code=409, detail="Aguarde o término do build antes de remover o índice.")
    if index_name not in _existing_index_names():
        raise HTTPException(status_code=404, detail="Índice não encontrado.")
    db[COLLECTION].drop_index(index_name)
    with _builds_lock:
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
def explain(scenario: str = Query("simples", pattern=r"^(simples|composto|parcial)$")):
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
