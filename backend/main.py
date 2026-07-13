import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from database import db, readiness
from routers import aggregations, change_streams, hot_cold, redis_vs_changestream, reindexacao, schema_validation, transactions
from security import ApiHardeningMiddleware, MutationGuardMiddleware
from settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("showcase.api")

app = FastAPI(title="MongoDB Atlas Feature Showcase", version="1.1.0")

app.add_middleware(ApiHardeningMiddleware)
app.add_middleware(MutationGuardMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-Demo-Token", "X-Request-ID"],
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(RequestValidationError)
async def validation_error(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=jsonable_encoder({"detail": "Parâmetros inválidos.", "errors": exc.errors()}),
    )


@app.exception_handler(Exception)
async def unexpected_error(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    logger.exception("Falha não tratada request_id=%s", request_id, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Falha interna na demonstração.", "request_id": request_id},
    )


app.include_router(reindexacao.router)
app.include_router(hot_cold.router)
app.include_router(aggregations.router)
app.include_router(schema_validation.router)
app.include_router(change_streams.router)
app.include_router(transactions.router)
app.include_router(redis_vs_changestream.router)


@app.get("/")
def root():
    return {"status": "ok", "poc": "MongoDB Atlas Feature Showcase", "version": app.version}


@app.get("/health/live")
def health_live():
    return {"status": "ok"}


@app.get("/health/ready")
def health_ready():
    ok, message = readiness()
    return JSONResponse(status_code=200 if ok else 503, content={"ready": ok, "message": message})


@app.get("/preflight")
def preflight():
    mongo_ok, mongo_message = readiness()
    checks = {
        "mongo_uri": {"ok": bool(settings.mongo_uri), "message": "configurada" if settings.mongo_uri else "ausente"},
        "mongodb": {"ok": mongo_ok, "message": mongo_message},
        "atlas_admin_api": {
            "ok": settings.atlas_configured,
            "message": "configurada" if settings.atlas_configured else "opcional; módulo Online Archive ficará limitado",
        },
        "mutation_guard": {
            "ok": True,
            "message": "token obrigatório" if settings.demo_admin_token else "somente localhost/origens permitidas",
        },
    }
    if mongo_ok:
        names = set(db.list_collection_names())
        for collection in ("produtos", "avaliacoes"):
            checks[f"collection_{collection}"] = {
                "ok": collection in names,
                "message": "disponível" if collection in names else "execute seed_data.py",
            }
    ready = all(check["ok"] for key, check in checks.items() if key != "atlas_admin_api")
    return JSONResponse(status_code=200 if ready else 503, content={"ready": ready, "checks": checks})


@app.get("/stats")
def stats():
    return {
        "produtos": db["produtos"].estimated_document_count(),
        "avaliacoes": db["avaliacoes"].estimated_document_count(),
        "db": db.name,
    }
