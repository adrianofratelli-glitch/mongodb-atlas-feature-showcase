from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import db
from routers import reindexacao, hot_cold, aggregations, schema_validation, change_streams, transactions, redis_vs_changestream

app = FastAPI(title="MongoDB Atlas Feature Showcase", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
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
    return {"status": "ok", "poc": "MongoDB Atlas Feature Showcase"}


@app.get("/stats")
def stats():
    """Contagens reais do cluster — a UI exibe estes números (nada hard-coded)."""
    return {
        "produtos":   db["produtos"].estimated_document_count(),
        "avaliacoes": db["avaliacoes"].estimated_document_count(),
        "db":         db.name,
    }
