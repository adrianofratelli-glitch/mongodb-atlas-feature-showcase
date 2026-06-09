from fastapi import APIRouter
from database import db
import os
from pymongo.errors import WriteError, OperationFailure

router = APIRouter(prefix="/schema", tags=["Schema Validation"])

DB_NAME = os.getenv("MONGO_DB", "POC")
COL = "schema_demo"

SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["nome", "preco", "categoria", "em_estoque"],
        "properties": {
            "nome":       {"bsonType": "string", "minLength": 2},
            "preco":      {"bsonType": "number",  "minimum": 0},
            "categoria":  {"bsonType": "string",  "enum": ["Eletrônicos", "Vestuário", "Alimentos", "Esportes", "Casa"]},
            "em_estoque": {"bsonType": "bool"},
            "sku":        {"bsonType": "string",  "pattern": "^[A-Z]{2}-[0-9]{4}$"},
        },
    }
}

INVALID_SCENARIOS = {
    "preco_negativo":     {"nome": "Produto Teste", "preco": -50,  "categoria": "Eletrônicos", "em_estoque": True},
    "categoria_invalida": {"nome": "Produto Teste", "preco": 100,  "categoria": "InvalidCategory", "em_estoque": True},
    "campo_faltando":     {"nome": "Produto Teste", "preco": 100,  "categoria": "Eletrônicos"},
    "sku_formato_errado": {"nome": "Produto Teste", "preco": 100,  "categoria": "Eletrônicos", "em_estoque": True, "sku": "abc123"},
}


def _col_exists():
    return COL in db.list_collection_names()


def _has_validator():
    if not _col_exists():
        return False
    info = db.command("listCollections", filter={"name": COL})
    cols = list(info["cursor"]["firstBatch"])
    return bool(cols and cols[0].get("options", {}).get("validator"))


@router.get("/status")
def status():
    exists = _col_exists()
    has_v  = _has_validator()
    count  = db[COL].count_documents({}) if exists else 0
    return {
        "collection_exists": exists,
        "schema_active": has_v,
        "document_count": count,
        "schema": SCHEMA if has_v else None,
    }


# ── Step 1: cria coleção SEM schema ──────────────────────────────────────────
@router.post("/step1-create-collection")
def step1_create():
    if _col_exists():
        db.drop_collection(COL)
    db.create_collection(COL)
    return {"status": "ok", "message": f"Coleção '{COL}' criada SEM nenhuma validação.", "schema_active": False}


# ── Step 2: insere documentos inválidos (sem schema, tudo passa) ──────────────
@router.post("/step2-insert-without-schema")
def step2_insert():
    if not _col_exists():
        db.create_collection(COL)

    docs = [
        {"nome": "OK",      "preco": 299.90, "categoria": "Eletrônicos", "em_estoque": True, "sku": "EL-0001"},
        {"nome": "X",       "preco": -50,    "categoria": "Eletrônicos", "em_estoque": True},   # preço negativo
        {"nome": "Produto", "preco": 100,    "categoria": "Categoria Inválida", "em_estoque": True},  # categoria errada
        {"nome": "Produto", "preco": 100,    "categoria": "Eletrônicos"},                        # campo faltando
        {"nome": "Produto", "preco": 100,    "categoria": "Eletrônicos", "em_estoque": True, "sku": "abc-wrong"},  # SKU errado
    ]
    result = db[COL].insert_many(docs)
    return {
        "status": "ok",
        "inserted": len(result.inserted_ids),
        "message": "Todos os 5 documentos foram aceitos — inclusive os inválidos. SEM schema, o banco não valida nada.",
        "docs_inserted": [str(i) for i in result.inserted_ids],
    }


# ── Step 3: ativa schema na coleção existente ────────────────────────────────
@router.post("/step3-activate-schema")
def step3_activate():
    if not _col_exists():
        db.create_collection(COL)
    db.command("collMod", COL,
               validator=SCHEMA,
               validationLevel="strict",
               validationAction="error")
    count = db[COL].count_documents({})
    return {
        "status": "ok",
        "message": "Schema JSON ativado na coleção. Os documentos já inseridos permanecem, mas novas inserções inválidas serão rejeitadas.",
        "schema_active": True,
        "existing_docs": count,
        "note": "DocumentDB não suporta collMod com validator — essa operação falharia.",
    }


# ── Step 4: tenta inserir documento inválido (com schema ativo) ──────────────
@router.post("/step4-insert-invalid")
def step4_insert_invalid(scenario: str = "preco_negativo"):
    if not _col_exists() or not _has_validator():
        return {"error": "Ative o schema primeiro (step3)"}
    doc = INVALID_SCENARIOS.get(scenario, INVALID_SCENARIOS["preco_negativo"])
    try:
        db[COL].insert_one(doc)
        return {"status": "inesperado — documento deveria ter sido rejeitado"}
    except WriteError as e:
        return {
            "status": "rejeitado",
            "scenario": scenario,
            "document_attempted": doc,
            "error_message": str(e).split(" full error:")[0],
            "note": "Rejeitado na camada do banco — sem precisar de validação no código da aplicação.",
        }


# ── Inserção válida (com schema) ─────────────────────────────────────────────
@router.post("/insert-valid")
def insert_valid():
    if not _col_exists():
        db.create_collection(COL)
    doc = {"nome": "Smartphone Pro X", "preco": 2499.90, "categoria": "Eletrônicos", "em_estoque": True, "sku": "EL-1234"}
    result = db[COL].insert_one(doc)
    return {"status": "aceito", "inserted_id": str(result.inserted_id), "document": doc}


@router.get("/documents")
def list_documents():
    docs = list(db[COL].find({}, {"_id": 0}).limit(10)) if _col_exists() else []
    return {"documents": docs, "count": len(docs), "schema_active": _has_validator()}


@router.delete("/reset")
def reset():
    if _col_exists():
        db.drop_collection(COL)
    return {"status": "resetado"}
