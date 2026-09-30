from fastapi import APIRouter, HTTPException, Query
from database import db
import json
from pymongo.errors import WriteError, OperationFailure

router = APIRouter(prefix="/schema", tags=["Schema Validation"])

COL = "schema_demo"

SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["nome", "preco", "categoria", "em_estoque"],
        "properties": {
            "nome":       {"bsonType": "string", "minLength": 2},
            "preco":      {"bsonType": "number",  "minimum": 0},
            # Mesmas categorias do dataset da POC (seed_data.py)
            "categoria":  {"bsonType": "string",  "enum": ["Eletrônicos", "Moda", "Casa", "Esportes", "Livros", "Brinquedos"]},
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


def _options():
    if not _col_exists():
        return {}
    info = db.command("listCollections", filter={"name": COL})
    cols = list(info["cursor"]["firstBatch"])
    return cols[0].get("options", {}) if cols else {}


def _supports_constraint():
    try:
        version = db.client.server_info().get("versionArray", [])
    except Exception:
        return False
    return tuple(version[:2]) >= (9, 0)


@router.get("/status")
def status():
    exists = _col_exists()
    has_v  = _has_validator()
    count  = db[COL].count_documents({}) if exists else 0
    options = _options() if exists else {}
    return {
        "collection_exists": exists,
        "schema_active": has_v,
        "validation_level": options.get("validationLevel"),
        "constraint_active": options.get("validationLevel") == "constraint",
        "constraint_supported": _supports_constraint(),
        "legacy_invalid_count": db[COL].count_documents({"_demo_invalid": {"$exists": True}}) if exists else 0,
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

    if _has_validator():
        raise HTTPException(status_code=409, detail="Schema já ativo. Reinicie a demo para executar o passo sem validação.")

    docs = [
        {"nome": "OK",      "preco": 299.90, "categoria": "Eletrônicos", "em_estoque": True, "sku": "EL-0001"},
        {"nome": "X",       "preco": -50,    "categoria": "Eletrônicos", "em_estoque": True, "_demo_invalid": "preco_negativo"},
        {"nome": "Produto", "preco": 100,    "categoria": "Categoria Inválida", "em_estoque": True, "_demo_invalid": "categoria_invalida"},
        {"nome": "Produto", "preco": 100,    "categoria": "Eletrônicos", "_demo_invalid": "campo_faltando"},
        {"nome": "Produto", "preco": 100,    "categoria": "Eletrônicos", "em_estoque": True, "sku": "abc-wrong", "_demo_invalid": "sku_formato_errado"},
    ]
    result = db[COL].insert_many(docs)
    return {
        "status": "ok",
        "inserted": len(result.inserted_ids),
        "message": "Os 5 documentos foram aceitos — quatro inválidos e um válido. SEM schema, o banco não valida nada.",
        "docs_inserted": [str(i) for i in result.inserted_ids],
    }


@router.post("/step4-try-constraint")
def step4_try_constraint():
    """Tenta garantir que todos os documentos, inclusive os antigos, são válidos."""
    if not _col_exists() or not _has_validator():
        raise HTTPException(status_code=409, detail="Ative o schema strict primeiro.")
    if not _supports_constraint():
        raise HTTPException(status_code=409, detail="validationLevel constraint requer MongoDB 9.0+.")
    if _options().get("validationLevel") == "constraint":
        return {"status": "constraint_ativo", "legacy_invalid_count": 0}
    try:
        db.command("collMod", COL, prepareConstraintValidationLevel=True)
        db.command("collMod", COL, validationLevel="constraint", validationAction="error")
    except OperationFailure as exc:
        # MongoDB 9.0 returns 12370902 when the collection still contains
        # documents that violate the validator. Code 121 is kept for server
        # variants that surface the underlying document-validation error.
        if exc.code not in (121, 12370902):
            raise
        return {
            "status": "bloqueado_por_legado",
            "legacy_invalid_count": db[COL].count_documents({"_demo_invalid": {"$exists": True}}),
            "error_message": str(exc).split(" full error:")[0],
            "note": "A promoção falhou porque já havia documentos fora da regra. Corrija-os e tente novamente.",
        }
    return {"status": "constraint_ativo", "legacy_invalid_count": 0}


@router.post("/step5-repair-legacy")
def step5_repair_legacy():
    """Corrige os quatro exemplos legados para permitir a promoção a constraint."""
    if not _col_exists() or not _has_validator():
        raise HTTPException(status_code=409, detail="Ative o schema strict primeiro.")
    col = db[COL]
    correcoes = [
        # O cenário também usa nome de 1 caractere; ajuste ambos os problemas.
        ("preco_negativo", {"$set": {"nome": "Produto Teste", "preco": 50}}),
        ("categoria_invalida", {"$set": {"categoria": "Eletrônicos"}}),
        ("campo_faltando", {"$set": {"em_estoque": True}}),
        ("sku_formato_errado", {"$set": {"sku": "EL-0002"}}),
    ]
    corrigidos = 0
    for tipo, atualizacao in correcoes:
        atualizacao["$unset"] = {"_demo_invalid": ""}
        corrigidos += col.update_many({"_demo_invalid": tipo}, atualizacao).modified_count
    restantes = col.count_documents({"_demo_invalid": {"$exists": True}})
    return {"status": "corrigidos" if restantes == 0 else "pendente", "corrigidos": corrigidos, "restantes": restantes}


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
        "note": "Validação aplicada via collMod — ativa imediatamente, sem recriar a coleção.",
    }


# ── Step 4: tenta inserir documento inválido (com schema ativo) ──────────────
@router.post("/step4-insert-invalid")
def step4_insert_invalid(
    scenario: str = Query("preco_negativo", pattern=r"^(preco_negativo|categoria_invalida|campo_faltando|sku_formato_errado)$")
):
    if not _col_exists() or not _has_validator():
        raise HTTPException(status_code=409, detail="Ative o schema primeiro (step3).")
    base = INVALID_SCENARIOS[scenario]
    doc = dict(base)  # cópia: insert_one injeta _id no dict original
    try:
        db[COL].insert_one(doc)
        return {"status": "inesperado — documento deveria ter sido rejeitado"}
    except (WriteError, OperationFailure) as e:
        if e.code != 121:
            raise
        # errInfo: o MongoDB devolve ESTRUTURADO exatamente qual regra do
        # $jsonSchema falhou — diferencial real vs validação na aplicação.
        # (json round-trip com default=str converte tipos BSON, ex. ObjectId)
        err_info = (e.details or {}).get("errInfo") if hasattr(e, "details") else None
        if err_info is not None:
            err_info = json.loads(json.dumps(err_info, default=str))
        return {
            "status": "rejeitado",
            "scenario": scenario,
            "document_attempted": base,  # sem o _id que o driver injetou
            "error_message": str(e).split(" full error:")[0],
            "error_detail": err_info,
            "note": "Rejeitado na camada do banco — sem precisar de validação no código da aplicação.",
        }


# ── Inserção válida (com schema) ─────────────────────────────────────────────
@router.post("/insert-valid")
def insert_valid():
    if not _col_exists() or not _has_validator():
        raise HTTPException(status_code=409, detail="Ative o schema primeiro (step3).")
    doc = {"nome": "Smartphone Pro X", "preco": 2499.90, "categoria": "Eletrônicos", "em_estoque": True, "sku": "EL-1234"}
    result = db[COL].insert_one(doc)
    return {"status": "aceito", "inserted_id": str(result.inserted_id), "document": {k: v for k, v in doc.items() if k != "_id"}}


@router.get("/documents")
def list_documents():
    exists = _col_exists()
    docs = list(db[COL].find({}, {"_id": 0}).limit(10)) if exists else []
    options = _options() if exists else {}
    return {"documents": docs, "count": len(docs), "schema_active": _has_validator(),
            "constraint_active": options.get("validationLevel") == "constraint"}


@router.delete("/reset")
def reset():
    if _col_exists():
        db.drop_collection(COL)
    return {"status": "resetado"}
