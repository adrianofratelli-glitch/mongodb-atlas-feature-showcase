"""
Popula o banco com dados sintéticos para a demo. Idempotente.

Uso:
    python seed_data.py                  # 100k produtos + 20k avaliações (rápido)
    python seed_data.py --full           # 5M produtos + 1M avaliações (como na demo original)
    python seed_data.py --produtos 500000 --avaliacoes 100000

Reexecutar não duplica nada: cada documento tem chave natural determinística
(`produto_id` = uuid5 do índice; `_id` = "seed-av-<n>" nas avaliações) e é
gravado por upsert. A geração usa um `random.Random` semeado, então a segunda
execução reescreve os mesmos documentos em vez de somar outros.

`produtos` e `avaliacoes` podem ser coleções compartilhadas com outra PoV no
mesmo banco (no cluster de demo, `POC` também atende o marketplace de busca).
Por isso o seed nunca dropa essas coleções: só faz upsert dos seus próprios
documentos e garante os índices B-tree que os módulos usam.

Guarda: sem ALLOW_DEMO_DB_WRITE=1, só aceita `showcase_*_test`; com a flag, só o MONGO_DB do backend/.env.

Requer backend/.env configurado (MONGO_URI, MONGO_DB).
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import OperationFailure

load_dotenv(Path(__file__).resolve().parent / ".env")

CATEGORIAS = ["Eletrônicos", "Moda", "Casa", "Esportes", "Livros", "Brinquedos"]
MARCAS     = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Omega"]
ADJETIVOS  = ["Pro", "Max", "Ultra", "Lite", "Plus", "Mini", "Smart", "Premium"]
PRODUTOS_BASE = ["Fone", "Notebook", "Camiseta", "Tênis", "Luminária", "Mochila",
                 "Teclado", "Monitor", "Livro", "Boneco", "Bola", "Cadeira",
                 "Mesa", "Relógio", "Caixa de Som", "Tablet", "Câmera", "Jaqueta"]
USUARIOS   = [f"usuario_{i:04d}" for i in range(2000)]
TITULOS    = ["Excelente!", "Muito bom", "Recomendo", "Poderia ser melhor",
              "Atendeu as expectativas", "Surpreendente", "Ok pelo preço", "Não gostei"]
COMENTARIOS = ["Produto de ótima qualidade, chegou rápido.",
               "Funciona bem, mas a embalagem veio amassada.",
               "Melhor custo-benefício que já vi.",
               "Esperava mais pela descrição.",
               "Já é minha segunda compra, aprovado.",
               "Entrega demorou, mas o produto compensa."]

BATCH = 10_000
SEED = 20260611
# Namespace fixo: o mesmo índice gera sempre o mesmo produto_id.
PRODUTO_NS = uuid.UUID("6f1d0c1e-5b7a-4c4e-9f61-0c5d2b8a7e10")
AVALIACAO_PREFIXO = "seed-av-"


ENV_FILE = Path(__file__).resolve().parent / ".env"


def mongo_db_da_demo(env_file: Path | None = None) -> str:
    """MONGO_DB segundo o ARQUIVO backend/.env (padrão `POC`), sem override do ambiente.

    A flag ALLOW_DEMO_DB_WRITE=1 autoriza o banco que a PoV está configurada para
    usar, não qualquer nome exportado no shell (um `POC_test` pode ser de outra PoV).
    """
    arquivo = ENV_FILE if env_file is None else env_file
    if arquivo.exists():
        for linha in arquivo.read_text(encoding="utf-8").splitlines():
            linha = linha.strip()
            if linha.startswith("MONGO_DB=") and not linha.startswith("#"):
                valor = linha.split("=", 1)[1].strip().strip("\"'")
                if valor:
                    return valor
    return "POC"


def assert_writable_db(name: str, env_file: Path | None = None) -> None:
    """Aceita `showcase_*_test`; com ALLOW_DEMO_DB_WRITE=1, também o MONGO_DB do backend/.env."""
    name = (name or "").strip()
    if not name:
        raise SystemExit("❌ MONGO_DB vazio — informe o banco de destino.")
    if name.endswith("_test_test"):
        raise SystemExit(f"❌ '{name}' tem sufixo _test duplicado — confira MONGO_DB.")
    if name.startswith("showcase_") and name.endswith("_test"):
        return
    if os.getenv("ALLOW_DEMO_DB_WRITE") == "1":
        demo = mongo_db_da_demo(env_file)
        if name == demo:
            print(f"⚠  ALLOW_DEMO_DB_WRITE=1 — escrevendo no banco '{name}'.")
            return
        raise SystemExit(
            f"❌ Recusado: MONGO_DB='{name}' não é o banco da demo do backend/.env ('{demo}') nem um "
            "banco próprio showcase_*_test. ALLOW_DEMO_DB_WRITE=1 só autoriza o banco do backend/.env; "
            "confira se há um override exportado no shell."
        )
    raise SystemExit(
        f"❌ Recusado: '{name}' não é um banco próprio desta PoV (showcase_*_test; um *_test genérico "
        "pode ser de outra PoV). Para o banco da demo (o do backend/.env), rode de novo com "
        "ALLOW_DEMO_DB_WRITE=1 (e nunca durante uma demo ao vivo)."
    )


def produto_id_de(indice: int) -> str:
    return str(uuid.uuid5(PRODUTO_NS, f"produto-{indice}"))


def gerar_produto(rng: random.Random | None = None, indice: int | None = None):
    rnd = rng or random.Random()
    total_av = rnd.choices([0, rnd.randint(1, 50), rnd.randint(51, 500), rnd.randint(501, 5000)],
                              weights=[20, 50, 25, 5])[0]
    return {
        "produto_id":       produto_id_de(indice) if indice is not None else str(uuid.uuid4()),
        "nome":             f"{rnd.choice(PRODUTOS_BASE)} {rnd.choice(MARCAS)} {rnd.choice(ADJETIVOS)}",
        "sku":              f"SKU-{rnd.randint(100000, 999999)}",
        "categoria":        rnd.choice(CATEGORIAS),
        "marca":            rnd.choice(MARCAS),
        "preco":            round(rnd.uniform(9.9, 9999.0), 2),
        "em_estoque":       rnd.random() < 0.7,
        "avaliacao_media":  round(rnd.uniform(1.0, 5.0), 1) if total_av else None,
        "total_avaliacoes": total_av,
        "created_at":       datetime.now(timezone.utc) - timedelta(days=rnd.randint(0, 1825)),
    }


def gerar_avaliacao(produtos_ref, rng: random.Random | None = None, indice: int | None = None):
    rnd = rng or random.Random()
    produto_id, categoria = rnd.choice(produtos_ref)
    doc = {
        "produto_id": produto_id,
        "categoria":  categoria,
        "usuario":    rnd.choice(USUARIOS),
        "nota":       rnd.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 35, 37])[0],
        "titulo":     rnd.choice(TITULOS),
        "comentario": rnd.choice(COMENTARIOS),
        "data":       datetime.now(timezone.utc) - timedelta(days=rnd.randint(0, 730)),
    }
    if indice is not None:
        doc["_id"] = f"{AVALIACAO_PREFIXO}{indice:07d}"
    return doc


# IndexOptionsConflict / IndexKeySpecsConflict: já existe índice com o mesmo
# nome ou a mesma chave, com outras opções.
_CONFLITO_DE_INDICE = {85, 86}


def _ensure(col, keys, **kwargs) -> None:
    """create_index tolerante a um índice equivalente que já existe.

    Numa coleção compartilhada (o marketplace de busca também usa
    `POC.produtos`) a outra PoV pode ter criado `produto_id_1` sem `unique`.
    Recriar o índice dela não é papel deste seed: o upsert por `produto_id`
    continua idempotente sem o unique, que só protege execuções paralelas.
    """
    try:
        col.create_index(keys, **kwargs)
    except OperationFailure as exc:
        if exc.code not in _CONFLITO_DE_INDICE:
            raise
        print(f"⚠  {col.name}: índice equivalente a {keys} já existe com outras opções; mantido.")


def ensure_indexes(db) -> None:
    """Índices usados pelas demos. create_index é idempotente."""
    _ensure(db["produtos"], "produto_id", unique=True)
    db["produtos"].create_index("em_estoque")
    db["produtos"].create_index("categoria")
    db["produtos"].create_index([("total_avaliacoes", -1)], name="total_av_idx")
    # Atende o match (categoria) + sort (total_avaliacoes desc) do módulo
    # $setWindowFields sem blocking sort em memória.
    db["produtos"].create_index([("categoria", 1), ("total_avaliacoes", -1)], name="cat_total_av_idx")
    db["produtos"].create_index(
        [("em_estoque", 1), ("total_avaliacoes", -1), ("avaliacao_media", -1)],
        name="destaque_idx",
    )
    db["avaliacoes"].create_index("produto_id")
    db["avaliacoes"].create_index([("data", -1), ("nota", -1)], name="recent_nota_idx")


# Índices que o seed cria. O módulo 01 nunca pode removê-los.
SEED_INDEX_NAMES = frozenset({
    "_id_", "produto_id_1", "em_estoque_1", "categoria_1", "total_av_idx",
    "cat_total_av_idx", "destaque_idx",
})


def _flush(col, ops):
    if ops:
        col.bulk_write(ops, ordered=False)
        ops.clear()


def seed(n_produtos, n_avaliacoes, *, db=None, verbose=True):
    """Upsert determinístico. Retorna o que foi gravado."""
    if n_avaliacoes and not n_produtos:
        raise ValueError("Não é possível gerar avaliações sem ao menos um produto.")
    client = None
    if db is None:
        mongo_uri = (os.getenv("MONGO_URI") or "").strip()
        if not mongo_uri:
            raise RuntimeError("MONGO_URI não configurada. Copie backend/.env.example para backend/.env.")
        db_name = os.getenv("MONGO_DB", "POC")
        assert_writable_db(db_name)
        client = MongoClient(mongo_uri, appname="mongodb-atlas-feature-showcase-seed")
        db = client[db_name]
    log = print if verbose else (lambda *a, **k: None)
    try:
        # O índice unique precisa existir antes: é ele que torna o upsert por
        # produto_id barato e impede duplicata mesmo com execuções paralelas.
        _ensure(db["produtos"], "produto_id", unique=True)

        log(f"Gravando {n_produtos:,} produtos (upsert)…")
        rng = random.Random(SEED)
        produtos_ref = []
        ops = []
        for i in range(n_produtos):
            p = gerar_produto(rng, i)
            if len(produtos_ref) < 50_000:
                produtos_ref.append((p["produto_id"], p["categoria"]))
            ops.append(ReplaceOne({"produto_id": p["produto_id"]}, p, upsert=True))
            if len(ops) >= BATCH:
                _flush(db["produtos"], ops)
                log(f"  {i + 1:,}/{n_produtos:,}", end="\r")
        _flush(db["produtos"], ops)
        log(f"\n✓ produtos: {db['produtos'].estimated_document_count():,}")

        log(f"Gravando {n_avaliacoes:,} avaliações (upsert)…")
        rng_av = random.Random(SEED + 1)
        for i in range(n_avaliacoes):
            a = gerar_avaliacao(produtos_ref, rng_av, i)
            ops.append(ReplaceOne({"_id": a["_id"]}, a, upsert=True))
            if len(ops) >= BATCH:
                _flush(db["avaliacoes"], ops)
                log(f"  {i + 1:,}/{n_avaliacoes:,}", end="\r")
        _flush(db["avaliacoes"], ops)
        log(f"\n✓ avaliações: {db['avaliacoes'].estimated_document_count():,}")

        log("Garantindo índices usados pelas demos…")
        ensure_indexes(db)
        log("✓ índices garantidos. Pronto!")
        return {"produtos": n_produtos, "avaliacoes": n_avaliacoes}
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed de dados sintéticos para a demo")
    parser.add_argument("--full", action="store_true", help="5M produtos + 1M avaliações")
    parser.add_argument("--produtos", type=int, default=100_000)
    parser.add_argument("--avaliacoes", type=int, default=20_000)
    args = parser.parse_args()

    if args.produtos < 0 or args.avaliacoes < 0:
        parser.error("As quantidades devem ser maiores ou iguais a zero.")

    if args.full:
        seed(5_000_000, 1_000_000)
    else:
        seed(args.produtos, args.avaliacoes)
