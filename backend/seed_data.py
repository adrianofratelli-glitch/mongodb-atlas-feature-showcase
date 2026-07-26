"""
Popula o banco com dados sintéticos para a demo.

Uso:
    python seed_data.py                  # 100k produtos + 20k avaliações (rápido)
    python seed_data.py --full           # 5M produtos + 1M avaliações (como na demo original)
    python seed_data.py --produtos 500000 --avaliacoes 100000

Requer backend/.env configurado (MONGO_URI, MONGO_DB).
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
import os

load_dotenv()

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


def gerar_produto():
    total_av = random.choices([0, random.randint(1, 50), random.randint(51, 500), random.randint(501, 5000)],
                              weights=[20, 50, 25, 5])[0]
    return {
        "produto_id":       str(uuid.uuid4()),
        "nome":             f"{random.choice(PRODUTOS_BASE)} {random.choice(MARCAS)} {random.choice(ADJETIVOS)}",
        "sku":              f"SKU-{random.randint(100000, 999999)}",
        "categoria":        random.choice(CATEGORIAS),
        "marca":            random.choice(MARCAS),
        "preco":            round(random.uniform(9.9, 9999.0), 2),
        "em_estoque":       random.random() < 0.7,
        "avaliacao_media":  round(random.uniform(1.0, 5.0), 1) if total_av else None,
        "total_avaliacoes": total_av,
        "created_at":       datetime.now(timezone.utc) - timedelta(days=random.randint(0, 1825)),
    }


def gerar_avaliacao(produtos_ref):
    produto_id, categoria = random.choice(produtos_ref)
    return {
        "produto_id": produto_id,
        "categoria":  categoria,
        "usuario":    random.choice(USUARIOS),
        "nota":       random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 35, 37])[0],
        "titulo":     random.choice(TITULOS),
        "comentario": random.choice(COMENTARIOS),
        "data":       datetime.now(timezone.utc) - timedelta(days=random.randint(0, 730)),
    }


def seed(n_produtos, n_avaliacoes):
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        raise RuntimeError("MONGO_URI não configurada. Copie backend/.env.example para backend/.env.")
    client = MongoClient(mongo_uri, appname="mongodb-atlas-feature-showcase-seed")
    db = client[os.getenv("MONGO_DB", "POC")]

    print(f"Inserindo {n_produtos:,} produtos…")
    produtos_ref = []
    buf = []
    for i in range(n_produtos):
        p = gerar_produto()
        # guarda uma amostra de ids para vincular as avaliações
        if len(produtos_ref) < 50_000:
            produtos_ref.append((p["produto_id"], p["categoria"]))
        buf.append(p)
        if len(buf) >= BATCH:
            db["produtos"].insert_many(buf, ordered=False)
            buf.clear()
            print(f"  {i + 1:,}/{n_produtos:,}", end="\r")
    if buf:
        db["produtos"].insert_many(buf, ordered=False)
    print(f"\n✓ produtos: {db['produtos'].estimated_document_count():,}")

    if n_avaliacoes and not produtos_ref:
        raise ValueError("Não é possível gerar avaliações sem ao menos um produto.")

    print(f"Inserindo {n_avaliacoes:,} avaliações…")
    buf = []
    for i in range(n_avaliacoes):
        buf.append(gerar_avaliacao(produtos_ref))
        if len(buf) >= BATCH:
            db["avaliacoes"].insert_many(buf, ordered=False)
            buf.clear()
            print(f"  {i + 1:,}/{n_avaliacoes:,}", end="\r")
    if buf:
        db["avaliacoes"].insert_many(buf, ordered=False)
    print(f"\n✓ avaliações: {db['avaliacoes'].estimated_document_count():,}")

    print("Criando índices usados pelas demos…")
    db["produtos"].create_index("em_estoque")
    db["produtos"].create_index("categoria")
    db["produtos"].create_index([("total_avaliacoes", -1)], name="total_av_idx")
    # Atende o match (categoria) + sort (total_avaliacoes desc) do módulo
    # $setWindowFields sem blocking sort em memória.
    db["produtos"].create_index([("categoria", 1), ("total_avaliacoes", -1)], name="cat_total_av_idx")
    db["produtos"].create_index("produto_id", unique=True)
    db["produtos"].create_index(
        [("em_estoque", 1), ("total_avaliacoes", -1), ("avaliacao_media", -1)],
        name="destaque_idx",
    )
    db["avaliacoes"].create_index("produto_id")
    db["avaliacoes"].create_index([("data", -1), ("nota", -1)], name="recent_nota_idx")
    print("✓ índices criados. Pronto!")


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
