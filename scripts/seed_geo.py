#!/usr/bin/env python3
"""Seed do módulo Geo — transações georreferenciadas para o database `geo`.

Gera `geo.transacoes` com entropia geográfica realista: os pontos são clusters
gaussianos ao redor de 40 municípios brasileiros reais, com peso proporcional à
população. Coordenada uniforme dentro do bounding box do país é visualmente
óbvia numa apresentação e destrói a credibilidade da demonstração.

O script é idempotente: o gerador usa uma semente fixa, então o `endToEndId` de
cada documento é sempre o mesmo, e o índice único bloqueia a reinserção. Rodar
duas vezes mantém o mesmo total.

    python scripts/seed_geo.py            # cria (ou completa) o dataset
    python scripts/seed_geo.py --drop     # recria do zero
    python scripts/seed_geo.py --clientes 500 --por-cliente 20   # dataset menor

Nunca aponte para um cluster que não seja descartável: `--drop` apaga a coleção.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bson import Decimal128
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient
from pymongo.errors import BulkWriteError

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / "backend" / ".env")

SEMENTE = 20260726
DIAS = 90
DIR_DADOS = RAIZ / "backend" / "data"
ARQUIVO_FRAUDES = DIR_DADOS / "fraud_seeds.json"

# (município, UF, latitude, longitude, peso ~ população em milhões)
MUNICIPIOS = [
    ("São Paulo", "SP", -23.5505, -46.6333, 12.3),
    ("Rio de Janeiro", "RJ", -22.9068, -43.1729, 6.7),
    ("Brasília", "DF", -15.7939, -47.8828, 3.0),
    ("Salvador", "BA", -12.9777, -38.5016, 2.9),
    ("Fortaleza", "CE", -3.7319, -38.5267, 2.7),
    ("Belo Horizonte", "MG", -19.9167, -43.9345, 2.5),
    ("Manaus", "AM", -3.1190, -60.0217, 2.2),
    ("Curitiba", "PR", -25.4284, -49.2733, 1.9),
    ("Recife", "PE", -8.0476, -34.8770, 1.6),
    ("Goiânia", "GO", -16.6869, -49.2648, 1.5),
    ("Belém", "PA", -1.4558, -48.5044, 1.5),
    ("Porto Alegre", "RS", -30.0346, -51.2177, 1.5),
    ("Guarulhos", "SP", -23.4538, -46.5333, 1.4),
    ("Campinas", "SP", -22.9099, -47.0626, 1.2),
    ("São Luís", "MA", -2.5297, -44.3028, 1.1),
    ("Maceió", "AL", -9.6658, -35.7353, 1.0),
    ("Campo Grande", "MS", -20.4697, -54.6201, 0.9),
    ("Natal", "RN", -5.7945, -35.2110, 0.9),
    ("Teresina", "PI", -5.0892, -42.8019, 0.87),
    ("João Pessoa", "PB", -7.1195, -34.8450, 0.83),
    ("Osasco", "SP", -23.5329, -46.7916, 0.75),
    ("Santo André", "SP", -23.6639, -46.5383, 0.72),
    ("Jaboatão dos Guararapes", "PE", -8.1128, -35.0147, 0.70),
    ("Ribeirão Preto", "SP", -21.1775, -47.8103, 0.70),
    ("Uberlândia", "MG", -18.9186, -48.2772, 0.70),
    ("Sorocaba", "SP", -23.5015, -47.4526, 0.69),
    ("Contagem", "MG", -19.9317, -44.0536, 0.67),
    ("Aracaju", "SE", -10.9472, -37.0731, 0.66),
    ("Feira de Santana", "BA", -12.2664, -38.9663, 0.62),
    ("Cuiabá", "MT", -15.6014, -56.0979, 0.62),
    ("Joinville", "SC", -26.3044, -48.8456, 0.60),
    ("Londrina", "PR", -23.3045, -51.1696, 0.58),
    ("Juiz de Fora", "MG", -21.7642, -43.3503, 0.57),
    ("Porto Velho", "RO", -8.7612, -63.9004, 0.54),
    ("Serra", "ES", -20.1211, -40.3078, 0.53),
    ("Niterói", "RJ", -22.8832, -43.1034, 0.51),
    ("Florianópolis", "SC", -27.5954, -48.5480, 0.51),
    ("Macapá", "AP", 0.0349, -51.0694, 0.51),
    ("Campos dos Goytacazes", "RJ", -21.7545, -41.3244, 0.51),
    ("Vila Velha", "ES", -20.3297, -40.2925, 0.50),
]

CATEGORIAS = {
    "alimentação": (
        ["Restaurante", "Padaria", "Lanchonete", "Cafeteria", "Pizzaria", "Mercado", "Açaí"],
        ["do Centro", "da Praça", "Bom Sabor", "da Esquina", "Sabor Caseiro", "Dona Zica", "Seu Antônio"],
    ),
    "combustível": (
        ["Posto", "Auto Posto", "Rede Posto"],
        ["Ipiranga Centro", "Estrela", "Bandeirantes", "São Cristóvão", "Rodovia Norte", "Cidade Alta"],
    ),
    "farmácia": (
        ["Farmácia", "Drogaria", "Farma"],
        ["Popular", "São Paulo", "do Bairro", "Saúde Total", "Vida Nova", "Central"],
    ),
    "vestuário": (
        ["Loja", "Boutique", "Magazine", "Confecções"],
        ["Estilo", "Moda Brasil", "Elegance", "Tropical", "Alfaiataria Souza", "Jeans & Cia"],
    ),
    "serviços": (
        ["Barbearia", "Oficina", "Lavanderia", "Salão", "Assistência"],
        ["do Zé", "Prime", "Express", "Rápida", "24 Horas", "Bairro Novo"],
    ),
}
NOMES_CATEGORIAS = list(CATEGORIAS)
TIPOS = ["PIX", "PIX", "PIX", "TED", "CARTAO", "CARTAO"]
STATUS = ["APROVADA"] * 8 + ["NEGADA", "PENDENTE"]

RAIO_TERRA_KM = 6371.0088


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Mesma fórmula que o pipeline de impossible travel executa em MQL puro."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * RAIO_TERRA_KM * math.asin(min(1.0, math.sqrt(a)))


def dispersar(rng: random.Random, lat: float, lng: float, sigma_km: float = 4.0) -> tuple[float, float]:
    """Ruído gaussiano de poucos quilômetros dentro do município."""
    d_lat = rng.gauss(0, sigma_km) / 111.32
    d_lng = rng.gauss(0, sigma_km) / (111.32 * max(0.2, math.cos(math.radians(lat))))
    return round(lat + d_lat, 6), round(lng + d_lng, 6)


def nome_estabelecimento(rng: random.Random, categoria: str) -> str:
    prefixos, sufixos = CATEGORIAS[categoria]
    return f"{rng.choice(prefixos)} {rng.choice(sufixos)}"


def municipio_distante(origem: int, minimo_km: float = 700.0) -> int:
    """Índice de um município a pelo menos `minimo_km` da origem — determinístico."""
    _, _, lat, lng, _ = MUNICIPIOS[origem]
    for i, (_, _, o_lat, o_lng, _) in enumerate(MUNICIPIOS):
        if i != origem and haversine_km(lat, lng, o_lat, o_lng) >= minimo_km:
            return i
    raise RuntimeError(f"nenhum município a mais de {minimo_km} km de {MUNICIPIOS[origem][0]}")


def gerar(clientes: int, por_cliente: int, fraudes: int):
    """Devolve (documentos, clientes com fraude plantada).

    Cada cliente tem um município de origem e uma sequência temporal ordenada ao
    longo de `DIAS`. O deslocamento entre transações consecutivas é plausível:
    a maioria fica no cluster de origem e as viagens ocorrem com horas de
    intervalo. Os casos de fraude quebram essa regra de propósito.
    """
    rng = random.Random(SEMENTE)
    pesos = [m[4] for m in MUNICIPIOS]
    agora = datetime(2026, 7, 26, 12, 0, 0, tzinfo=timezone.utc)
    inicio = agora - timedelta(days=DIAS)
    espaco = (DIAS * 24 * 3600) / por_cliente

    documentos, plantados = [], []
    for c in range(clientes):
        cliente_id = f"CLI{c:05d}"
        origem = rng.choices(range(len(MUNICIPIOS)), weights=pesos, k=1)[0]
        plantar = c < fraudes
        # O par plantado fica no meio da sequência, longe das bordas.
        idx_fraude = por_cliente // 2 if plantar else -1
        destino_fraude = municipio_distante(origem) if plantar else -1

        anterior_ts = None
        for j in range(por_cliente):
            # Grade regular com jitter: mantém a ordem temporal e um intervalo
            # mínimo de horas entre transações normais.
            desloc = espaco * (j + rng.uniform(0.15, 0.85))
            ts = inicio + timedelta(seconds=desloc)

            if j == idx_fraude:
                # Mesmo cliente, ~5 min depois, a 700+ km de distância.
                ts = anterior_ts + timedelta(minutes=5)
                cidade = destino_fraude
            elif j == idx_fraude - 1:
                # A transação anterior ao par plantado fica ancorada na origem,
                # senão uma viagem aleatória poderia encurtar a distância.
                cidade = origem
            elif rng.random() < 0.08:
                cidade = rng.choices(range(len(MUNICIPIOS)), weights=pesos, k=1)[0]
            else:
                cidade = origem

            nome, uf, lat, lng, _ = MUNICIPIOS[cidade]
            p_lat, p_lng = dispersar(rng, lat, lng)
            categoria = rng.choice(NOMES_CATEGORIAS)
            documentos.append({
                "clienteId": cliente_id,
                "endToEndId": f"E{cliente_id}{j:04d}",
                "valor": Decimal128(f"{rng.lognormvariate(3.9, 0.9):.2f}"),
                "tipo": rng.choice(TIPOS),
                "status": rng.choice(STATUS),
                "estabelecimento": {
                    "nome": nome_estabelecimento(rng, categoria),
                    "categoria": categoria,
                },
                "local": {"type": "Point", "coordinates": [p_lng, p_lat]},
                "uf": uf,
                "municipio": nome,
                "ts": ts,
            })
            anterior_ts = ts

        if plantar:
            plantados.append(cliente_id)

    return documentos, plantados


def criar_indices(colecao) -> list[str]:
    """Índices que as três demonstrações assumem, com nomes explícitos."""
    return [
        colecao.create_index([("endToEndId", ASCENDING)], name="e2e_unq_idx", unique=True),
        # Demo A: igualdade primeiro, geo por último. O campo geo NÃO precisa ser
        # prefixo para $geoWithin/$geoIntersects.
        colecao.create_index(
            [("clienteId", ASCENDING), ("status", ASCENDING), ("local", GEOSPHERE)],
            name="cliente_status_local_idx",
        ),
        # Demo A: geo puro, o outro lado do comparativo de explain.
        colecao.create_index([("local", GEOSPHERE)], name="local_2dsphere_idx"),
        # Demo B: $setWindowFields particionado por cliente e ordenado por ts.
        colecao.create_index([("clienteId", ASCENDING), ("ts", ASCENDING)], name="cliente_ts_idx"),
        colecao.create_index([("uf", ASCENDING), ("ts", DESCENDING)], name="uf_ts_idx"),
        colecao.create_index(
            [("estabelecimento.categoria", ASCENDING), ("local", GEOSPHERE)],
            name="categoria_local_idx",
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed do módulo Geo")
    parser.add_argument("--drop", action="store_true", help="apaga a coleção antes de gerar")
    parser.add_argument("--clientes", type=int, default=2_000)
    parser.add_argument("--por-cliente", type=int, default=75)
    parser.add_argument("--fraudes", type=int, default=40)
    args = parser.parse_args()

    if args.fraudes > args.clientes:
        print("--fraudes não pode ser maior que --clientes", file=sys.stderr)
        return 2
    if args.por_cliente < 4:
        print("--por-cliente precisa ser ao menos 4 para plantar um par", file=sys.stderr)
        return 2

    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        print("MONGO_URI ausente — configure backend/.env", file=sys.stderr)
        return 1

    cliente = MongoClient(uri, appname="showcase-seed-geo", serverSelectionTimeoutMS=15_000)
    banco = cliente[os.getenv("GEO_DB", "geo").strip() or "geo"]
    colecao = banco["transacoes"]

    if args.drop:
        colecao.drop()
        print(f"coleção {banco.name}.transacoes removida")

    alvo = args.clientes * args.por_cliente
    documentos, plantados = gerar(args.clientes, args.por_cliente, args.fraudes)
    print(f"gerados {len(documentos)} documentos em memória ({args.clientes} clientes)")

    print("criando índices…")
    for nome in criar_indices(colecao):
        print(f"  · {nome}")

    inseridos = 0
    for i in range(0, len(documentos), 5_000):
        lote = documentos[i:i + 5_000]
        try:
            inseridos += len(colecao.insert_many(lote, ordered=False).inserted_ids)
        except BulkWriteError as erro:
            # Reexecução: o índice único em endToEndId rejeita o que já existe.
            # Qualquer outra causa de erro precisa aparecer.
            outros = [e for e in erro.details.get("writeErrors", []) if e.get("code") != 11000]
            if outros:
                raise
            inseridos += erro.details.get("nInserted", 0)
        print(f"  {min(i + 5_000, len(documentos))}/{len(documentos)}", end="\r")

    DIR_DADOS.mkdir(parents=True, exist_ok=True)
    ARQUIVO_FRAUDES.write_text(
        json.dumps(
            {
                "gerado_em": datetime.now(timezone.utc).isoformat(),
                "semente": SEMENTE,
                "limite_kmh": 900,
                "clientes": plantados,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    total = colecao.count_documents({})
    print(f"\ninseridos nesta execução: {inseridos}")
    print(f"total em {banco.name}.transacoes: {total} (alvo {alvo})")
    print(f"clientes com par de fraude plantado: {len(plantados)} → {ARQUIVO_FRAUDES}")
    if total != alvo:
        print("aviso: total diferente do alvo — rode com --drop para recriar do zero")
    cliente.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
