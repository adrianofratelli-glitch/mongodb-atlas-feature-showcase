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
    python scripts/seed_geo.py --ensure   # só recria se versão/volume estiverem divergentes
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
from pymongo.errors import BulkWriteError, PyMongoError

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / "backend" / ".env")

SEMENTE = 20260726
# v4: estabelecimentos e terminais passam a ser entidades estáveis. Na v3 um
# terminal era sorteado por compra e `precisaoMetros=0` sugeria uma exatidão que
# cadastro de adquirente não garante. A proveniência continua forte, mas agora
# é modelada sem exagerar a qualidade da fonte.
VERSAO_DATASET = 4
ID_METADATA = "geo_seed"
DIAS = 90
DIR_DADOS = RAIZ / "backend" / "data"
ARQUIVO_FRAUDES = DIR_DADOS / "fraud_seeds.json"

# (município, UF, latitude, longitude, peso ~ população em milhões)
#
# A tabela vive em backend/data/municipios.json porque o canal de cartão
# presencial do módulo Streaming gera pontos ao vivo sobre as MESMAS
# coordenadas. Duas listas divergentes fariam o sinal ao vivo e o dataset
# histórico apontarem para cidades diferentes com o mesmo nome.
_MUN_JSON = json.loads((DIR_DADOS / "municipios.json").read_text(encoding="utf-8"))
MUNICIPIOS = [(m["municipio"], m["uf"], m["lat"], m["lng"], m["peso"]) for m in _MUN_JSON["municipios"]]


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
# Compra presencial: o que existe aqui é captura em terminal, não transferência.
# PIX vive no módulo 07 e não carrega coordenada — misturar os dois faria a tela
# sugerir que o arranjo PIX fornece geolocalização, o que é falso.
TIPOS = ["CARTAO_DEBITO"] * 6 + ["CARTAO_CREDITO"] * 4
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

    # Catálogo determinístico: o mesmo estabelecimento/terminal reaparece em
    # muitas compras e sua coordenada permanece fixa. Isso representa o dado
    # cadastral do adquirente, em vez de inventar uma maquininha por transação.
    terminais: dict[tuple[int, str], list[dict]] = {}
    for cidade, (_, _, lat, lng, _) in enumerate(MUNICIPIOS):
        for categoria in NOMES_CATEGORIAS:
            terminais[(cidade, categoria)] = [
                {
                    "id": f"POS{cidade:02d}{NOMES_CATEGORIAS.index(categoria):02d}{numero:02d}",
                    "nome": nome_estabelecimento(rng, categoria),
                    "coordinates": list(reversed(dispersar(rng, lat, lng, sigma_km=4.0))),
                }
                for numero in range(12)
            ]

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

            nome, uf, _, _, _ = MUNICIPIOS[cidade]
            categoria = rng.choice(NOMES_CATEGORIAS)
            terminal = rng.choice(terminais[(cidade, categoria)])
            capturada_em = ts - timedelta(seconds=rng.randint(1, 20))
            documentos.append({
                "clienteId": cliente_id,
                "endToEndId": f"E{cliente_id}{j:04d}",
                "datasetVersion": VERSAO_DATASET,
                "valor": Decimal128(f"{rng.lognormvariate(3.9, 0.9):.2f}"),
                "tipo": rng.choice(TIPOS),
                "status": rng.choice(STATUS),
                "estabelecimento": {
                    "nome": terminal["nome"],
                    "categoria": categoria,
                },
                "local": {"type": "Point", "coordinates": terminal["coordinates"]},
                # Compra PRESENCIAL com cartão: a coordenada é a do terminal do
                # estabelecimento, cadastrada pelo adquirente — não o GPS do
                # celular. Essa distinção é o que sustenta o impossible travel:
                # a localização cadastral não é controlada pelo aparelho do
                # cliente, mas ainda pode estar desatualizada ou incorreta. A
                # proveniência viaja junto para o sinal nunca parecer verdade
                # sem origem.
                "dispositivo": {"id": terminal["id"], "canal": "POS_PRESENCIAL"},
                "localizacaoMeta": {
                    "origem": "TERMINAL_ADQUIRENTE",
                    "qualidade": "CADASTRAL",
                    "capturadaEm": capturada_em,
                },
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


INDICES_OBRIGATORIOS = {
    "e2e_unq_idx",
    "cliente_status_local_idx",
    "local_2dsphere_idx",
    "cliente_ts_idx",
    "uf_ts_idx",
    "categoria_local_idx",
}


def registrar_dataset(banco, alvo: int) -> None:
    banco["demo_metadata"].replace_one(
        {"_id": ID_METADATA},
        {
            "_id": ID_METADATA,
            "datasetVersion": VERSAO_DATASET,
            "semente": SEMENTE,
            "documentos": alvo,
            "atualizadoEm": datetime.now(timezone.utc),
        },
        upsert=True,
    )


def verificar_prontidao(banco, colecao, alvo: int) -> tuple[bool, list[str]]:
    """Preflight rápido e read-only usado no início da apresentação."""
    problemas = []
    metadata = banco["demo_metadata"].find_one({"_id": ID_METADATA}) or {}
    esperado = (VERSAO_DATASET, SEMENTE, alvo)
    encontrado = (
        metadata.get("datasetVersion"),
        metadata.get("semente"),
        metadata.get("documentos"),
    )
    if encontrado != esperado:
        problemas.append("metadata do dataset Geo ausente ou divergente")

    total = colecao.estimated_document_count()
    if total != alvo:
        problemas.append(f"Geo contém {total} documentos; esperado {alvo}")

    indices = {indice["name"] for indice in colecao.list_indexes()}
    ausentes = sorted(INDICES_OBRIGATORIOS - indices)
    if ausentes:
        problemas.append("índices MongoDB ausentes: " + ", ".join(ausentes))

    nome_search = os.getenv("GEO_SEARCH_INDEX", "idx_geo_estabelecimento").strip()
    try:
        search = list(colecao.list_search_indexes(nome_search))
    except PyMongoError as erro:
        problemas.append(f"não foi possível consultar Atlas Search: {erro}")
    else:
        indice = search[0] if search else {}
        if indice.get("status") != "READY" or indice.get("queryable") is False:
            problemas.append(f"Atlas Search '{nome_search}' não está READY/consultável")
    return not problemas, problemas


def dataset_atual(colecao, alvo: int) -> tuple[bool, str]:
    """Valida a identidade do dataset sem depender de uma amostra otimista."""
    total = colecao.count_documents({})
    if total != alvo:
        return False, f"volume {total}, esperado {alvo}"
    divergente = colecao.find_one(
        {"datasetVersion": {"$ne": VERSAO_DATASET}},
        {"_id": 1},
    )
    if divergente is not None:
        return False, f"há documentos anteriores à versão {VERSAO_DATASET}"
    if not ARQUIVO_FRAUDES.exists():
        return False, f"manifesto ausente: {ARQUIVO_FRAUDES}"
    try:
        manifesto = json.loads(ARQUIVO_FRAUDES.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False, f"manifesto inválido: {ARQUIVO_FRAUDES}"
    if manifesto.get("dataset_version") != VERSAO_DATASET or manifesto.get("semente") != SEMENTE:
        return False, "manifesto pertence a outra versão/semente"
    return True, f"{total} documentos na versão {VERSAO_DATASET}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed do módulo Geo")
    modo = parser.add_mutually_exclusive_group()
    modo.add_argument("--drop", action="store_true", help="apaga a coleção antes de gerar")
    modo.add_argument(
        "--ensure",
        action="store_true",
        help="mantém o dataset atual ou recria automaticamente se estiver divergente",
    )
    modo.add_argument(
        "--check",
        action="store_true",
        help="preflight rápido e read-only; não cria nem altera dados/índices",
    )
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

    alvo = args.clientes * args.por_cliente
    if args.check:
        pronto, problemas = verificar_prontidao(banco, colecao, alvo)
        if pronto:
            print(f"demo pronta: Geo v{VERSAO_DATASET}, {alvo} documentos e Atlas Search READY")
            cliente.close()
            return 0
        for problema in problemas:
            print(f"❌ {problema}", file=sys.stderr)
        print("rode ./scripts/prepare-demo.sh antes da apresentação", file=sys.stderr)
        cliente.close()
        return 1

    if args.ensure:
        atual, detalhe = dataset_atual(colecao, alvo)
        if atual:
            print(f"dataset Geo pronto: {detalhe}")
            print("validando índices idempotentes…")
            for nome in criar_indices(colecao):
                print(f"  · {nome}")
            registrar_dataset(banco, alvo)
            cliente.close()
            return 0
        print(f"dataset Geo divergente ({detalhe}); recriando a coleção dedicada")
        banco["demo_metadata"].delete_one({"_id": ID_METADATA})
        colecao.drop()

    if args.drop:
        banco["demo_metadata"].delete_one({"_id": ID_METADATA})
        colecao.drop()
        print(f"coleção {banco.name}.transacoes removida")

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
                "dataset_version": VERSAO_DATASET,
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
        print("erro: total diferente do alvo — rode com --drop para recriar do zero", file=sys.stderr)
        cliente.close()
        return 1
    registrar_dataset(banco, alvo)
    cliente.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
