"""
Router: Streaming — as três formas de reagir a mudanças no Atlas, lado a lado.

Um único gerador de escritas (pix.transacoes) alimenta as três colunas:

  1. Change Streams          — collection.watch() dentro do próprio backend.
  2. MongoDB Kafka Connector — source connector publica em atlas.pix.transacoes;
                               o backend consome o tópico.
  3. Atlas Stream Processing — processor gerenciado agrega em janelas de 10s e
                               faz $merge em pix.metricas_janela; o backend lê o
                               resultado por change stream (fecho didático: o
                               resultado do ASP volta pela coluna 1).

REGRA: o workload é sintético, mas todos os caminhos e resultados são reais.
O objetivo é provar integridade, recuperação e capacidades com carga moderada,
não produzir um benchmark ou sizing de produção.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import math
import random
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from bson import Decimal128
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure, PyMongoError

from database import client
from settings import settings

router = APIRouter(prefix="/streaming", tags=["Streaming"])
logger = logging.getLogger("showcase.streaming")

STREAM_DB = os.getenv("STREAMING_DB", "pix").strip() or "pix"
COL_TX = "transacoes"
COL_WINDOWS = "metricas_janela"
COL_DLQ = "dlq"
COL_DLQ_AUDIT = "dlq_audit"
COL_CHECKPOINTS = "consumer_checkpoints"
TOPIC = f"atlas.{STREAM_DB}.{COL_TX}"
CONNECTOR_NAME = os.getenv("CONNECT_CONNECTOR_NAME", "atlas-pix-source").strip() or "atlas-pix-source"
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083").strip()
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092").strip()
KAFKA_CONSUMER_GROUP = (
    os.getenv("KAFKA_CONSUMER_GROUP", "showcase-pix-observer").strip()
    or "showcase-pix-observer"
)
# Modo ao vivo = ambiente de GRAVAÇÃO (ASP + Kafka provisionados). Fora dele, a
# aba 07 reproduz uma execução já gravada e esses serviços ficam desligados de
# propósito — ver `scripts/ambiente.sh` e `bin/overview --ao-vivo`.
AO_VIVO = os.getenv("STREAMING_AO_VIVO", "0").strip().lower() in {"1", "true", "yes"}
ASP_ENABLED = os.getenv("ASP_ENABLED", "false").strip().lower() in {"1", "true", "yes"}
ASP_CONNECTION_STRING = os.getenv("ASP_CONNECTION_STRING", "").strip()
ASP_PROCESSOR_NAME = os.getenv("ASP_PROCESSOR_NAME", "pixJanelas5s").strip() or "pixJanelas5s"
ASP_CONNECTION_NAME = os.getenv("ASP_CONNECTION_NAME", "atlasCluster").strip() or "atlasCluster"
# Segundo processor: o sinal de risco geográfico em event time. Existe separado
# porque um pipeline implantado tem um único sink terminal — este termina em
# geo.sinais_ao_vivo, o outro em pix.metricas_janela.
ASP_GEO_PROCESSOR_NAME = (
    os.getenv("ASP_GEO_PROCESSOR_NAME", "geoSinais30s").strip() or "geoSinais30s"
)
# Só para exibição: o que está provisionado nesta PoV.
CLUSTER_TIER = os.getenv("CLUSTER_TIER", "M20").strip() or "M20"
# O teto permite calibrar acima do pico Brasil sem transformar o gerador em uma
# ferramenta de carga irrestrita. O default de palco e 8 mil TPS; isso nao
# promete permanencia em M20: a
# execucao mostra o tier real e pode escalar a M30 se a telemetria exigir.
# Capacidade e sizing continuam dependendo de teste representativo, working set,
# indices, duracao, latencia de rede e SLOs do cliente.
TPS_MAX = 15_000
CONCEPT_TPS = min(TPS_MAX, max(10, int(os.getenv("STREAMING_CONCEPT_TPS", "200"))))
# Modo de escrita da demo. "individual" é o padrão porque é o que corresponde a
# um PIX real: uma transação, um insert. Ele só se tornou viável com o cluster
# na mesma região do gerador.
MODO_ESCRITA_PADRAO = os.getenv("STREAMING_MODO_ESCRITA", "individual").strip() or "individual"
if MODO_ESCRITA_PADRAO not in ("individual", "lote"):
    MODO_ESCRITA_PADRAO = "individual"
DEMO_TPS_DEFAULT = min(TPS_MAX, max(1, int(os.getenv("STREAMING_DEMO_TPS", "8000"))))
# Alvo do modo individual: 2.000 TPS.
#
# Curva medida em sa-east-1 com os três consumidores ativos (alvo → medido,
# Kafka p50, que é sempre o primeiro a degradar):
#
#   1.000 → 1.018,  28 ms      2.500 → 2.277,  100 ms
#   2.000 → 2.095,  35 ms      3.000 → 2.544,  263 ms
#                              4.000 → 3.015, 1.003 ms
#
# 2.000 é o último patamar que ENTREGA o alvo e mantém todo o caminho
# pós-commit abaixo de 35 ms. Acima disso o consumidor Kafka local vira o
# gargalo — não o Atlas, que segue ingerindo cada PIX em 3 ms.
DEMO_TPS_INDIVIDUAL = min(TPS_MAX, max(1, int(os.getenv("STREAMING_DEMO_TPS_INDIVIDUAL", "2000"))))
DEMO_DURATION_DEFAULT_S = min(120, max(10, int(os.getenv("STREAMING_DEMO_DURATION_S", "30"))))

# Referencia de escala do PIX no Brasil, a partir de numeros PUBLICOS do BCB.
# Nao e sizing e nao pressupoe nenhuma instituicao especifica: cada plateia
# calcula a propria participacao a partir daqui.
# - recorde de 313.339.828 PIX em 05/12/2025 -> 3.627 TPS medios no dia;
# - planejamento do BCB para 10 mil TPS de pico sustentado.
#
# A fatia usada nos presets e configuravel (`PIX_PARTICIPACAO_PCT`) justamente
# para a PoV servir a qualquer instituicao: 10% e so o valor de partida.
PIX_RECORDE_DIA = 313_339_828
PIX_RECORDE_DATA = "2025-12-05"
PIX_BRASIL_MEDIA_TPS = round(PIX_RECORDE_DIA / 86_400)
PIX_BRASIL_PICO_TPS = 10_000
PIX_PARTICIPACAO = min(1.0, max(0.01, float(os.getenv("PIX_PARTICIPACAO_PCT", "10")) / 100))
PIX_FATIA_MEDIA_TPS = round(PIX_BRASIL_MEDIA_TPS * PIX_PARTICIPACAO)
PIX_FATIA_PICO_TPS = round(PIX_BRASIL_PICO_TPS * PIX_PARTICIPACAO)
PIX_FONTE_RECORDE = "https://www.bcb.gov.br/estabilidadefinanceira/estatisticaspix"
PIX_FONTE_PICO = "https://www.bcb.gov.br/Adm/Edital/pregaoe/DEMAP901082024/arq01_DEMAP901082024.pdf"

# TTL = rede de segurança, não a limpeza principal (essa é o botão Reset, que
# dropa a coleção na hora).
#
# Em regime o deletor do TTL remove na mesma taxa em que se insere, qualquer que
# seja a janela; o que a janela decide é o TAMANHO do conjunto vivo. A 1800 s a
# coleção estabilizava perto de um milhão de documentos — dados e índices
# maiores que o cache do WiredTiger de um M20, o que sozinho sustentava a
# pressão de memória que dispara o auto-scaling.
#
# A 300 s o TTL continua maior que a rodada e sua drenagem. Ele é apenas uma
# rede de segurança: em 8 mil TPS o Reset imediato é obrigatório entre ensaios
# para o conjunto vivo não crescer até milhões de documentos.
TTL_SECONDS = int(os.getenv("STREAMING_TTL_SEGUNDOS", "300"))
# Partições do consumo do change stream (um cursor + uma thread por partição).
CS_PARTICOES = max(1, int(os.getenv("STREAMING_CS_PARTICOES", "4")))
# Amostragem do feed SSE (contadores e percentis seguem cobrindo 100%).
FEED_INTERVALO_S = 0.12
METRICAS_INTERVALO_S = 0.5
# Janela de 5 s + 2 s de allowed lateness. Depois que o gerador para, um evento
# tecnico posterior avanca o watermark e fecha a ultima janela da execucao.
ASP_WATERMARK_FLUSH_S = 7.2
ASP_WATERMARK_RUN_ID = "__demo_watermark__"

sdb = client[STREAM_DB]

# Cliente assíncrono, usado SÓ pela escrita do modo individual. Tudo o mais
# (change streams, reconciliação, preflight) segue no cliente síncrono: a
# mudança é cirúrgica de propósito, para não reescrever o módulo inteiro atrás
# de uma otimização do gerador.
#
# Criado sob demanda porque um AsyncMongoClient precisa nascer dentro de um
# event loop em execução; instanciá-lo no import prenderia o cliente ao loop
# errado e o primeiro insert falharia.
_acliente: "AsyncMongoClient | None" = None


def acol_tx():
    """Coleção de transações no driver assíncrono."""
    global _acliente
    if _acliente is None:
        _acliente = AsyncMongoClient(
            settings.mongo_uri,
            appname="atlas-showcase-async",
            maxPoolSize=Generator.WORKERS_MAX + 20,
            serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        )
    return _acliente[STREAM_DB][COL_TX]


async def fechar_cliente_async() -> None:
    """Encerra o cliente assíncrono no shutdown da aplicação."""
    global _acliente
    if _acliente is not None:
        await _acliente.close()
        _acliente = None

# Distribuição sintética de UFs para produzir grupos diferentes nas janelas.
_UFS = ["SP", "RJ", "MG", "RS", "PR", "BA", "PE", "SC", "GO", "CE"]
_UF_PESOS = [30, 14, 11, 8, 8, 7, 6, 6, 5, 5]

# ---------------------------------------------------------------------------
# Canal de cartão presencial — a metade georreferenciada do mesmo stream.
#
# PIX não carrega coordenada, e essa afirmação continua valendo. O que muda é o
# escopo do stream: ele deixa de ser "PIX" e passa a ser "eventos de pagamento
# da plataforma", com dois canais. PIX segue sem ponto; compra presencial com
# cartão carrega a coordenada CADASTRAL do terminal do adquirente — a mesma
# modelagem do módulo 08, lendo a mesma tabela de municípios.
#
# É isso que permite ao ASP calcular risco geográfico EM EVENT TIME, na
# passagem, em vez de o módulo 08 varrer histórico sob demanda.
# ---------------------------------------------------------------------------
CARTAO_PCT = min(90, max(0, int(os.getenv("STREAMING_CARTAO_PCT", "18"))))
GEO_DB_NOME = os.getenv("GEO_DB", "geo").strip() or "geo"
COL_SINAIS = "sinais_ao_vivo"
# Velocidade acima da qual o par vira sinal. 900 km/h ~ jato comercial: é o
# limiar didático do módulo 08, mantido igual para as duas telas concordarem.
SINAL_LIMITE_KMH = float(os.getenv("STREAMING_SINAL_KMH", "900"))

_MUNICIPIOS: list[dict[str, Any]] = json.loads(
    (Path(__file__).resolve().parent.parent / "data" / "municipios.json").read_text(encoding="utf-8")
)["municipios"]
_MUN_PESOS = [m["peso"] for m in _MUNICIPIOS]
_CATEGORIAS_CARTAO = ["alimentação", "combustível", "farmácia", "vestuário", "serviços"]
_TIPOS_CARTAO = ["CARTAO_DEBITO"] * 6 + ["CARTAO_CREDITO"] * 4
# Duas populações de portadores, e a separação é deliberada.
#
# O tráfego comum precisa ser dilúido: com poucos cartões, cada um compraria
# dezenas de vezes por janela e qualquer viagem legítima viraria alerta — a
# 2.000 TPS isso dava 0,4% das compras sinalizadas, taxa que não existe em
# operação real.
#
# Os pares plantados usam uma faixa PRÓPRIA de cartões, que o tráfego comum
# nunca toca. Sem isso, as duas compras do par eram diluídas entre as outras
# compras do mesmo cartão na janela, os extremos deixavam de ser os dois pontos
# plantados, e o sinal garantido da demo se classificava como emergente —
# destruindo justamente a distinção que a tela usa como evidência.
CARTAO_CLIENTES = 12_000
CARTAO_CLIENTES_PLANTADOS = 400


def _catalogo_terminais() -> dict[tuple[int, str], list[dict[str, Any]]]:
    """
    Terminais estáveis: mesma ideia (e mesmo formato de id) do seed do módulo 08.

    Gerado uma vez, com semente fixa, para que o terminal `POS0301` esteja
    sempre no mesmo ponto — um terminal que muda de lugar a cada compra
    inviabilizaria qualquer conversa sobre proveniência.
    """
    rng = random.Random(20260807)
    catalogo: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for cidade, mun in enumerate(_MUNICIPIOS):
        for indice, categoria in enumerate(_CATEGORIAS_CARTAO):
            catalogo[(cidade, categoria)] = [
                {
                    "id": f"POS{cidade:02d}{indice:02d}{numero:02d}",
                    "coordinates": [
                        round(mun["lng"] + rng.gauss(0, 4.0) / (111.32 * max(0.2, math.cos(math.radians(mun["lat"])))), 6),
                        round(mun["lat"] + rng.gauss(0, 4.0) / 111.32, 6),
                    ],
                }
                for numero in range(8)
            ]
    return catalogo


_TERMINAIS = _catalogo_terminais()
# Cidade "de casa" de cada portador: sem isso todo cartão viajaria o tempo todo
# e o sinal perderia qualquer significado.
_CASA_CLIENTE = [
    random.Random(20260807 + i).choices(range(len(_MUNICIPIOS)), weights=_MUN_PESOS, k=1)[0]
    for i in range(CARTAO_CLIENTES + CARTAO_CLIENTES_PLANTADOS)
]


def _municipio_distante(origem: int, minimo_km: float = 700.0) -> int:
    """Índice de um município a pelo menos `minimo_km` — mesma regra do módulo 08.

    Sorteia entre TODOS os candidatos válidos: devolver sempre o primeiro fazia
    quase todo par plantado terminar na mesma cidade, e a coluna de trajeto da
    tela virava uma lista repetida.
    """
    base = _MUNICIPIOS[origem]
    candidatos = [
        i for i, m in enumerate(_MUNICIPIOS)
        if i != origem and _haversine_km(base["lat"], base["lng"], m["lat"], m["lng"]) >= minimo_km
    ]
    return random.choice(candidatos) if candidatos else (origem + 1) % len(_MUNICIPIOS)


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2)
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(a)))

# ---------------------------------------------------------------------------
# Perfil de valores — PREMISSA declarada, exposta em /streaming/cenario.
#
# Um sorteio uniforme entre R$ 5 e R$ 9.500 dá ticket médio de ~R$ 4.750, o que
# não se parece com PIX nenhum: o fluxo real é MUITO assimétrico — muita
# transferência pequena do dia a dia e uma cauda longa de valores altos, que
# concentra a maior parte do dinheiro em poucas transações.
#
# Cada tipo tem seu peso na CONTAGEM de transações e suas faixas de valor com
# pesos próprios. Assim a mediana fica baixa, a média fica bem acima da
# mediana, e o volume financeiro se concentra na cauda — as três propriedades
# que uma squad de pagamentos espera ver.
# ---------------------------------------------------------------------------
# Duas calibrações, trocáveis por STREAMING_PERFIL_VALORES:
#
#   varejo (default) — mediana ~R$ 90, média ~R$ 570. Muita transferência
#     pequena do dia a dia; é o formato clássico do PIX P2P.
#   corpo_medio      — mediana ~R$ 490, com ~73% das transações entre R$ 100 e
#     R$ 2.000. Para quando o mix do cliente é mais de pagamento a lojista.
#
# As duas mantêm a cauda longa DE PROPÓSITO: sem ela, média e mediana ficam
# quase iguais (num sorteio uniforme de R$ 100 a 2.000, a razão cai para 1,4×
# e o 1% maior carrega só 3% do volume), e some justamente a assimetria que
# faz uma squad de pagamentos reconhecer o próprio fluxo.
PERFIS_VALORES: dict[str, dict[str, Any]] = {
    "varejo": {
        "tipos": [("PIX", 100)],
        "faixas": {
            "PIX": [
                (46, 5.0, 60.0), (33, 60.0, 250.0), (17, 250.0, 1_200.0),
                (3.7, 1_200.0, 6_000.0), (0.3, 6_000.0, 30_000.0),
            ],
        },
    },
    "corpo_medio": {
        "tipos": [("PIX", 100)],
        "faixas": {
            "PIX": [
                (15, 20.0, 100.0), (45, 100.0, 700.0), (30, 700.0, 2_000.0),
                (9, 2_000.0, 8_000.0), (1, 8_000.0, 25_000.0),
            ],
        },
    },
}
PERFIL_ATIVO = os.getenv("STREAMING_PERFIL_VALORES", "varejo").strip() or "varejo"
if PERFIL_ATIVO not in PERFIS_VALORES:
    PERFIL_ATIVO = "varejo"
PERFIL_TIPOS: list[tuple[str, int]] = PERFIS_VALORES[PERFIL_ATIVO]["tipos"]
PERFIL_VALORES: dict[str, list[tuple[float, float, float]]] = PERFIS_VALORES[PERFIL_ATIVO]["faixas"]
_TIPOS = [t for t, _ in PERFIL_TIPOS]
_TIPO_PESOS = [p for _, p in PERFIL_TIPOS]


def _sorteia_valor(tipo: str) -> float:
    faixas = PERFIL_VALORES[tipo]
    peso, minimo, maximo = random.choices(faixas, weights=[f[0] for f in faixas])[0]
    # Uniforme em escala LOG dentro da faixa: sem isso, cada faixa teria média
    # no seu ponto médio e a distribuição ficaria com degraus artificiais.
    return round(math.exp(random.uniform(math.log(minimo), math.log(maximo))), 2)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, default=str)}\n\n"


# ---------------------------------------------------------------------------
# Hub de broadcast — uma fila por assinante SSE
# ---------------------------------------------------------------------------
class Hub:
    def __init__(self, maxsize: int = 500):
        self._subs: set[asyncio.Queue] = set()
        self._maxsize = maxsize

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    @property
    def subscribers(self) -> int:
        return len(self._subs)

    def publish(self, payload: dict[str, Any]) -> None:
        for q in list(self._subs):
            if q.full():                     # assinante lento não pode travar o produtor
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(payload)

    def publish_threadsafe(self, loop: asyncio.AbstractEventLoop, payload: dict[str, Any]) -> None:
        loop.call_soon_threadsafe(self.publish, payload)


hub_cs = Hub()
hub_kafka = Hub()
hub_asp = Hub()


class Meter:
    """
    Vazão e percentis de latência a partir das medições reais de cada coluna.

    Percentis importam mais que a média para uma squad de plataforma: o p99 é o
    que aparece no SLA. Guardamos as últimas AMOSTRAS latências e os instantes
    dos últimos eventos (janela de 10 s) — memória constante, sem depender de
    biblioteca externa.
    """

    AMOSTRAS = 2_000
    JANELA_S = 10.0

    def __init__(self) -> None:
        self._lat: deque[float] = deque(maxlen=self.AMOSTRAS)
        self._marcas: deque[float] = deque(maxlen=100_000)
        self._lock = threading.Lock()

    def record(self, latency_ms: float | None) -> None:
        # record(), snapshot() e reset() podem vir de threads diferentes. O
        # mesmo lock evita iterar/ordenar um deque enquanto outro produtor o
        # altera; o trecho crítico contém apenas dois appends.
        with self._lock:
            self._marcas.append(time.monotonic())
            if latency_ms is not None:
                self._lat.append(latency_ms)

    def reset(self) -> None:
        with self._lock:
            self._lat.clear()
            self._marcas.clear()

    def snapshot(self) -> dict[str, Any]:
        agora = time.monotonic()
        with self._lock:
            while self._marcas and self._marcas[0] < agora - self.JANELA_S:
                self._marcas.popleft()
            eventos_s = round(len(self._marcas) / self.JANELA_S, 1)
            lat = sorted(self._lat)
        if not lat:
            return {"eventos_s": eventos_s, "p50": None, "p95": None, "p99": None, "amostras": 0}

        def pct(p: float) -> float:
            idx = min(len(lat) - 1, int(round((len(lat) - 1) * p)))
            return round(lat[idx], 1)

        return {
            "eventos_s": eventos_s,
            "p50": pct(0.50),
            "p95": pct(0.95),
            "p99": pct(0.99),
            "amostras": len(lat),
        }


meter_cs = Meter()
meter_leitura = Meter()
meter_kafka = Meter()
meter_asp = Meter()
# Tempo do insert_many até o ACK do Atlas. Uma amostra representa um
# micro-batch confirmado, não um evento de CDC; por isso a UI o apresenta fora
# das três colunas de propagação pós-commit.
meter_write_ack = Meter()


# ---------------------------------------------------------------------------
# Ingestão medida DENTRO do servidor
#
# `meter_write_ack` mede o round-trip do cliente e, com o cluster em us-east-1 e
# o gerador em São Paulo, carrega ~148 ms de rede que não são do MongoDB. Para
# responder "quanto tempo o Atlas leva para ingerir", a única fonte honesta é o
# próprio servidor: `serverStatus().opLatencies.writes` contabiliza a execução do
# comando no mongod, sem rede e sem a fila do cliente.
#
# O histograma é acumulado desde o boot, então o que vale é o DELTA entre o
# início e o fim da execução — caso contrário a demo mostraria a média de toda a
# vida do processo, não a da rodada.
#
# Uma amostra é um COMANDO, não um documento: um insert_many de 800 docs conta
# como uma escrita de latência alta. Por isso o payload separa as duas leituras
# e a UI nunca apresenta o número por comando como se fosse por transação.
# ---------------------------------------------------------------------------
def _oplatencies_writes() -> dict[str, Any] | None:
    """Lê opLatencies.writes com histograma. `None` quando indisponível."""
    try:
        status = client.admin.command("serverStatus", opLatencies={"histograms": True})
        w = (status.get("opLatencies") or {}).get("writes") or {}
    except PyMongoError:
        return None
    if "ops" not in w or "latency" not in w:
        return None
    hist = [(int(b.get("micros", 0)), int(b.get("count", 0))) for b in (w.get("histogram") or [])]
    return {"ops": int(w["ops"]), "latency_us": int(w["latency"]), "hist": hist}


def _percentis_do_histograma(buckets: list[tuple[int, int]]) -> dict[str, float | None]:
    """Percentis a partir dos buckets do mongod.

    O bucket dá a fronteira inferior da faixa, então o valor é uma cota
    inferior — declarada como aproximada na UI, nunca apresentada como medida
    exata.
    """
    total = sum(c for _, c in buckets)
    if total <= 0:
        return {"p50": None, "p95": None, "p99": None}
    saida: dict[str, float | None] = {}
    for rotulo, fracao in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
        alvo, acumulado, valor = total * fracao, 0, None
        for micros, count in buckets:
            acumulado += count
            if acumulado >= alvo:
                valor = round(micros / 1000, 2)
                break
        saida[rotulo] = valor
    return saida


class IngestaoServidor:
    """Delta de opLatencies entre o início e o instante consultado."""

    def __init__(self) -> None:
        self._base: dict[str, Any] | None = None
        self._lock = threading.Lock()

    def marcar_inicio(self) -> None:
        base = _oplatencies_writes()
        with self._lock:
            self._base = base

    def medir(self) -> dict[str, Any]:
        with self._lock:
            base = self._base
        atual = _oplatencies_writes()
        if atual is None:
            return {"disponivel": False, "motivo": "serverStatus indisponível para este usuário"}
        if base is None:
            return {"disponivel": False, "motivo": "sem baseline: inicie uma execução"}

        ops = atual["ops"] - base["ops"]
        if ops <= 0:
            return {"disponivel": False, "motivo": "nenhuma escrita no intervalo"}

        us = atual["latency_us"] - base["latency_us"]
        antes = dict(base["hist"])
        delta = [(m, c - antes.get(m, 0)) for m, c in atual["hist"] if c - antes.get(m, 0) > 0]
        return {
            "disponivel": True,
            "fonte": "serverStatus.opLatencies.writes",
            "escopo": "todas as escritas do cluster durante a janela da execução, sem rede",
            "atribuicao": "representativa do PIX somente com workload isolado",
            "comandos": ops,
            "media_ms_por_comando": round(us / ops / 1000, 2),
            **_percentis_do_histograma(delta),
        }


ingestao_servidor = IngestaoServidor()


def centavos_de(valor: Any) -> int | None:
    """Valor monetário em centavos inteiros, ou None se não for numérico.

    Centavo inteiro, e não float, porque a conferência de valor da reconciliação
    precisa ser uma igualdade exata: somar 60 mil doubles em três caminhos
    diferentes produz resíduo na última casa, e um resíduo na tela é
    indistinguível de divergência real.

    None é resposta legítima: o evento inválido injetado de propósito grava
    `valor` como texto. Ele conta no total de documentos, não na soma.
    """
    if isinstance(valor, Decimal128):
        valor = valor.to_decimal()
    if isinstance(valor, bool) or valor is None:
        return None
    if isinstance(valor, (int, Decimal, float)):
        try:
            return int((Decimal(str(valor)) * 100).to_integral_value(rounding=ROUND_HALF_UP))
        except (InvalidOperation, ValueError):
            return None
    return None


def _valor_json(valor: Any) -> Any:
    """Normaliza o `valor` que chega pelo Kafka para algo que `centavos_de` entenda.

    O connector serializa `Decimal128` em JSON estendido (`{"$numberDecimal": …}`)
    ou como string decimal, conforme o modo de saída configurado. Os dois viram
    Decimal aqui. O que não for número continua não sendo — é assim que o evento
    inválido injetado de propósito segue fora da soma, e não por acidente.
    """
    if isinstance(valor, dict):
        valor = valor.get("$numberDecimal", valor.get("$numberDouble", valor.get("$numberLong")))
    if isinstance(valor, str):
        try:
            return Decimal(valor)
        except InvalidOperation:
            return None
    return valor


def _digest_de(end_to_end_id: str) -> int:
    """8 bytes de blake2b do identificador, para XOR acumulado."""
    return int.from_bytes(hashlib.blake2b(end_to_end_id.encode(), digest_size=8).digest(), "big")


class RunTracker:
    """Contagem idempotente, por execução, dos caminhos observados pela PoV.

    Além da contagem, acumula duas evidências que contagem sozinha não dá:

    * soma em centavos — contagem igual com valor diferente é transformação
      errada no caminho, não perda; um banco confere o valor antes do volume.
    * digest XOR dos identificadores — é comutativo e associativo, então
      independe da ordem de chegada, e só bate quando os dois caminhos viram
      exatamente o mesmo *conjunto*. Contagem igual com um documento trocado por
      outro passa despercebida; o digest não.
    """

    MAX_IDS_PER_CHANNEL = 500_000

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: dict[str, dict[str, set[str]]] = {}
        self._duplicates: dict[str, dict[str, int]] = {}
        self._truncated: set[tuple[str, str]] = set()
        self._cents: dict[str, dict[str, int]] = {}
        self._nao_numericos: dict[str, dict[str, int]] = {}
        self._digests: dict[str, dict[str, int]] = {}

    def record(
        self,
        channel: str,
        run_id: str | None,
        end_to_end_id: str | None,
        valor: Any = None,
    ) -> None:
        if not run_id or not end_to_end_id:
            return
        with self._lock:
            by_channel = self._ids.setdefault(run_id, {})
            ids = by_channel.setdefault(channel, set())
            duplicates = self._duplicates.setdefault(run_id, {})
            if end_to_end_id in ids:
                duplicates[channel] = duplicates.get(channel, 0) + 1
                return
            if len(ids) >= self.MAX_IDS_PER_CHANNEL:
                self._truncated.add((run_id, channel))
                return
            ids.add(end_to_end_id)
            cents = centavos_de(valor)
            if cents is None:
                nao_num = self._nao_numericos.setdefault(run_id, {})
                nao_num[channel] = nao_num.get(channel, 0) + 1
            else:
                soma = self._cents.setdefault(run_id, {})
                soma[channel] = soma.get(channel, 0) + cents
            digests = self._digests.setdefault(run_id, {})
            digests[channel] = digests.get(channel, 0) ^ _digest_de(end_to_end_id)

    def snapshot(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            channels = self._ids.get(run_id, {})
            duplicates = self._duplicates.get(run_id, {})
            cents = self._cents.get(run_id, {})
            nao_num = self._nao_numericos.get(run_id, {})
            digests = self._digests.get(run_id, {})
            return {
                channel: {
                    "unicos": len(ids),
                    "duplicados": duplicates.get(channel, 0),
                    "completo_em_memoria": (run_id, channel) not in self._truncated,
                    "centavos": cents.get(channel, 0),
                    "nao_numericos": nao_num.get(channel, 0),
                    "digest": digests.get(channel, 0),
                }
                for channel, ids in channels.items()
            }

    def reset(self) -> None:
        with self._lock:
            self._ids.clear()
            self._duplicates.clear()
            self._truncated.clear()
            self._cents.clear()
            self._nao_numericos.clear()
            self._digests.clear()


run_tracker = RunTracker()


async def _sse_stream(request: Request, hub: Hub, hello: dict[str, Any]):
    """Gerador SSE comum às três colunas: evento inicial + broadcast + keepalive."""
    q = hub.subscribe()
    try:
        yield _sse(hello)
        while True:
            if await request.is_disconnected():
                break
            try:
                payload = await asyncio.wait_for(q.get(), timeout=15)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield _sse(payload)
    finally:
        hub.unsubscribe(q)


def _sse_response(request: Request, hub: Hub, hello: dict[str, Any]) -> StreamingResponse:
    return StreamingResponse(
        _sse_stream(request, hub, hello),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


# ---------------------------------------------------------------------------
# Gerador de escritas — alimenta as três colunas
# ---------------------------------------------------------------------------
TTL_INDEX_NAME = "ts_ttl"


def _ensure_indexes() -> None:
    """
    Garante o índice único de endToEndId, o índice de reconciliação em `run_id`
    e o TTL em `ts` com a janela atual.

    `run_id` existe por causa de /streaming/reconciliacao: a tela conta a fonte
    a cada poucos segundos, e sem índice esse count_documents é um COLLSCAN da
    coleção inteira. Repetido em laço durante a demo, ele puxava todos os
    documentos vivos pelo cache do WiredTiger — era o maior consumidor de CPU e
    de memória do cluster, e o que fazia o auto-scaling sair do M20.

    O TTL é ajustado por collMod no índice que JÁ existe (procurado pela chave,
    não pelo nome): create_index recusa um segundo índice sobre {ts: 1} com
    expireAfterSeconds diferente, e versões anteriores desta PoV criaram esse
    índice com outro nome.
    """
    sdb[COL_TX].create_index("endToEndId", unique=True, name="endToEndId_unique")
    sdb[COL_TX].create_index("run_id", name="run_id_reconciliacao")

    existente = None
    for indice in sdb[COL_TX].list_indexes():
        if dict(indice.get("key", {})) == {"ts": 1}:
            existente = indice
            break

    if existente is None:
        sdb[COL_TX].create_index("ts", expireAfterSeconds=TTL_SECONDS, name=TTL_INDEX_NAME)
        return

    if existente["name"] != TTL_INDEX_NAME:
        # Nome legado (ts_ttl_2h de versões anteriores): recria com o nome atual
        # para não deixar um índice chamado "2h" valendo outra coisa no Compass.
        sdb[COL_TX].drop_index(existente["name"])
        sdb[COL_TX].create_index("ts", expireAfterSeconds=TTL_SECONDS, name=TTL_INDEX_NAME)
    elif existente.get("expireAfterSeconds") != TTL_SECONDS:
        sdb.command({
            "collMod": COL_TX,
            "index": {"name": TTL_INDEX_NAME, "expireAfterSeconds": TTL_SECONDS},
        })


def _ponto_cartao(cliente: int, cidade: int, quando: datetime,
                  run_id: str, sequencia: int | None,
                  origem_sinal: str | None = None) -> dict[str, Any]:
    """Uma compra presencial: coordenada do terminal, com proveniência junto."""
    mun = _MUNICIPIOS[cidade]
    categoria = random.choice(_CATEGORIAS_CARTAO)
    terminal = random.choice(_TERMINAIS[(cidade, categoria)])
    tipo = random.choice(_TIPOS_CARTAO)
    doc = {
        "endToEndId": f"C{uuid.uuid4().hex[:31].upper()}",
        "run_id": run_id,
        "sequencia": sequencia,
        "canal": "CARTAO_PRESENCIAL",
        "clienteId": f"CLI{cliente:05d}",
        "particao": cliente % CS_PARTICOES,
        "valor": Decimal128(f"{_sorteia_valor('PIX'):.2f}"),
        "tipo": tipo,
        "uf": mun["uf"],
        "municipio": mun["municipio"],
        "estabelecimento": {"categoria": categoria},
        "local": {"type": "Point", "coordinates": terminal["coordinates"]},
        "dispositivo": {"id": terminal["id"], "canal": "POS_PRESENCIAL"},
        "localizacaoMeta": {"origem": "TERMINAL_ADQUIRENTE", "qualidade": "CADASTRAL"},
        # Dois instantes distintos, e a distinção não é cosmética:
        #
        #   ts          — quando o evento entrou no stream. É o campo do TTL e o
        #                 que ordena o fluxo.
        #   compradaEm  — quando a compra aconteceu no terminal, que pode ser
        #                 minutos antes: a captura do adquirente atrasa.
        #
        # A velocidade do sinal de risco se calcula sobre `compradaEm`. Colocar
        # esse instante retroativo em `ts` fazia o TTL apagar a compra mais
        # antiga do par antes da reconciliação — a fonte contava menos que os
        # três consumidores, e a diferença parecia perda quando era expiração.
        "ts": _now(),
        "compradaEm": quando,
        "status": "liquidada",
    }
    if origem_sinal:
        # Rastro honesto: a tela separa o par que o gerador armou do par que
        # emergiu do tráfego. Sem isso, "detectamos 5" é indistinguível de
        # "plantamos 5".
        doc["origemSinal"] = origem_sinal
    return doc


def _par_impossivel(run_id: str, sequencia: int | None) -> list[dict[str, Any]]:
    """
    Duas compras do mesmo cartão, distantes demais para o tempo entre elas.

    As duas chegam juntas ao stream — é a captura que chega junta, não a compra.
    O intervalo real entre as compras (`ts`) fica em minutos, então a velocidade
    resultante é da ordem de milhares de km/h, e não de um número absurdo que
    só existiria por causa do tamanho da janela.
    """
    # Faixa exclusiva dos pares: nenhum destes cartões aparece no tráfego comum.
    cliente = CARTAO_CLIENTES + random.randrange(CARTAO_CLIENTES_PLANTADOS)
    origem = _CASA_CLIENTE[cliente]
    destino = _municipio_distante(origem)
    agora = _now()
    # O intervalo sai da velocidade desejada, não o contrário. Sorteando minutos
    # direto, a velocidade implícita ficava na casa das dezenas de milhares de
    # km/h — ordens de grandeza além de qualquer padrão real de cartão clonado.
    # Entre 1.100 e 9.000 km/h os casos nascem na faixa em que fraude acontece,
    # incluindo os limítrofes logo acima do limiar configurado.
    origem_mun, destino_mun = _MUNICIPIOS[origem], _MUNICIPIOS[destino]
    km_par = _haversine_km(
        origem_mun["lat"], origem_mun["lng"], destino_mun["lat"], destino_mun["lng"],
    )
    minutos = (km_par / random.uniform(1_100.0, 9_000.0)) * 60
    return [
        _ponto_cartao(cliente, origem, agora - timedelta(minutes=minutos), run_id, sequencia, "plantado"),
        _ponto_cartao(cliente, destino, agora, run_id, sequencia, "plantado"),
    ]


def _new_cartao(run_id: str = "execucao-local", sequencia: int | None = None) -> dict[str, Any]:
    """Compra presencial de rotina: o portador compra perto de casa."""
    cliente = random.randrange(CARTAO_CLIENTES)
    cidade = _CASA_CLIENTE[cliente]
    if random.random() < 0.0006:
        # Viagem legítima, e rara. Em 6% o portador teleportava a cada poucos
        # segundos e a tela virava uma parede de alertas — mais falso positivo
        # do que qualquer detector produziria com dado real. Nesta ordem de
        # grandeza, um par distante do mesmo cartão na mesma janela é
        # coincidência do tráfego: quando aparece, o sinal foi EMERGENTE,
        # achado pelo pipeline e não montado pelo gerador.
        cidade = random.choices(range(len(_MUNICIPIOS)), weights=_MUN_PESOS, k=1)[0]
    # Atraso de captura do adquirente: a compra aconteceu até 10 min antes de o
    # evento chegar. É o que dá escala de minutos ao intervalo entre duas
    # compras do mesmo cartão — sem isso toda velocidade sai na casa das
    # dezenas de milhares de km/h só porque os eventos chegaram juntos.
    atraso = timedelta(seconds=random.uniform(0, 600))
    return _ponto_cartao(cliente, cidade, _now() - atraso, run_id, sequencia)


def _new_transacao(run_id: str = "execucao-local", sequencia: int | None = None) -> dict[str, Any]:
    if CARTAO_PCT and random.randrange(100) < CARTAO_PCT:
        return _new_cartao(run_id, sequencia)
    pagador = random.randint(1, 5000)
    tipo = random.choices(_TIPOS, weights=_TIPO_PESOS)[0]
    return {
        "canal": "PIX",
        "endToEndId": f"E{uuid.uuid4().hex[:31].upper()}",
        "run_id": run_id,
        "sequencia": sequencia,
        "pagadorId": f"P{pagador:06d}",
        # Partição de consumo derivada do pagador — é assim que um banco
        # particionaria o fluxo (por conta), e é o que permite um consumidor
        # por partição em vez de um cursor único para tudo.
        "particao": pagador % CS_PARTICOES,
        "recebedorId": f"R{random.randint(1, 800):06d}",
        "valor": Decimal128(f"{_sorteia_valor(tipo):.2f}"),
        "tipo": tipo,
        "uf": random.choices(_UFS, weights=_UF_PESOS)[0],
        "ts": _now(),
        "status": "liquidada",
    }


class Generator:
    """
    Task asyncio que insere em micro-batches a cada 100 ms; o TPS exibido é o
    MEDIDO (janela deslizante de 5 s), nunca o pedido.

    O insert_many roda numa thread e NÃO é aguardado dentro do tick: um
    round-trip ao Atlas custa mais que os 100 ms do tick, então esperar por ele
    derrubaria o TPS efetivo. Os batches em voo são limitados por MAX_INFLIGHT —
    ao atingir o teto, os documentos voltam para o carry (backpressure) em vez
    de acumular tasks indefinidamente.
    """

    TICK_S = 0.1
    MAX_INFLIGHT = 16

    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.tps_alvo = 0
        self.inserted = 0
        self.started_at: datetime | None = None
        self.run_id: str | None = None
        self.duration_s: int | None = None
        self.ends_at: datetime | None = None
        self._auto_stop_task: asyncio.Task | None = None
        self.stopping = False
        self._stop_depth = 0
        # Escritas que falharam mesmo depois da retentativa do driver. É a
        # evidência quantitativa do failover: o número costuma ser 0.
        self.write_errors = 0
        self.modo = MODO_ESCRITA_PADRAO
        self.workers_ativos = 0
        self._sequence = 0
        self._recent: list[tuple[float, int]] = []   # (monotonic, docs) p/ TPS medido
        self.tps_pico = 0.0                          # maior TPS observado na execução
        self._start_mono: float | None = None
        self._lock = threading.Lock()                # _recent/_inserted são tocados pelas threads de insert
        self._inflight: set[asyncio.Task] = set()

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def _record(self, docs: int) -> None:
        with self._lock:
            agora = time.monotonic()
            self.inserted += docs
            self._recent.append((agora, docs))
            # O pico é apurado AQUI, na escrita, e não em measured_tps(): aquele
            # só roda quando alguém consulta o status, então uma execução sem
            # ninguém na tela terminava com pico 0 e o painel de custo comparava
            # a CPU do cluster com TPS nenhum. A evidência não pode depender de
            # haver um espectador.
            inicio = self._start_mono
            if inicio is not None:
                recentes = [(t, n) for t, n in self._recent if t >= agora - self.JANELA_TPS_S]
                janela = min(self.JANELA_TPS_S, max(agora - inicio, 0.001))
                tps = round(sum(n for _, n in recentes) / janela, 1)
                if tps > self.tps_pico:
                    self.tps_pico = tps

    JANELA_TPS_S = 5.0

    def measured_tps(self) -> float:
        # Janela FIXA de 5 s: dividir pelo intervalo entre a primeira e a última
        # amostra dava números absurdos (1,8 M TPS) quando sobravam duas marcas
        # quase simultâneas no deque.
        agora = time.monotonic()
        cutoff = agora - self.JANELA_TPS_S
        with self._lock:
            self._recent = [(t, n) for t, n in self._recent if t >= cutoff]
            docs = sum(n for _, n in self._recent)
            inicio = self._start_mono
        if not docs or inicio is None:
            return 0.0
        janela = min(self.JANELA_TPS_S, max(agora - inicio, 0.001))
        return round(docs / janela, 1)

    async def start(self, tps: int, duration_s: int | None = None,
                    modo: str | None = None) -> None:
        await asyncio.to_thread(_ensure_indexes)
        if self._auto_stop_task and not self._auto_stop_task.done():
            return
        if modo in ("individual", "lote"):
            self.modo = modo
        self.tps_alvo = tps
        if self.running:
            return                                    # já rodando: só ajusta o TPS
        self.started_at = _now()
        self.run_id = f"pix-{self.started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        self._sequence = 0
        self._recent = []
        self.tps_pico = 0.0
        self._start_mono = time.monotonic()
        meter_write_ack.reset()
        # Definido aqui, e não em `_run_individual`: a resposta do /start é
        # enviada antes da task entrar em execução, e reportava workers=0.
        self.workers_ativos = self._workers_para(tps) if self.modo == "individual" else 0
        await asyncio.to_thread(ingestao_servidor.marcar_inicio)
        self.duration_s = duration_s
        self.ends_at = self.started_at + timedelta(seconds=duration_s) if duration_s else None
        self.task = asyncio.create_task(self._run())
        if duration_s:
            self._auto_stop_task = asyncio.create_task(self._stop_after(duration_s))

    def estender(self, segundos: int) -> datetime | None:
        """Empurra o fim da execução, mantendo o auto-stop coerente.

        Uma eleição de primary leva mais que a janela de 30 s da demo: sem
        esticar a carga, o `stop` automático fecharia a execução no meio do
        evento e a retomada seria demonstrada com o gerador já parado — o que
        não prova nada.
        """
        if not self.running or not self.ends_at:
            return self.ends_at
        self.ends_at = self.ends_at + timedelta(seconds=segundos)
        self.duration_s = (self.duration_s or 0) + segundos
        restante = (self.ends_at - _now()).total_seconds()
        if self._auto_stop_task and not self._auto_stop_task.done():
            self._auto_stop_task.cancel()
        self._auto_stop_task = asyncio.create_task(self._stop_after(max(1, int(restante))))
        return self.ends_at

    async def _stop_after(self, duration_s: int) -> None:
        current = asyncio.current_task()
        try:
            await asyncio.sleep(duration_s)
            await self.stop(advance_watermark=True, from_timer=True)
        except asyncio.CancelledError:
            pass
        finally:
            if self._auto_stop_task is current:
                self._auto_stop_task = None

    async def stop(self, advance_watermark: bool = False, from_timer: bool = False) -> None:
        # `stopping` é sinalizado aqui, e não só no timer: o Parar manual passa
        # pela mesma espera de watermark (ASP_WATERMARK_FLUSH_S) e a UI precisa
        # mostrar "fechando janelas" nos dois caminhos, não apenas no automático.
        #
        # O contador existe porque o Parar manual cancela o timer e o aguarda:
        # um `stopping = False` simples no fim do stop do timer apagaria o sinal
        # no meio do stop manual, que é justamente o mais longo.
        self._stop_depth += 1
        self.stopping = True
        try:
            await self._stop(advance_watermark=advance_watermark, from_timer=from_timer)
        finally:
            self._stop_depth -= 1
            if self._stop_depth <= 0:
                self._stop_depth = 0
                self.stopping = False

    async def _stop(self, advance_watermark: bool = False, from_timer: bool = False) -> None:
        if not from_timer:
            timer, self._auto_stop_task = self._auto_stop_task, None
            if timer and not timer.done():
                timer.cancel()
                try:
                    await timer
                except asyncio.CancelledError:
                    pass
        task, self.task = self.task, None
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # asyncio.to_thread não interrompe um insert_many já iniciado. Esperar
        # os batches evita que /reset apague a coleção e um insert atrasado
        # volte a preenchê-la logo depois.
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)
        self.tps_alvo = 0
        if advance_watermark and ASP_ENABLED and self.run_id and self.inserted:
            # Em event time, fonte ociosa nao fecha a ultima janela. O marcador
            # usa outro run_id, portanto avanca o watermark sem entrar na
            # reconciliacao da execucao que acabou de ser demonstrada.
            await asyncio.sleep(ASP_WATERMARK_FLUSH_S)
            marcador = _new_transacao(ASP_WATERMARK_RUN_ID, 0)
            marcador["controle_demo"] = "avancar_watermark"
            try:
                await asyncio.to_thread(sdb[COL_TX].insert_one, marcador)
            except PyMongoError:
                logger.exception("Falha ao inserir marcador de fechamento da janela ASP")

    def reset_counters(self) -> None:
        with self._lock:
            self.inserted = 0
            self._recent = []
            self._start_mono = time.monotonic()
        self.run_id = None
        self.started_at = None
        self.duration_s = None
        self.ends_at = None
        self.tps_pico = 0.0
        self._sequence = 0
        self.write_errors = 0
        meter_write_ack.reset()

    def _insert_batch(self, docs: list[dict[str, Any]]) -> None:
        """
        Roda na thread: carimba `ts`, insere e contabiliza no mesmo passo.

        O carimbo fica aqui, e não na geração do documento, porque `ts` é a
        origem da latência ponta a ponta exibida nas três colunas. Carimbar na
        geração incluiria o tempo de fila do próprio gerador na conta e a
        latência medida deixaria de ser a do caminho de entrega.

        Contabilizar aqui também garante que um cancelamento do gerador nunca
        perca a contagem de um batch já gravado.
        """
        agora = _now()
        for doc in docs:
            doc["ts"] = agora
        inicio_ack = time.monotonic()
        sdb[COL_TX].insert_many(docs, ordered=False)
        meter_write_ack.record((time.monotonic() - inicio_ack) * 1000)
        self._record(len(docs))

    async def _batch(self, docs: list[dict[str, Any]]) -> None:
        try:
            await asyncio.to_thread(self._insert_batch, docs)
        except PyMongoError:
            self.write_errors += 1
            logger.exception("Falha ao inserir micro-batch do gerador")

    # ── Modo PIX individual ────────────────────────────────────────────────
    #
    # A escrita usa o driver ASSÍNCRONO. Com `asyncio.to_thread(insert_one)`
    # cada PIX ocupava uma thread do pool, e como o mesmo processo mantém os
    # quatro cursores de change stream, o consumidor Kafka e os polls da tela,
    # o GIL travava a vazão em ~1.000 TPS. Medido lado a lado, 1 insert = 1 PIX
    # nos dois casos: to_thread ~1.000 TPS, async 1.829 TPS (p50 17,7 ms).
    #
    # O gargalo nunca foi o Atlas: durante 1.000 TPS o servidor ingeria cada
    # PIX em 3,07 ms usando 157 de 3.000 conexões.
    #
    # Um PIX de verdade chega sozinho, não em lote de 800. Neste modo cada
    # transação é um `insert_one`, o que torna a demonstração fiel e — mais
    # importante para a conversa com a squad — faz `opLatencies.writes` medir
    # UMA transação por comando, em vez de um micro-batch inteiro.
    #
    # Isto só é viável com o cluster na mesma região: a 148 ms de RTT cada
    # conexão sustentava ~6,7 escritas/s e o teto ficava em ~260 TPS. A 7,4 ms
    # o mesmo cliente passa de 2.300 TPS, acima do marco de 1.000 TPS do PIX.
    #
    # O paralelismo é deliberadamente modesto: acima de ~50 threads o gargalo
    # vira o CPython (GIL + encoding BSON) e a vazão CAI enquanto o p95 explode
    # (medido: 50 → 2.376 TPS/p95 34 ms; 600 → 2.041 TPS/p95 2.038 ms).
    # Com I/O assíncrono a "concorrência" é barata: são corrotinas, não threads.
    # O teto medido fica perto de 200 em voo; acima disso a vazão para de subir
    # e só o p95 piora, porque o custo passa a ser encoding BSON no event loop.
    WORKERS_MAX = 200
    # Custo de UMA escrita confirmada, não o RTT puro: o ACK medido em sa-east-1
    # com os três consumidores ativos fica perto de 15-18 ms (o ping sozinho dá
    # 7,4 ms). Dimensionar pelo RTT entregava 70% do alvo.
    CUSTO_ESCRITA_S = 0.020

    def _workers_para(self, tps: int) -> int:
        """Corrotinas suficientes para o alvo, sem passar do ponto de contenção."""
        necessarios = math.ceil(tps * self.CUSTO_ESCRITA_S) + 1
        return max(2, min(self.WORKERS_MAX, necessarios))

    async def _insert_um(self, doc: dict[str, Any]) -> None:
        doc["ts"] = _now()
        inicio = time.monotonic()
        await acol_tx().insert_one(doc)
        meter_write_ack.record((time.monotonic() - inicio) * 1000)
        self._record(1)

    async def _worker_individual(self, intervalo_s: float) -> None:
        """Uma corrotina: insere e espera o bastante para manter a taxa."""
        proximo = time.monotonic()
        while True:
            with self._lock:
                seq = self._sequence
                self._sequence += 1
            doc = _new_transacao(self.run_id or "execucao-local", seq)
            try:
                await self._insert_um(doc)
            except PyMongoError:
                # Durante uma eleição de primary o driver reabsorve a escrita
                # sozinho (retryWrites). O que chega aqui é o que nem a
                # retentativa salvou: conta como erro e some do fluxo, sem ser
                # persistido — a reconciliação compara contra o que está na
                # coleção, então continua fechando.
                self.write_errors += 1
                logger.exception("Falha ao inserir PIX individual")
            proximo += intervalo_s
            await asyncio.sleep(max(0.0, proximo - time.monotonic()))

    async def _run_individual(self) -> None:
        workers = self.workers_ativos or self._workers_para(self.tps_alvo)
        intervalo = workers / max(self.tps_alvo, 1)
        tarefas = [asyncio.create_task(self._worker_individual(intervalo))
                   for _ in range(workers)]
        try:
            await asyncio.gather(*tarefas)
        except asyncio.CancelledError:
            for t in tarefas:
                t.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)
            raise

    # Um par a cada ~6 s: perto o bastante para a demo de 30 s render vários
    # sinais, longe o bastante para não virar o fluxo dominante da tela.
    INTERVALO_PAR_S = 6.0

    async def _plantar_pares(self) -> None:
        """
        Injeta pares de impossible travel enquanto o gerador roda.

        Existe para a demo ter um sinal garantido no palco. Cada documento sai
        marcado com `origemSinal: "plantado"`, e a tela conta plantados e
        emergentes separadamente — a garantia não pode virar a evidência.
        """
        while True:
            await asyncio.sleep(self.INTERVALO_PAR_S)
            with self._lock:
                seq = self._sequence
                self._sequence += 2
            try:
                for doc in _par_impossivel(self.run_id or "execucao-local", seq):
                    await self._insert_um(doc)
            except PyMongoError:
                logger.exception("Falha ao inserir par de impossible travel")

    async def _run(self) -> None:
        pares = asyncio.create_task(self._plantar_pares()) if CARTAO_PCT else None
        try:
            await self._run_modo()
        finally:
            if pares:
                pares.cancel()

    async def _run_modo(self) -> None:
        if self.modo == "individual":
            await self._run_individual()
            return
        carry = 0.0
        next_tick = time.monotonic()
        while True:
            carry += self.tps_alvo * self.TICK_S
            batch_size = int(carry)
            carry -= batch_size
            if batch_size > 0:
                if len(self._inflight) >= self.MAX_INFLIGHT:
                    # Devolve ao próximo tick, mas com TETO: sem isso o carry
                    # cresce sem limite enquanto o Atlas não acompanha e depois
                    # o gerador dispara uma rajada bem acima do alvo pedido.
                    carry = min(carry + batch_size, self.tps_alvo * self.TICK_S * 2)
                else:
                    inicio = self._sequence
                    self._sequence += batch_size
                    docs = [
                        _new_transacao(self.run_id or "execucao-local", inicio + i)
                        for i in range(batch_size)
                    ]
                    task = asyncio.create_task(self._batch(docs))
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)
            next_tick += self.TICK_S
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))


generator = Generator()


class GeneratorStart(BaseModel):
    tps: int = Field(default=DEMO_TPS_DEFAULT, ge=1, le=TPS_MAX)
    duration_s: int = Field(default=DEMO_DURATION_DEFAULT_S, ge=10, le=120)
    # "individual" = 1 insert por PIX (fiel ao fluxo de um banco, e o que faz
    # opLatencies medir uma transação). "lote" = micro-batches, necessário só
    # para volumes que o modo individual não alcança do cliente Python.
    modo: str = Field(default=MODO_ESCRITA_PADRAO, pattern="^(individual|lote)$")


@router.post("/generator/start")
async def generator_start(body: GeneratorStart):
    await generator.start(body.tps, body.duration_s, body.modo)
    return {
        "running": True,
        "run_id": generator.run_id,
        "tps_alvo": generator.tps_alvo,
        "duration_s": generator.duration_s,
        "modo": generator.modo,
        "workers": generator.workers_ativos,
        "ends_at": generator.ends_at.isoformat() if generator.ends_at else None,
        "colecao": f"{STREAM_DB}.{COL_TX}",
    }


@router.post("/generator/stop")
async def generator_stop():
    await generator.stop(advance_watermark=True)
    return {"running": False, "inseridos": generator.inserted}


@router.get("/generator/status")
async def generator_status():
    total = await asyncio.to_thread(sdb[COL_TX].estimated_document_count)
    medido = generator.measured_tps()
    write_ack = meter_write_ack.snapshot()
    return {
        "running": generator.running,
        "stopping": generator.stopping,
        "modo": generator.modo,
        "workers": generator.workers_ativos,
        "tps_alvo": generator.tps_alvo,
        "tps_medido": medido,
        "inseridos": generator.inserted,
        "docs_na_colecao": total,
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "ttl_segundos": TTL_SECONDS,
        "ttl_ativo": True,
        "started_at": generator.started_at.isoformat() if generator.started_at else None,
        "duration_s": generator.duration_s,
        "ends_at": generator.ends_at.isoformat() if generator.ends_at else None,
        "run_id": generator.run_id,
        "write_ack": {
            "p50": write_ack["p50"],
            "p95": write_ack["p95"],
            "p99": write_ack["p99"],
            "amostras": write_ack["amostras"],
            "microbatches_s": write_ack["eventos_s"],
        },
        "ingestao_servidor": await asyncio.to_thread(ingestao_servidor.medir),
        "entrega": _diagnostico_entrega(medido, write_ack),
        "escritas_rejeitadas": generator.write_errors,
        # Vai junto do status para a tela não abrir mais um poll só por causa
        # de um evento que dura dois minutos.
        "failover": failover_tracker.snapshot(),
    }


# Abaixo desta fração do alvo a entrega deixa de ser "quase o alvo" e passa a
# precisar de explicação na tela. 70% é onde uma variação normal de rede acaba e
# um gargalo começa.
ENTREGA_OK = 0.70
# Um round-trip acima disto domina o tempo de cada escrita: com um insert por
# PIX, o TPS entregue vira 1/RTT por worker, independentemente do cluster.
ACK_DOMINADO_POR_REDE_MS = 40.0


def _diagnostico_entrega(medido: float, write_ack: dict[str, Any]) -> dict[str, Any]:
    """Por que o TPS entregue ficou abaixo do alvo — dito pela própria tela.

    Sem isto, apresentar por VPN mostra "medido 64 · alvo 2.000" sem contexto, e
    a plateia lê capacidade do Atlas onde o gargalo é o round-trip do notebook
    que apresenta. O servidor mede as mesmas escritas em poucos milissegundos, e
    esse número já está ao lado; aqui fica apenas a atribuição do limite.
    """
    alvo = generator.tps_alvo or 0
    ack_p50 = write_ack.get("p50")
    if not alvo:
        # Fora de uma execução o alvo volta a zero e não há o que atribuir.
        return {"estado": "sem_execucao"}
    if medido >= alvo * ENTREGA_OK:
        return {
            "estado": "no_alvo",
            "fracao_do_alvo": round(medido / alvo, 3) if alvo else None,
            "limitador": None,
        }
    rede = ack_p50 is not None and ack_p50 >= ACK_DOMINADO_POR_REDE_MS
    return {
        "estado": "abaixo_do_alvo",
        "fracao_do_alvo": round(medido / alvo, 3) if alvo else None,
        "limitador": "rede_do_apresentador" if rede else "cliente_local",
        "ack_p50_ms": ack_p50,
        "detalhe": (
            f"O ACK do cliente está em {ack_p50:.0f} ms: com um insert por PIX, cada worker "
            f"entrega no máximo {1000 / ack_p50:.1f} escritas/s, e o teto da carga passa a ser o "
            f"round-trip até o cluster — não o Atlas, que gravou as mesmas escritas em poucos "
            f"milissegundos. Rede corporativa, VPN ou saída por outra região reproduzem isto."
            if rede else
            "A carga não atingiu o alvo por limite do processo gerador local (CPU do notebook "
            "que apresenta), não por limite do cluster. O tempo medido pelo servidor está ao lado."
        ),
    }


_cluster_cache: dict[str, Any] = {"ts": 0.0, "dados": None}


def _cluster_info_sync() -> dict[str, Any]:
    """
    Tier REAL do cluster, lido da Admin API (cache de 60 s).

    O cluster desta PoV tem auto-scaling M20↔M30 com scale-down: parado, ele
    volta sozinho para M20. Exibir um tier fixo do .env faria a tela anunciar
    M30 rodando em M20 — e a demo pode começar no tier menor e escalar no meio.

    A PoV é calibrada para rodar inteira no tier de entrada; `escalou` marca
    quando ela saiu dele, que é sinal de custo a investigar.
    """
    agora = time.monotonic()
    if _cluster_cache["dados"] and agora - _cluster_cache["ts"] < 60:
        return _cluster_cache["dados"]

    if not settings.atlas_configured:
        return {"tier": CLUSTER_TIER, "fonte": "env", "autoscaling": None, "escalou": None}

    import requests
    from requests.auth import HTTPDigestAuth

    try:
        resp = requests.get(
            f"https://cloud.mongodb.com/api/atlas/v2/groups/{settings.atlas_project_id}"
            f"/clusters/{settings.atlas_cluster}",
            auth=HTTPDigestAuth(settings.atlas_public_key, settings.atlas_private_key),
            headers={"Accept": "application/vnd.atlas.2025-03-12+json"},
            timeout=8,
        )
        resp.raise_for_status()
        corpo = resp.json()
        rc = corpo["replicationSpecs"][0]["regionConfigs"][0]
        tier = rc["electableSpecs"]["instanceSize"]
        auto = (rc.get("autoScaling") or {}).get("compute") or {}
        maximo = auto.get("maxInstanceSize")
        minimo = auto.get("minInstanceSize")
        dados = {
            "tier": tier,
            "fonte": "atlas",
            "estado": corpo.get("stateName"),
            "autoscaling": {
                "ativo": bool(auto.get("enabled")),
                "min": minimo,
                "max": maximo,
            } if auto else None,
            # "escalou" = o cluster saiu do tier de entrada do auto-scaling.
            #
            # Versões anteriores expunham o inverso ("aquecido": já está no tier
            # máximo) e o preflight tratava isso como pré-requisito da demo —
            # mandava rodar carga até subir de tier. A PoV é calibrada para
            # rodar inteira no tier de entrada; subir virou sinal de alerta, não
            # de prontidão.
            "escalou": (tier != minimo) if (auto.get("enabled") and minimo) else False,
        }
    except Exception as exc:  # noqa: BLE001 - Admin API é opcional
        dados = {"tier": CLUSTER_TIER, "fonte": f"env ({type(exc).__name__})",
                 "autoscaling": None, "escalou": None}

    _cluster_cache.update(ts=agora, dados=dados)
    return dados


def _perfil_medido(amostra: int = 20_000) -> dict[str, Any]:
    """
    Percentis de valor MEDIDOS sobre uma amostra da coleção.

    Ticket médio sozinho engana num fluxo assimétrico: mediana e p99 é que
    mostram o formato. Isto é medição, não premissa — roda $percentile de
    verdade sobre o que o gerador escreveu.
    """
    pipeline = [
        {"$sample": {"size": amostra}},
        {"$group": {
            "_id": None,
            "n": {"$sum": 1},
            "media": {"$avg": {"$toDouble": "$valor"}},
            "total": {"$sum": {"$toDouble": "$valor"}},
            "percentis": {"$percentile": {
                "input": {"$toDouble": "$valor"},
                "p": [0.5, 0.9, 0.99],
                "method": "approximate",
            }},
        }},
    ]
    docs = list(sdb[COL_TX].aggregate(pipeline))
    if not docs:
        return {"amostra": 0}
    d = docs[0]
    p50, p90, p99 = (d.get("percentis") or [None, None, None])[:3]
    return {
        "amostra": d["n"],
        "mediana": round(p50, 2) if p50 is not None else None,
        "p90": round(p90, 2) if p90 is not None else None,
        "p99": round(p99, 2) if p99 is not None else None,
        "media": round(d["media"], 2) if d.get("media") else None,
    }


@router.get("/perfil-valores")
async def perfil_valores():
    medido = await asyncio.to_thread(_perfil_medido)
    return {
        "medido": medido,
        "premissa": {
            "perfil": PERFIL_ATIVO,
            "perfis_disponiveis": sorted(PERFIS_VALORES),
            "tipos": [{"tipo": t, "peso_pct": p} for t, p in PERFIL_TIPOS],
            "faixas": {
                tipo: [{"peso": f[0], "min": f[1], "max": f[2]} for f in faixas]
                for tipo, faixas in PERFIL_VALORES.items()
            },
            "nota": "A distribuição de valores é uma PREMISSA calibrada para se parecer com "
                    "o PIX: muita transferência pequena e cauda longa. Os percentis ao lado "
                    "são MEDIDOS sobre a coleção real.",
        },
    }


_oplog_cache: dict[str, Any] = {"ts": 0.0, "dados": None}


def _oplog_janela() -> dict[str, Any]:
    """
    Janela de retenção do oplog, em minutos.

    É o LIMITE OPERACIONAL da garantia do resume token: ele recupera uma queda
    do consumidor apenas enquanto o ponto de retomada ainda estiver no oplog.
    Sem este número, "recupera tudo" é uma promessa sem prazo — e é a primeira
    pergunta de quem opera pagamento ("e se cair 4 horas no domingo?").
    """
    agora = time.monotonic()
    if _oplog_cache["dados"] and agora - _oplog_cache["ts"] < 30:
        return _oplog_cache["dados"]

    dados: dict[str, Any] = {"janela_min": None, "detalhe": None}
    try:
        oplog = client["local"]["oplog.rs"]
        primeiro = next(oplog.find({}, {"ts": 1}).sort("$natural", 1).limit(1), None)
        ultimo = next(oplog.find({}, {"ts": 1}).sort("$natural", -1).limit(1), None)
        if primeiro and ultimo:
            segundos = ultimo["ts"].time - primeiro["ts"].time
            dados["janela_min"] = round(segundos / 60, 1)
        trunc = client.admin.command("serverStatus").get("oplogTruncation") or {}
        retencao = trunc.get("oplogMinRetentionHours")
        dados["retencao_minima_h"] = retencao
        dados["detalhe"] = (
            f"retenção mínima configurada: {retencao}h" if retencao
            else "sem retenção mínima configurada — a janela varia com o volume de escrita"
        )
    except PyMongoError as exc:
        dados["detalhe"] = f"oplog não legível ({type(exc).__name__})"

    _oplog_cache.update(ts=agora, dados=dados)
    return dados


class SondaLeitura:
    """
    Consulta pontual de UMA transação enquanto o cluster está sob escrita.

    A vazão isolada não responde à pergunta operacional: "enquanto o fluxo
    contínuo grava, meu app consulta uma transação em quanto tempo?". A sonda
    faz find_one por endToEndId (índice único), do mesmo
    processo, e mede.
    """

    INTERVALO_S = 0.25

    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.consultas = 0
        self.erros = 0

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def ensure_started(self) -> None:
        if not self.running:
            self.task = asyncio.create_task(self._run())

    def reset_counters(self) -> None:
        self.consultas = 0
        self.erros = 0
        meter_leitura.reset()

    def _consulta(self, e2e: str) -> float | None:
        inicio = time.perf_counter()
        try:
            achado = sdb[COL_TX].find_one({"endToEndId": e2e}, {"_id": 1})
        except PyMongoError:
            self.erros += 1
            return None
        if achado is None:
            return None
        return (time.perf_counter() - inicio) * 1000

    async def _run(self) -> None:
        while True:
            e2e = cs_worker.um_e2e_recente()
            if e2e:
                ms = await asyncio.to_thread(self._consulta, e2e)
                if ms is not None:
                    self.consultas += 1
                    meter_leitura.record(ms)
            await asyncio.sleep(self.INTERVALO_S)


sonda_leitura = SondaLeitura()


@router.get("/leitura")
async def leitura():
    """Latência de consulta pontual medida enquanto o gerador escreve."""
    sonda_leitura.ensure_started()
    return {
        "ativa": sonda_leitura.running,
        "consultas": sonda_leitura.consultas,
        "erros": sonda_leitura.erros,
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "indice": "endToEndId_unique",
        **meter_leitura.snapshot(),
    }


@router.get("/oplog")
async def oplog():
    return await asyncio.to_thread(_oplog_janela)


@router.get("/cluster")
async def cluster():
    return await asyncio.to_thread(_cluster_info_sync)


# Acima disto o cluster deixa de ser espectador da carga e passa a ser parte da
# conta. Abaixo, dizer "o gargalo não é o Atlas" é medição, não retórica.
CPU_COM_FOLGA_PCT = 30.0
# A partir daqui o tier vira o próximo assunto: ainda há folga, mas a curva já é
# do cluster e não adianta procurar o limite no gerador.
CPU_TIER_EM_DISCUSSAO_PCT = 70.0
_folga_cache: dict[str, Any] = {"ts": 0.0, "dados": None}

# Métricas de processo lidas da Admin API. Nomes divergem entre versões da API e
# entre tiers; pedir uma lista e tolerar ausência evita que uma métrica
# renomeada derrube o painel inteiro.
#
# OPCOUNTER_INSERT fica de fora do veredito de propósito: numa carga de
# insert_many ele volta zerado (o contador segue outro caminho), e um número que
# lê 0 durante 45 mil escritas destrói a confiança no painel inteiro.
FOLGA_METRICAS = (
    "SYSTEM_NORMALIZED_CPU_USER",
    "PROCESS_NORMALIZED_CPU_USER",
    "CONNECTIONS",
)


def _atlas_get(caminho: str, params: dict[str, Any] | None = None) -> Any:
    import requests
    from requests.auth import HTTPDigestAuth

    resp = requests.get(
        f"https://cloud.mongodb.com/api/atlas/v2/groups/{settings.atlas_project_id}{caminho}",
        auth=HTTPDigestAuth(settings.atlas_public_key, settings.atlas_private_key),
        headers={"Accept": "application/vnd.atlas.2025-03-12+json"},
        params=params,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _dp_ts(ponto: dict[str, Any]) -> datetime | None:
    bruto = ponto.get("timestamp")
    if not bruto:
        return None
    try:
        return datetime.fromisoformat(bruto.replace("Z", "+00:00"))
    except ValueError:
        return None


def _serie_resumo(medicao: dict[str, Any], desde: datetime | None = None) -> dict[str, Any] | None:
    """Máximo e último ponto de uma série, ignorando os buracos.

    A Admin API devolve `value: null` para minutos sem amostra, e um `max()`
    ingênuo estoura em TypeError no primeiro buraco — que aparece sempre no
    minuto corrente, ainda em formação.

    `desde` recorta a série na janela da execução. Sem esse recorte o painel
    resume dez minutos, e num run de 30 s o pico da carga fica diluído entre
    nove minutos de cluster parado: mediu-se ociosidade e chamou-se de folga.
    """
    pontos: list[tuple[datetime | None, float]] = []
    for ponto in medicao.get("dataPoints") or []:
        if ponto.get("value") is None:
            continue
        ts = _dp_ts(ponto)
        if desde is not None and (ts is None or ts < desde):
            continue
        pontos.append((ts, ponto["value"]))
    if not pontos:
        return None
    valores = [v for _, v in pontos]
    ultimo_ts = pontos[-1][0]
    return {
        "unidade": medicao.get("units"),
        "max": round(max(valores), 2),
        "ultimo": round(valores[-1], 2),
        "amostras": len(valores),
        "ate": ultimo_ts.isoformat() if ultimo_ts else None,
    }


def _folga_cluster_sync() -> dict[str, Any]:
    """
    Quanto do cluster a carga desta PoV realmente consumiu.

    O TPS entregue por esta demo é modesto e sozinho convida à leitura errada:
    "então o Atlas só aguenta isso". Sem o custo em CPU ao lado, o número não
    diz de quem é o teto — do gerador, da rede ou do cluster.

    Duas armadilhas, ambas encontradas medindo:

    1. As métricas de processo da Admin API são publicadas com um a dois
       minutos de atraso. Consultadas logo depois de uma execução de 30 s, elas
       ainda descrevem o cluster PARADO — e o painel concluía "ocioso" a partir
       de dados anteriores à carga. Por isso a série é recortada em
       `started_at` e, se nenhum ponto cobrir a execução, o veredito é
       `metricas_pendentes` em vez de um número.
    2. Resumir dez minutos dilui um run curto em nove minutos de ociosidade.
       O recorte na janela resolve os dois.

    O que se transfere para produção não é o TPS: é o par (TPS, %CPU) no tier
    declarado. Sizing de verdade exige medir no volume de verdade.
    """
    if not settings.atlas_configured:
        return {"estado": "nao_configurado",
                "motivo": "Admin API não configurada; a folga do cluster não pode ser medida."}

    agora = time.monotonic()
    if _folga_cache["dados"] and agora - _folga_cache["ts"] < 30:
        return _folga_cache["dados"]

    try:
        processos = _atlas_get("/processes").get("results") or []
        alvo = next(
            (p for p in processos
             if p.get("typeName") == "REPLICA_PRIMARY" and settings.atlas_cluster in (p.get("userAlias") or p.get("hostname") or "")),
            None,
        )
        # Um cluster com nome que não bate (alias diferente do informado no .env)
        # ainda tem um primário: preferir o primeiro a devolver "sem dados".
        if alvo is None:
            alvo = next((p for p in processos if p.get("typeName") == "REPLICA_PRIMARY"), None)
        if alvo is None:
            return {"estado": "indisponivel", "motivo": "Nenhum primário retornado pela Admin API."}

        medicoes = _atlas_get(
            f"/processes/{alvo['id']}/measurements",
            params=[("granularity", "PT1M"), ("period", "PT10M"), *[("m", m) for m in FOLGA_METRICAS]],
        )
    except Exception as exc:  # noqa: BLE001 - Admin API é opcional em toda a PoV
        return {"estado": "indisponivel", "motivo": f"{type(exc).__name__} ao consultar a Admin API."}

    # Janela da execução: sem execução conhecida não há recorte e o painel não
    # conclui nada. `started_at` sobrevive ao fim do run (só o /reset o limpa),
    # que é justamente quando as métricas atrasadas finalmente chegam.
    inicio_run = generator.started_at
    tps_pico = generator.tps_pico
    series = {}
    for medicao in medicoes.get("measurements") or []:
        resumo = _serie_resumo(medicao, desde=inicio_run)
        if resumo:
            series[medicao["name"]] = resumo

    # CPU normalizada do sistema é a leitura honesta (já dividida pelos núcleos);
    # a do processo entra só como reserva quando o tier não expõe a primeira.
    cpu = series.get("SYSTEM_NORMALIZED_CPU_USER") or series.get("PROCESS_NORMALIZED_CPU_USER")
    cpu_max = cpu["max"] if cpu else None

    entrega = _diagnostico_entrega(generator.measured_tps(), meter_write_ack.snapshot())
    limitador = entrega.get("limitador")
    tier = _cluster_info_sync().get("tier")

    # A Admin API publica em baldes de 1 minuto. Uma execução mais curta que
    # isso quase nunca cai inteira dentro de um balde: ela é somada com o
    # cluster parado do resto do minuto e a CPU sai diluída. Medido: o MESMO
    # perfil de carga leu 43% num run alinhado ao balde e 15% num run partido
    # entre dois. O número continua válido como PISO, e é assim que a tela o
    # apresenta — um run de 2 min ou mais elimina a ressalva.
    duracao = generator.duration_s
    subestimado = bool(duracao and duracao < 120)
    # Formatado isolado, e não com um replace na frase pronta: trocar "," por
    # "." no texto inteiro comia a pontuação das orações ("trabalho real. ainda
    # com folga."). O mesmo erro já estava documentado em routers/geo.py.
    pico_fmt = f"{tps_pico:,.0f}".replace(",", ".")

    if inicio_run is None or not tps_pico:
        veredito, detalhe = "sem_execucao", (
            "Nenhuma execução para atribuir. Uma leitura de CPU só significa alguma coisa ao "
            "lado do TPS que a produziu — rode o gerador e volte a esta tela."
        )
    elif cpu_max is None:
        veredito, detalhe = "metricas_pendentes", (
            "A execução terminou, mas o Atlas ainda não publicou as métricas do processo para "
            "essa janela — elas saem com um a dois minutos de atraso. A tela prefere dizer isto "
            "a concluir com o número anterior à carga."
        )
    elif cpu_max < CPU_COM_FOLGA_PCT:
        veredito = "cluster_com_folga"
        detalhe = (
            f"{pico_fmt} TPS entregues com o primário {tier or 'do tier atual'} em "
            f"{cpu_max:.1f}% de CPU. O teto desta execução "
            f"{'foi o ' + limitador.replace('_', ' ') if limitador else 'não foi o cluster'}"
            # Sob diluição, "sobra folga" é exatamente a conclusão que o número
            # não sustenta: parte do minuto é cluster parado.
            + ("." if subestimado else ": há folga de sobra no tier de entrada.")
        )
    elif cpu_max < CPU_TIER_EM_DISCUSSAO_PCT:
        veredito = "cluster_participando"
        detalhe = (
            f"{pico_fmt} TPS entregues com o primário {tier or 'do tier atual'} em "
            f"{cpu_max:.1f}% de CPU. Este é o número honesto para levar a uma conversa de "
            f"capacidade: o cluster está fazendo trabalho real, ainda com folga, e a alavanca "
            f"para o volume seguinte é o tier — com a conta visível, não estimada."
        )
    else:
        veredito = "cluster_no_limite"
        detalhe = (
            f"{pico_fmt} TPS levaram o primário {tier or 'do tier atual'} a {cpu_max:.1f}% "
            f"de CPU. Perto do teto do tier: daqui em diante o assunto é dimensionamento, e "
            f"nenhum número desta tela deve ser extrapolado."
        )

    if subestimado and cpu_max is not None:
        detalhe += (
            f" Leia como PISO: a execução durou {duracao}s e o Atlas publica em intervalos de "
            f"1 minuto, então o resto do minuto — com o cluster parado — entra na média. "
            f"Uma execução de 2 minutos ou mais dá o número cheio."
        )

    dados = {
        "estado": "ok",
        # Só o papel do nó, nunca o hostname: o host do Atlas carrega o nome do
        # cluster, e o nome do cluster costuma ser o nome do cliente. Este campo
        # vai para a tela e para os prints de um repositório público.
        "processo": "primário do replica set",
        "tier": tier,
        "cpu_subestimada": subestimado,
        "duracao_execucao_s": duracao,
        "janela": "execução atual" if inicio_run else "PT10M",
        "inicio_execucao": inicio_run.isoformat() if inicio_run else None,
        "series": series,
        "cpu_max_pct": cpu_max,
        "tps_pico": tps_pico,
        "limitador_entrega": limitador,
        "veredito": veredito,
        "detalhe": detalhe,
        "nota": (
            "Utilização do primário lida da Atlas Admin API, não estimada. Ela responde de quem "
            "é o teto nesta execução — não é sizing: um volume de produção precisa de medição no "
            "volume de produção."
        ),
    }
    _folga_cache.update(ts=agora, dados=dados)
    return dados


@router.get("/folga")
async def folga():
    return await asyncio.to_thread(_folga_cluster_sync)


def _medir_rtt(amostras: int = 5) -> dict[str, Any]:
    """
    RTT app ↔ cluster, medido com ping no admin.

    Sem isto a latência das colunas é lida como custo do change stream, quando
    boa parte é distância: apresentar do Brasil contra um cluster em us-east-1
    coloca ~1 RTT em cada salto. O número deixa a conta explícita.
    """
    medidas = []
    for _ in range(amostras):
        inicio = time.perf_counter()
        try:
            client.admin.command("ping")
        except PyMongoError:
            return {"rtt_ms": None, "erro": "cluster inacessível"}
        medidas.append((time.perf_counter() - inicio) * 1000)
    medidas.sort()
    return {"rtt_ms": round(medidas[len(medidas) // 2], 1), "amostras": len(medidas)}


@router.get("/rede")
async def rede():
    return await asyncio.to_thread(_medir_rtt)


def preflight_checks() -> dict[str, dict[str, Any]]:
    """
    Checagens do módulo Streaming para o /preflight global.

    Sem isto, o comando que o apresentador roda para dizer "estou pronto"
    respondia ok sem olhar para nada do módulo 07 — processor parado, connector
    com task morta e índice TTL divergente passavam batido.
    """
    checks: dict[str, dict[str, Any]] = {}

    try:
        indices = {i["name"]: i for i in sdb[COL_TX].list_indexes()}
        ttl = next((i for i in indices.values() if dict(i.get("key", {})) == {"ts": 1}), None)
        ttl_ok = bool(ttl) and ttl.get("expireAfterSeconds") == TTL_SECONDS
        unique_ok = "endToEndId_unique" in indices
        run_id_ok = "run_id_reconciliacao" in indices
        indices_ok = ttl_ok and unique_ok and run_id_ok
        faltantes = []
        if not ttl_ok:
            atual = ttl.get("expireAfterSeconds") if ttl else "ausente"
            faltantes.append(f"TTL esperado {TTL_SECONDS}s (atual: {atual})")
        if not unique_ok:
            faltantes.append("índice único endToEndId")
        if not run_id_ok:
            faltantes.append("índice run_id de reconciliação")
        checks["streaming_indices"] = {
            "ok": indices_ok,
            "message": (
                f"TTL {TTL_SECONDS}s + endToEndId único + run_id"
                if indices_ok
                else f"{'; '.join(faltantes)}; corrigidos no primeiro start do gerador"
            ),
        }
        docs = sdb[COL_TX].estimated_document_count()
        checks["streaming_colecao"] = {
            "ok": docs < DROP_ACIMA_DE,
            "message": f"{docs} documentos" + ("" if docs < DROP_ACIMA_DE else " — rode o Reset antes da demo"),
        }
    except PyMongoError as exc:
        checks["streaming_colecao"] = {"ok": False, "message": f"inacessível: {type(exc).__name__}"}

    info = _cluster_info_sync()
    auto = info.get("autoscaling") or {}
    if auto.get("ativo"):
        checks["cluster_tier"] = {
            "ok": True,
            "message": f"cluster em {info['tier']} — dentro do auto-scaling "
                       f"{auto.get('min')}→{auto.get('max')}",
        }
    else:
        checks["cluster_tier"] = {"ok": True, "message": f"cluster em {info['tier']}"}

    # ASP e Kafka são o equipamento de GRAVAÇÃO, não o de demonstração: a aba 07
    # reproduz uma execução já medida contra eles. Fora do modo ao vivo, estarem
    # desligados é o estado correto — reportá-los em vermelho faria o pré-voo
    # parecer quebrado justamente quando está como deveria.
    if not AO_VIVO:
        checks["streaming_asp"] = {
            "ok": True,
            "message": "não provisionado — a aba 07 reproduz uma execução gravada",
        }
        checks["streaming_kafka"] = {
            "ok": True,
            "message": "não provisionado — a aba 07 reproduz uma execução gravada",
        }
        return checks

    ok_asp, detalhe_asp, tier = _asp_reachable()
    atraso = _asp_atraso_s() if ok_asp else None
    if atraso is not None and atraso > ASP_ATRASO_ALERTA_S:
        ok_asp, detalhe_asp = False, f"processor {tier} drenando backlog ({atraso:.0f}s) — rode o Reset"
    if tier and tier not in detalhe_asp:
        detalhe_asp = f"{detalhe_asp} ({tier})"
    checks["streaming_asp"] = {"ok": ok_asp, "message": detalhe_asp}

    try:
        connector = _connector_status_sync()
        checks["streaming_kafka"] = {
            "ok": connector["estado"] == "RUNNING",
            "message": f"{connector['estado']} — {connector['detalhe']}",
        }
    except Exception as exc:  # noqa: BLE001 - Kafka é opcional
        checks["streaming_kafka"] = {"ok": False, "message": f"Connect indisponível ({type(exc).__name__})"}

    return checks


@router.get("/cenario")
async def cenario():
    """Marcos comparaveis com o PIX, sem converter a PoV em benchmark."""
    return {
        "premissas": {
            "workload": "sintético",
            "objetivo": "provar integridade, recuperação, fan-out, janela, estado e DLQ",
            "sizing": False,
        },
        "presets": ([
            {"label": "Operação típica", "tps": min(TPS_MAX, PIX_FATIA_PICO_TPS), "modo": "individual",
             "detalhe": "uma transação por insert · latência real de um PIX individual"},
            {"label": "Pico sustentado", "tps": min(TPS_MAX, PIX_FATIA_PICO_TPS * 2),
             "modo": "individual",
             "detalhe": "dobro da operação típica com o caminho pós-commit abaixo de 35 ms"},
            # 12k só existe em modo lote, e com o gargalo dito em voz alta: no
            # modo individual o teto é o cliente Python, não o Atlas. Oferecer
            # 12k individual convidava a demo a quebrar no palco.
            {"label": "Volume em lote", "tps": min(TPS_MAX, 12_000), "modo": "lote",
             "detalhe": "micro-batches; história de volume, não de latência por transação"},
        ] if MODO_ESCRITA_PADRAO == "individual" else [
            {"label": "Escala de referência", "tps": min(TPS_MAX, PIX_FATIA_PICO_TPS),
             "detalhe": "fatia configurável do pico sustentado planejado pelo BCB"},
            {"label": "Confortável medido", "tps": min(TPS_MAX, 4_000),
             "detalhe": "abaixo do ponto em que um cursor único acumulou lag"},
            {"label": "Recomendado para a PoV", "tps": min(TPS_MAX, 8_000),
             "detalhe": "carga ponta a ponta medida com latência controlada"},
            {"label": "Stress — escala Brasil", "tps": min(TPS_MAX, PIX_BRASIL_PICO_TPS),
             "detalhe": "10 mil TPS testa headroom; pode abrir backlog pós-commit"},
        ]),
        "default_tps": DEMO_TPS_INDIVIDUAL if MODO_ESCRITA_PADRAO == "individual" else DEMO_TPS_DEFAULT,
        "default_duration_s": DEMO_DURATION_DEFAULT_S,
        "modo_escrita": MODO_ESCRITA_PADRAO,
        "teto_individual_tps": DEMO_TPS_INDIVIDUAL,
        "referencia_pix": {
            "premissa_participacao_pct": round(PIX_PARTICIPACAO * 100, 1),
            "recorde_brasil_transacoes_dia": PIX_RECORDE_DIA,
            "recorde_brasil_data": PIX_RECORDE_DATA,
            "media_brasil_tps": PIX_BRASIL_MEDIA_TPS,
            "media_fatia_tps": PIX_FATIA_MEDIA_TPS,
            "pico_sustentado_brasil_tps": PIX_BRASIL_PICO_TPS,
            "pico_sustentado_fatia_tps": PIX_FATIA_PICO_TPS,
            "fontes": [PIX_FONTE_RECORDE, PIX_FONTE_PICO],
            "nota": (
                "Numeros publicos do BCB. A fatia e uma premissa de apresentacao "
                "(PIX_PARTICIPACAO_PCT), nao distribuicao intradia real nem "
                "capacidade certificada do Atlas."
            ),
        },
        "tps_max": TPS_MAX,
        "ambiente": {
            "cluster": (await asyncio.to_thread(_cluster_info_sync))["tier"],
            "particoes_consumo": CS_PARTICOES,
            "nota": "Os números mostram apenas esta execução. Não são capacidade do produto nem sizing.",
        },
    }


# Acima de uma rodada pequena, delete_many mantém índices e change streams mas
# é lento para palco (~10 s com 60 mil documentos no M20). O caminho controlado
# de drop para o processor, recria os três índices e retoma ASP/Kafka em menos
# tempo. Configurável para ensaios com outro perfil de carga.
DROP_ACIMA_DE = max(1_000, int(os.getenv("STREAMING_DROP_ACIMA_DE", "25000")))


def _asp_command(cmd: dict[str, Any]) -> bool:
    """Executa um comando na SPI (stop/startStreamProcessor). Best-effort."""
    if not (ASP_ENABLED and ASP_CONNECTION_STRING):
        return False
    from pymongo import MongoClient

    try:
        spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=8000)
        try:
            spi.admin.command(cmd)
            return True
        finally:
            spi.close()
    except PyMongoError:
        logger.exception("Comando do ASP falhou: %s", cmd)
        return False


def _asp_stop_wait(timeout_s: int = 60) -> bool:
    """Para o processor e só retorna depois que não pode mais gravar saídas."""
    if not (ASP_ENABLED and ASP_CONNECTION_STRING):
        return False
    from pymongo import MongoClient

    spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=8000)
    try:
        resposta = spi.admin.command({"listStreamProcessors": 1})
        processor = next(
            (p for p in resposta.get("streamProcessors", []) if p.get("name") == ASP_PROCESSOR_NAME),
            None,
        )
        if not processor:
            return False
        if processor.get("state") != "STOPPED":
            spi.admin.command({"stopStreamProcessor": ASP_PROCESSOR_NAME})
        limite = time.monotonic() + timeout_s
        while time.monotonic() < limite:
            atual = spi.admin.command({"listStreamProcessors": 1})
            state = next(
                (p.get("state") for p in atual.get("streamProcessors", [])
                 if p.get("name") == ASP_PROCESSOR_NAME),
                None,
            )
            if state == "STOPPED":
                return True
            time.sleep(1)
        raise RuntimeError(f"processor {ASP_PROCESSOR_NAME} não chegou a STOPPED em {timeout_s}s")
    finally:
        spi.close()


def _drop_and_recreate() -> int:
    """
    Caminho rápido do Reset: dropa a coleção em vez de apagar documento a
    documento.

    A poucos milhares de TPS a coleção passa de milhões de documentos em
    minutos, e aí um delete_many leva mais tempo que a própria demo. O drop é
    instantâneo, mas invalida os change streams abertos em cima da coleção — os
    workers reabrem sem token (tratado em _run) e o processor do ASP é
    reiniciado logo abaixo.
    """
    total = sdb[COL_TX].estimated_document_count()
    sdb[COL_TX].drop()
    _ensure_indexes()
    return total


def _resto_exato(target) -> int:
    """Sobrou algo? Contagem exata, limitada a 1 documento.

    `estimated_document_count` lê metadado da coleção, que ainda não reflete um
    `delete_many` recém-executado: o Reset acusava resíduo que não existia,
    respondia 503 e o Play era abortado sem explicação na tela. O `limit=1`
    mantém a checagem barata — a pergunta é "está vazia?", não "quantos são".
    """
    try:
        return target.count_documents({}, limit=1)
    except PyMongoError:
        logger.warning("Não foi possível confirmar se a coleção ficou vazia")
        return 0


def _purge(col: str) -> tuple[int, int]:
    """
    Esvazia a coleção reutilizando o cliente já conectado da aplicação.

    Alguns minutos a 5000 TPS deixam centenas de milhares de documentos, e um
    delete_many nesse volume pode ser caro; acima de `DROP_ACIMA_DE`, o Reset já
    usa o caminho de drop controlado. Para volumes menores, abrir um novo
    MongoClient era pior: cada coleção refazia a resolução DNS SRV e o Play
    ficava 20 s em "Preparando" ou devolvia 500 quando o DNS oscilava, embora o
    cliente principal continuasse conectado ao Atlas.
    """
    target = sdb[col]
    removed = 0
    try:
        # Um delete grande sob carga pode pegar uma eleição no meio
        # (NotPrimaryError). Nesse caso parte já foi removida: reencaminhar o
        # delete no novo primário termina o serviço em vez de devolver a demo
        # com meio milhão de documentos.
        for tentativa in range(3):
            try:
                removed += target.delete_many({}).deleted_count
                break
            except PyMongoError:
                logger.warning("Purga de %s falhou (tentativa %d), repetindo", col, tentativa + 1)
                time.sleep(2)
        return removed, _resto_exato(target)
    except PyMongoError:
        logger.exception("Falha ao esvaziar %s", col)
        return removed, _resto_exato(sdb[col])


@router.post("/reset")
async def reset(finalizar: bool = False):
    await generator.stop()
    # Contratos baratos e idempotentes. Além de evitar COLLSCAN na
    # reconciliação, isto cura ambientes preparados por versões antigas do
    # cleanup que não criavam o índice run_id.
    await asyncio.to_thread(_ensure_indexes)
    # O worker do change stream NÃO é parado aqui: ele só volta a subir quando um
    # novo assinante SSE chega, e a aba já aberta na demo não reabre o
    # EventSource — a coluna 1 ficaria muda depois do Reset até dar F5.
    deleted: dict[str, Any] = {}
    restantes = 0
    asp_reiniciado = False
    kafka_reiniciado = False
    asp_parado = False

    if finalizar:
        try:
            asp_parado = await asyncio.to_thread(_asp_stop_wait)
        except (PyMongoError, RuntimeError) as exc:
            raise HTTPException(
                status_code=409,
                detail=f"Limpeza cancelada: o processor não parou com segurança ({exc}).",
            ) from exc
        await asyncio.to_thread(cs_worker.discard_checkpoints)

    grande = await asyncio.to_thread(sdb[COL_TX].estimated_document_count)
    if grande > DROP_ACIMA_DE:
        if not finalizar:
            try:
                asp_parado = await asyncio.to_thread(_asp_stop_wait)
            except (PyMongoError, RuntimeError) as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Reset cancelado: o processor não parou com segurança ({exc}).",
                ) from exc
        await asyncio.to_thread(cs_worker.discard_checkpoints)
        deleted[COL_TX] = await asyncio.to_thread(_drop_and_recreate)
        if not finalizar:
            asp_reiniciado = await asyncio.to_thread(
                _asp_command,
                {"startStreamProcessor": ASP_PROCESSOR_NAME},
            )
        # O drop invalida o change stream dos DOIS processors. Reiniciar só o de
        # janelas deixaria o de sinais geográficos parado sem sinal na tela.
        await asyncio.to_thread(
            _asp_command,
            {"stopStreamProcessor": ASP_GEO_PROCESSOR_NAME},
        )
        if not finalizar:
            await asyncio.to_thread(
                _asp_command,
                {"startStreamProcessor": ASP_GEO_PROCESSOR_NAME},
            )
        alvos = (COL_WINDOWS, COL_DLQ, COL_DLQ_AUDIT)
    else:
        alvos = (COL_TX, COL_WINDOWS, COL_DLQ, COL_DLQ_AUDIT)

    if finalizar:
        alvos = (*alvos, COL_CHECKPOINTS)

    # Coleções independentes: limpar em paralelo elimina quatro round-trips
    # sequenciais antes do Play sem mudar a semântica do Reset.
    resultados_purge = await asyncio.gather(*(
        asyncio.to_thread(_purge, col) for col in alvos
    ))
    restantes_por_colecao: dict[str, int] = {}
    for col, (removed, left) in zip(alvos, resultados_purge, strict=True):
        deleted[col] = removed
        restantes += left
        restantes_por_colecao[col] = left
    # Somente o drop invalida o change stream do connector. Reiniciá-lo em toda
    # rodada limpa adicionava segundos ao Play e perturbava uma task saudável.
    if not finalizar and grande > DROP_ACIMA_DE:
        try:
            kafka_reiniciado = (await asyncio.to_thread(_connector_restart_sync)).get("reiniciado", False)
        except Exception:  # noqa: BLE001 - Kafka é opcional na demo
            kafka_reiniciado = False

    # Sinais da rodada anterior vivem no database `geo`, fora das coleções
    # acima. Deixá-los para trás faria a próxima execução começar com um
    # contador de detecções que não é dela.
    try:
        deleted[f"{GEO_DB_NOME}.{COL_SINAIS}"] = (
            await asyncio.to_thread(lambda: client[GEO_DB_NOME][COL_SINAIS].delete_many({}).deleted_count)
        )
    except PyMongoError:
        logger.warning("Não foi possível limpar %s.%s", GEO_DB_NOME, COL_SINAIS)

    generator.reset_counters()
    failover_tracker.reset()
    cs_worker.reset_counters()
    kafka_consumer.reset_counters()
    run_tracker.reset()
    sonda_leitura.reset_counters()
    # O medidor do ASP também: sem isto a coluna 3 seguia exibindo percentis de
    # rodadas anteriores (35 s, 100 s) depois de um Reset, enquanto as outras
    # duas colunas já tinham zerado.
    meter_asp.reset()
    for hub in (hub_cs, hub_kafka, hub_asp):
        hub.publish({"type": "reset"})
    # Só resíduo na coleção de origem impede começar: é ela que a reconciliação
    # conta como fonte. Janela ou DLQ que aparecem depois da purga são o
    # processor terminando de fechar a rodada anterior — um evento normal, e
    # tudo abaixo é filtrado por `run_id` de qualquer forma. Tratar esse
    # atraso como falha derrubava o Play com 503 de vez em quando, sem que
    # nada estivesse errado.
    if restantes_por_colecao.get(COL_TX):
        raise HTTPException(
            status_code=503,
            detail=(
                f"Preparação incompleta: {restantes_por_colecao[COL_TX]} documento(s) ainda em "
                f"{COL_TX}. Tente Reset novamente."
            ),
        )
    return {"reset": True, "removidos": deleted, "restantes": restantes,
            "restantes_por_colecao": restantes_por_colecao,
            "via_drop": grande > DROP_ACIMA_DE, "asp_reiniciado": asp_reiniciado,
            "asp_parado": asp_parado, "kafka_reiniciado": kafka_reiniciado,
            "finalizado": finalizar}


# ---------------------------------------------------------------------------
# COLUNA 1 — Change Streams
# ---------------------------------------------------------------------------
CHANGE_STREAM_HISTORY_LOST_CODES = {286}


def _resume_token_invalido(exc: PyMongoError) -> bool:
    """Só abandona checkpoint quando o servidor confirma perda do histórico."""
    return isinstance(exc, OperationFailure) and exc.code in CHANGE_STREAM_HISTORY_LOST_CODES


class ChangeStreamWorker:
    """
    UM cursor de collection.watch() por PARTIÇÃO, cada um na sua thread,
    publicando no mesmo hub SSE.

    Um cursor sozinho satura por volta de 5.000 eventos/s (medido contra este
    cluster): acima disso ele fica para trás e a latência cresce sem parar —
    18 s a 8.000 TPS, 50 s a 10.000. Escalar o cluster não resolve, porque o
    gargalo é o consumidor, não a escrita: o mesmo M20 aceitou ~14.000
    inserts/s. A saída é a mesma de produção — particionar o consumo e ter um
    consumidor por partição. Cada worker filtra `particao` no próprio pipeline
    do cursor, então o Atlas só entrega a fatia daquele consumidor.

    Cada partição carrega o SEU resume token. O botão "Derrubar e retomar"
    derruba todas e cada uma volta pelo seu próprio token — os eventos com ts
    anterior à reabertura entram marcados como recuperados.
    """

    # $project no próprio cursor: o custo por evento é dominado pela decodificação
    # do BSON, então o servidor manda só o que a tela e as métricas usam. O _id do
    # evento (resume token) precisa continuar vindo.
    PIPELINE = [
        {"$match": {"operationType": "insert"}},
        {"$project": {
            "_id": 1,
            "fullDocument.endToEndId": 1,
            "fullDocument.run_id": 1,
            "fullDocument.uf": 1,
            "fullDocument.tipo": 1,
            "fullDocument.valor": 1,
            "fullDocument.ts": 1,
        }},
    ]

    def __init__(self, particao: int = 0, particoes: int = 1) -> None:
        self.particao = particao
        self.particoes = particoes
        self.active = False
        self.thread = None
        self.token: dict | None = None
        self.events = 0
        self.recovered = 0
        self.ultimo_e2e: str | None = None
        self.reopen_at: datetime | None = None
        self._drop_requested = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ultimo_feed = 0.0
        self._ultima_metrica = 0.0
        self._ultimo_checkpoint = 0.0
        self._checkpoint_carregado = False
        # At-least-once significa que o MESMO evento pode chegar duas vezes
        # (retomada a partir de um token anterior, por exemplo). Aqui a gente
        # MEDE em vez de afirmar: janela limitada dos últimos ids vistos.
        self.duplicados = 0
        self._vistos: deque[str] = deque(maxlen=200_000)
        self._vistos_set: set[str] = set()

    def pipeline(self) -> list[dict[str, Any]]:
        if self.particoes <= 1:
            return self.PIPELINE
        filtro = {"$match": {"operationType": "insert", "fullDocument.particao": self.particao}}
        return [filtro, *self.PIPELINE[1:]]

    # A 3000+ eventos/s não dá para empurrar cada evento para o browser — a aba
    # morre. O feed vira uma AMOSTRA (rotulada como tal na UI); os contadores e
    # os percentis continuam cobrindo 100% dos eventos, medidos no worker.
    def reset_counters(self) -> None:
        # O resume token é preservado de propósito: o cursor continua aberto
        # durante o Reset, e zerar o token aqui faria a próxima reabertura
        # perder a continuidade que o botão "Derrubar e retomar" demonstra.
        self.events = 0
        self.recovered = 0
        self.duplicados = 0
        self.reopen_at = None
        self._vistos.clear()
        self._vistos_set.clear()
        meter_cs.reset()

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.active:
            return
        if not self._checkpoint_carregado:
            self._load_checkpoint()
        self._loop = loop
        self.active = True
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"streaming-cs-{self.particao}")
        self.thread.start()

    def stop(self) -> None:
        self.active = False

    def request_drop(self) -> None:
        if not self.active:
            raise HTTPException(status_code=409, detail="Change stream não está aberto.")
        self._drop_requested = True

    @property
    def checkpoint_id(self) -> str:
        return f"change-stream-partition-{self.particao}"

    def _load_checkpoint(self) -> None:
        try:
            saved = sdb[COL_CHECKPOINTS].find_one({"_id": self.checkpoint_id})
            self.token = saved.get("resume_token") if saved else None
        except PyMongoError:
            logger.exception("Falha ao carregar checkpoint da partição %d", self.particao)
        finally:
            self._checkpoint_carregado = True

    def _persist_checkpoint(self, force: bool = False) -> None:
        if not self.token:
            return
        agora = time.monotonic()
        if not force and agora - self._ultimo_checkpoint < 0.5:
            return
        try:
            sdb[COL_CHECKPOINTS].replace_one(
                {"_id": self.checkpoint_id},
                {
                    "_id": self.checkpoint_id,
                    "resume_token": self.token,
                    "updated_at": _now(),
                },
                upsert=True,
            )
            self._ultimo_checkpoint = agora
        except PyMongoError:
            # Um checkpoint anterior provoca reentrega, não perda. O consumidor
            # idempotente continua sendo a última linha de defesa.
            logger.exception("Falha ao persistir checkpoint da partição %d", self.particao)

    def discard_checkpoint(self) -> None:
        self.token = None
        self._checkpoint_carregado = True
        try:
            sdb[COL_CHECKPOINTS].delete_one({"_id": self.checkpoint_id})
        except PyMongoError:
            logger.exception("Falha ao remover checkpoint da partição %d", self.particao)

    def _publish(self, payload: dict[str, Any]) -> None:
        if self._loop:
            hub_cs.publish_threadsafe(self._loop, payload)

    def _run(self) -> None:
        while self.active:
            try:
                # Iteração bloqueante + batch grande: com try_next() em laço
                # apertado cada poll vazio custa um round-trip ao Atlas e o
                # consumidor empaca em ~170 eventos/s — abaixo do pico do
                # cenário. Iterando o cursor, o servidor entrega lotes e a
                # mesma thread acompanha milhares de eventos por segundo.
                # max_await_time_ms limita quanto o laço demora a perceber o
                # pedido de "derrubar e retomar" quando o fluxo está parado.
                # Sem full_document="updateLookup": o pipeline só casa inserts, e
                # em insert o documento já vem no próprio evento do oplog.
                kwargs: dict[str, Any] = {
                    "max_await_time_ms": 500,
                    "batch_size": 2_000,
                }
                if self.token:
                    kwargs["resume_after"] = self.token
                with sdb[COL_TX].watch(self.pipeline(), **kwargs) as stream:
                    self._publish({
                        "type": "aberto",
                        "particao": self.particao,
                        "retomado": bool(self.token),
                        "token": self.token_str(),
                        "eventos": cs_worker.events,
                    })
                    for change in stream:
                        self.token = change["_id"]
                        self._persist_checkpoint()
                        self._emit(change)
                        if not self.active or self._drop_requested:
                            break
            except PyMongoError as exc:
                if self.token is not None and _resume_token_invalido(exc):
                    logger.warning("Partição %d: token inválido (%s); reabrindo sem resume",
                                   self.particao, type(exc).__name__)
                    self.discard_checkpoint()
                    continue
                # Falha transitória mantém o último checkpoint. Reabrir sem ele
                # criaria uma janela silenciosa de perda.
                logger.warning("Change stream da partição %d falhou; retomando do checkpoint: %s",
                               self.particao, type(exc).__name__)
                self._publish({
                    "type": "erro",
                    "detalhe": f"{type(exc).__name__}: retomando do checkpoint persistido",
                })
                time.sleep(2)
                continue

            if self._drop_requested:
                self._drop_requested = False
                self._persist_checkpoint(force=True)
                self._publish({"type": "derrubado", "espera_s": 3, "token": self.token_str()})
                time.sleep(3)                      # gerador continua escrevendo
                self.reopen_at = _now()
        self._publish({"type": "encerrado", "eventos": self.events})

    def token_str(self) -> str | None:
        if not self.token:
            return None
        raw = self.token.get("_data", "")
        return f"{raw[:14]}…{raw[-6:]}" if len(raw) > 24 else raw

    def _emit(self, change: dict) -> None:
        doc = change.get("fullDocument") or {}
        ts = doc.get("ts")
        latency_ms = None
        recuperado = False
        if isinstance(ts, datetime):
            ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            latency_ms = round((_now() - ts).total_seconds() * 1000, 1)
            recuperado = self.reopen_at is not None and ts < self.reopen_at
        self.events += 1
        if recuperado:
            self.recovered += 1

        e2e = doc.get("endToEndId")
        run_tracker.record("change_streams", doc.get("run_id"), e2e, doc.get("valor"))
        if e2e:
            if e2e in self._vistos_set:
                self.duplicados += 1
            else:
                if len(self._vistos) == self._vistos.maxlen:
                    self._vistos_set.discard(self._vistos[0])
                self._vistos.append(e2e)
                self._vistos_set.add(e2e)
        self.ultimo_e2e = e2e                # alimenta a sonda de leitura

        meter_cs.record(latency_ms)          # 100% dos eventos entram nos percentis

        agora = time.monotonic()
        # Um evento recuperado nunca é suprimido: é a prova do "derrubar e retomar".
        if recuperado or agora - self._ultimo_feed >= FEED_INTERVALO_S:
            self._ultimo_feed = agora
            self._publish({
                "type": "evento",
                "endToEndId": doc.get("endToEndId"),
                "uf": doc.get("uf"),
                "tipo": doc.get("tipo"),
                "valor": str(doc.get("valor")),
                "latency_ms": latency_ms,
                "recuperado": recuperado,
            })
        if agora - self._ultima_metrica >= METRICAS_INTERVALO_S:
            self._ultima_metrica = agora
            self._publish({
                "type": "metricas",
                "eventos": cs_worker.events,
                "recuperados": cs_worker.recovered,
                "duplicados": cs_worker.duplicados,
                "particoes": cs_worker.particoes,
                "token": self.token_str(),
                **meter_cs.snapshot(),
            })


class ChangeStreamCluster:
    """
    Conjunto de workers (um por partição) apresentado à API como um só
    consumidor: contadores somados, drop/resume coordenado.
    """

    def __init__(self, particoes: int) -> None:
        self.particoes = max(1, particoes)
        self.workers = [ChangeStreamWorker(i, self.particoes) for i in range(self.particoes)]

    @property
    def active(self) -> bool:
        return any(w.active for w in self.workers)

    @property
    def events(self) -> int:
        return sum(w.events for w in self.workers)

    @property
    def recovered(self) -> int:
        return sum(w.recovered for w in self.workers)

    @property
    def duplicados(self) -> int:
        return sum(w.duplicados for w in self.workers)

    def um_e2e_recente(self) -> str | None:
        """Um endToEndId visto há pouco, para a sonda de leitura consultar."""
        for w in self.workers:
            if w.ultimo_e2e:
                return w.ultimo_e2e
        return None

    def token_str(self) -> str | None:
        # O token exibido é o da partição 0; cada worker guarda o seu.
        return self.workers[0].token_str()

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        for w in self.workers:
            w.ensure_started(loop)

    def stop(self) -> None:
        for w in self.workers:
            w.stop()

    def reset_counters(self) -> None:
        for w in self.workers:
            w.reset_counters()
        meter_cs.reset()

    def request_drop(self) -> None:
        if not self.active:
            raise HTTPException(status_code=409, detail="Change stream não está aberto.")
        for w in self.workers:
            w.request_drop()

    def discard_checkpoints(self) -> None:
        for w in self.workers:
            w.discard_checkpoint()


cs_worker = ChangeStreamCluster(CS_PARTICOES)


@router.get("/changestream")
async def changestream_sse(request: Request):
    cs_worker.ensure_started(asyncio.get_running_loop())
    hello = {
        "type": "hello",
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "eventos": cs_worker.events,
        "recuperados": cs_worker.recovered,
        "particoes": cs_worker.particoes,
        "token": cs_worker.token_str(),
    }
    return _sse_response(request, hub_cs, hello)


@router.post("/changestream/drop-resume")
async def changestream_drop_resume():
    cs_worker.request_drop()
    return {"derrubado": True, "espera_s": 3, "resume_after": cs_worker.token_str()}


@router.get("/changestream/status")
async def changestream_status():
    return {
        "aberto": cs_worker.active,
        "eventos": cs_worker.events,
        "recuperados": cs_worker.recovered,
        "duplicados": cs_worker.duplicados,
        "particoes": cs_worker.particoes,
        "token": cs_worker.token_str(),
        "checkpoint": f"{STREAM_DB}.{COL_CHECKPOINTS}",
        "checkpoint_persistente": True,
        **meter_cs.snapshot(),
    }


# ---------------------------------------------------------------------------
# COLUNA 2 — MongoDB Kafka Connector
# ---------------------------------------------------------------------------
def _parse_ts(value: Any) -> datetime | None:
    """O connector serializa datas como {"$date": ...} ou ISO-8601, conforme a config."""
    if isinstance(value, dict):
        value = value.get("$date", value.get("$numberLong"))
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


class KafkaConsumer:
    """
    Consumidor do tópico publicado pelo source connector. aiokafka é importado
    tarde e de forma opcional: sem a dependência (ou sem broker no ar) a coluna
    apenas reporta "nao_configurado" — o resto do módulo continua funcionando.
    """

    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.messages = 0
        self.last_offset: int | None = None
        self.estado = "nao_configurado"
        self.detalhe = "consumidor ainda não iniciado"
        self._ultimo_feed = 0.0
        self._ultima_metrica = 0.0

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def reset_counters(self) -> None:
        self.messages = 0
        self.last_offset = None
        meter_kafka.reset()

    async def restart(self) -> None:
        task, self.task = self.task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.estado = "reiniciando"
        self.detalhe = "reabrindo o mesmo consumer group a partir do offset confirmado"
        self.ensure_started()

    def ensure_started(self) -> None:
        if not self.running:
            self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer          # import tardio/opcional
        except ImportError:
            self.estado = "nao_configurado"
            self.detalhe = "aiokafka não instalado (pip install -r backend/requirements.txt)"
            hub_kafka.publish({"type": "status", "estado": self.estado, "detalhe": self.detalhe})
            return

        consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1_000,
        )
        try:
            await consumer.start()
        except Exception as exc:  # noqa: BLE001 - broker fora do ar é estado esperado da demo
            self.estado = "nao_configurado"
            self.detalhe = f"broker {KAFKA_BROKERS} inacessível: {type(exc).__name__}"
            hub_kafka.publish({"type": "status", "estado": self.estado, "detalhe": self.detalhe})
            return

        self.estado = "consumindo"
        self.detalhe = f"tópico {TOPIC} em {KAFKA_BROKERS}"
        hub_kafka.publish({"type": "status", "estado": self.estado, "detalhe": self.detalhe})
        try:
            async for msg in consumer:
                try:
                    doc = json.loads(msg.value)
                except (ValueError, TypeError):
                    continue
                ts = _parse_ts(doc.get("ts"))
                latency_ms = round((_now() - ts).total_seconds() * 1000, 1) if ts else None
                self.messages += 1
                self.last_offset = msg.offset
                # O connector publica JSON: `valor` chega como número ou como
                # `{"$numberDecimal": "..."}`, dependendo do modo de saída.
                run_tracker.record(
                    "kafka", doc.get("run_id"), doc.get("endToEndId"), _valor_json(doc.get("valor")),
                )
                meter_kafka.record(latency_ms)      # percentis sobre 100% das mensagens

                agora = time.monotonic()
                if agora - self._ultimo_feed >= FEED_INTERVALO_S:
                    self._ultimo_feed = agora
                    hub_kafka.publish({
                        "type": "mensagem",
                        "endToEndId": doc.get("endToEndId"),
                        "run_id": doc.get("run_id"),
                        "uf": doc.get("uf"),
                        "tipo": doc.get("tipo"),
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "latency_ms": latency_ms,
                    })
                if agora - self._ultima_metrica >= METRICAS_INTERVALO_S:
                    self._ultima_metrica = agora
                    hub_kafka.publish({
                        "type": "metricas",
                        "mensagens": self.messages,
                        "offset_atual": self.last_offset,
                        **meter_kafka.snapshot(),
                    })
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.estado = "erro"
            self.detalhe = f"{type(exc).__name__}: {exc}"
            hub_kafka.publish({"type": "status", "estado": self.estado, "detalhe": self.detalhe})
        finally:
            await consumer.stop()


kafka_consumer = KafkaConsumer()


def _connectores_do_showcase(sessao) -> list[str]:
    """Nomes dos connectors desta PoV: o único, ou os particionados -0, -1, ..."""
    resp = sessao.get(f"{CONNECT_URL.rstrip('/')}/connectors", timeout=3)
    resp.raise_for_status()
    return sorted(
        n for n in resp.json()
        if n == CONNECTOR_NAME or n.startswith(f"{CONNECTOR_NAME}-")
    )


def classifica_connectors(
    quantos: int, estados_connector: list[str], tasks: list[dict[str, Any]]
) -> tuple[str, str]:
    """
    Veredito único a partir do estado dos connectors e da saúde das tasks.

    É a TASK que move dado: um connector RUNNING com todas as tasks FAILED
    precisa ser reportado como FAILED, senão a coluna fica verde com o pipeline
    parado. Função pura, para poder ser testada sem Kafka.
    """
    falhas = [t for t in tasks if t.get("state") == "FAILED"]
    rodando = [t for t in tasks if t.get("state") == "RUNNING"]
    sufixo = f"{quantos} connector(s), {len(tasks)} task(s)"

    if falhas and not rodando:
        return "FAILED", f"{sufixo} — todas FAILED, use Reiniciar"
    if falhas:
        return "DEGRADADO", f"{sufixo} — {len(falhas)} FAILED, use Reiniciar"
    if not tasks:
        return "SEM_TASK", f"{sufixo} — nenhuma task ativa"
    if any(e != "RUNNING" for e in estados_connector):
        return "DEGRADADO", f"{sufixo} — connector em {', '.join(sorted(set(estados_connector)))}"
    return "RUNNING", f"{sufixo}; {CONNECT_URL}"


def _connector_status_sync() -> dict[str, Any]:
    """
    Estado agregado dos connectors da PoV.

    Com o consumo particionado existe mais de um connector publicando no mesmo
    tópico; a coluna precisa de UM veredito. E o estado é rebaixado pela saúde
    das TASKS: um connector RUNNING com todas as tasks FAILED pintava a coluna
    de verde com o pipeline parado.
    """
    import requests

    with requests.Session() as sessao:
        nomes = _connectores_do_showcase(sessao)
        if not nomes:
            return {
                "estado": "nao_configurado",
                "detalhe": f"nenhum connector '{CONNECTOR_NAME}*' em {CONNECT_URL}",
                "tasks": [],
                "connectors": 0,
            }

        tasks: list[dict[str, Any]] = []
        estados_connector: list[str] = []
        for nome in nomes:
            resp = sessao.get(f"{CONNECT_URL.rstrip('/')}/connectors/{nome}/status", timeout=3)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            body = resp.json()
            estados_connector.append(body.get("connector", {}).get("state", "DESCONHECIDO"))
            for t in body.get("tasks", []):
                tasks.append({
                    "id": f"{nome}#{t.get('id')}",
                    "state": t.get("state"),
                    "trace": (t.get("trace") or "")[:200],
                })

    estado, detalhe = classifica_connectors(len(nomes), estados_connector, tasks)
    return {"estado": estado, "detalhe": detalhe, "tasks": tasks, "connectors": len(nomes)}


def _connector_restart_sync() -> dict[str, Any]:
    """Reinicia connector e tasks. Uma task morta (queda de rede, restart do
    cluster) não se recupera sozinha — e o connector segue dizendo RUNNING."""
    import requests

    with requests.Session() as sessao:
        nomes = _connectores_do_showcase(sessao)
        reiniciados = 0
        for nome in nomes:
            resp = sessao.post(
                f"{CONNECT_URL.rstrip('/')}/connectors/{nome}/restart?includeTasks=true&onlyFailed=false",
                timeout=10,
            )
            if resp.status_code < 400:
                reiniciados += 1
    if not reiniciados:
        return {"reiniciado": False, "detalhe": "nenhum connector reiniciado"}
    return {"reiniciado": True, "detalhe": f"{reiniciados} connector(s) e suas tasks reiniciados"}


# ---------------------------------------------------------------------------
# Injeção de falha — a parte da demo que vale mais que os painéis verdes
#
# Reconciliação fechando no caminho feliz não prova recuperação: prova que nada
# deu errado. Estes dois endpoints derrubam o que dá para derrubar em segurança
# e deixam a própria reconciliação responder se houve perda.
# ---------------------------------------------------------------------------
def _connector_derrubar_sync(segundos: float) -> dict[str, Any]:
    """
    Para o connector e o traz de volta depois de `segundos`.

    Parar não apaga o offset: o resume token fica no tópico connect-offsets, e
    ao voltar o connector retoma do ponto onde estava. Tudo o que foi gravado no
    Atlas durante a parada é entregue depois — é exatamente isso que a
    reconciliação tem de mostrar fechando.
    """
    import requests

    with requests.Session() as sessao:
        nomes = _connectores_do_showcase(sessao)
        if not nomes:
            return {"derrubado": False, "detalhe": "nenhum connector do showcase encontrado"}
        for nome in nomes:
            sessao.put(f"{CONNECT_URL.rstrip('/')}/connectors/{nome}/stop", timeout=10)
        time.sleep(max(1.0, min(segundos, 30.0)))
        for nome in nomes:
            sessao.put(f"{CONNECT_URL.rstrip('/')}/connectors/{nome}/resume", timeout=10)
    return {
        "derrubado": True,
        "segundos": round(max(1.0, min(segundos, 30.0)), 1),
        "detalhe": f"{len(nomes)} connector(s) parado(s) e retomado(s) pelo offset guardado",
    }


class FalhaConnector(BaseModel):
    segundos: float = Field(default=8, ge=1, le=30)


# ── Failover de primary ─────────────────────────────────────────────────────
#
# A única falha desta PoV que atinge o MongoDB, e não um terceiro. O Atlas
# expõe o teste de failover como operação suportada: ele força uma eleição no
# replica set do cluster de demonstração. A carga continua correndo, o driver
# reabsorve as escritas (`retryWrites` é o padrão), o cursor retoma pelo resume
# token e o connector pelo offset — e a reconciliação tem de fechar em contagem,
# valor e conjunto no fim.
#
# A eleição leva bem mais que a janela de 30 s da demo, então a injeção estica a
# execução. Sem isso o stop automático fecharia a rodada no meio do evento.
FAILOVER_EXTENSAO_S = 150
# Enquanto o cluster não volta para IDLE a eleição ainda está em curso.
FAILOVER_TIMEOUT_S = 600
# Mesma versão usada pelos outros consumidores da Admin API nesta PoV.
ATLAS_ACCEPT_VERSION = "application/vnd.atlas.2025-03-12+json"


class FailoverTracker:
    """Estado do teste de failover, para a tela narrar o evento enquanto ocorre."""

    def __init__(self) -> None:
        self.estado = "ocioso"        # ocioso | em_curso | concluido | falhou
        self.iniciado_em: datetime | None = None
        self.concluido_em: datetime | None = None
        self.detalhe = ""
        self.run_id: str | None = None
        self.erros_no_inicio = 0
        self.erros_durante = 0
        self.eventos_cs_no_inicio = 0
        self.inseridos_no_inicio = 0

    def snapshot(self) -> dict[str, Any]:
        duracao = None
        if self.iniciado_em:
            fim = self.concluido_em or _now()
            duracao = round((fim - self.iniciado_em).total_seconds(), 1)
        return {
            "estado": self.estado,
            "detalhe": self.detalhe,
            "run_id": self.run_id,
            "iniciado_em": self.iniciado_em.isoformat() if self.iniciado_em else None,
            "duracao_s": duracao,
            "escritas_rejeitadas": self.erros_durante,
            "escritas_confirmadas": max(generator.inserted - self.inseridos_no_inicio, 0),
            "eventos_apos_eleicao": max(cs_worker.events - self.eventos_cs_no_inicio, 0),
        }

    def reset(self) -> None:
        self.__init__()


failover_tracker = FailoverTracker()


def _atlas_test_failover_sync() -> str:
    """Dispara o teste de failover pela Atlas Admin API.

    O recurso mudou de forma entre as versões da API e um projeto pode estar em
    qualquer uma delas, então as formas conhecidas são tentadas em ordem. Só um
    405/404 faz passar para a próxima: erro de credencial ou de access list é
    definitivo e precisa chegar à tela como está, sem ser mascarado pela
    tentativa seguinte.
    """
    import requests
    from requests.auth import HTTPDigestAuth

    auth = HTTPDigestAuth(settings.atlas_public_key, settings.atlas_private_key)
    base_v2 = (
        f"https://cloud.mongodb.com/api/atlas/v2/groups/{settings.atlas_project_id}"
        f"/clusters/{settings.atlas_cluster}"
    )
    base_v1 = (
        f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{settings.atlas_project_id}"
        f"/clusters/{settings.atlas_cluster}"
    )
    candidatos = [
        ("v2/restartPrimaries", f"{base_v2}/restartPrimaries", {"Accept": ATLAS_ACCEPT_VERSION}),
        ("v2/:testFailover", f"{base_v2}:testFailover", {"Accept": ATLAS_ACCEPT_VERSION}),
        ("v1.0/restartPrimaries", f"{base_v1}/restartPrimaries", {}),
    ]

    ultimo = ""
    for nome, url, headers in candidatos:
        resp = requests.post(url, auth=auth, headers=headers, timeout=30)
        if 200 <= resp.status_code < 300:
            logger.info("Teste de failover aceito pelo Atlas via %s", nome)
            return nome
        detalhe = ""
        try:
            corpo = resp.json()
            detalhe = corpo.get("detail") or corpo.get("errorCode") or ""
        except ValueError:
            detalhe = resp.text[:200]
        ultimo = f"HTTP {resp.status_code} {detalhe}".strip()
        if resp.status_code not in (404, 405):
            raise RuntimeError(ultimo)
    raise RuntimeError(ultimo or "nenhuma forma conhecida do recurso foi aceita")


# A sondagem da Admin API custa dois round-trips (autenticação digest), e o
# pré-voo é chamado pelo cabeçalho da aplicação. Sem cache ele levou /preflight
# de ~1 s para ~6 s numa rede de 250 ms de RTT — a checagem que existe para a
# demo não quebrar não pode ser o que deixa a demo lenta. Mesmo padrão do tier.
_atlas_probe_cache: dict[str, Any] = {"ts": 0.0, "dados": None}
ATLAS_PROBE_TTL_S = 60


def preflight_atlas_admin() -> dict[str, Any]:
    """A Admin API responde de verdade, deste IP, com estas chaves?

    Conferir apenas se a credencial está no `.env` deixava o pré-voo verde
    enquanto a API recusava toda chamada — e a tela caía em silêncio para o tier
    do `.env` (`fonte: env (HTTPError)`). Na prática o Online Archive e o teste
    de failover quebrariam no palco, depois do pré-voo ter dito que estava tudo
    pronto. O IP de saída muda ao ligar ou desligar VPN, então este é o check
    que mais falha na vida real, e o mais barato de fazer certo.
    """
    if not settings.atlas_configured:
        return {"ok": False, "message": "opcional; módulo Online Archive ficará limitado"}

    agora = time.monotonic()
    if _atlas_probe_cache["dados"] and agora - _atlas_probe_cache["ts"] < ATLAS_PROBE_TTL_S:
        return _atlas_probe_cache["dados"]

    import requests
    from requests.auth import HTTPDigestAuth

    try:
        resp = requests.get(
            f"https://cloud.mongodb.com/api/atlas/v2/groups/{settings.atlas_project_id}"
            f"/clusters/{settings.atlas_cluster}",
            auth=HTTPDigestAuth(settings.atlas_public_key, settings.atlas_private_key),
            headers={"Accept": ATLAS_ACCEPT_VERSION},
            # Curto de propósito: o pré-voo prefere dizer "inalcançável" rápido a
            # segurar o cabeçalho da aplicação esperando a Admin API.
            timeout=5,
        )
    except requests.RequestException as exc:
        return _lembra_probe({"ok": False, "message": f"inalcançável ({type(exc).__name__})"})

    if 200 <= resp.status_code < 300:
        return _lembra_probe({"ok": True, "message": "credenciais válidas e IP autorizado"})

    detalhe = ""
    try:
        detalhe = resp.json().get("detail") or ""
    except ValueError:
        pass
    if resp.status_code in (401, 403):
        return _lembra_probe({
            "ok": False,
            # A mensagem do Atlas já traz o IP recusado: repassar é mais útil que
            # traduzir, porque é ele que precisa entrar na access list.
            "message": (
                f"recusada: {detalhe or 'credencial ou IP não autorizado'} "
                "— Atlas → Access Manager → API Keys → Access List"
            ),
        })
    return _lembra_probe({"ok": False, "message": f"HTTP {resp.status_code} {detalhe}".strip()})


def _lembra_probe(resultado: dict[str, Any]) -> dict[str, Any]:
    _atlas_probe_cache.update(ts=time.monotonic(), dados=resultado)
    return resultado


def _cluster_estado_sync() -> str | None:
    import requests
    from requests.auth import HTTPDigestAuth

    try:
        resp = requests.get(
            f"https://cloud.mongodb.com/api/atlas/v2/groups/{settings.atlas_project_id}"
            f"/clusters/{settings.atlas_cluster}",
            auth=HTTPDigestAuth(settings.atlas_public_key, settings.atlas_private_key),
            headers={"Accept": "application/vnd.atlas.2025-03-12+json"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("stateName")
    except Exception:  # noqa: BLE001 - o acompanhamento é best-effort
        return None


async def _acompanha_failover() -> None:
    """Segue o cluster até ele voltar a IDLE e fecha a narrativa na tela."""
    limite = time.monotonic() + FAILOVER_TIMEOUT_S
    try:
        # Uma pausa antes da primeira leitura: o Atlas leva alguns segundos para
        # sair de IDLE, e perguntar cedo demais concluiria o evento na hora.
        await asyncio.sleep(15)
        while time.monotonic() < limite:
            estado = await asyncio.to_thread(_cluster_estado_sync)
            failover_tracker.erros_durante = max(
                generator.write_errors - failover_tracker.erros_no_inicio, 0
            )
            if estado == "IDLE":
                failover_tracker.estado = "concluido"
                failover_tracker.concluido_em = _now()
                failover_tracker.detalhe = (
                    "Eleição concluída e cluster de volta em IDLE. Confira a reconciliação: "
                    "ela precisa fechar em contagem, valor e conjunto."
                )
                return
            await asyncio.sleep(10)
        failover_tracker.estado = "concluido"
        failover_tracker.concluido_em = _now()
        failover_tracker.detalhe = (
            "Acompanhamento encerrado por tempo; verifique o estado do cluster no Atlas."
        )
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - acompanhar não pode derrubar a demo
        logger.exception("Falha ao acompanhar o failover")
        failover_tracker.estado = "concluido"
        failover_tracker.concluido_em = _now()
        failover_tracker.detalhe = "Não foi possível acompanhar o estado do cluster."


@router.post("/falha/failover")
async def falha_failover():
    """Força uma eleição de primary no cluster de demonstração.

    É a falha que um time de plataforma pede: não derrubar um conector, e sim
    tirar o primary do banco embaixo de uma carga confirmada.
    """
    if not settings.atlas_configured:
        raise HTTPException(
            status_code=503,
            detail="Credenciais da Atlas Admin API não configuradas; o teste de failover precisa delas.",
        )
    if not generator.running:
        raise HTTPException(
            status_code=409,
            detail="Inicie o fluxo antes: uma eleição sem carga em andamento não demonstra retomada.",
        )
    if failover_tracker.estado == "em_curso":
        raise HTTPException(status_code=409, detail="Já existe uma eleição em curso.")

    failover_tracker.reset()
    failover_tracker.estado = "em_curso"
    failover_tracker.iniciado_em = _now()
    failover_tracker.run_id = generator.run_id
    failover_tracker.erros_no_inicio = generator.write_errors
    failover_tracker.inseridos_no_inicio = generator.inserted
    failover_tracker.eventos_cs_no_inicio = cs_worker.events

    try:
        via = await asyncio.to_thread(_atlas_test_failover_sync)
    except Exception as exc:  # noqa: BLE001 - a mensagem precisa chegar à tela
        failover_tracker.estado = "falhou"
        failover_tracker.detalhe = str(exc)
        raise HTTPException(
            status_code=503,
            detail=f"O Atlas recusou o teste de failover: {exc}",
        ) from exc

    novo_fim = generator.estender(FAILOVER_EXTENSAO_S)
    failover_tracker.detalhe = (
        "Eleição solicitada ao Atlas. A carga continua correndo e foi estendida para o evento "
        "caber inteiro na execução."
    )
    asyncio.create_task(_acompanha_failover())
    return {
        "disparado": True,
        "via": via,
        "extensao_s": FAILOVER_EXTENSAO_S,
        "ends_at": novo_fim.isoformat() if novo_fim else None,
        "detalhe": failover_tracker.detalhe,
    }


@router.get("/falha/failover/status")
async def falha_failover_status():
    return failover_tracker.snapshot()


@router.post("/falha/connector")
async def falha_connector(body: FalhaConnector):
    """Derruba a coluna 2 no meio do fluxo e a traz de volta."""
    try:
        return await asyncio.to_thread(_connector_derrubar_sync, body.segundos)
    except Exception as exc:  # noqa: BLE001 - Kafka é opcional
        raise HTTPException(status_code=503, detail=f"Connect indisponível: {type(exc).__name__}") from exc


@router.post("/falha/evento-invalido")
async def falha_evento_invalido():
    """
    Grava um documento que viola o `$validate` do processor.

    Ele é uma transação legítima do ponto de vista da coleção — passa no índice
    único e entra na contagem da fonte —, mas tem `valor` como string. O ASP o
    desvia para a DLQ e continua rodando: é a diferença entre um evento ruim e
    um pipeline parado.
    """
    doc = {
        "endToEndId": f"X{uuid.uuid4().hex[:31].upper()}",
        "run_id": generator.run_id or "execucao-local",
        "canal": "PIX",
        "particao": 0,
        "valor": "isto-nao-e-um-numero",
        "tipo": "PIX",
        "uf": "SP",
        "ts": _now(),
        "status": "liquidada",
        "injetado": "falha-demo",
    }
    await acol_tx().insert_one(doc)
    return {
        "injetado": True,
        "endToEndId": doc["endToEndId"],
        "detalhe": f"documento com `valor` string gravado; o ASP deve desviá-lo para {STREAM_DB}.{COL_DLQ}",
    }


# Contrato do evento publicado, espelhando o `$validate` do processor em
# `scripts/setup-asp.js`. Fica aqui para a tela poder mostrar o contrato que
# está valendo em vez de descrevê-lo em prosa — e para a injeção de mudança
# incompatível ser lida contra o mesmo texto que o cliente acabou de ver.
CONTRATO_EVENTO = [
    {"campo": "endToEndId", "tipo": "string", "obrigatorio": True,
     "nota": "chave de negócio; índice único e base da idempotência"},
    {"campo": "run_id", "tipo": "string", "obrigatorio": True,
     "nota": "identifica a execução na reconciliação"},
    {"campo": "valor", "tipo": "decimal | double | int | long", "obrigatorio": True,
     "nota": "monetário; a reconciliação soma em centavos inteiros"},
    {"campo": "tipo", "tipo": "enum: PIX, CARTAO_DEBITO, CARTAO_CREDITO", "obrigatorio": True,
     "nota": "canal do evento; valor fora do enum vai para a DLQ"},
    {"campo": "uf", "tipo": "string", "obrigatorio": True,
     "nota": "chave de agregação das janelas"},
]


@router.get("/contrato")
async def contrato():
    """O contrato que o processor exige, e o que acontece quando ele é violado."""
    return {
        "fonte": "$validate ($jsonSchema) do processor, com validationAction: dlq",
        "processor": ASP_PROCESSOR_NAME,
        "campos": CONTRATO_EVENTO,
        "chave_kafka": "document key do MongoDB (_id)",
        "formato": "JSON, sem Schema Registry nesta PoV",
        "politica": (
            "Documento fora do contrato não para o pipeline e não é descartado em silêncio: "
            "vai para a DLQ com o motivo, e a reconciliação o contabiliza."
        ),
        "fora_de_escopo": (
            "Registro versionado, compatibilidade entre versões e ownership entre squads são "
            "decisões de produção. O que esta PoV demonstra é o comportamento diante da violação."
        ),
    }


@router.post("/falha/schema-incompativel")
async def falha_schema_incompativel():
    """Publica uma "nova versão" do evento que quebra o contrato.

    O caso real: alguém renomeia `valor` para `amount` numa squad e publica.
    O campo obrigatório some, o processor rejeita pelo `$jsonSchema` e o
    documento vai para a DLQ com o motivo — o pipeline não para e nenhum
    consumidor recebe um evento que não sabe ler.
    """
    doc = {
        "endToEndId": f"S{uuid.uuid4().hex[:31].upper()}",
        "run_id": generator.run_id or "execucao-local",
        "canal": "PIX",
        "particao": 0,
        # `valor` renomeado: é exatamente a mudança incompatível que um
        # Schema Registry recusaria no registro.
        "amount": 123.45,
        "tipo": "PIX",
        "uf": "SP",
        "ts": _now(),
        "status": "liquidada",
        "injetado": "falha-demo-schema",
        "schema_versao": "2",
    }
    await acol_tx().insert_one(doc)
    return {
        "injetado": True,
        "endToEndId": doc["endToEndId"],
        "mudanca": "campo obrigatório `valor` renomeado para `amount`",
        "detalhe": (
            f"evento fora do contrato gravado; o processor deve recusá-lo pelo $jsonSchema e "
            f"registrá-lo em {STREAM_DB}.{COL_DLQ} sem parar"
        ),
    }


@router.get("/kafka/status")
async def kafka_status():
    kafka_consumer.ensure_started()
    try:
        connector = await asyncio.to_thread(_connector_status_sync)
    except Exception as exc:  # noqa: BLE001 - Connect fora do ar é estado esperado da demo
        connector = {
            "estado": "nao_configurado",
            "detalhe": f"Kafka Connect indisponível em {CONNECT_URL} ({type(exc).__name__})",
            "tasks": [],
        }
    return {
        "connector": {"nome": CONNECTOR_NAME, **connector},
        "particoes_consumo": CS_PARTICOES,
        "consumidor": {
            "estado": kafka_consumer.estado,
            "detalhe": kafka_consumer.detalhe,
            "mensagens": kafka_consumer.messages,
            "offset_atual": kafka_consumer.last_offset,
            "group_id": KAFKA_CONSUMER_GROUP,
            "offset_persistente": True,
        },
        "topico": TOPIC,
        "brokers": KAFKA_BROKERS,
        "connect_url": CONNECT_URL,
    }


@router.post("/kafka/restart")
async def kafka_restart():
    try:
        return await asyncio.to_thread(_connector_restart_sync)
    except Exception as exc:  # noqa: BLE001 - Connect fora do ar é estado esperado
        raise HTTPException(status_code=409, detail=f"Kafka Connect indisponível ({type(exc).__name__}).") from exc


@router.post("/kafka/consumer/restart")
async def kafka_consumer_restart():
    """Reinicia o observador mantendo o consumer group e seus offsets."""
    await kafka_consumer.restart()
    return {
        "reiniciado": True,
        "group_id": KAFKA_CONSUMER_GROUP,
        "mensagem": "Consumidor reaberto com o mesmo group.id; reentregas seguem visíveis na reconciliação.",
    }


@router.get("/kafka")
async def kafka_sse(request: Request):
    kafka_consumer.ensure_started()
    hello = {
        "type": "hello",
        "topico": TOPIC,
        "estado": kafka_consumer.estado,
        "detalhe": kafka_consumer.detalhe,
        "mensagens": kafka_consumer.messages,
    }
    return _sse_response(request, hub_kafka, hello)


# ---------------------------------------------------------------------------
# COLUNA 3 — Atlas Stream Processing
# ---------------------------------------------------------------------------
ASP_PIPELINE_SNIPPET = """[
  { $source: { connectionName: __CONNECTION__, db: __DB__, coll: "transacoes",
               config: { fullDocument: "updateLookup" } } },
  { $match: { operationType: "insert" } },

  // documento malformado vai para a DLQ; o processor nao cai
  { $validate: { validator: { $jsonSchema: {
        bsonType: "object", required: ["fullDocument"], properties: {
          fullDocument: { bsonType: "object",
            required: ["endToEndId","run_id","valor","tipo","uf"], properties: {
              endToEndId: { bsonType: "string" }, run_id: { bsonType: "string" },
              valor: { bsonType: ["decimal","double","int","long"] },
              tipo: { enum: ["PIX"] }, uf: { bsonType: "string" } } } } } },
      validationAction: "dlq" } },

  { $tumblingWindow: {
      boundary: "eventTime",
      interval: { size: 5, unit: "second" },
      allowedLateness: { size: 2, unit: "second" },
      pipeline: [
        { $group: {
            _id: { run_id: "$fullDocument.run_id", uf: "$fullDocument.uf", tipo: "$fullDocument.tipo" },
            qtd:    { $count: {} },
            volume: { $sum: { $toDouble: "$fullDocument.valor" } },
            ticket: { $avg: { $toDouble: "$fullDocument.valor" } },
            alertas_valor_alto: { $sum: { $cond: [
              { $gte: [ { $toDouble: "$fullDocument.valor" }, 5000 ] }, 1, 0 ] } },
            maior_valor: { $max: { $toDouble: "$fullDocument.valor" } } } },
        { $set: { run_id: "$_id.run_id", uf: "$_id.uf", tipo: "$_id.tipo",
                  volume: { $round: ["$volume", 2] },
                  ticket: { $round: ["$ticket", 2] },
                  maior_valor: { $round: ["$maior_valor", 2] } } } ] } },

  // bordas oficiais da janela, estáveis mesmo com eventos atrasados
  { $set: { window_start: { $meta: "stream.window.start" },
            window_end:   { $meta: "stream.window.end" } } },

  // _id deterministico por (execucao, janela, uf, tipo) => $merge idempotente
  { $set: { _id: { $concat: [ "$run_id", "|", { $toString: "$window_start" }, "|", "$uf", "|", "$tipo" ] } } },
  { $merge: { into: { connectionName: __CONNECTION__, db: __DB__, coll: "metricas_janela" },
              whenMatched: "replace", whenNotMatched: "insert" } }
]""".replace("__CONNECTION__", json.dumps(ASP_CONNECTION_NAME)).replace("__DB__", json.dumps(STREAM_DB))


def _asp_reachable() -> tuple[bool, str, str | None]:
    """(configurado, detalhe, tier_real_do_processor)."""
    if not ASP_ENABLED:
        return False, "ASP_ENABLED=false", None
    if not ASP_CONNECTION_STRING:
        return False, "ASP_CONNECTION_STRING ausente", None
    from pymongo import MongoClient

    try:
        spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=5000, connect=False)
        try:
            # Estado REAL do processor, direto da SPI — não basta a conexão abrir.
            resposta = spi.admin.command({"listStreamProcessors": 1})
            processors = resposta.get("streamProcessors", [])
            if not processors:
                return False, "SPI acessível, mas nenhum processor criado (rode scripts/setup-asp.js)", None
            processor = next((p for p in processors if p.get("name") == ASP_PROCESSOR_NAME), None)
            if processor is None:
                nomes = ", ".join(p.get("name", "?") for p in processors)
                return False, f"processor {ASP_PROCESSOR_NAME!r} não encontrado (existentes: {nomes})", None
            if processor.get("state") != "STARTED":
                return False, f"processor {ASP_PROCESSOR_NAME}={processor.get('state')}", None
            # Tier REAL em execução: o processor mantém o tier com que foi
            # iniciado, que pode diferir do default do workspace.
            tier = processor.get("effectiveTier") or processor.get("tier")
            return True, f"processor STARTED: {ASP_PROCESSOR_NAME}", tier
        finally:
            spi.close()
    except Exception as exc:  # noqa: BLE001 - SPI ausente é estado esperado da demo
        return False, f"SPI inacessível: {type(exc).__name__}", None


def _asp_runtime_stats() -> dict[str, Any]:
    """Métricas oficiais do processor; indisponibilidade não derruba a demo."""
    if not (ASP_ENABLED and ASP_CONNECTION_STRING):
        return {"disponivel": False}
    from pymongo import MongoClient

    try:
        spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=5000, connect=False)
        try:
            resposta = spi.admin.command({
                "getStreamProcessorStats": ASP_PROCESSOR_NAME,
                "options": {"scale": 1, "verbose": True},
            })
        finally:
            spi.close()
    except PyMongoError as exc:
        logger.warning("Stats do ASP indisponíveis: %s", type(exc).__name__)
        return {"disponivel": False, "detalhe": type(exc).__name__}

    stats = resposta.get("stats") or {}
    operators = stats.get("operatorStats") or []
    max_memory = max((op.get("maxMemoryUsage") or 0 for op in operators), default=0)
    return {
        "disponivel": True,
        "input": stats.get("inputMessageCount"),
        "output": stats.get("outputMessageCount"),
        "dlq": stats.get("dlqMessageCount"),
        "lag_oplog_s": stats.get("changeStreamTimeDifferenceSecs"),
        "state_bytes": stats.get("stateSize"),
        "watermark": stats.get("watermark"),
        "max_memory_bytes": max_memory,
        "latencia": stats.get("latency"),
    }


def _asp_restart_from_checkpoint() -> dict[str, Any]:
    """Stop/start controlado: o start padrão retoma do último checkpoint."""
    if not (ASP_ENABLED and ASP_CONNECTION_STRING):
        raise RuntimeError("ASP não configurado")
    from pymongo import MongoClient

    spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=8000)
    try:
        antes = spi.admin.command({"listStreamProcessors": 1})
        processor = next(
            (p for p in antes.get("streamProcessors", []) if p.get("name") == ASP_PROCESSOR_NAME),
            None,
        )
        if not processor:
            raise RuntimeError(f"processor {ASP_PROCESSOR_NAME} não encontrado")
        if processor.get("state") == "STARTED":
            spi.admin.command({"stopStreamProcessor": ASP_PROCESSOR_NAME})
            limite = time.monotonic() + 60
            while time.monotonic() < limite:
                atual = spi.admin.command({"listStreamProcessors": 1})
                state = next(
                    (p.get("state") for p in atual.get("streamProcessors", [])
                     if p.get("name") == ASP_PROCESSOR_NAME),
                    None,
                )
                if state == "STOPPED":
                    break
                time.sleep(1)
            else:
                raise RuntimeError("processor não chegou ao estado STOPPED")
        spi.admin.command({"startStreamProcessor": ASP_PROCESSOR_NAME})
        return {
            "reiniciado": True,
            "processor": ASP_PROCESSOR_NAME,
            "retomada": "checkpoint_gerenciado",
            "mensagem": "Processor reiniciado; o ASP retoma automaticamente do último checkpoint.",
        }
    finally:
        spi.close()


ASP_ATRASO_ALERTA_S = 30.0


def _asp_atraso_s() -> float | None:
    """
    Segundos entre a última janela FECHADA e agora.

    Depois de uma rajada o processor fica drenando backlog, e os percentis da
    coluna passam a medir a fila, não o regime — a tela chegava a mostrar
    p50 de 23 s. Com este número a UI diz "drenando backlog" em vez de exibir
    um percentil que não representa nada.
    """
    if not generator.running:
        return None
    doc = sdb[COL_WINDOWS].find_one({}, {"window_end": 1}, sort=[("window_end", -1)])
    if not doc or not isinstance(doc.get("window_end"), datetime):
        return None
    fim = doc["window_end"]
    fim = fim if fim.tzinfo else fim.replace(tzinfo=timezone.utc)
    return round((_now() - fim).total_seconds(), 1)


def _asp_totais() -> dict[str, Any]:
    """Soma real do que o processor já agregou — qtd e volume financeiro das janelas."""
    pipeline = [{"$group": {"_id": None, "qtd": {"$sum": "$qtd"}, "volume": {"$sum": "$volume"}}}]
    docs = list(sdb[COL_WINDOWS].aggregate(pipeline))
    if not docs:
        return {"transacoes_agregadas": 0, "volume_agregado": 0.0}
    return {
        "transacoes_agregadas": int(docs[0].get("qtd") or 0),
        "volume_agregado": round(float(docs[0].get("volume") or 0.0), 2),
    }


@router.get("/asp/status")
async def asp_status():
    ok, detalhe, tier = await asyncio.to_thread(_asp_reachable)
    janelas = await asyncio.to_thread(sdb[COL_WINDOWS].count_documents, {})
    dlq = await asyncio.to_thread(sdb[COL_DLQ].count_documents, {})
    totais = await asyncio.to_thread(_asp_totais)
    runtime = await asyncio.to_thread(_asp_runtime_stats) if ok else {"disponivel": False}
    atraso = await asyncio.to_thread(_asp_atraso_s) if ok else None
    return {
        "estado": "configurado" if ok else "nao_configurado",
        "detalhe": detalhe,
        "tier": tier,
        "atraso_s": atraso,
        "drenando_backlog": bool(atraso is not None and atraso > ASP_ATRASO_ALERTA_S),
        "atraso_alerta_s": ASP_ATRASO_ALERTA_S,
        **totais,
        "asp_enabled": ASP_ENABLED,
        "colecao_janelas": f"{STREAM_DB}.{COL_WINDOWS}",
        "colecao_dlq": f"{STREAM_DB}.{COL_DLQ}",
        "janelas": janelas,
        "dlq": dlq,
        "pipeline": ASP_PIPELINE_SNIPPET,
        "runtime": json.loads(json.dumps(runtime, default=str)),
    }


@router.post("/asp/restart-checkpoint")
async def asp_restart_checkpoint():
    try:
        return await asyncio.to_thread(_asp_restart_from_checkpoint)
    except (PyMongoError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=f"Não foi possível reiniciar o processor: {exc}") from exc


class AspWatcher:
    """
    O resultado do ASP chega na tela POR CHANGE STREAM: o processor faz $merge em
    pix.metricas_janela (e manda documento malformado para pix.dlq) e nós
    assistimos essas coleções com collection.watch(). É o fecho didático das três
    colunas — a coluna 3 é entregue pela mecânica da coluna 1.
    """

    def __init__(self) -> None:
        self.active = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.active:
            return
        self._loop = loop
        self.active = True
        for col, kind in ((COL_WINDOWS, "janela"), (COL_DLQ, "dlq")):
            threading.Thread(target=self._run, args=(col, kind), daemon=True, name=f"streaming-asp-{kind}").start()

    def _run(self, col: str, kind: str) -> None:
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "replace", "update"]}}}]
        while self.active:
            try:
                with sdb[col].watch(pipeline, full_document="updateLookup", max_await_time_ms=400) as stream:
                    while self.active:
                        change = stream.try_next()
                        if change is None:
                            continue
                        doc = change.get("fullDocument") or {}
                        payload = {"type": kind, "recebido_em": _now().isoformat()}
                        if kind == "janela":
                            key = doc.get("_id") if isinstance(doc.get("_id"), dict) else {}
                            # Latência da janela: da última transação que entrou nela
                            # até o resultado agregado chegar aqui (fecho + $merge +
                            # change stream). É o custo real do caminho gerenciado.
                            fim = doc.get("window_end")
                            latency_ms = None
                            if isinstance(fim, datetime):
                                fim = fim if fim.tzinfo else fim.replace(tzinfo=timezone.utc)
                                latency_ms = round((_now() - fim).total_seconds() * 1000, 1)
                            meter_asp.record(latency_ms)
                            payload.update({
                                "run_id": doc.get("run_id") or key.get("run_id"),
                                "uf": doc.get("uf") or key.get("uf"),
                                "tipo": doc.get("tipo") or key.get("tipo"),
                                "qtd": doc.get("qtd"),
                                "volume": doc.get("volume"),
                                "ticket": doc.get("ticket"),
                                "alertas_valor_alto": doc.get("alertas_valor_alto", 0),
                                "maior_valor": doc.get("maior_valor"),
                                "window_start": doc.get("window_start"),
                                "window_end": doc.get("window_end"),
                                "latency_ms": latency_ms,
                                **meter_asp.snapshot(),
                            })
                        else:
                            payload.update({
                                "motivo": doc.get("errorMsg") or doc.get("motivo") or "documento rejeitado",
                                "documento": {k: str(v) for k, v in list(doc.items())[:8] if k != "_id"},
                            })
                        if self._loop:
                            hub_asp.publish_threadsafe(self._loop, payload)
            except PyMongoError as exc:
                logger.warning("Watcher ASP (%s) reiniciando: %s", col, type(exc).__name__)
                time.sleep(2)


asp_watcher = AspWatcher()


@router.get("/asp")
async def asp_sse(request: Request):
    asp_watcher.ensure_started(asyncio.get_running_loop())
    ultimas = await asyncio.to_thread(
        lambda: list(sdb[COL_WINDOWS].find({}, {"_id": 0}).sort("window_end", -1).limit(20))
    )
    hello = {
        "type": "hello",
        "colecao": f"{STREAM_DB}.{COL_WINDOWS}",
        "janelas_existentes": len(ultimas),
        "asp_enabled": ASP_ENABLED,
    }
    return _sse_response(request, hub_asp, hello)


# Defeitos realistas: cada um viola o validador de um jeito diferente, para a
# DLQ mostrar MOTIVOS distintos em vez de mil cópias do mesmo erro.
# Cada defeito viola o validador de um jeito diferente, para a DLQ mostrar
# MOTIVOS distintos. Nenhum deles pode colidir com o índice único: um
# endToEndId nulo passaria no $validate errado — e o índice só aceita UM null,
# então o segundo doc quebraria o insert em vez de chegar à DLQ.
DEFEITOS = [
    ("valor_nao_numerico", {"valor": "isto-nao-e-um-numero"}),
    ("tipo_fora_do_enum", {"tipo": "CRIPTO"}),
    ("uf_invalida", {"uf": None}),
    ("end_to_end_id_nao_texto", {"endToEndId": 0}),   # sobrescrito abaixo com um número único
]


def _doc_invalido(indice: int) -> dict[str, Any]:
    defeito, patch = DEFEITOS[indice % len(DEFEITOS)]
    doc = _new_transacao(generator.run_id or "teste-dlq")
    doc["endToEndId"] = f"INVALIDO-{defeito.upper()}-{uuid.uuid4().hex[:10].upper()}"
    doc["status"] = "malformado"
    doc["defeito"] = defeito
    doc.update(patch)
    if defeito == "end_to_end_id_nao_texto":
        # Número em vez de texto: viola o $type do validador e continua único.
        doc["endToEndId"] = random.randint(10**12, 10**13)
    return doc


@router.post("/asp/inject-invalid")
async def asp_inject_invalid(quantidade: int = 1):
    """
    Injeta documentos que violam o schema esperado.

    Um doc só prova o mecanismo; mil provam a OPERAÇÃO — que o processor não cai,
    que a DLQ acumula com motivo, e que dá para reprocessar depois.
    """
    ok, detalhe, _ = await asyncio.to_thread(_asp_reachable)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Atlas Stream Processing não configurado ({detalhe}).")
    quantidade = max(1, min(quantidade, 5_000))
    docs = [_doc_invalido(i) for i in range(quantidade)]

    def _injeta() -> int:
        try:
            return len(sdb[COL_TX].insert_many(docs, ordered=False).inserted_ids)
        except BulkWriteError as erro:
            # Falha parcial é aceitável aqui: o que importa é o que chegou à DLQ.
            gravados = erro.details.get("nInserted", 0)
            logger.warning("Injeção parcial: %d de %d (%d erros)", gravados, quantidade,
                           len(erro.details.get("writeErrors", [])))
            return gravados

    gravados = await asyncio.to_thread(_injeta)
    return {
        "injetados": gravados,
        "solicitados": quantidade,
        "defeitos": sorted({d["defeito"] for d in docs}),
        "colecao_dlq": f"{STREAM_DB}.{COL_DLQ}",
    }


def _dlq_resumo() -> dict[str, Any]:
    """Agrupa a DLQ por motivo — é assim que se opera uma fila de rejeitados."""
    pipeline = [
        {"$group": {
            "_id": {"$ifNull": ["$errInfo.reason", "motivo não informado"]},
            "qtd": {"$sum": 1},
            "primeiro": {"$min": "$_stream_meta.source.ts"},
            "ultimo": {"$max": "$_stream_meta.source.ts"},
        }},
        {"$sort": {"qtd": -1}},
        {"$limit": 10},
    ]
    grupos = list(sdb[COL_DLQ].aggregate(pipeline))
    return {
        "total": sdb[COL_DLQ].count_documents({}),
        "por_motivo": [
            {"motivo": g["_id"], "qtd": g["qtd"],
             "primeiro": str(g.get("primeiro") or ""), "ultimo": str(g.get("ultimo") or "")}
            for g in grupos
        ],
    }


@router.get("/asp/dlq/resumo")
async def asp_dlq_resumo():
    return await asyncio.to_thread(_dlq_resumo)


def _reprocessa_dlq(limite: int) -> dict[str, Any]:
    """
    Reprocessa a DLQ: corrige o defeito conhecido e reinsere na coleção.

    É o que um job de reprocessamento faz na vida real — a DLQ não é um cemitério,
    é uma fila de retentativa. Cada documento volta com o MESMO endToEndId de
    origem (sem o prefixo INVALIDO-), então reprocessar duas vezes não duplica:
    o índice único barra. Idempotência por chave de negócio.
    """
    corrigidos = 0
    ja_existiam = 0
    falharam = 0
    sem_origem = 0
    remover: list[Any] = []

    for dlq_doc in sdb[COL_DLQ].find().limit(limite):
        # A DLQ do ASP guarda o evento de change stream inteiro em `doc`; o
        # documento original fica em doc.fullDocument.
        origem = (dlq_doc.get("doc") or {}).get("fullDocument") or dlq_doc.get("fullDocument") or {}
        if not origem:
            sem_origem += 1
            continue

        e2e = str(origem.get("endToEndId") or "")
        # Chave de negócio preservada: reprocessar duas vezes não duplica, o
        # índice único barra a segunda tentativa. Idempotência por endToEndId.
        limpo = {
            "endToEndId": f"E{e2e.rsplit('-', 1)[-1]}" if e2e.startswith("INVALIDO-")
                          else (e2e or f"E{uuid.uuid4().hex[:31].upper()}"),
            "run_id": origem.get("run_id") or "reprocessamento-dlq",
            "sequencia": origem.get("sequencia"),
            "pagadorId": origem.get("pagadorId") or "P000000",
            "recebedorId": origem.get("recebedorId") or "R000000",
            "particao": origem.get("particao", 0),
            # Correções determinísticas do defeito que mandou o doc para a DLQ:
            "valor": Decimal128(f"{_sorteia_valor('PIX'):.2f}") if not isinstance(
                origem.get("valor"), Decimal128) else origem["valor"],
            "tipo": origem["tipo"] if origem.get("tipo") in _TIPOS else "PIX",
            "uf": origem["uf"] if origem.get("uf") in _UFS else "SP",
            "ts": _now(),
            "status": "reprocessada",
        }
        resultado = "reprocessado"
        try:
            sdb[COL_TX].insert_one(limpo)
            corrigidos += 1
        except DuplicateKeyError:
            ja_existiam += 1          # índice único confirma que já foi reprocessado
            resultado = "ja_existia"
        except PyMongoError:
            # Falha transitória não é confirmação de duplicidade: preservar o
            # item na DLQ evita perda de evento e permite nova tentativa.
            falharam += 1
            logger.exception("Falha transitória ao reprocessar item da DLQ")
            continue
        try:
            sdb[COL_DLQ_AUDIT].replace_one(
                {"_id": dlq_doc["_id"]},
                {
                    "_id": dlq_doc["_id"],
                    "run_id": limpo["run_id"],
                    "endToEndId": limpo["endToEndId"],
                    "resultado": resultado,
                    "resolvido_em": _now(),
                },
                upsert=True,
            )
        except PyMongoError:
            # Sem trilha de auditoria não removemos a evidência original.
            falharam += 1
            logger.exception("Falha ao registrar auditoria do reprocessamento da DLQ")
            continue
        remover.append(dlq_doc["_id"])

    if remover:
        sdb[COL_DLQ].delete_many({"_id": {"$in": remover}})

    return {
        "reprocessados": corrigidos,
        "ja_existiam": ja_existiam,
        "falharam": falharam,
        "sem_documento_de_origem": sem_origem,
        "removidos_da_dlq": len(remover),
    }


@router.post("/asp/dlq/reprocessar")
async def asp_dlq_reprocessar(limite: int = 1_000):
    limite = max(1, min(limite, 5_000))
    return await asyncio.to_thread(_reprocessa_dlq, limite)


@router.get("/asp/dlq")
async def asp_dlq(limit: int = 20):
    docs = await asyncio.to_thread(
        lambda: list(sdb[COL_DLQ].find({}, {"_id": 0}).limit(max(1, min(limit, 100))))
    )
    return {"total": len(docs), "documentos": json.loads(json.dumps(docs, default=str))}


@router.get("/asp/janelas")
async def asp_janelas(limit: int = 30):
    """Últimas janelas fechadas, direto da coleção que o processor alimenta."""
    corte = _now() - timedelta(hours=2)
    docs = await asyncio.to_thread(
        lambda: list(
            sdb[COL_WINDOWS]
            .find({"window_end": {"$gte": corte}}, {"_id": 0})
            .sort("window_end", -1)
            .limit(max(1, min(limit, 200)))
        )
    )
    return {
        "colecao": f"{STREAM_DB}.{COL_WINDOWS}",
        "desde": corte.isoformat(),
        "total": len(docs),
        "janelas": json.loads(json.dumps(docs, default=str)),
    }


# Acima disto o digest do conjunto deixa de ser calculado: ele exige projetar
# um identificador por documento da execução, e a varredura passaria a competir
# com a própria demonstração. Contagem e valor continuam valendo em qualquer
# volume — é o digest que é opcional, e a tela diz quando ele não foi calculado.
MAX_DOCS_DIGEST = 200_000


def _fonte_conferivel(run_id: str) -> dict[str, Any]:
    """Contagem, soma em centavos e digest do conjunto, lidos da fonte no Atlas.

    Um `$group` só: contar e somar em passadas separadas custaria duas varreduras
    do mesmo índice de `run_id`.
    """
    linhas = list(sdb[COL_TX].aggregate([
        {"$match": {"run_id": run_id}},
        {"$group": {
            "_id": None,
            "documentos": {"$sum": 1},
            # `valor` é Decimal128 no caminho normal e string no evento inválido
            # injetado de propósito. O `$isNumber` mantém o inválido fora da soma
            # e contado à parte, que é exatamente como a DLQ o trata.
            "centavos": {"$sum": {"$cond": [
                {"$isNumber": "$valor"},
                {"$round": [{"$multiply": [{"$toDecimal": "$valor"}, 100]}, 0]},
                0,
            ]}},
            "nao_numericos": {"$sum": {"$cond": [{"$isNumber": "$valor"}, 0, 1]}},
        }},
    ]))
    linha = linhas[0] if linhas else {}
    documentos = int(linha.get("documentos") or 0)
    bruto = linha.get("centavos") or 0
    centavos = int(bruto.to_decimal() if isinstance(bruto, Decimal128) else bruto)

    digest: int | None = None
    if 0 < documentos <= MAX_DOCS_DIGEST:
        digest = 0
        for doc in sdb[COL_TX].find({"run_id": run_id}, {"_id": 0, "endToEndId": 1}):
            e2e = doc.get("endToEndId")
            if e2e:
                digest ^= _digest_de(e2e)

    return {
        "documentos": documentos,
        "centavos": centavos,
        "nao_numericos": int(linha.get("nao_numericos") or 0),
        "digest": digest,
    }


def _reconcile_run(run_id: str) -> dict[str, Any]:
    fonte = _fonte_conferivel(run_id)
    source = fonte["documentos"]
    asp_rows = list(sdb[COL_WINDOWS].aggregate([
        {"$match": {"run_id": run_id}},
        {"$group": {
            "_id": None,
            "processadas": {"$sum": "$qtd"},
            "alertas_valor_alto": {"$sum": "$alertas_valor_alto"},
            "volume": {"$sum": "$volume"},
            "janelas": {"$sum": 1},
        }},
    ]))
    asp_processed = int(asp_rows[0].get("processadas") or 0) if asp_rows else 0
    alertas = int(asp_rows[0].get("alertas_valor_alto") or 0) if asp_rows else 0
    asp_janelas = int(asp_rows[0].get("janelas") or 0) if asp_rows else 0
    asp_centavos = centavos_de(asp_rows[0].get("volume")) if asp_rows else 0
    dlq_aberta = sdb[COL_DLQ].count_documents({
        "$or": [
            {"doc.fullDocument.run_id": run_id},
            {"fullDocument.run_id": run_id},
        ]
    })
    dlq_resolvida = sdb[COL_DLQ_AUDIT].count_documents({"run_id": run_id})
    dlq = dlq_aberta + dlq_resolvida
    observed = run_tracker.snapshot(run_id)

    def channel(name: str) -> dict[str, Any]:
        data = observed.get(name, {
            "unicos": 0, "duplicados": 0, "completo_em_memoria": True,
            "centavos": 0, "nao_numericos": 0, "digest": 0,
        })
        contagem_ok = source > 0 and data["unicos"] == source
        # Valor e digest só têm significado quando a contagem já fechou: no meio
        # do fluxo eles divergem porque falta evento, não porque algo mudou.
        valor_ok = contagem_ok and data.get("centavos", 0) == fonte["centavos"]
        digest_ok = (
            contagem_ok
            and fonte["digest"] is not None
            and data.get("digest", 0) == fonte["digest"]
        )
        return {
            **data,
            "pendentes": max(source - data["unicos"], 0),
            "reconciliado": contagem_ok and valor_ok and (digest_ok or fonte["digest"] is None),
            "contagem_confere": contagem_ok,
            "valor_confere": valor_ok,
            "digest_confere": digest_ok if fonte["digest"] is not None else None,
            "valor": round(data.get("centavos", 0) / 100, 2),
        }

    asp_accounted = asp_processed + dlq
    # O ASP soma doubles e arredonda o volume em cada janela fechada: o erro é de
    # até meio centavo por janela, e comparar ao centavo exato acusaria
    # divergência onde só existe arredondamento. A tolerância é declarada, não
    # escondida — e cresce só com o número de janelas, nunca com o volume.
    dlq_centavos = 0
    for dlq_doc in sdb[COL_DLQ].find(
        {"$or": [{"doc.fullDocument.run_id": run_id}, {"fullDocument.run_id": run_id}]},
        {"_id": 0, "doc.fullDocument.valor": 1, "fullDocument.valor": 1},
    ):
        origem = dlq_doc.get("doc", {}).get("fullDocument") or dlq_doc.get("fullDocument") or {}
        dlq_centavos += centavos_de(origem.get("valor")) or 0
    tolerancia_centavos = max(1, asp_janelas)
    asp_valor_ok = (
        asp_accounted == source
        and source > 0
        and abs((asp_centavos + dlq_centavos) - fonte["centavos"]) <= tolerancia_centavos
    )
    return {
        "run_id": run_id,
        "gerador_ativo": generator.running and generator.run_id == run_id,
        "fonte": {
            "inseridas": source,
            "colecao": f"{STREAM_DB}.{COL_TX}",
            "valor": round(fonte["centavos"] / 100, 2),
            "centavos": fonte["centavos"],
            "nao_numericos": fonte["nao_numericos"],
            "digest": f"{fonte['digest']:016x}" if fonte["digest"] is not None else None,
        },
        "change_streams": channel("change_streams"),
        "kafka": channel("kafka"),
        "asp": {
            "agregadas": asp_processed,
            "dlq_aberta": dlq_aberta,
            "dlq_resolvida": dlq_resolvida,
            "dlq_total": dlq,
            "contabilizadas": asp_accounted,
            "pendentes": max(source - asp_accounted, 0),
            "reconciliado": source > 0 and asp_accounted == source and asp_valor_ok,
            "contagem_confere": source > 0 and asp_accounted == source,
            "valor_confere": asp_valor_ok,
            "digest_confere": None,   # o ASP entrega agregado; não há conjunto de ids para comparar
            "alertas_valor_alto": alertas,
            "valor": round((asp_centavos + dlq_centavos) / 100, 2),
            "janelas": asp_janelas,
            "tolerancia_valor": round(tolerancia_centavos / 100, 2),
        },
        "conferencia": {
            "valor_fonte": round(fonte["centavos"] / 100, 2),
            "digest_fonte": f"{fonte['digest']:016x}" if fonte["digest"] is not None else None,
            "digest_calculado": fonte["digest"] is not None,
            "limite_digest": MAX_DOCS_DIGEST,
            "nota": (
                "Contagem prova que nada faltou. A soma em centavos prova que nada foi transformado "
                "no caminho. O digest XOR dos endToEndId prova que os três caminhos viram o mesmo "
                "conjunto, e não apenas a mesma quantidade. O ASP entrega agregado por janela: ele "
                f"confere por valor dentro de ±R$ {tolerancia_centavos / 100:.2f} "
                f"({asp_janelas} janela(s) arredondadas a 2 casas), sem digest."
            ),
        },
        "final": (
            "reconciliado"
            if source > 0
            and not generator.running
            and channel("change_streams")["reconciliado"]
            and channel("kafka")["reconciliado"]
            and asp_accounted == source
            and asp_valor_ok
            else "em_processamento"
        ),
        "escopo": (
            "Contadores de Change Streams e Kafka valem desde o início deste processo da API; "
            "fonte, ASP e DLQ são consultados no Atlas."
        ),
    }


@router.get("/reconciliacao")
async def reconciliacao(run_id: str | None = None):
    alvo = run_id or generator.run_id
    if not alvo:
        ultimo = await asyncio.to_thread(
            lambda: sdb[COL_TX].find_one(
                {"run_id": {"$type": "string"}},
                {"run_id": 1},
                sort=[("ts", -1)],
            )
        )
        alvo = ultimo.get("run_id") if ultimo else None
    if not alvo:
        return {"estado": "sem_execucao", "mensagem": "Inicie o gerador para criar uma execução reconciliável."}
    return await asyncio.to_thread(_reconcile_run, alvo)
