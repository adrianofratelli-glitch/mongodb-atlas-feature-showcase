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
from typing import Any

from bson import Decimal128
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
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
# Só para exibição: o que está provisionado nesta PoV.
CLUSTER_TIER = os.getenv("CLUSTER_TIER", "M20").strip() or "M20"
# Cargas deliberadamente moderadas: a PoV prova conceitos sem induzir sizing
# nem exigir tiers maiores só para produzir um número de palco.
#
# O teto existe para manter a demo reproduzível em M20 sem disparar o
# auto-scaling do Atlas (que sobe por CPU/memória sustentadas e vira custo).
# `scripts/ambiente.sh` fecha o mesmo contrato do outro lado, limitando
# `autoScaling.compute.maxInstanceSize`. Subir daqui sem subir o tier lá só
# transfere o problema para a fatura.
TPS_MAX = 1_000
CONCEPT_TPS = min(TPS_MAX, max(10, int(os.getenv("STREAMING_CONCEPT_TPS", "200"))))

# TTL = rede de segurança, não a limpeza principal (essa é o botão Reset, que
# dropa a coleção na hora).
#
# Em regime o deletor do TTL remove na mesma taxa em que se insere, qualquer que
# seja a janela; o que a janela decide é o TAMANHO do conjunto vivo. A 1800 s a
# coleção estabilizava perto de um milhão de documentos — dados e índices
# maiores que o cache do WiredTiger de um M20, o que sozinho sustentava a
# pressão de memória que dispara o auto-scaling.
#
# A 600 s e no TPS moderado atual o conjunto vivo fica na casa das dezenas de
# milhares: cabe no cache, e a taxa de deleção é baixa demais para concorrer com
# o pico ou para inflar o oplog de que o resume token depende. A janela continua
# maior que a rajada de uma demo.
TTL_SECONDS = int(os.getenv("STREAMING_TTL_SEGUNDOS", "600"))
PURGE_TIMEOUT_MS = 180_000
# Partições do consumo do change stream (um cursor + uma thread por partição).
CS_PARTICOES = max(1, int(os.getenv("STREAMING_CS_PARTICOES", "1")))
# Amostragem do feed SSE (contadores e percentis seguem cobrindo 100%).
FEED_INTERVALO_S = 0.12
METRICAS_INTERVALO_S = 0.5

sdb = client[STREAM_DB]

# Distribuição sintética de UFs para produzir grupos diferentes nas janelas.
_UFS = ["SP", "RJ", "MG", "RS", "PR", "BA", "PE", "SC", "GO", "CE"]
_UF_PESOS = [30, 14, 11, 8, 8, 7, 6, 6, 5, 5]

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


class RunTracker:
    """Contagem idempotente, por execução, dos caminhos observados pela PoV."""

    MAX_IDS_PER_CHANNEL = 500_000

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: dict[str, dict[str, set[str]]] = {}
        self._duplicates: dict[str, dict[str, int]] = {}
        self._truncated: set[tuple[str, str]] = set()

    def record(self, channel: str, run_id: str | None, end_to_end_id: str | None) -> None:
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

    def snapshot(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            channels = self._ids.get(run_id, {})
            duplicates = self._duplicates.get(run_id, {})
            return {
                channel: {
                    "unicos": len(ids),
                    "duplicados": duplicates.get(channel, 0),
                    "completo_em_memoria": (run_id, channel) not in self._truncated,
                }
                for channel, ids in channels.items()
            }

    def reset(self) -> None:
        with self._lock:
            self._ids.clear()
            self._duplicates.clear()
            self._truncated.clear()


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


def _new_transacao(run_id: str = "execucao-local", sequencia: int | None = None) -> dict[str, Any]:
    pagador = random.randint(1, 5000)
    tipo = random.choices(_TIPOS, weights=_TIPO_PESOS)[0]
    return {
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
        self._sequence = 0
        self._recent: list[tuple[float, int]] = []   # (monotonic, docs) p/ TPS medido
        self._start_mono: float | None = None
        self._lock = threading.Lock()                # _recent/_inserted são tocados pelas threads de insert
        self._inflight: set[asyncio.Task] = set()

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def _record(self, docs: int) -> None:
        with self._lock:
            self.inserted += docs
            self._recent.append((time.monotonic(), docs))

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

    async def start(self, tps: int) -> None:
        await asyncio.to_thread(_ensure_indexes)
        self.tps_alvo = tps
        if self.running:
            return                                    # já rodando: só ajusta o TPS
        self.started_at = _now()
        self.run_id = f"pix-{self.started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        self._sequence = 0
        self._recent = []
        self._start_mono = time.monotonic()
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
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

    def reset_counters(self) -> None:
        with self._lock:
            self.inserted = 0
            self._recent = []
            self._start_mono = time.monotonic()
        self.run_id = None
        self.started_at = None
        self._sequence = 0

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
        sdb[COL_TX].insert_many(docs, ordered=False)
        self._record(len(docs))

    async def _batch(self, docs: list[dict[str, Any]]) -> None:
        try:
            await asyncio.to_thread(self._insert_batch, docs)
        except PyMongoError:
            logger.exception("Falha ao inserir micro-batch do gerador")

    async def _run(self) -> None:
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
    tps: int = Field(default=CONCEPT_TPS, ge=1, le=TPS_MAX)


@router.post("/generator/start")
async def generator_start(body: GeneratorStart):
    await generator.start(body.tps)
    return {
        "running": True,
        "run_id": generator.run_id,
        "tps_alvo": generator.tps_alvo,
        "colecao": f"{STREAM_DB}.{COL_TX}",
    }


@router.post("/generator/stop")
async def generator_stop():
    await generator.stop()
    return {"running": False, "inseridos": generator.inserted}


@router.get("/generator/status")
async def generator_status():
    total = await asyncio.to_thread(sdb[COL_TX].estimated_document_count)
    medido = generator.measured_tps()
    return {
        "running": generator.running,
        "tps_alvo": generator.tps_alvo,
        "tps_medido": medido,
        "inseridos": generator.inserted,
        "docs_na_colecao": total,
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "ttl_segundos": TTL_SECONDS,
        "ttl_ativo": True,
        "started_at": generator.started_at.isoformat() if generator.started_at else None,
        "run_id": generator.run_id,
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
    if auto.get("ativo") and info.get("escalou"):
        checks["cluster_tier"] = {
            "ok": False,
            "message": f"cluster em {info['tier']}, acima do tier de entrada {auto.get('min')} "
                       f"(auto-scaling {auto.get('min')}→{auto.get('max')}); "
                       "a PoV é calibrada para rodar no tier de entrada",
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
    """Cargas moderadas para provar capacidades sem sugerir sizing."""
    return {
        "premissas": {
            "workload": "sintético",
            "objetivo": "provar integridade, recuperação, fan-out, janela, estado e DLQ",
            "sizing": False,
        },
        "presets": [
            {"label": "Passo a passo", "tps": max(10, CONCEPT_TPS // 5),
             "detalhe": "fluxo leve para acompanhar cada mecanismo"},
            {"label": "Fluxo contínuo", "tps": CONCEPT_TPS,
             "detalhe": "carga moderada para observar janelas e reconciliação"},
            {"label": "Rajada controlada", "tps": min(TPS_MAX, CONCEPT_TPS * 2),
             "detalhe": "demonstra backlog e recuperação sem pretensão de benchmark"},
        ],
        "tps_max": TPS_MAX,
        "ambiente": {
            "cluster": (await asyncio.to_thread(_cluster_info_sync))["tier"],
            "particoes_consumo": CS_PARTICOES,
            "nota": "Os números mostram apenas esta execução. Não são capacidade do produto nem sizing.",
        },
    }


DROP_ACIMA_DE = 300_000


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


def _purge(col: str) -> tuple[int, int]:
    """
    Esvazia a coleção usando um cliente dedicado com socket timeout longo.

    Alguns minutos a 5000 TPS deixam centenas de milhares de documentos, e um
    delete_many nesse volume estoura o socketTimeoutMS padrão (16 s) — o Reset
    morria com 500 no meio da demo. Dropar a coleção seria instantâneo, mas
    invalidaria o change stream do processor do ASP e o do próprio backend.
    """
    from pymongo import MongoClient

    purge_client = MongoClient(
        settings.mongo_uri or "mongodb://127.0.0.1:27017",
        appname="mongodb-atlas-feature-showcase-purge",
        serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        socketTimeoutMS=PURGE_TIMEOUT_MS,
    )
    target = purge_client[STREAM_DB][col]
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
        return removed, target.estimated_document_count()
    except PyMongoError:
        logger.exception("Falha ao esvaziar %s", col)
        return removed, sdb[col].estimated_document_count()
    finally:
        purge_client.close()


@router.post("/reset")
async def reset(finalizar: bool = False):
    await generator.stop()
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
        alvos = (COL_WINDOWS, COL_DLQ, COL_DLQ_AUDIT)
    else:
        alvos = (COL_TX, COL_WINDOWS, COL_DLQ, COL_DLQ_AUDIT)

    if finalizar:
        alvos = (*alvos, COL_CHECKPOINTS)

    for col in alvos:
        removed, left = await asyncio.to_thread(_purge, col)
        deleted[col] = removed
        restantes += left
    # Religa os connectors SEMPRE: um drop anterior invalida o change stream
    # deles e as tasks não se recuperam sozinhas — sem isto a coluna 2 fica
    # vermelha depois do Reset. É idempotente e barato.
    if not finalizar:
        try:
            kafka_reiniciado = (await asyncio.to_thread(_connector_restart_sync)).get("reiniciado", False)
        except Exception:  # noqa: BLE001 - Kafka é opcional na demo
            kafka_reiniciado = False

    generator.reset_counters()
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
    # `restantes` só é > 0 se o orçamento de tempo acabou antes de esvaziar tudo;
    # a UI mostra o número em vez de fingir que a limpeza terminou.
    return {"reset": True, "removidos": deleted, "restantes": restantes,
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
        run_tracker.record("change_streams", doc.get("run_id"), e2e)
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
                run_tracker.record("kafka", doc.get("run_id"), doc.get("endToEndId"))
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
  { $source: { connectionName: __CONNECTION__, db: __DB__, coll: "transacoes" } },
  { $match: { operationType: "insert" } },

  // documento malformado vai para a DLQ; o processor nao cai
  { $validate: { validator: { $and: [
        { "fullDocument.endToEndId": { $type: "string" } },
        { "fullDocument.run_id": { $type: "string" } },
        { "fullDocument.valor": { $type: ["decimal","double","int","long"] } },
        { "fullDocument.tipo":  { $eq: "PIX" } },
        { "fullDocument.uf": { $type: "string" } } ] },
      validationAction: "dlq" } },

  { $tumblingWindow: {
      boundary: "eventTime",
      interval: { size: 10, unit: "second" },
      allowedLateness: { size: 3, unit: "second" },
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


def _reconcile_run(run_id: str) -> dict[str, Any]:
    source = sdb[COL_TX].count_documents({"run_id": run_id})
    asp_rows = list(sdb[COL_WINDOWS].aggregate([
        {"$match": {"run_id": run_id}},
        {"$group": {
            "_id": None,
            "processadas": {"$sum": "$qtd"},
            "alertas_valor_alto": {"$sum": "$alertas_valor_alto"},
        }},
    ]))
    asp_processed = int(asp_rows[0].get("processadas") or 0) if asp_rows else 0
    alertas = int(asp_rows[0].get("alertas_valor_alto") or 0) if asp_rows else 0
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
        data = observed.get(name, {"unicos": 0, "duplicados": 0, "completo_em_memoria": True})
        return {
            **data,
            "pendentes": max(source - data["unicos"], 0),
            "reconciliado": source > 0 and data["unicos"] == source,
        }

    asp_accounted = asp_processed + dlq
    return {
        "run_id": run_id,
        "gerador_ativo": generator.running and generator.run_id == run_id,
        "fonte": {"inseridas": source, "colecao": f"{STREAM_DB}.{COL_TX}"},
        "change_streams": channel("change_streams"),
        "kafka": channel("kafka"),
        "asp": {
            "agregadas": asp_processed,
            "dlq_aberta": dlq_aberta,
            "dlq_resolvida": dlq_resolvida,
            "dlq_total": dlq,
            "contabilizadas": asp_accounted,
            "pendentes": max(source - asp_accounted, 0),
            "reconciliado": source > 0 and asp_accounted == source,
            "alertas_valor_alto": alertas,
        },
        "final": (
            "reconciliado"
            if source > 0
            and not generator.running
            and channel("change_streams")["reconciliado"]
            and channel("kafka")["reconciliado"]
            and asp_accounted == source
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
