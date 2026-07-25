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

REGRA: nada é mockado. Toda métrica exibida vem de uma operação real. Quando um
componente não está configurado (Kafka fora do ar, ASP_ENABLED=false), o
endpoint responde estado "nao_configurado" e a UI mostra as instruções de setup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
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
from pymongo.errors import PyMongoError

from database import client
from settings import settings

router = APIRouter(prefix="/streaming", tags=["Streaming"])
logger = logging.getLogger("showcase.streaming")

STREAM_DB = os.getenv("STREAMING_DB", "pix").strip() or "pix"
COL_TX = "transacoes"
COL_WINDOWS = "metricas_janela"
COL_DLQ = "dlq"
TOPIC = f"atlas.{STREAM_DB}.{COL_TX}"
CONNECTOR_NAME = os.getenv("CONNECT_CONNECTOR_NAME", "atlas-pix-source").strip() or "atlas-pix-source"
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083").strip()
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092").strip()
ASP_ENABLED = os.getenv("ASP_ENABLED", "false").strip().lower() in {"1", "true", "yes"}
ASP_CONNECTION_STRING = os.getenv("ASP_CONNECTION_STRING", "").strip()

TTL_SECONDS = 2 * 60 * 60  # a coleção não pode crescer entre demos
PURGE_TIMEOUT_MS = 180_000
# Amostragem do feed SSE (contadores e percentis seguem cobrindo 100%).
FEED_INTERVALO_S = 0.12
METRICAS_INTERVALO_S = 0.5

sdb = client[STREAM_DB]

# ---------------------------------------------------------------------------
# Cenário PIX — PREMISSAS declaradas, não medições.
#
# Tudo aqui é rotulado como premissa na UI e serve só para dar escala aos
# números MEDIDOS (o TPS que a demo realmente sustenta é medido contra o
# Atlas). Nenhum destes valores é apresentado como resultado da demo.
# ---------------------------------------------------------------------------
PIX_BRASIL_TX_DIA = 300_000_000          # ordem de grandeza divulgada pelo BCB
INTER_SHARE = 0.10                       # premissa do cenário: 10% do mercado
SEGUNDOS_DIA = 86_400
INTER_TX_DIA = int(PIX_BRASIL_TX_DIA * INTER_SHARE)
INTER_TPS_MEDIO = round(INTER_TX_DIA / SEGUNDOS_DIA)          # ~347 TPS
PICO_FATOR = 3                                                 # premissa: pico ≈ 3× a média
INTER_TPS_PICO = INTER_TPS_MEDIO * PICO_FATOR                  # ~1041 TPS
BRASIL_TPS_MEDIO = round(PIX_BRASIL_TX_DIA / SEGUNDOS_DIA)     # ~3472 TPS

_UFS = ["SP", "RJ", "MG", "RS", "PR", "BA", "PE", "SC", "GO", "CE"]
_TIPOS = ["PIX", "TED", "BOLETO"]


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
meter_kafka = Meter()
meter_asp = Meter()


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
def _ensure_indexes() -> None:
    sdb[COL_TX].create_index("endToEndId", unique=True, name="endToEndId_unique")
    sdb[COL_TX].create_index("ts", expireAfterSeconds=TTL_SECONDS, name="ts_ttl_2h")


def _new_transacao() -> dict[str, Any]:
    return {
        "endToEndId": f"E{uuid.uuid4().hex[:31].upper()}",
        "pagadorId": f"P{random.randint(1, 5000):06d}",
        "recebedorId": f"R{random.randint(1, 800):06d}",
        "valor": Decimal128(f"{random.uniform(5.0, 9500.0):.2f}"),
        "tipo": random.choice(_TIPOS),
        "uf": random.choice(_UFS),
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
        self._recent: list[tuple[float, int]] = []   # (monotonic, docs) p/ TPS medido
        self._lock = threading.Lock()                # _recent/_inserted são tocados pelas threads de insert

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def _record(self, docs: int) -> None:
        with self._lock:
            self.inserted += docs
            self._recent.append((time.monotonic(), docs))

    def measured_tps(self) -> float:
        cutoff = time.monotonic() - 5.0
        with self._lock:
            self._recent = [(t, n) for t, n in self._recent if t >= cutoff]
            recent = list(self._recent)
        if len(recent) < 2:
            return 0.0
        span = recent[-1][0] - recent[0][0]
        if span <= 0:
            return 0.0
        return round(sum(n for _, n in recent[1:]) / span, 1)

    async def start(self, tps: int) -> None:
        await asyncio.to_thread(_ensure_indexes)
        self.tps_alvo = tps
        if self.running:
            return                                    # já rodando: só ajusta o TPS
        self.started_at = _now()
        self._recent = [(time.monotonic(), 0)]
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task, self.task = self.task, None
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.tps_alvo = 0

    def reset_counters(self) -> None:
        with self._lock:
            self.inserted = 0
            self._recent = [(time.monotonic(), 0)]

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
        inflight: set[asyncio.Task] = set()
        next_tick = time.monotonic()
        while True:
            carry += self.tps_alvo * self.TICK_S
            batch_size = int(carry)
            carry -= batch_size
            if batch_size > 0:
                if len(inflight) >= self.MAX_INFLIGHT:
                    carry += batch_size          # Atlas não acompanha: devolve ao próximo tick
                else:
                    task = asyncio.create_task(self._batch([_new_transacao() for _ in range(batch_size)]))
                    inflight.add(task)
                    task.add_done_callback(inflight.discard)
            next_tick += self.TICK_S
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))


generator = Generator()


class GeneratorStart(BaseModel):
    tps: int = Field(default=350, ge=1, le=5000)


@router.post("/generator/start")
async def generator_start(body: GeneratorStart):
    await generator.start(body.tps)
    return {"running": True, "tps_alvo": generator.tps_alvo, "colecao": f"{STREAM_DB}.{COL_TX}"}


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
        "started_at": generator.started_at.isoformat() if generator.started_at else None,
        # Escala derivada do TPS MEDIDO — projeção aritmética, não uma medição
        # de 24 h. A UI rotula como projeção.
        "projecao_dia": int(medido * SEGUNDOS_DIA),
        "pct_dia_inter": round(medido * SEGUNDOS_DIA / INTER_TX_DIA * 100, 1) if INTER_TX_DIA else None,
        "pct_dia_brasil": round(medido * SEGUNDOS_DIA / PIX_BRASIL_TX_DIA * 100, 1) if PIX_BRASIL_TX_DIA else None,
    }


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


@router.get("/cenario")
async def cenario():
    """
    Premissas do cenário PIX usadas como RÉGUA para os números medidos.

    Devolve explicitamente o que é premissa e o que é derivado dela, para a UI
    poder rotular. Nada aqui é resultado de medição — o que a demo mede é o TPS
    sustentado contra o Atlas, exposto em /streaming/generator/status.
    """
    return {
        "premissas": {
            "pix_brasil_tx_dia": PIX_BRASIL_TX_DIA,
            "inter_share": INTER_SHARE,
            "pico_fator": PICO_FATOR,
            "fonte": "volume diário do PIX na ordem de grandeza divulgada pelo BCB; "
                     "participação do Inter e fator de pico são premissas deste cenário",
        },
        "derivados": {
            "inter_tx_dia": INTER_TX_DIA,
            "inter_tps_medio": INTER_TPS_MEDIO,
            "inter_tps_pico": INTER_TPS_PICO,
            "brasil_tps_medio": BRASIL_TPS_MEDIO,
        },
        "presets": [
            {"label": "Média Inter", "tps": INTER_TPS_MEDIO,
             "detalhe": f"{INTER_TX_DIA:,} transações/dia ÷ 86.400 s".replace(",", ".")},
            {"label": "Pico Inter", "tps": INTER_TPS_PICO,
             "detalhe": f"{PICO_FATOR}× a média (premissa de pico intradiário)"},
            {"label": "PIX Brasil inteiro", "tps": BRASIL_TPS_MEDIO,
             "detalhe": f"{PIX_BRASIL_TX_DIA:,} transações/dia ÷ 86.400 s".replace(",", ".")},
        ],
    }


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
async def reset():
    await generator.stop()
    # O worker do change stream NÃO é parado aqui: ele só volta a subir quando um
    # novo assinante SSE chega, e a aba já aberta na demo não reabre o
    # EventSource — a coluna 1 ficaria muda depois do Reset até dar F5.
    deleted: dict[str, Any] = {}
    restantes = 0
    for col in (COL_TX, COL_WINDOWS, COL_DLQ):
        removed, left = await asyncio.to_thread(_purge, col)
        deleted[col] = removed
        restantes += left
    generator.reset_counters()
    cs_worker.reset_counters()
    kafka_consumer.reset_counters()
    for hub in (hub_cs, hub_kafka, hub_asp):
        hub.publish({"type": "reset"})
    # `restantes` só é > 0 se o orçamento de tempo acabou antes de esvaziar tudo;
    # a UI mostra o número em vez de fingir que a limpeza terminou.
    return {"reset": True, "removidos": deleted, "restantes": restantes}


# ---------------------------------------------------------------------------
# COLUNA 1 — Change Streams
# ---------------------------------------------------------------------------
class ChangeStreamWorker:
    """
    Cursor único de collection.watch() rodando numa thread; os eventos são
    publicados no hub para todos os assinantes SSE.

    O botão "Derrubar e retomar" pede o drop: o cursor é fechado, esperamos 3s
    com o gerador ainda escrevendo e reabrimos com resume_after(resumeToken).
    Os eventos cujo ts é anterior à reabertura são marcados como recuperados —
    é a prova visual de que nada se perdeu.
    """

    PIPELINE = [{"$match": {"operationType": "insert"}}]

    def __init__(self) -> None:
        self.active = False
        self.thread = None
        self.token: dict | None = None
        self.events = 0
        self.recovered = 0
        self.reopen_at: datetime | None = None
        self._drop_requested = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ultimo_feed = 0.0
        self._ultima_metrica = 0.0

    # A 3000+ eventos/s não dá para empurrar cada evento para o browser — a aba
    # morre. O feed vira uma AMOSTRA (rotulada como tal na UI); os contadores e
    # os percentis continuam cobrindo 100% dos eventos, medidos no worker.
    def reset_counters(self) -> None:
        # O resume token é preservado de propósito: o cursor continua aberto
        # durante o Reset, e zerar o token aqui faria a próxima reabertura
        # perder a continuidade que o botão "Derrubar e retomar" demonstra.
        self.events = 0
        self.recovered = 0
        self.reopen_at = None
        meter_cs.reset()

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.active:
            return
        self._loop = loop
        self.active = True
        self.thread = threading.Thread(target=self._run, daemon=True, name="streaming-cs")
        self.thread.start()

    def stop(self) -> None:
        self.active = False

    def request_drop(self) -> None:
        if not self.active:
            raise HTTPException(status_code=409, detail="Change stream não está aberto.")
        self._drop_requested = True

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
                kwargs: dict[str, Any] = {
                    "full_document": "updateLookup",
                    "max_await_time_ms": 500,
                    "batch_size": 1_000,
                }
                if self.token:
                    kwargs["resume_after"] = self.token
                with sdb[COL_TX].watch(self.PIPELINE, **kwargs) as stream:
                    self._publish({
                        "type": "aberto",
                        "retomado": bool(self.token),
                        "token": self.token_str(),
                        "eventos": self.events,
                    })
                    for change in stream:
                        self.token = change["_id"]
                        self._emit(change)
                        if not self.active or self._drop_requested:
                            break
            except PyMongoError as exc:
                logger.exception("Change stream do módulo Streaming falhou")
                self._publish({"type": "erro", "detalhe": f"{type(exc).__name__}: {exc}"})
                time.sleep(2)
                continue

            if self._drop_requested:
                self._drop_requested = False
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
                "eventos": self.events,
                "recuperados": self.recovered,
                "token": self.token_str(),
                **meter_cs.snapshot(),
            })


cs_worker = ChangeStreamWorker()


@router.get("/changestream")
async def changestream_sse(request: Request):
    cs_worker.ensure_started(asyncio.get_running_loop())
    hello = {
        "type": "hello",
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "eventos": cs_worker.events,
        "recuperados": cs_worker.recovered,
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
        "token": cs_worker.token_str(),
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
            group_id=f"showcase-streaming-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
            enable_auto_commit=False,
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
                meter_kafka.record(latency_ms)      # percentis sobre 100% das mensagens

                agora = time.monotonic()
                if agora - self._ultimo_feed >= FEED_INTERVALO_S:
                    self._ultimo_feed = agora
                    hub_kafka.publish({
                        "type": "mensagem",
                        "endToEndId": doc.get("endToEndId"),
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


def _connector_status_sync() -> dict[str, Any]:
    import requests

    url = f"{CONNECT_URL.rstrip('/')}/connectors/{CONNECTOR_NAME}/status"
    resp = requests.get(url, timeout=2)
    if resp.status_code == 404:
        return {"estado": "nao_configurado", "detalhe": f"connector {CONNECTOR_NAME} não existe em {CONNECT_URL}"}
    resp.raise_for_status()
    body = resp.json()
    tasks = body.get("tasks", [])
    return {
        "estado": body.get("connector", {}).get("state", "DESCONHECIDO"),
        "detalhe": f"{len(tasks)} task(s); {CONNECT_URL}",
        "tasks": [{"id": t.get("id"), "state": t.get("state"), "trace": (t.get("trace") or "")[:200]} for t in tasks],
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
        "consumidor": {
            "estado": kafka_consumer.estado,
            "detalhe": kafka_consumer.detalhe,
            "mensagens": kafka_consumer.messages,
            "offset_atual": kafka_consumer.last_offset,
        },
        "topico": TOPIC,
        "brokers": KAFKA_BROKERS,
        "connect_url": CONNECT_URL,
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
  { $source: { connectionName: "atlasCluster", db: "pix", coll: "transacoes",
               config: { fullDocument: "required" } } },
  { $match: { operationType: "insert" } },

  // documento malformado vai para a DLQ; o processor nao cai
  { $validate: { validator: { $and: [
        { "fullDocument.valor": { $type: ["decimal","double","int","long"] } },
        { "fullDocument.tipo":  { $in: ["PIX","TED","BOLETO"] } } ] },
      validationAction: "dlq" } },

  { $tumblingWindow: {
      interval: { size: 10, unit: "second" },
      pipeline: [
        { $group: {
            _id: { uf: "$fullDocument.uf", tipo: "$fullDocument.tipo" },
            qtd:    { $count: {} },
            volume: { $sum: { $toDouble: "$fullDocument.valor" } },
            ticket: { $avg: { $toDouble: "$fullDocument.valor" } },
            window_start: { $min: "$fullDocument.ts" },
            window_end:   { $max: "$fullDocument.ts" } } },
        { $set: { uf: "$_id.uf", tipo: "$_id.tipo",
                  volume: { $round: ["$volume", 2] },
                  ticket: { $round: ["$ticket", 2] } } } ] } },

  // _id deterministico por (janela, uf, tipo) => $merge idempotente
  { $set: { _id: { $concat: [ { $toString: "$window_start" }, "|", "$uf", "|", "$tipo" ] } } },
  { $merge: { into: { connectionName: "atlasCluster", db: "pix", coll: "metricas_janela" },
              whenMatched: "replace", whenNotMatched: "insert" } }
]"""


def _asp_reachable() -> tuple[bool, str]:
    if not ASP_ENABLED:
        return False, "ASP_ENABLED=false"
    if not ASP_CONNECTION_STRING:
        return False, "ASP_CONNECTION_STRING ausente"
    from pymongo import MongoClient

    try:
        spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=5000, connect=False)
        try:
            # Estado REAL do processor, direto da SPI — não basta a conexão abrir.
            resposta = spi.admin.command({"listStreamProcessors": 1})
            processors = resposta.get("streamProcessors", [])
            ativos = [p for p in processors if p.get("state") == "STARTED"]
            if not processors:
                return False, "SPI acessível, mas nenhum processor criado (rode scripts/setup-asp.js)"
            if not ativos:
                estados = ", ".join(f"{p.get('name')}={p.get('state')}" for p in processors)
                return False, f"processor parado: {estados}"
            nomes = ", ".join(p.get("name", "?") for p in ativos)
            return True, f"processor STARTED: {nomes}"
        finally:
            spi.close()
    except Exception as exc:  # noqa: BLE001 - SPI ausente é estado esperado da demo
        return False, f"SPI inacessível: {type(exc).__name__}"


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
    ok, detalhe = await asyncio.to_thread(_asp_reachable)
    janelas = await asyncio.to_thread(sdb[COL_WINDOWS].count_documents, {})
    dlq = await asyncio.to_thread(sdb[COL_DLQ].count_documents, {})
    totais = await asyncio.to_thread(_asp_totais)
    return {
        "estado": "configurado" if ok else "nao_configurado",
        "detalhe": detalhe,
        **totais,
        "asp_enabled": ASP_ENABLED,
        "colecao_janelas": f"{STREAM_DB}.{COL_WINDOWS}",
        "colecao_dlq": f"{STREAM_DB}.{COL_DLQ}",
        "janelas": janelas,
        "dlq": dlq,
        "pipeline": ASP_PIPELINE_SNIPPET,
    }


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
                                "uf": doc.get("uf") or key.get("uf"),
                                "tipo": doc.get("tipo") or key.get("tipo"),
                                "qtd": doc.get("qtd"),
                                "volume": doc.get("volume"),
                                "ticket": doc.get("ticket"),
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
        lambda: list(sdb[COL_WINDOWS].find({}, {"_id": 0}).limit(20))
    )
    hello = {
        "type": "hello",
        "colecao": f"{STREAM_DB}.{COL_WINDOWS}",
        "janelas_existentes": len(ultimas),
        "asp_enabled": ASP_ENABLED,
    }
    return _sse_response(request, hub_asp, hello)


@router.post("/asp/inject-invalid")
async def asp_inject_invalid():
    """Insere um documento que viola o schema esperado — o validador do processor
    deve mandá-lo para a DLQ em vez de derrubar o processor."""
    ok, detalhe = await asyncio.to_thread(_asp_reachable)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Atlas Stream Processing não configurado ({detalhe}).")
    doc = {
        "endToEndId": f"INVALIDO-{uuid.uuid4().hex[:12].upper()}",
        "pagadorId": None,
        "recebedorId": None,
        "valor": "isto-nao-e-um-numero",     # viola o tipo esperado
        "tipo": "CRIPTO",                    # fora do enum PIX|TED|BOLETO
        "uf": "ZZ",                          # UF inexistente
        "ts": _now(),
        "status": "malformado",
    }
    await asyncio.to_thread(sdb[COL_TX].insert_one, doc)
    return {"injetado": True, "endToEndId": doc["endToEndId"], "colecao_dlq": f"{STREAM_DB}.{COL_DLQ}"}


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
        lambda: list(sdb[COL_WINDOWS].find({}, {"_id": 0}).limit(max(1, min(limit, 200))))
    )
    return {
        "colecao": f"{STREAM_DB}.{COL_WINDOWS}",
        "desde": corte.isoformat(),
        "total": len(docs),
        "janelas": json.loads(json.dumps(docs, default=str)),
    }
