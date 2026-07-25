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
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from bson import Decimal128
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError

from database import client

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

sdb = client[STREAM_DB]

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
    """Task asyncio que insere em micro-batches; o TPS exibido é o MEDIDO."""

    TICK_S = 0.1

    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.tps_alvo = 0
        self.inserted = 0
        self.started_at: datetime | None = None
        self._recent: list[tuple[float, int]] = []   # (monotonic, docs) p/ TPS medido

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def measured_tps(self) -> float:
        cutoff = time.monotonic() - 5.0
        self._recent = [(t, n) for t, n in self._recent if t >= cutoff]
        if len(self._recent) < 2:
            return 0.0
        span = self._recent[-1][0] - self._recent[0][0]
        if span <= 0:
            return 0.0
        return round(sum(n for _, n in self._recent[1:]) / span, 1)

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
        self.inserted = 0
        self._recent = [(time.monotonic(), 0)]

    async def _run(self) -> None:
        carry = 0.0
        next_tick = time.monotonic()
        while True:
            carry += self.tps_alvo * self.TICK_S
            batch_size = int(carry)
            carry -= batch_size
            if batch_size > 0:
                docs = [_new_transacao() for _ in range(batch_size)]
                try:
                    await asyncio.to_thread(sdb[COL_TX].insert_many, docs, ordered=False)
                    self.inserted += batch_size
                    self._recent.append((time.monotonic(), batch_size))
                except PyMongoError:
                    logger.exception("Falha ao inserir micro-batch do gerador")
            next_tick += self.TICK_S
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))


generator = Generator()


class GeneratorStart(BaseModel):
    tps: int = Field(default=50, ge=1, le=200)


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
    return {
        "running": generator.running,
        "tps_alvo": generator.tps_alvo,
        "tps_medido": generator.measured_tps(),
        "inseridos": generator.inserted,
        "docs_na_colecao": total,
        "colecao": f"{STREAM_DB}.{COL_TX}",
        "ttl_segundos": TTL_SECONDS,
        "started_at": generator.started_at.isoformat() if generator.started_at else None,
    }


@router.post("/reset")
async def reset():
    await generator.stop()
    cs_worker.stop()
    deleted = {}
    for col in (COL_TX, COL_WINDOWS, COL_DLQ):
        res = await asyncio.to_thread(sdb[col].delete_many, {})
        deleted[col] = res.deleted_count
    generator.reset_counters()
    cs_worker.reset_counters()
    kafka_consumer.reset_counters()
    for hub in (hub_cs, hub_kafka, hub_asp):
        hub.publish({"type": "reset"})
    return {"reset": True, "removidos": deleted}


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

    def reset_counters(self) -> None:
        self.events = 0
        self.recovered = 0
        self.token = None
        self.reopen_at = None

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.active:
            return
        import threading

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
                kwargs: dict[str, Any] = {"full_document": "updateLookup", "max_await_time_ms": 300}
                if self.token:
                    kwargs["resume_after"] = self.token
                with sdb[COL_TX].watch(self.PIPELINE, **kwargs) as stream:
                    self._publish({
                        "type": "aberto",
                        "retomado": bool(self.token),
                        "token": self.token_str(),
                        "eventos": self.events,
                    })
                    while self.active and not self._drop_requested:
                        change = stream.try_next()
                        if change is None:
                            continue
                        self.token = change["_id"]
                        self._emit(change)
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
        self._publish({
            "type": "evento",
            "endToEndId": doc.get("endToEndId"),
            "uf": doc.get("uf"),
            "tipo": doc.get("tipo"),
            "valor": str(doc.get("valor")),
            "latency_ms": latency_ms,
            "recuperado": recuperado,
            "token": self.token_str(),
            "eventos": self.events,
            "recuperados": self.recovered,
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

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def reset_counters(self) -> None:
        self.messages = 0
        self.last_offset = None

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
                hub_kafka.publish({
                    "type": "mensagem",
                    "endToEndId": doc.get("endToEndId"),
                    "uf": doc.get("uf"),
                    "tipo": doc.get("tipo"),
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "latency_ms": latency_ms,
                    "mensagens": self.messages,
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
  { $source: { connectionName: "atlasCluster", db: "pix", coll: "transacoes" } },
  { $match: { operationType: "insert" } },
  { $validate: { validator: { ... }, validationAction: "dlq" } },
  { $tumblingWindow: {
      interval: { size: 10, unit: "second" },
      pipeline: [ { $group: {
        _id: { uf: "$fullDocument.uf", tipo: "$fullDocument.tipo" },
        qtd: { $count: {} },
        volume: { $sum: { $toDouble: "$fullDocument.valor" } },
        ticket: { $avg: { $toDouble: "$fullDocument.valor" } } } } ] } },
  { $merge: { into: { connectionName: "atlasCluster", db: "pix", coll: "metricas_janela" } } }
]"""


def _asp_reachable() -> tuple[bool, str]:
    if not ASP_ENABLED:
        return False, "ASP_ENABLED=false"
    if not ASP_CONNECTION_STRING:
        return False, "ASP_CONNECTION_STRING ausente"
    from pymongo import MongoClient

    try:
        spi = MongoClient(ASP_CONNECTION_STRING, serverSelectionTimeoutMS=3000, connect=False)
        spi.admin.command("ping")
        spi.close()
        return True, "Stream Processing Instance acessível"
    except Exception as exc:  # noqa: BLE001 - SPI ausente é estado esperado da demo
        return False, f"SPI inacessível: {type(exc).__name__}"


@router.get("/asp/status")
async def asp_status():
    ok, detalhe = await asyncio.to_thread(_asp_reachable)
    janelas = await asyncio.to_thread(sdb[COL_WINDOWS].count_documents, {})
    dlq = await asyncio.to_thread(sdb[COL_DLQ].count_documents, {})
    return {
        "estado": "configurado" if ok else "nao_configurado",
        "detalhe": detalhe,
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
        import threading

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
                            payload.update({
                                "uf": doc.get("uf") or key.get("uf"),
                                "tipo": doc.get("tipo") or key.get("tipo"),
                                "qtd": doc.get("qtd"),
                                "volume": doc.get("volume"),
                                "ticket": doc.get("ticket"),
                                "window_start": doc.get("window_start"),
                                "window_end": doc.get("window_end"),
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
