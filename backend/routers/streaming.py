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
ASP_PROCESSOR_NAME = os.getenv("ASP_PROCESSOR_NAME", "pixJanelas10s").strip() or "pixJanelas10s"
# Só para exibição: o que está provisionado nesta PoV.
CLUSTER_TIER = os.getenv("CLUSTER_TIER", "M30").strip() or "M30"
# Preço de lista do ambiente, US$/hora. Premissa editável: entra na conta de
# custo por milhão de transações, que é o número que um gestor leva para a
# reunião de orçamento.
CUSTO_CLUSTER_USD_HORA = float(os.getenv("CUSTO_CLUSTER_USD_HORA", "0.54"))
# Custo/hora do tier do Stream Processing em uso. Sem um valor confirmado o
# custo por milhão sairia otimista — a UI avisa quando isto está zerado.
CUSTO_ASP_USD_HORA = float(os.getenv("CUSTO_ASP_USD_HORA", "0"))
CUSTO_AMBIENTE_USD_HORA = CUSTO_CLUSTER_USD_HORA + CUSTO_ASP_USD_HORA
# Sistemas a operar para o mesmo resultado, com e sem change stream nativo.
SISTEMAS_COM_MONGO = 1
SISTEMAS_SEM_MONGO = 3
# Teto MEDIDO com as três colunas ativas: acima disso o Kafka e o ASP ficam para trás.
TETO_MEDIDO_TPS = int(os.getenv("TETO_MEDIDO_TPS", "9500"))

# TTL = rede de segurança, não a limpeza principal (essa é o botão Reset, que
# dropa a coleção na hora).
#
# A janela é deliberadamente MAIOR que a rajada mais longa de uma demo. Em
# regime o deletor do TTL remove na mesma taxa em que se insere — 10 mil/s
# inserindo é 10 mil/s deletando, com TTL de 2 min ou de 30. O que a janela
# decide é QUANDO isso acontece: curta demais, a deleção concorre com o pico
# enquanto a squad olha os números e ainda enche o oplog de que o resume token
# depende; longa o bastante, a limpeza cai depois da apresentação, com o
# cluster ocioso.
TTL_SECONDS = int(os.getenv("STREAMING_TTL_SEGUNDOS", "1800"))
PURGE_TIMEOUT_MS = 180_000
# Partições do consumo do change stream (um cursor + uma thread por partição).
CS_PARTICOES = max(1, int(os.getenv("STREAMING_CS_PARTICOES", "6")))
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
        # Sem lock no caminho quente: cada meter tem um único produtor e
        # deque.append é atômico. Aos milhares de eventos por segundo, pegar um
        # lock por evento aparece no perfil. snapshot() continua sob lock.
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
TTL_INDEX_NAME = "ts_ttl"


def _ensure_indexes() -> None:
    """
    Garante o índice único de endToEndId e o TTL em `ts` com a janela atual.

    O TTL é ajustado por collMod no índice que JÁ existe (procurado pela chave,
    não pelo nome): create_index recusa um segundo índice sobre {ts: 1} com
    expireAfterSeconds diferente, e versões anteriores desta PoV criaram esse
    índice com outro nome.
    """
    sdb[COL_TX].create_index("endToEndId", unique=True, name="endToEndId_unique")

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


def _new_transacao() -> dict[str, Any]:
    pagador = random.randint(1, 5000)
    return {
        "endToEndId": f"E{uuid.uuid4().hex[:31].upper()}",
        "pagadorId": f"P{pagador:06d}",
        # Partição de consumo derivada do pagador — é assim que um banco
        # particionaria o fluxo (por conta), e é o que permite um consumidor
        # por partição em vez de um cursor único para tudo.
        "particao": pagador % CS_PARTICOES,
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
        self._start_mono: float | None = None
        self._lock = threading.Lock()                # _recent/_inserted são tocados pelas threads de insert

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
        self.tps_alvo = 0

    def reset_counters(self) -> None:
        with self._lock:
            self.inserted = 0
            self._recent = []
            self._start_mono = time.monotonic()

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
                    # Devolve ao próximo tick, mas com TETO: sem isso o carry
                    # cresce sem limite enquanto o Atlas não acompanha e depois
                    # o gerador dispara uma rajada bem acima do alvo pedido.
                    carry = min(carry + batch_size, self.tps_alvo * self.TICK_S * 2)
                else:
                    task = asyncio.create_task(self._batch([_new_transacao() for _ in range(batch_size)]))
                    inflight.add(task)
                    task.add_done_callback(inflight.discard)
            next_tick += self.TICK_S
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))


generator = Generator()


class GeneratorStart(BaseModel):
    tps: int = Field(default=BRASIL_TPS_MEDIO, ge=1, le=20000)


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
        "ttl_ativo": True,
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
        checks["streaming_indices"] = {
            "ok": bool(ttl) and "endToEndId_unique" in indices,
            "message": (
                f"TTL {ttl.get('expireAfterSeconds')}s + índice único"
                if ttl and "endToEndId_unique" in indices
                else "faltando; sobem no primeiro start do gerador"
            ),
        }
        docs = sdb[COL_TX].estimated_document_count()
        checks["streaming_colecao"] = {
            "ok": docs < DROP_ACIMA_DE,
            "message": f"{docs} documentos" + ("" if docs < DROP_ACIMA_DE else " — rode o Reset antes da demo"),
        }
    except PyMongoError as exc:
        checks["streaming_colecao"] = {"ok": False, "message": f"inacessível: {type(exc).__name__}"}

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


@router.get("/negocio")
async def negocio():
    """
    Traduz as métricas técnicas medidas em números de negócio.

    Tudo aqui é DERIVADO de medição real (TPS medido, latência p50, ticket médio
    que o processor somou, eventos recuperados). O que é premissa — preço de
    lista por hora — vem rotulado, para a UI poder separar as duas coisas.
    """
    tps = generator.measured_tps()
    cs = meter_cs.snapshot()
    latencia_ms = cs.get("p50")
    totais = await asyncio.to_thread(_asp_totais)

    agregadas = totais["transacoes_agregadas"]
    volume = totais["volume_agregado"]
    ticket_medio = round(volume / agregadas, 2) if agregadas else None

    # 1. Custo por milhão de transações
    custo_por_milhao = None
    if tps > 0:
        custo_por_segundo = CUSTO_AMBIENTE_USD_HORA / 3600
        custo_por_milhao = round(custo_por_segundo / tps * 1_000_000, 2)

    # 3. Volume financeiro "em trânsito" na janela de latência
    reais_por_segundo = round(ticket_medio * tps, 2) if (ticket_medio and tps) else None
    valor_em_transito = (
        round(reais_por_segundo * (latencia_ms / 1000), 2)
        if (reais_por_segundo and latencia_ms) else None
    )

    # Potencial: uma queda de 3 s no ritmo atual. Sem resume token, cada evento
    # dessa janela vira conferência manual.
    potencial_queda_3s = int(cs.get("eventos_s") or 0) * 3

    return {
        "custo_por_milhao_usd": custo_por_milhao,
        "custo_inclui_asp": CUSTO_ASP_USD_HORA > 0,
        "custo_cluster_usd_hora": CUSTO_CLUSTER_USD_HORA,
        "custo_asp_usd_hora": CUSTO_ASP_USD_HORA,
        "reconciliacoes_potenciais_3s": potencial_queda_3s,
        "latencia_reacao_ms": latencia_ms,
        "reais_por_segundo": reais_por_segundo,
        "valor_em_transito_brl": valor_em_transito,
        "reconciliacoes_evitadas": cs_worker.recovered,
        "sistemas_com_mongo": SISTEMAS_COM_MONGO,
        "sistemas_sem_mongo": SISTEMAS_SEM_MONGO,
        "ticket_medio": ticket_medio,
        "transacoes_agregadas": agregadas,
        "premissas": {
            "custo_ambiente_usd_hora": round(CUSTO_AMBIENTE_USD_HORA, 4),
            "nota": "TPS, latência e contagem são MEDIDOS nesta sessão. O custo usa o preço "
                    "de lista por hora do ambiente. O ticket médio é real no sentido de que o "
                    "processor o calculou sobre as transações que passaram — mas os valores "
                    "vêm da distribuição sintética do gerador, então o fluxo em R$ mostra a "
                    "ordem de grandeza, não o ticket real do Inter.",
        },
    }


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
        # Só cargas altas: mostrar 300 TPS para uma squad que opera PIX não diz
        # nada. O menor preset já é o PICO do Inter.
        "presets": [
            {"label": "Pico Inter", "tps": INTER_TPS_PICO,
             "detalhe": f"{PICO_FATOR}× a média do Inter ({INTER_TPS_MEDIO} TPS) — premissa de pico intradiário"},
            {"label": "PIX Brasil inteiro", "tps": BRASIL_TPS_MEDIO,
             "detalhe": f"{PIX_BRASIL_TX_DIA:,} transações/dia ÷ 86.400 s".replace(",", ".")},
            {"label": "2× PIX Brasil", "tps": BRASIL_TPS_MEDIO * 2,
             "detalhe": "o dobro do PIX nacional inteiro, no mesmo cluster"},
            {"label": "Teto medido", "tps": TETO_MEDIDO_TPS,
             "detalhe": f"máximo sustentado no cluster {CLUSTER_TIER} com o Stream Processing "
                        f"no menor tier (SP10): Change Streams e ASP acompanham"},
        ],
        "ambiente": {
            "cluster": CLUSTER_TIER,
            # tier do ASP não vem daqui: /streaming/asp/status lê o real da SPI.
            "particoes_consumo": CS_PARTICOES,
            "teto_medido_tps": TETO_MEDIDO_TPS,
            "nota": f"{TETO_MEDIDO_TPS:,}".replace(",", ".") +
                    " TPS é o teto MEDIDO com o Stream Processing no menor tier (SP10), "
                    "escolhido de propósito para manter a PoV barata: Change Streams entregam "
                    "9.507 ev/s e o processor agrega 9.483 tx/s. Acima disso o SP10 satura e "
                    "cai para ~7.500 tx/s. O Kafka satura antes (~7.000 msg/s) por rodar "
                    "1 task por coleção.",
            "asterisco": "* Teto do AMBIENTE, não do produto. Só trocando o tier do Stream "
                         "Processing para SP30 — sem tocar em cluster, código ou partições — "
                         "o mesmo pipeline agregou 9.968 tx/s a 10.000 TPS de entrada, onde o "
                         "SP10 já tinha quebrado. Escalar é uma linha de configuração.",
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
async def reset():
    await generator.stop()
    # O worker do change stream NÃO é parado aqui: ele só volta a subir quando um
    # novo assinante SSE chega, e a aba já aberta na demo não reabre o
    # EventSource — a coluna 1 ficaria muda depois do Reset até dar F5.
    deleted: dict[str, Any] = {}
    restantes = 0
    asp_reiniciado = False
    kafka_reiniciado = False

    grande = await asyncio.to_thread(sdb[COL_TX].estimated_document_count)
    if grande > DROP_ACIMA_DE:
        await asyncio.to_thread(_asp_command, {"stopStreamProcessor": ASP_PROCESSOR_NAME})
        deleted[COL_TX] = await asyncio.to_thread(_drop_and_recreate)
        asp_reiniciado = await asyncio.to_thread(_asp_command, {"startStreamProcessor": ASP_PROCESSOR_NAME})
        alvos = (COL_WINDOWS, COL_DLQ)
    else:
        alvos = (COL_TX, COL_WINDOWS, COL_DLQ)

    for col in alvos:
        removed, left = await asyncio.to_thread(_purge, col)
        deleted[col] = removed
        restantes += left
    # Religa os connectors SEMPRE: um drop anterior invalida o change stream
    # deles e as tasks não se recuperam sozinhas — sem isto a coluna 2 fica
    # vermelha depois do Reset. É idempotente e barato.
    try:
        kafka_reiniciado = (await asyncio.to_thread(_connector_restart_sync)).get("reiniciado", False)
    except Exception:  # noqa: BLE001 - Kafka é opcional na demo
        kafka_reiniciado = False

    generator.reset_counters()
    cs_worker.reset_counters()
    kafka_consumer.reset_counters()
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
            "kafka_reiniciado": kafka_reiniciado}


# ---------------------------------------------------------------------------
# COLUNA 1 — Change Streams
# ---------------------------------------------------------------------------
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
        self.reopen_at: datetime | None = None
        self._drop_requested = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ultimo_feed = 0.0
        self._ultima_metrica = 0.0

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
        self.reopen_at = None
        meter_cs.reset()

    def ensure_started(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.active:
            return
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
                        self._emit(change)
                        if not self.active or self._drop_requested:
                            break
            except PyMongoError as exc:
                # Depois de um drop da coleção o resume token guardado deixa de
                # ser válido: insistir nele deixaria a coluna morta para sempre.
                # Descarta o token e reabre do zero.
                if self.token is not None:
                    logger.warning("Partição %d: token inválido (%s); reabrindo sem resume",
                                   self.particao, type(exc).__name__)
                    self.token = None
                    continue
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
                "eventos": cs_worker.events,
                "recuperados": cs_worker.recovered,
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
        "particoes": cs_worker.particoes,
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
            ativos = [p for p in processors if p.get("state") == "STARTED"]
            if not processors:
                return False, "SPI acessível, mas nenhum processor criado (rode scripts/setup-asp.js)", None
            if not ativos:
                estados = ", ".join(f"{p.get('name')}={p.get('state')}" for p in processors)
                return False, f"processor parado: {estados}", None
            nomes = ", ".join(p.get("name", "?") for p in ativos)
            # Tier REAL em execução: o processor mantém o tier com que foi
            # iniciado, que pode diferir do default do workspace.
            tier = ativos[0].get("effectiveTier") or ativos[0].get("tier")
            return True, f"processor STARTED: {nomes}", tier
        finally:
            spi.close()
    except Exception as exc:  # noqa: BLE001 - SPI ausente é estado esperado da demo
        return False, f"SPI inacessível: {type(exc).__name__}", None


ASP_ATRASO_ALERTA_S = 30.0


def _asp_atraso_s() -> float | None:
    """
    Segundos entre a última janela FECHADA e agora.

    Depois de uma rajada o processor fica drenando backlog, e os percentis da
    coluna passam a medir a fila, não o regime — a tela chegava a mostrar
    p50 de 23 s. Com este número a UI diz "drenando backlog" em vez de exibir
    um percentil que não representa nada.
    """
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
    ok, detalhe, _ = await asyncio.to_thread(_asp_reachable)
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
