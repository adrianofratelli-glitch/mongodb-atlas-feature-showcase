"""
Router: Redis vs MongoDB Change Streams (request-reply device-facing).

Serve as duas provas ao vivo da UI (lotes de 20 operações, feed lado a lado):

  • /demo/lote-consistencia — crashes aleatórios do worker no meio do fluxo.
    No dual-write do Redis parte das ops termina INCONSISTENTE; no MongoDB o
    mesmo crash nunca gera inconsistência (persist e sinal são o mesmo commit).
  • /demo/lote-resiliencia  — o consumidor cai numa janela no meio da rotina.
    No Pub/Sub as notificações da janela se perdem; no change stream o
    resumeToken recupera todas ao religar (dentro da retenção do oplog).

Cenário Banco Inter: request síncrono device-facing → worker assíncrono → a API
precisa saber que terminou para responder o dispositivo. O resultado precisa
ser durável e auditado.

  • Caminho A (Redis, dual-write): worker faz 2 ops separadas — persistir no
    banco durável + sinalizar (Pub/Sub). Redis não é a fonte de verdade, então
    a durabilidade obriga ao dual-write → janela de inconsistência.
  • Caminho B (MongoDB Change Stream, single-write): 1 update; o change stream
    deriva do write JÁ commitado. Persistência e sinal são o mesmo commit.

O Redis é SIMULADO in-process (otimista de propósito: memória, sem rede) — mesmo
dando a ele a melhor latência possível, o argumento estrutural de
durabilidade/consistência do MongoDB se mantém. Isso está declarado na UI.
"""
import asyncio
import threading
import uuid
import random
from collections import defaultdict
from datetime import datetime, timezone

from fastapi import APIRouter
from pymongo import ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.errors import PyMongoError

from database import db

router = APIRouter(prefix="/redis-changestream", tags=["Redis vs Change Streams"])

# Coleções exclusivas da demo (prefixo demo_rvc_) — não colidem com a POC.
COL_JOBS = "demo_rvc_jobs"
COL_RESUME = "demo_rvc_resume_tokens"
COL_AUDIT = "demo_rvc_auditoria"
REDIS_SIM_LATENCY_S = 0.0003


def _now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Mongo helpers (reaproveitam database.db)
# ---------------------------------------------------------------------------
def _insert_pending(cid, payload):
    db[COL_JOBS].insert_one({
        "correlationId": cid, "status": "pending", "payload": payload,
        "result": None, "createdAt": _now(), "completedAt": None,
    })


def _persist_done(cid, result):
    db[COL_JOBS].update_one(
        {"correlationId": cid},
        {"$set": {"status": "done", "result": result, "completedAt": _now()}},
    )


def _write_audit(cid, result, caminho):
    db[COL_AUDIT].insert_one({
        "correlationId": cid, "result": result, "caminho": caminho, "auditedAt": _now(),
    })


# CRÍTICO: o checkpoint do resumeToken precisa de read-your-own-writes. Escrita
# com w=majority e leitura com readConcern=majority no primário evitam ler um
# token à frente do último evento (o resume_after pularia a conclusão perdida).
def _resume_col():
    return db.get_collection(
        COL_RESUME,
        write_concern=WriteConcern(w="majority"),
        read_concern=ReadConcern("majority"),
        read_preference=ReadPreference.PRIMARY,
    )


def _save_token(token):
    _resume_col().update_one({"_id": "jobs_stream"},
                             {"$set": {"token": token, "updatedAt": _now()}}, upsert=True)


def _load_token():
    doc = _resume_col().find_one({"_id": "jobs_stream"})
    return doc["token"] if doc else None


def _clear_token():
    _resume_col().delete_one({"_id": "jobs_stream"})


def _limpar():
    for c in (COL_JOBS, COL_AUDIT, COL_RESUME):
        db[c].delete_many({})


# ---------------------------------------------------------------------------
# Cenário Inter
# ---------------------------------------------------------------------------
_TIPOS = ["pix", "ted", "boleto", "recarga", "cartao_credito"]
_ORIGENS = ["app_mobile", "internet_banking", "pos_maquininha", "caixa_eletronico"]
_TIPO_LABEL = {"pix": "PIX", "ted": "TED", "boleto": "Boleto",
               "recarga": "Recarga", "cartao_credito": "Cartão"}


def _new_cid():
    return uuid.uuid4().hex


def _payload():
    return {"tipo": random.choice(_TIPOS), "origem": random.choice(_ORIGENS),
            "valor": round(random.uniform(10.0, 5000.0), 2)}


def _resultado(p):
    return {"autorizado": True, "codigo_autorizacao": uuid.uuid4().hex[:10].upper(),
            "tipo": p.get("tipo"), "valor": p.get("valor")}


class CrashInjected(Exception):
    pass


# ---------------------------------------------------------------------------
# Redis SIMULADO in-process
# ---------------------------------------------------------------------------
class FakeRedis:
    """Pub/Sub em memória — o melhor caso possível de latência para o Redis."""

    def __init__(self, latency_s=REDIS_SIM_LATENCY_S):
        self.latency = latency_s
        self._channels = defaultdict(set)
        self.publicados_entregues = 0
        self.publicados_perdidos = 0

    async def _tick(self):
        if self.latency:
            await asyncio.sleep(self.latency)

    async def subscribe(self, ch):
        q = asyncio.Queue(); self._channels[ch].add(q); return q

    async def unsubscribe(self, ch, q):
        self._channels[ch].discard(q)
        if not self._channels[ch]:
            self._channels.pop(ch, None)

    async def publish(self, ch, msg):
        await self._tick()
        subs = self._channels.get(ch)
        if subs:
            for q in subs:
                q.put_nowait(msg)
            self.publicados_entregues += 1
            return len(subs)
        self.publicados_perdidos += 1   # sem subscriber = perdido (fire-and-forget)
        return 0


# ---------------------------------------------------------------------------
# Caminho B — dispatcher do change stream (UM cursor compartilhado)
# ---------------------------------------------------------------------------
PIPELINE = [{"$match": {"operationType": "update",
                        "updateDescription.updatedFields.status": "done"}}]


class ChangeStreamDispatcher:
    def __init__(self, loop):
        self.loop = loop
        self.pending = {}
        self.reconciliados = []
        self._thread = None
        self._active = False
        self._ready = threading.Event()
        self._erro = None

    def start(self, resume=True):
        self._active = True
        self._ready.clear()
        self._erro = None
        self._thread = threading.Thread(target=self._run, args=(resume,), daemon=True, name="rvc-cs")
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError(f"change stream não abriu: {self._erro}")
        if self._erro:
            raise RuntimeError(self._erro)

    def stop(self):
        self._active = False
        if self._thread:
            self._thread.join(timeout=8)
            self._thread = None

    def register(self, cid):
        fut = self.loop.create_future()
        self.pending[cid] = fut
        return fut

    def unregister(self, cid):
        self.pending.pop(cid, None)

    def _run(self, resume):
        token = _load_token() if resume else None
        # max_await_time_ms baixo = poll apertado do change stream → entrega em
        # dezenas de ms (como um consumidor dedicado em produção), em vez de somar
        # até 300ms de janela de espera à latência device-facing.
        kwargs = dict(full_document="updateLookup", max_await_time_ms=25)
        if token:
            kwargs["resume_after"] = token
        try:
            with db[COL_JOBS].watch(PIPELINE, **kwargs) as stream:
                self._ready.set()
                while self._active:
                    try:
                        change = stream.try_next()
                    except PyMongoError as e:
                        self._erro = f"erro no change stream: {e}"
                        break
                    if change is None:
                        # NÃO persistir postBatchResumeToken ocioso (high-water mark):
                        # poderia cobrir um update do mesmo segundo e o resume_after
                        # pularia a conclusão. Persistimos só fronteiras de evento reais.
                        continue
                    token = change["_id"]
                    _save_token(token)
                    doc = change.get("fullDocument") or {}
                    self.loop.call_soon_threadsafe(self._resolve, doc.get("correlationId"), doc.get("result"))
        except PyMongoError as e:
            self._erro = f"falha ao abrir change stream: {e}"
            self._ready.set()
        finally:
            self._ready.set()

    def _resolve(self, cid, result):
        fut = self.pending.pop(cid, None)
        if fut and not fut.done():
            fut.set_result(result)
        else:
            self.reconciliados.append({"correlationId": cid, "result": result})


# ---------------------------------------------------------------------------
# Serviços request-reply
# ---------------------------------------------------------------------------
def _canal(cid):
    return f"response:{cid}"


async def _worker_mongo(cid, result, crash_point=None):
    if crash_point == "before_commit":
        raise CrashInjected("crash ANTES do commit — nada gravado, job segue pending")
    await asyncio.to_thread(_persist_done, cid, result)   # ÚNICA operação
    if crash_point == "after_commit":
        raise CrashInjected("crash APÓS o commit — durável e já no oplog")


async def _rr_mongo(disp, payload, timeout=3.0):
    cid = _new_cid()
    fut = disp.register(cid)
    t0 = asyncio.get_event_loop().time()
    await asyncio.to_thread(_insert_pending, cid, payload)
    asyncio.create_task(_worker_mongo(cid, _resultado(payload)))
    try:
        entregue = await asyncio.wait_for(fut, timeout)
        return {"correlationId": cid, "result": entregue,
                "latency_ms": (asyncio.get_event_loop().time() - t0) * 1000, "timeout": False}
    except asyncio.TimeoutError:
        disp.unregister(cid)
        return {"correlationId": cid, "result": None,
                "latency_ms": (asyncio.get_event_loop().time() - t0) * 1000, "timeout": True}


async def _worker_redis(redis, cid, result, crash_point=None,
                        ordem="persist_then_signal", auditar=True):
    async def _persistir():
        await asyncio.to_thread(_persist_done, cid, result)

    async def _sinalizar():
        await redis.publish(_canal(cid), result)

    if ordem == "persist_then_signal":
        if crash_point == "before_persist":
            raise CrashInjected("crash ANTES de persistir")
        await _persistir()
        if crash_point == "after_persist":
            raise CrashInjected("crash APÓS persistir, ANTES de sinalizar")
        await _sinalizar()
        if crash_point == "after_signal":
            raise CrashInjected("crash APÓS sinalizar")
    else:
        await _sinalizar()
        if crash_point == "after_signal":
            raise CrashInjected("crash APÓS sinalizar, ANTES de persistir")
        await _persistir()

    if auditar:
        await asyncio.to_thread(_write_audit, cid, result, "redis_pubsub")   # escrita EXTRA


# ===========================================================================
# LOTES DE 20 OPERAÇÕES — feed ao vivo lado a lado (Redis | MongoDB) p/ a PoV.
# Cada op vira uma linha nos DOIS feeds; o placar acumula. As operações MongoDB
# são REAIS contra o Atlas (rodadas concorrentes para caber em poucos segundos).
# ===========================================================================
def _op_base(i, p):
    return {"i": i + 1, "tipo_label": _TIPO_LABEL.get(p["tipo"], p["tipo"]),
            "valor": p["valor"], "origem": p["origem"]}


@router.post("/demo/lote-resiliencia")
async def lote_resiliencia(n: int = 20):
    """ETAPA 2 — Resiliência. O consumidor cai numa janela no meio da rotina.
    No Pub/Sub as notificações da janela se PERDEM; no change stream o
    resumeToken RECUPERA todas ao religar."""
    _limpar(); _clear_token()
    n = max(6, min(n, 40))
    loop = asyncio.get_running_loop()
    payloads = [_payload() for _ in range(n)]
    janela = set(range(n // 3, n // 3 + max(2, n // 5)))   # bloco caído no meio

    # --- Redis Pub/Sub: publish sem ouvinte na janela = perdido para sempre ---
    redis = FakeRedis()

    async def _redis_op(i):
        cid = _new_cid()
        await asyncio.to_thread(_insert_pending, cid, payloads[i])
        if i in janela:
            await _worker_redis(redis, cid, _resultado(payloads[i]), auditar=False)  # sem subscriber
            return True
        fila = await redis.subscribe(_canal(cid))
        await _worker_redis(redis, cid, _resultado(payloads[i]), auditar=False)
        await redis.unsubscribe(_canal(cid), fila)
        return False

    flags = await asyncio.gather(*[_redis_op(i) for i in range(n)])
    perdidos = [i for i in range(n) if flags[i]]

    # --- Mongo change stream: janela concluída com dispatcher fora → resume recupera ---
    disp = ChangeStreamDispatcher(loop); disp.start(resume=False)
    await _rr_mongo(disp, _payload())                       # aquece resumeToken
    antes = [i for i in range(n) if i < min(janela)]        # antes da janela: entregue ao vivo
    await asyncio.gather(*[_rr_mongo(disp, payloads[i]) for i in antes])
    disp.stop()                                            # >>> dispatcher CAI
    cids_janela = {}
    for i in sorted(janela):                                # concluem com o consumidor fora
        cid = _new_cid(); cids_janela[i] = cid
        await asyncio.to_thread(_insert_pending, cid, payloads[i])
        await _worker_mongo(cid, _resultado(payloads[i]))   # commit vai pro oplog
    disp2 = ChangeStreamDispatcher(loop)
    for cid in cids_janela.values():
        disp2.register(cid)
    disp2.start(resume=True)                               # <<< religa: resume_after(resumeToken)
    recuperados = set()
    for _ in range(80):
        await asyncio.sleep(0.05)
        for i, cid in cids_janela.items():
            if i not in recuperados and (cid not in disp2.pending or
               any(r["correlationId"] == cid for r in disp2.reconciliados)):
                recuperados.add(i)
        if len(recuperados) == len(janela):
            break
    depois = [i for i in range(n) if i > max(janela)]      # depois da janela: entregue ao vivo
    await asyncio.gather(*[_rr_mongo(disp2, payloads[i]) for i in depois])
    disp2.stop()

    ops = []
    for i in range(n):
        o = _op_base(i, payloads[i])
        caiu = i in janela
        perdido = i in perdidos
        recuperado = i in recuperados
        o["redis"] = {"ok": not perdido, "tag": "PERDIDO" if perdido else "avisado", "durado": None,
                      "note": "consumidor fora — aviso perdido p/ sempre" if perdido else "entregue"}
        o["mongo"] = {"ok": True, "tag": "recuperado" if (caiu and recuperado) else "avisado", "durado": None,
                      "note": "replay via resumeToken" if (caiu and recuperado) else "entregue"}
        ops.append(o)

    _limpar(); _clear_token()
    return {
        "n": n, "ops": ops,
        "placar": {
            "redis": {"avisados": n - len(perdidos), "perdidos": len(perdidos)},
            "mongo": {"avisados": n, "perdidos": 0, "recuperados": len(recuperados)},
            "veredito": f"O consumidor caiu e o Redis perdeu {len(perdidos)} notificações para sempre. "
                        f"O change stream recuperou todas via resumeToken — 0 perdidas.",
        },
    }


@router.post("/demo/lote-consistencia")
async def lote_consistencia(n: int = 20):
    """ETAPA 3 — Consistência. Crashes aleatórios do worker no meio do fluxo.
    No dual-write do Redis, parte das ops fica INCONSISTENTE; no MongoDB o mesmo
    crash nunca gera inconsistência (persist e sinal são o mesmo commit)."""
    _limpar(); _clear_token()
    n = max(6, min(n, 40))
    loop = asyncio.get_running_loop()
    disp = ChangeStreamDispatcher(loop); disp.start(resume=False)
    await _rr_mongo(disp, _payload())

    # sorteio determinístico por índice p/ o placar variar mas ser reproduzível na rodada
    rolls = [random.random() for _ in range(n)]
    try:
        async def uma(i):
            p = _payload(); roll = rolls[i]
            o = _op_base(i, p)

            # ---- Redis dual-write com crash sorteado ----
            fake = FakeRedis(); cid = _new_cid()
            await asyncio.to_thread(_insert_pending, cid, p)
            if roll < 0.30:            # crash após persistir, antes de sinalizar
                try:
                    await _worker_redis(fake, cid, _resultado(p),
                                        crash_point="after_persist", ordem="persist_then_signal")
                except CrashInjected:
                    pass
                o["redis"] = {"ok": False, "tag": "INCONSISTENTE", "durado": True,
                              "note": "durável, mas device NUNCA avisado e sem auditoria"}
            elif roll < 0.45:          # crash após sinalizar, antes de persistir (pior caso)
                try:
                    await _worker_redis(fake, cid, _resultado(p),
                                        crash_point="after_signal", ordem="signal_then_persist")
                except CrashInjected:
                    pass
                o["redis"] = {"ok": False, "tag": "INCONSISTENTE", "durado": False,
                              "note": "device avisado de sucesso — mas NADA foi persistido"}
            else:                      # sem crash
                await _worker_redis(fake, cid, _resultado(p))
                o["redis"] = {"ok": True, "tag": "consistente", "durado": True,
                              "note": "persistido + avisado + auditado"}

            # ---- Mongo: MESMO crash logo após o commit ----
            cidm = _new_cid(); fut = disp.register(cidm)
            await asyncio.to_thread(_insert_pending, cidm, p)
            try:
                await _worker_mongo(cidm, _resultado(p), crash_point="after_commit")
            except CrashInjected:
                pass
            entregue = False
            try:
                await asyncio.wait_for(fut, timeout=5.0)
                entregue = True
            except asyncio.TimeoutError:
                disp.unregister(cidm)
            o["mongo"] = {"ok": True, "tag": "consistente", "durado": True,
                          "note": "sinal derivou do commit já durável" + ("" if entregue else " (recuperável)")}
            return o

        ops = await asyncio.gather(*[uma(i) for i in range(n)])
    finally:
        disp.stop()

    redis_incon = sum(1 for o in ops if not o["redis"]["ok"])
    _limpar(); _clear_token()
    return {
        "n": n, "ops": list(ops),
        "placar": {
            "redis": {"consistentes": n - redis_incon, "inconsistentes": redis_incon},
            "mongo": {"consistentes": n, "inconsistentes": 0},
            "veredito": f"Com o dual-write, {redis_incon} das {n} operações terminaram INCONSISTENTES no Redis. "
                        f"No MongoDB, o mesmo crash gerou 0 — persist e sinal são o mesmo commit.",
        },
    }
