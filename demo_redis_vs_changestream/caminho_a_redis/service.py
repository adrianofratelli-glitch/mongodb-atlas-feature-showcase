"""
CAMINHO A — Fluxo request-reply device-facing via Redis (dual-write).

Como o Redis NÃO persiste o resultado de negócio, o worker é OBRIGADO a fazer
DUAS operações separadas para atender o cenário (resultado durável + auditável):
  (a) persistir o resultado na coleção durável `jobs`  (banco)
  (b) sinalizar a conclusão via Redis                  (Pub/Sub ou LPUSH)
  (+ auditoria = uma terceira escrita EXTRA)

Duas operações não-atômicas contra dois sistemas diferentes = janela de
dual-write. Um crash entre (a) e (b) — ou entre (b) e (a) — deixa o sistema
em estado inconsistente. É exatamente isso que a demo 3 evidencia.

Redis tem, honestamente, latência MENOR no sinal isolado (op em memória). O
custo aparece quando o cenário exige durabilidade: aí entram o segundo write e
a inconsistência possível.
"""
import asyncio
from time import perf_counter

from shared import mongo
from shared.fake_redis import FakeRedis
from shared.models import new_correlation_id, gerar_resultado, CrashInjected


def _canal(cid: str) -> str:
    return f"response:{cid}"


async def worker_dual_write(redis: FakeRedis, cid: str, result: dict, *,
                            variante: str = "pubsub",
                            crash_point: str | None = None,
                            ordem: str = "persist_then_signal",
                            auditar: bool = True):
    """
    Worker do caminho Redis. DUAS operações obrigatórias (a) persistir + (b) sinalizar.

    crash_point: "before_persist" | "after_persist" | "after_signal" | None
    ordem:
      - "persist_then_signal": grava no banco e depois sinaliza (padrão)
      - "signal_then_persist": sinaliza antes de gravar (o "vice-versa" da demo 3)
    """
    async def _persistir():
        await asyncio.to_thread(mongo.persist_job_done, cid, result)

    async def _sinalizar():
        if variante == "pubsub":
            await redis.publish(_canal(cid), result)      # fire-and-forget
        else:  # blpop
            await redis.lpush(_canal(cid), result)

    if ordem == "persist_then_signal":
        if crash_point == "before_persist":
            raise CrashInjected("crash ANTES de persistir")
        await _persistir()                                 # (a)
        if crash_point == "after_persist":
            raise CrashInjected("crash APÓS persistir, ANTES de sinalizar")
        await _sinalizar()                                 # (b)
        if crash_point == "after_signal":
            raise CrashInjected("crash APÓS sinalizar")
    else:  # signal_then_persist
        await _sinalizar()                                 # (b) primeiro
        if crash_point == "after_signal":
            raise CrashInjected("crash APÓS sinalizar, ANTES de persistir")
        await _persistir()                                 # (a) depois

    # (+) Auditoria: escrita EXTRA (mais um passo do dual-write).
    if auditar:
        await asyncio.to_thread(mongo.write_audit, cid, result, f"redis_{variante}")


async def request_reply(redis: FakeRedis, payload: dict, *,
                        variante: str = "pubsub", timeout: float = 2.0,
                        crash_point: str | None = None) -> dict:
    """Fluxo device-facing completo pelo Redis. Retorna resultado + latência."""
    cid = new_correlation_id()
    t0 = perf_counter()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)
    result = gerar_resultado(payload)

    if variante == "pubsub":
        fila = await redis.subscribe(_canal(cid))
        worker = asyncio.create_task(
            worker_dual_write(redis, cid, result, variante="pubsub", crash_point=crash_point)
        )
        try:
            entregue = await asyncio.wait_for(fila.get(), timeout)
            return {"correlationId": cid, "result": entregue,
                    "latency_ms": (perf_counter() - t0) * 1000, "timeout": False}
        except asyncio.TimeoutError:
            return {"correlationId": cid, "result": None,
                    "latency_ms": (perf_counter() - t0) * 1000, "timeout": True}
        finally:
            await redis.unsubscribe(_canal(cid), fila)
            await _drenar(worker)
    else:  # blpop
        worker = asyncio.create_task(
            worker_dual_write(redis, cid, result, variante="blpop", crash_point=crash_point)
        )
        entregue = await redis.blpop(_canal(cid), timeout)
        await _drenar(worker)
        return {"correlationId": cid, "result": entregue,
                "latency_ms": (perf_counter() - t0) * 1000, "timeout": entregue is None}


async def sinal_isolado(redis: FakeRedis, payload: dict, *, timeout: float = 2.0) -> dict:
    """
    Redis puro, NÃO durável: só o sinal, sem nenhum write no banco.
    É o melhor caso de latência do Redis — e também o que perde tudo num crash.
    Usado na demo 1 para mostrar honestamente onde o Redis é rápido.
    """
    cid = new_correlation_id()
    result = gerar_resultado(payload)
    t0 = perf_counter()
    fila = await redis.subscribe(_canal(cid))
    asyncio.create_task(redis.publish(_canal(cid), result))
    try:
        entregue = await asyncio.wait_for(fila.get(), timeout)
        return {"correlationId": cid, "result": entregue,
                "latency_ms": (perf_counter() - t0) * 1000, "timeout": False}
    finally:
        await redis.unsubscribe(_canal(cid), fila)


async def _drenar(task: asyncio.Task):
    """Aguarda o worker terminar; engole o CrashInjected (esperado nas demos)."""
    try:
        await task
    except CrashInjected:
        pass
