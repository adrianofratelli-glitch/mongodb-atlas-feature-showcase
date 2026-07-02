"""
CAMINHO B — Fluxo request-reply device-facing via MongoDB Change Stream.

A API cria o job durável e segura a request numa asyncio.Future keyed por
correlationId. O worker faz UMA ÚNICA operação (update -> status=done). O change
stream compartilhado deriva o evento DO WRITE JÁ COMMITADO e o dispatcher resolve
a Future. Persistência e sinal são o MESMO commit: não há dual-write.
"""
import asyncio
from time import perf_counter

from shared import mongo
from shared.models import new_correlation_id, gerar_resultado, CrashInjected
from caminho_b_changestream.dispatcher import ChangeStreamDispatcher


async def worker_single_write(cid: str, result: dict, *, crash_point: str | None = None):
    """
    Worker do caminho Mongo. UMA ÚNICA escrita durável.

    crash_point:
      - "before_commit": crash antes de gravar  -> job continua 'pending' (nada parcial)
      - "after_commit":  crash após o commit     -> já é durável E já está no oplog;
                          o change stream entrega mesmo com o worker morto.
    """
    if crash_point == "before_commit":
        raise CrashInjected("crash ANTES do commit — nada foi gravado, job segue pending")

    # >>> ÚNICA operação: persistir = sinalizar (o change stream é derivado daqui). <<<
    await asyncio.to_thread(mongo.persist_job_done, cid, result)

    if crash_point == "after_commit":
        raise CrashInjected("crash APÓS o commit — resultado durável e já no oplog")
    # Sem sinal separado. Sem auditoria extra. O doc + change stream JÁ são a trilha.


async def request_reply(dispatcher: ChangeStreamDispatcher, payload: dict,
                        *, timeout: float = 2.0) -> dict:
    """Fluxo síncrono device-facing completo. Retorna resultado + latência."""
    cid = new_correlation_id()
    fut = dispatcher.register(cid)
    t0 = perf_counter()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)

    result = gerar_resultado(payload)
    asyncio.create_task(worker_single_write(cid, result))

    try:
        entregue = await asyncio.wait_for(fut, timeout)
        latency_ms = (perf_counter() - t0) * 1000
        return {"correlationId": cid, "result": entregue, "latency_ms": latency_ms, "timeout": False}
    except asyncio.TimeoutError:
        dispatcher.unregister(cid)
        return {"correlationId": cid, "result": None,
                "latency_ms": (perf_counter() - t0) * 1000, "timeout": True}
