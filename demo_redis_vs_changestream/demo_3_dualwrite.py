"""
DEMO 3 — DUAL-WRITE INCONSISTENCY (o ponto principal).

Injeta um crash do worker ENTRE a persistência durável e o sinal (e vice-versa).

  • Redis: persistência (banco) e sinal (Redis) são DUAS operações contra DOIS
    sistemas. Um crash no meio deixa estado inconsistente:
      - persistido mas NÃO sinalizado (device nunca avisado), ou
      - sinalizado mas NÃO persistido (device avisado, sem registro durável).
  • MongoDB Change Stream: persistência e sinal são o MESMO commit. O sinal é
    DERIVADO do write já commitado. O mesmo crash é incapaz de gerar
    inconsistência: ou o commit aconteceu (durável + no oplog + entregue), ou não.
"""
import asyncio

from shared import mongo
from shared.models import new_correlation_id, gerar_payload, gerar_resultado, CrashInjected
from shared.fake_redis import FakeRedis
from caminho_a_redis import service as redis_svc
from caminho_b_changestream.dispatcher import ChangeStreamDispatcher
from caminho_b_changestream import service as mongo_svc
import demo_common as ui


async def _estado(cid: str) -> tuple[str, int]:
    job = await asyncio.to_thread(mongo.get_job, cid)
    aud = await asyncio.to_thread(mongo.count_audit, cid)
    return (job["status"] if job else "inexistente"), aud


async def cenario_redis_persist_sem_signal():
    ui.secao("CAMINHO A · Cenário 1 — crash ENTRE persistir e sinalizar")
    redis = FakeRedis()
    cid = new_correlation_id(); payload = gerar_payload()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)

    ui.info("Worker: (a) persiste no banco → 💥 CRASH → (b) sinal nunca acontece")
    try:
        await redis_svc.worker_dual_write(
            redis, cid, gerar_resultado(payload),
            variante="pubsub", ordem="persist_then_signal", crash_point="after_persist")
    except CrashInjected as e:
        ui.aviso(f"crash injetado: {e}")

    status, aud = await _estado(cid)
    ui.info(f"Banco: job.status = {ui.BOLD}{status}{ui.RESET}  |  auditoria: {aud} registro(s)  "
            f"|  sinais Redis enviados: {redis.publicados_entregues}")
    ui.falha("INCONSISTENTE: resultado DURÁVEL, mas device NUNCA avisado e SEM auditoria.")


async def cenario_redis_signal_sem_persist():
    ui.secao("CAMINHO A · Cenário 2 — crash ENTRE sinalizar e persistir (vice-versa)")
    redis = FakeRedis()
    cid = new_correlation_id(); payload = gerar_payload()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)

    fila = await redis.subscribe(f"response:{cid}")  # device escutando
    ui.info("Worker: (b) sinaliza device → 💥 CRASH → (a) persist nunca acontece")
    try:
        await redis_svc.worker_dual_write(
            redis, cid, gerar_resultado(payload),
            variante="pubsub", ordem="signal_then_persist", crash_point="after_signal")
    except CrashInjected as e:
        ui.aviso(f"crash injetado: {e}")

    device_avisado = not fila.empty()
    status, aud = await _estado(cid)
    ui.info(f"Device recebeu 'autorizado'? {ui.BOLD}{'SIM' if device_avisado else 'não'}{ui.RESET}  "
            f"|  Banco: job.status = {ui.BOLD}{status}{ui.RESET}  |  auditoria: {aud}")
    ui.falha("INCONSISTENTE (pior caso): device avisado de sucesso, mas SEM registro "
             "durável nem auditoria.")


async def cenario_mongo_impossivel():
    ui.secao("CAMINHO B · MESMO crash — impossível gerar inconsistência")
    mongo.clear_resume_token()
    loop = asyncio.get_running_loop()
    disp = ChangeStreamDispatcher(loop)
    disp.start(resume=False)
    await mongo_svc.request_reply(disp, gerar_payload())  # aquece token

    # (i) crash APÓS o commit: já é durável E já está no oplog → stream entrega.
    cid = new_correlation_id(); payload = gerar_payload()
    fut = disp.register(cid)
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)
    ui.info("Worker: update ÚNICO (persist=sinal) → 💥 CRASH logo após o commit")
    try:
        await mongo_svc.worker_single_write(cid, gerar_resultado(payload), crash_point="after_commit")
    except CrashInjected as e:
        ui.aviso(f"crash injetado: {e}")

    entregue = False
    try:
        await asyncio.wait_for(fut, timeout=5.0)
        entregue = True
    except asyncio.TimeoutError:
        pass
    status, aud = await _estado(cid)
    ui.info(f"Device notificado via change stream? {ui.BOLD}{'SIM' if entregue else 'não'}{ui.RESET}  "
            f"|  Banco: job.status = {ui.BOLD}{status}{ui.RESET}  |  auditoria extra: {aud}")
    ui.ok("CONSISTENTE: o sinal derivou do write já commitado — mesmo com o worker morto.")

    # (ii) crash ANTES do commit: nada parcial, job segue pending.
    cid2 = new_correlation_id(); payload2 = gerar_payload()
    await asyncio.to_thread(mongo.insert_pending_job, cid2, payload2)
    ui.info("Outro job: 💥 CRASH ANTES do commit")
    try:
        await mongo_svc.worker_single_write(cid2, gerar_resultado(payload2), crash_point="before_commit")
    except CrashInjected as e:
        ui.aviso(f"crash injetado: {e}")
    status2, _ = await _estado(cid2)
    ui.ok(f"CONSISTENTE: nada parcial — job.status = {ui.BOLD}{status2}{ui.RESET} "
          f"(nenhum sinal indevido).")

    disp.stop()
    print(f"\n  {ui.VERDE}▪ Não existe estado 'sinalizado mas não persistido' no caminho Mongo:"
          f"\n    persistência e sinal são o MESMO commit atômico. Zero janela de dual-write.{ui.RESET}")


async def run():
    ui.banner("DEMO 3 — DUAL-WRITE INCONSISTENCY", "Redis (2 sistemas) vs MongoDB (1 commit)")
    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()
    await cenario_redis_persist_sem_signal()
    await cenario_redis_signal_sem_persist()
    await cenario_mongo_impossivel()
    ui.secao("Talking point")
    print(f"  {ui.DIM}Dual-write = duas escritas não-atômicas contra dois sistemas = janela de"
          f"\n  inconsistência SEMPRE presente (só muda a probabilidade). Mitigar exige outbox,"
          f"\n  sagas, reconciliação — complexidade que você passa a operar."
          f"\n  MongoDB Change Stream elimina a classe inteira do problema: single write,"
          f"\n  single source of truth, sinal derivado do commit. Um sistema a menos.{ui.RESET}")
    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()


if __name__ == "__main__":
    asyncio.run(run())
