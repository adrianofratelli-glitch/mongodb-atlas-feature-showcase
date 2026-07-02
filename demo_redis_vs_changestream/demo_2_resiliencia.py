"""
DEMO 2 — RESILIÊNCIA (fire-and-forget do Pub/Sub vs resumeToken do Change Stream).

Cenário: o consumidor/API cai NO MEIO do fluxo, o job é concluído enquanto ele
está fora, e ele religa.

  • Redis Pub/Sub: PUBLISH é fire-and-forget. Sem subscriber no instante do
    PUBLISH, a notificação evapora. Ao religar, não há como recuperá-la.
  • MongoDB Change Stream: o resumeToken é persistido. Ao religar, retomamos com
    resume_after e a conclusão perdida é REPROCESSADA em ordem — o evento nunca
    se perde, porque deriva de um write já commitado (fonte única confiável).
"""
import asyncio

from shared import mongo
from shared.models import new_correlation_id, gerar_payload, gerar_resultado
from shared.fake_redis import FakeRedis
from caminho_a_redis import service as redis_svc
from caminho_b_changestream.dispatcher import ChangeStreamDispatcher
from caminho_b_changestream import service as mongo_svc
import demo_common as ui


async def parte_a_pubsub():
    ui.secao("CAMINHO A — Redis Pub/Sub (fire-and-forget)")
    redis = FakeRedis()
    cid = new_correlation_id()
    payload = gerar_payload()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)

    fila = await redis.subscribe(f"response:{cid}")
    ui.info(f"API inscrita em response:{cid[:12]}… e segurando a request.")

    ui.aviso(">>> API/subscriber CAI no meio do fluxo (processo morto).")
    await redis.unsubscribe(f"response:{cid}", fila)  # subscriber some

    ui.info("Worker conclui o job (dual-write: persiste + PUBLISH)…")
    await redis_svc.worker_dual_write(redis, cid, gerar_resultado(payload), variante="pubsub")

    ui.info("<<< API RELIGA e volta a se inscrever, esperando a resposta…")
    fila2 = await redis.subscribe(f"response:{cid}")
    try:
        await asyncio.wait_for(fila2.get(), timeout=1.0)
        ui.ok("Recebeu a notificação.")
    except asyncio.TimeoutError:
        ui.falha("TIMEOUT — a notificação foi PERDIDA para sempre.")

    job = await asyncio.to_thread(mongo.get_job, cid)
    ui.info(f"Estado no Atlas: job.status = {ui.BOLD}{job['status']}{ui.RESET} (durável)")
    ui.falha(f"Notificações Pub/Sub perdidas nesta sessão: {redis.publicados_perdidos}")
    print(f"\n  {ui.VERMELHO}▪ Resultado: o job ESTÁ pronto e durável no banco, mas o device"
          f"\n    nunca foi avisado. Recuperar isso exige um mecanismo de reconciliação"
          f"\n    que o Pub/Sub NÃO oferece (fire-and-forget).{ui.RESET}")


async def parte_b_changestream():
    ui.secao("CAMINHO B — MongoDB Change Stream (resumeToken)")
    mongo.clear_resume_token()
    loop = asyncio.get_running_loop()

    disp = ChangeStreamDispatcher(loop)
    disp.start(resume=False)
    # aquece: processa 1 job para garantir um resumeToken persistido.
    await mongo_svc.request_reply(disp, gerar_payload())
    ui.info("Dispatcher ativo; resumeToken já persistido no Atlas.")

    cid = new_correlation_id()
    payload = gerar_payload()
    await asyncio.to_thread(mongo.insert_pending_job, cid, payload)
    ui.info(f"Request device-facing pendente (correlationId {cid[:12]}…).")

    ui.aviso(">>> Dispatcher/API CAI (change stream fechado).")
    disp.stop()

    ui.info("Worker conclui o job enquanto o dispatcher está FORA (single-write)…")
    await mongo_svc.worker_single_write(cid, gerar_resultado(payload))
    tok = await asyncio.to_thread(mongo.load_resume_token)
    ui.info(f"Update foi para o oplog; NÃO foi entregue. Token durável presente: {bool(tok)}")

    ui.info("<<< Dispatcher RELIGA com resume_after(resumeToken)…")
    disp2 = ChangeStreamDispatcher(loop)
    disp2.register(cid)                    # API re-registra a request que segurava
    disp2.start(resume=True)               # retoma exatamente de onde parou

    # espera o replay da conclusão perdida
    recuperado = False
    for _ in range(40):
        await asyncio.sleep(0.1)
        if cid not in disp2.pending or any(r["correlationId"] == cid for r in disp2.reconciliados):
            recuperado = True
            break
    disp2.stop()

    job = await asyncio.to_thread(mongo.get_job, cid)
    if recuperado:
        ui.ok("RECUPERADO — a conclusão perdida foi reprocessada em ordem via resumeToken.")
    else:
        ui.falha("Não recuperou (inesperado).")
    ui.info(f"Estado no Atlas: job.status = {ui.BOLD}{job['status']}{ui.RESET}")
    print(f"\n  {ui.VERDE}▪ Resultado: mesmo com o consumidor fora no momento da conclusão,"
          f"\n    o evento NÃO se perdeu. O change stream deriva do write já commitado"
          f"\n    e o resumeToken garante replay em ordem — fonte única confiável.{ui.RESET}")


async def run():
    ui.banner("DEMO 2 — RESILIÊNCIA", "Notificação perdida (Pub/Sub) vs recuperada (Change Stream)")
    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()
    await parte_a_pubsub()
    await parte_b_changestream()
    ui.secao("Talking point")
    print(f"  {ui.DIM}Pub/Sub = fire-and-forget: rápido, porém sem garantia de entrega."
          f"\n  Change Stream = notificação derivada de um write durável + resumeToken:"
          f"\n  entregue ao menos uma vez, recuperável e em ordem. Um sistema a menos"
          f"\n  para operar e nenhuma reconciliação caseira para manter.{ui.RESET}")
    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()


if __name__ == "__main__":
    asyncio.run(run())
