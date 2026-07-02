"""
DEMO 1 — LATÊNCIA ponta-a-ponta (device-facing).

Roda N jobs no MESMO fluxo request-reply por três variantes e mede a latência
device-facing (p50/p95/p99):

  A1) Redis Pub/Sub — SINAL ISOLADO (não durável): só o sinal em memória.
  A2) Redis dual-write (durável): persiste no banco + sinaliza (2 escritas).
  B ) MongoDB Change Stream (single-write, durável): 1 escrita; o stream deriva dela.

HONESTIDADE (a pedido do time): os números ABSOLUTOS deste ambiente são
dominados pela rede — o cluster Atlas está a ~1 RTT de distância do laptop
(cross-region). Esse RTT é pago pelos DOIS caminhos, pois ambos escrevem no
mesmo Atlas. Por isso a demo separa:
  • números absolutos reais (network-bound) e
  • a projeção co-localizada (app + cluster na mesma região), onde o SLA de
    100ms é o que vale em produção.

Onde o Redis GANHA: no sinal isolado (memória, sub-ms) e, no dual-write, por
~1 round-trip a menos que o change stream (não espera propagação). Esse é o
trade-off real — e ele custa durabilidade/consistência (demos 2 e 3).
"""
import asyncio
import argparse

import config
from shared import mongo
from shared.models import gerar_payload
from shared.fake_redis import FakeRedis
from caminho_a_redis import service as redis_svc
from caminho_b_changestream.dispatcher import ChangeStreamDispatcher
from caminho_b_changestream import service as mongo_svc
import demo_common as ui


async def _coletar(nome, coro_factory, n, warmup):
    lat = []
    for i in range(n + warmup):
        r = await coro_factory()
        if r.get("timeout"):
            ui.aviso(f"{nome}: job {i} deu TIMEOUT")
            continue
        if i >= warmup:
            lat.append(r["latency_ms"])
    return lat


async def run(n: int):
    ui.banner("DEMO 1 — LATÊNCIA DEVICE-FACING", "Redis vs MongoDB Change Streams · cenário Banco Inter")
    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()

    ui.secao("Baseline de rede do ambiente")
    rtt = ui.medir_rtt_atlas()
    ui.info(f"RTT p50 laptop → Atlas: {ui.BOLD}{rtt:.0f} ms{ui.RESET} "
            f"(cross-region — cada round-trip ao cluster paga isso)")
    ui.info("Em produção o app fica NA MESMA região do cluster: RTT ~1–3 ms.")

    redis = FakeRedis()
    loop = asyncio.get_running_loop()
    disp = ChangeStreamDispatcher(loop)
    disp.start(resume=False)

    ui.secao(f"Rodando {n} jobs por variante (device-facing)")

    lat_sinal = await _coletar(
        "Redis sinal-isolado",
        lambda: redis_svc.sinal_isolado(redis, gerar_payload()), n, warmup=2)
    ui.ok(f"Redis Pub/Sub (sinal isolado, NÃO durável) — {len(lat_sinal)} amostras")

    lat_dual = await _coletar(
        "Redis dual-write",
        lambda: redis_svc.request_reply(redis, gerar_payload(), variante="pubsub"), n, warmup=2)
    ui.ok(f"Redis dual-write (durável: persist + sinal) — {len(lat_dual)} amostras")

    lat_mongo = await _coletar(
        "Mongo change stream",
        lambda: mongo_svc.request_reply(disp, gerar_payload()), n, warmup=2)
    ui.ok(f"MongoDB Change Stream (single-write, durável) — {len(lat_mongo)} amostras")

    disp.stop()

    # ---- Tabela absoluta (real, network-bound) -----------------------------
    ui.secao("Latência device-facing MEDIDA (absoluta, dominada pela rede)")
    larg = [40, 8, 8, 8, 9, 9]
    print("  " + ui.BOLD + ui.linha_tabela(
        ["Variante", "p50", "p95", "p99", "Durável?", "RT Atlas"], larg) + ui.RESET)
    print("  " + "─" * (sum(larg) + 3 * 5))

    def row(nome, lat, duravel, rts, cor=""):
        p = ui.percentis(lat)
        print("  " + cor + ui.linha_tabela(
            [nome, f"{p['p50']:.0f}ms", f"{p['p95']:.0f}ms", f"{p['p99']:.0f}ms", duravel, rts],
            larg) + ui.RESET)

    row("A1) Redis Pub/Sub (sinal isolado)", lat_sinal, "✗ não", "0", ui.AMARELO)
    row("A2) Redis dual-write (persist+sinal)", lat_dual, "⚠ sim*", "2", "")
    row("B)  MongoDB Change Stream", lat_mongo, "✓ sim", "3", ui.VERDE)
    print(f"\n  {ui.DIM}* durável só se AS DUAS escritas do dual-write sucederem — ver demo 3."
          f"\n    RT Atlas = round-trips ao cluster no caminho crítico (× {rtt:.0f}ms de rede cada).{ui.RESET}")

    # ---- Projeção co-localizada (o que vale para o SLA em produção) ---------
    ui.secao("Projeção co-localizada (app + cluster na MESMA região, RTT ~2 ms)")
    RTT_REGIAO = 2.0
    p_dual = ui.percentis(lat_dual); p_mongo = ui.percentis(lat_mongo)
    # A latência device-facing é dominada pelos round-trips ao cluster. Em produção
    # co-localizada cada round-trip custa ~RTT_REGIAO → projetamos nº de round-trips
    # × RTT_REGIAO (o sinal Redis em memória é ~0). O custo do change stream vs o
    # sinal em memória, MEDIDO aqui, é a diferença de p50 (ambos fazem 2 writes; o
    # Mongo troca o sinal em memória por ~1 round-trip de propagação do oplog).
    proj_dual = 2 * RTT_REGIAO
    proj_mongo = 3 * RTT_REGIAO
    prop_cs = max(0.0, p_mongo["p50"] - p_dual["p50"])
    ui.info(f"Redis dual-write  → ~{proj_dual:5.1f} ms   "
            f"({'DENTRO' if proj_dual < config.SLA_MS else 'FORA'} do SLA de {config.SLA_MS}ms)")
    verdict = ui.VERDE + "DENTRO" + ui.RESET if proj_mongo < config.SLA_MS else ui.VERMELHO + "FORA" + ui.RESET
    ui.info(f"MongoDB Change St. → ~{proj_mongo:5.1f} ms   ({verdict} do SLA de {config.SLA_MS}ms)")
    ui.info(f"Custo do change stream vs sinal em memória, neste ambiente: "
            f"~{prop_cs:.0f}ms (≈ 1 round-trip de propagação do oplog).")

    ui.secao("Leitura para o SA")
    print(f"""  {ui.DIM}• Redis TEM latência menor no sinal (memória, sub-ms) e, no dual-write,
    por ~1 round-trip a menos que o change stream. Isso é real e honesto.
  • Mas o cenário exige DURABILIDADE + AUDITORIA. Aí o Redis paga o 2º write
    e abre a janela de inconsistência (demo 3); o Pub/Sub ainda perde
    notificação sem subscriber (demo 2).
  • Co-localizado, os DOIS caminhos ficam muito abaixo de 100ms — logo a
    latência NÃO é o fator decisivo. O que decide é durabilidade e a fonte
    única confiável. O change stream entrega notificação derivada de um write
    JÁ durável, dentro do SLA.{ui.RESET}""")

    mongo.limpar_colecoes_demo(); mongo.clear_resume_token()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=15, help="jobs por variante (default 15)")
    args = ap.parse_args()
    asyncio.run(run(args.n))
