#!/usr/bin/env python3
"""
Grava UMA execução real do módulo Streaming para reprodução offline.

Por que existe: um M20 é uma instância burstable, e o auto-scaling do Atlas
dispara por CPU RELATIVA (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`), não
absoluta. Só o polling do dashboard aberto já sustenta essa métrica perto do
limite. Reproduzir uma execução gravada deixa a demo rodar com o cluster
PAUSADO, sem custo de compute.

O que é gravado é medição real: os mesmos eventos SSE e os mesmos snapshots dos
endpoints que a tela consome ao vivo. Nada é sintetizado aqui — o replay não
inventa número, ele repete o que aconteceu. A tela marca o modo replay de forma
visível; ver `frontend/src/pages/Streaming.jsx`.

Uso (backend no ar, ambiente ligado):
    python scripts/capture_replay.py                    # 60 s a 200 TPS
    python scripts/capture_replay.py --segundos 90 --tps 200
"""
from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "backend" / "data" / "replay_streaming.json"

# Snapshots: endpoint -> intervalo de amostragem em segundos. Espelha o que a
# tela realmente faz, para o replay ter o mesmo ritmo.
SNAPSHOTS = {
    "/streaming/generator/status": 1.0,
    "/streaming/kafka/status": 4.0,
    "/streaming/asp/status": 5.0,
    "/streaming/oplog": 4.0,
    "/streaming/leitura": 4.0,
    "/streaming/asp/dlq/resumo": 4.0,
}
SSE = ["/streaming/changestream", "/streaming/kafka", "/streaming/asp"]
# Feed é amostrado no backend; ainda assim limitamos para o arquivo não explodir.
MAX_EVENTOS_POR_STREAM = 4_000


class Captura:
    def __init__(self, api: str):
        self.api = api
        self.t0 = time.monotonic()
        self.lock = threading.Lock()
        self.eventos: list[dict] = []
        self.parar = threading.Event()

    def agora(self) -> float:
        return round(time.monotonic() - self.t0, 3)

    def registra(self, canal: str, tipo: str, dado) -> None:
        with self.lock:
            self.eventos.append({"t": self.agora(), "canal": canal, "tipo": tipo, "dado": dado})

    # -- HTTP ---------------------------------------------------------------
    def get(self, caminho: str):
        req = urllib.request.Request(f"{self.api}{caminho}")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    def post(self, caminho: str, corpo=None):
        dados = json.dumps(corpo).encode() if corpo is not None else b""
        req = urllib.request.Request(f"{self.api}{caminho}", data=dados, method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    # -- coletores ----------------------------------------------------------
    def coleta_sse(self, caminho: str) -> None:
        """Lê um stream SSE até `parar`, gravando cada `data:` com seu instante."""
        vistos = 0
        try:
            req = urllib.request.Request(f"{self.api}{caminho}", headers={"Accept": "text/event-stream"})
            with urllib.request.urlopen(req, timeout=None) as r:
                for linha_bruta in r:
                    if self.parar.is_set() or vistos >= MAX_EVENTOS_POR_STREAM:
                        return
                    linha = linha_bruta.decode("utf-8", "replace").strip()
                    if not linha.startswith("data:"):
                        continue
                    try:
                        payload = json.loads(linha[5:].strip())
                    except json.JSONDecodeError:
                        continue
                    self.registra(caminho, "sse", payload)
                    vistos += 1
        except Exception as exc:  # noqa: BLE001 - coletor é best-effort
            print(f"   SSE {caminho} encerrado: {type(exc).__name__}")

    def coleta_snapshot(self, caminho: str, intervalo: float) -> None:
        while not self.parar.is_set():
            inicio = time.monotonic()
            try:
                self.registra(caminho, "snapshot", self.get(caminho))
            except Exception as exc:  # noqa: BLE001
                self.registra(caminho, "erro", {"erro": type(exc).__name__})
            self.parar.wait(max(0.0, intervalo - (time.monotonic() - inicio)))

    def coleta_reconciliacao(self, run_id: str) -> None:
        caminho = f"/streaming/reconciliacao?run_id={run_id}"
        while not self.parar.is_set():
            inicio = time.monotonic()
            try:
                self.registra("/streaming/reconciliacao", "snapshot", self.get(caminho))
            except Exception as exc:  # noqa: BLE001
                pass
            self.parar.wait(max(0.0, 5.0 - (time.monotonic() - inicio)))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://127.0.0.1:8002")
    p.add_argument("--segundos", type=int, default=60, help="duração da escrita real")
    p.add_argument("--tps", type=int, default=200)
    p.add_argument("--cauda", type=int, default=150,
                   help="segundos extras de gravação após parar o gerador")
    p.add_argument("--saida", default=str(DESTINO))
    args = p.parse_args()

    cap = Captura(args.api)

    print("▶ Contexto estático (cenário, rede, cluster)…")
    estatico = {}
    for caminho in ("/streaming/cenario", "/streaming/rede", "/streaming/cluster"):
        try:
            estatico[caminho] = cap.get(caminho)
        except Exception as exc:  # noqa: BLE001
            print(f"   {caminho}: indisponível ({type(exc).__name__})")

    # Os consumidores das colunas 1 e 2 sobem sob demanda, na primeira assinatura
    # SSE. O do Kafka entra no grupo com auto.offset.reset=latest: se o gerador
    # começar antes de o ingresso terminar, o connector já publicou milhares de
    # mensagens e o consumidor as pula — a coluna Kafka fica em zero e a execução
    # nunca reconcilia. Por isso: assinar, esperar aquecer, e só então escrever.
    threads = [threading.Thread(target=cap.coleta_sse, args=(c,), daemon=True) for c in SSE]
    for t in threads:
        t.start()

    print("▶ Aguardando os consumidores aquecerem…")
    limite_aquecimento = time.monotonic() + 45
    while time.monotonic() < limite_aquecimento:
        try:
            estado = (cap.get("/streaming/kafka/status").get("consumidor") or {}).get("estado")
        except Exception:  # noqa: BLE001
            estado = None
        if estado == "consumindo":
            break
        time.sleep(2)
    else:
        print("   ⚠️ consumidor Kafka não confirmou 'consumindo'; a coluna 2 pode ficar vazia")
    # Margem depois do ingresso no grupo: `consumindo` aparece antes de o
    # consumidor ter de fato posicionado o offset.
    time.sleep(8)

    print(f"▶ Iniciando o gerador a {args.tps} TPS…")
    cap.post("/streaming/generator/start", {"tps": args.tps})
    run_id = cap.get("/streaming/generator/status")["run_id"]
    print(f"   run_id: {run_id}")

    # t0 no início da escrita: o aquecimento não faz parte da execução gravada.
    cap.t0 = time.monotonic()
    with cap.lock:
        cap.eventos.clear()

    snaps = [threading.Thread(target=cap.coleta_snapshot, args=(c, i), daemon=True)
             for c, i in SNAPSHOTS.items()]
    snaps.append(threading.Thread(target=cap.coleta_reconciliacao, args=(run_id,), daemon=True))
    for t in snaps:
        t.start()

    print(f"▶ Gravando {args.segundos}s de escrita real…")
    time.sleep(args.segundos)
    cap.post("/streaming/generator/stop")
    t_parada = cap.agora()
    print(f"   gerador parado em t={t_parada}s; gravando a drenagem…")

    # A janela do ASP usa boundary eventTime: a última janela só fecha quando um
    # evento MAIS NOVO avança a watermark. Sem isso o replay terminaria com a
    # execução eternamente "drenando". Uma rajada curta depois da parada é o que
    # acontece numa demo real quando se roda a próxima carga.
    fecho_em = None
    limite = time.monotonic() + args.cauda
    disparou_fecho = False
    while time.monotonic() < limite:
        time.sleep(5)
        try:
            rec = cap.get(f"/streaming/reconciliacao?run_id={run_id}")
        except Exception:  # noqa: BLE001
            continue
        if rec.get("final") == "reconciliado":
            fecho_em = cap.agora()
            print(f"   reconciliado em t={fecho_em}s")
            break
        if not disparou_fecho and cap.agora() - t_parada > 25:
            print("   emitindo rajada curta para avançar a watermark do ASP…")
            cap.post("/streaming/generator/start", {"tps": args.tps})
            time.sleep(10)
            cap.post("/streaming/generator/stop")
            disparou_fecho = True

    cap.parar.set()
    time.sleep(1.5)

    with cap.lock:
        eventos = sorted(cap.eventos, key=lambda e: e["t"])

    saida = {
        "versao": 1,
        "gravado_em": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "run_id": run_id,
        "tps_alvo": args.tps,
        "segundos_de_escrita": args.segundos,
        "parou_em_s": t_parada,
        "reconciliado_em_s": fecho_em,
        "duracao_s": eventos[-1]["t"] if eventos else 0,
        "estatico": estatico,
        "eventos": eventos,
    }
    destino = Path(args.saida)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(saida, ensure_ascii=False))

    canais = {}
    for e in eventos:
        canais[e["canal"]] = canais.get(e["canal"], 0) + 1
    print(f"\n✅ {destino}  ({destino.stat().st_size/1024:.0f} KB, {len(eventos)} eventos)")
    for canal, n in sorted(canais.items()):
        print(f"   {canal:<42} {n}")
    if fecho_em is None:
        print("⚠️ A execução não chegou a 'reconciliado' — o replay termina drenando.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
