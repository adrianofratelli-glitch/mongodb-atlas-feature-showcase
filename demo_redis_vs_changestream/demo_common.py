"""Utilitários de apresentação para as demos (percentis, tabelas, banner)."""
import time
import statistics

from shared import mongo

RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
VERDE = "\033[32m"; VERMELHO = "\033[31m"; AMARELO = "\033[33m"; CIANO = "\033[36m"


def banner(titulo: str, subtitulo: str = ""):
    linha = "═" * 74
    print(f"\n{CIANO}{linha}{RESET}")
    print(f"{BOLD}{CIANO}  {titulo}{RESET}")
    if subtitulo:
        print(f"{DIM}  {subtitulo}{RESET}")
    print(f"{CIANO}{linha}{RESET}")


def secao(txt: str):
    print(f"\n{BOLD}▸ {txt}{RESET}")


def ok(txt: str):    print(f"  {VERDE}✓{RESET} {txt}")
def falha(txt: str): print(f"  {VERMELHO}✗{RESET} {txt}")
def aviso(txt: str): print(f"  {AMARELO}!{RESET} {txt}")
def info(txt: str):  print(f"  {DIM}·{RESET} {txt}")


def percentis(amostras: list[float]) -> dict:
    xs = sorted(amostras)
    def p(q):
        if not xs:
            return 0.0
        i = min(len(xs) - 1, int(round(q * (len(xs) - 1))))
        return xs[i]
    return {
        "p50": statistics.median(xs) if xs else 0.0,
        "p95": p(0.95),
        "p99": p(0.99),
        "min": xs[0] if xs else 0.0,
        "max": xs[-1] if xs else 0.0,
    }


def medir_rtt_atlas(n: int = 10) -> float:
    """RTT p50 (ms) de um comando trivial ao Atlas — o baseline de rede do ambiente."""
    db = mongo.get_db()
    db.command("ping")  # aquece
    amostras = []
    for _ in range(n):
        t0 = time.perf_counter()
        db.command("ping")
        amostras.append((time.perf_counter() - t0) * 1000)
    return statistics.median(amostras)


def linha_tabela(cols: list[str], larguras: list[int]) -> str:
    return " │ ".join(c.ljust(w) for c, w in zip(cols, larguras))
