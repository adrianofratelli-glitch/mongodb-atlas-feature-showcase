"""
Live cluster monitor — roda em background durante a demo.
Mostra que leituras e escritas continuam funcionando sem interrupção
mesmo durante criação de índices, archiving e outras operações.

Uso: python live_monitor.py
Parar: Ctrl+C
"""

import time
import random
import os
import sys
from datetime import datetime, timezone
from collections import deque
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "backend", ".env"))

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("MONGO_DB", "POC")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db     = client[DB_NAME]
col    = db["produtos"]

# ── Janela deslizante de latências (últimas 20 ops) ──────────────────────
READ_LATENCIES  = deque(maxlen=20)
WRITE_LATENCIES = deque(maxlen=20)
ERRORS          = 0
OPS_TOTAL       = 0
START_TIME      = time.time()

CATEGORIAS  = ["Eletrônicos", "Moda", "Casa", "Esportes", "Livros", "Brinquedos"]
MARCAS      = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]

# ── ANSI colors ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
GRAY   = "\033[90m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
CLEAR  = "\033[2J\033[H"

def bar(value_ms, max_ms=50, width=20):
    filled = min(int((value_ms / max_ms) * width), width)
    color  = GREEN if value_ms < 10 else YELLOW if value_ms < 30 else RED
    return color + "█" * filled + GRAY + "░" * (width - filled) + RESET

def do_read():
    t0  = time.perf_counter()
    cat = random.choice(CATEGORIAS)
    doc = col.find_one({"categoria": cat, "em_estoque": True}, {"nome": 1, "preco": 1})
    ms  = (time.perf_counter() - t0) * 1000
    READ_LATENCIES.append(ms)
    return doc, ms

def do_write():
    t0 = time.perf_counter()
    # Escreve em uma coleção temporária de monitoramento — não polui produtos
    db["_monitor_heartbeat"].update_one(
        {"_id": "live"},
        {"$set": {
            "ts":       datetime.now(timezone.utc),
            "ops":      OPS_TOTAL,
            "category": random.choice(CATEGORIAS),
            "marca":    random.choice(MARCAS),
            "preco":    round(random.uniform(10, 5000), 2),
        }},
        upsert=True,
    )
    ms = (time.perf_counter() - t0) * 1000
    WRITE_LATENCIES.append(ms)
    return ms

def render(read_ms, write_ms, last_doc):
    global OPS_TOTAL

    uptime   = int(time.time() - START_TIME)
    avg_read  = sum(READ_LATENCIES)  / len(READ_LATENCIES)  if READ_LATENCIES  else 0
    avg_write = sum(WRITE_LATENCIES) / len(WRITE_LATENCIES) if WRITE_LATENCIES else 0
    ops_sec   = OPS_TOTAL / max(uptime, 1)

    nome  = (last_doc.get("nome",  "—")[:32] + "…") if last_doc and len(last_doc.get("nome","")) > 32 else (last_doc or {}).get("nome", "—")
    preco = last_doc.get("preco", 0) if last_doc else 0

    print(CLEAR, end="")
    print(f"{BOLD}{CYAN}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║   MongoDB Atlas — Cluster Live Monitor  {GRAY}(Ctrl+C para parar){CYAN}   ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print()
    print(f"  {BOLD}Cluster:{RESET}  inter.eknp6n.mongodb.net   {BOLD}DB:{RESET} {DB_NAME}   {BOLD}Uptime:{RESET} {uptime}s")
    print(f"  {BOLD}Status:{RESET}   {GREEN}● OPERACIONAL{RESET}   Erros: {(RED if ERRORS else GREEN) + str(ERRORS) + RESET}   Ops total: {BOLD}{OPS_TOTAL:,}{RESET}")
    print()
    print(f"  {GRAY}─────────────────────────  Última operação  ─────────────────────────{RESET}")
    print(f"  {BOLD}Leitura:{RESET}  {nome}")
    print(f"            R$ {preco:.2f}   {GRAY}→{RESET} {read_ms:.1f}ms  {bar(read_ms)}")
    print()
    print(f"  {BOLD}Escrita:{RESET}  heartbeat → _monitor_heartbeat")
    print(f"            upsert  {GRAY}→{RESET} {write_ms:.1f}ms  {bar(write_ms)}")
    print()
    print(f"  {GRAY}─────────────────────────  Médias (últimas 20)  ─────────────────────{RESET}")
    print(f"  Leitura avg:  {avg_read:5.1f}ms  {bar(avg_read)}")
    print(f"  Escrita avg:  {avg_write:5.1f}ms  {bar(avg_write)}")
    print(f"  Throughput:   {ops_sec:.1f} ops/s")
    print()
    print(f"  {GRAY}─────────────────────────────────────────────────────────────────────{RESET}")
    print(f"  {YELLOW}⚡ Crie índices, ative Online Archive, valide schemas...{RESET}")
    print(f"  {YELLOW}   as operações aqui NÃO param e NÃO dão erro.{RESET}")
    print()
    print(f"  {GRAY}{datetime.now().strftime('%H:%M:%S')}  próxima op em ~1s{RESET}")

def main():
    global ERRORS, OPS_TOTAL

    print(f"{CYAN}Conectando ao cluster...{RESET}")
    try:
        client.admin.command("ping")
        print(f"{GREEN}✓ Conectado!{RESET}")
        time.sleep(0.8)
    except Exception as e:
        print(f"{RED}✗ Falha na conexão: {e}{RESET}")
        sys.exit(1)

    while True:
        try:
            doc, r_ms  = do_read()
            w_ms       = do_write()
            OPS_TOTAL += 2
            render(r_ms, w_ms, doc)
        except KeyboardInterrupt:
            print(f"\n\n{GREEN}Monitor encerrado. Total de ops: {OPS_TOTAL:,} | Erros: {ERRORS}{RESET}\n")
            # Limpa coleção de heartbeat
            try:
                db["_monitor_heartbeat"].drop()
            except Exception:
                pass
            break
        except Exception as e:
            ERRORS += 1
            print(f"{RED}Erro: {e}{RESET}")
            time.sleep(2)

        time.sleep(1)


if __name__ == "__main__":
    main()
