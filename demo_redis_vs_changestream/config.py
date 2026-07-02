"""
Configuração central do módulo de demonstração Redis vs MongoDB Change Streams.

Reaproveita a MONGO_URI já configurada no backend da POC (../backend/.env),
sem duplicar credenciais e sem tocar em nada do que já existe.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Reaproveita o .env do backend existente (fonte única de credenciais do Atlas).
_BACKEND_ENV = Path(__file__).resolve().parent.parent / "backend" / ".env"
load_dotenv(_BACKEND_ENV)

# Permite um .env local do módulo sobrescrever, se existir (opcional).
load_dotenv(Path(__file__).resolve().parent / ".env", override=True)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "POC")

# Coleções EXCLUSIVAS desta demo (prefixo demo_rvc_) para não colidir com
# nenhuma coleção da POC existente. Podem ser dropadas sem risco.
COL_JOBS = "demo_rvc_jobs"                 # documento durável do job (single source of truth)
COL_RESUME = "demo_rvc_resume_tokens"      # resumeToken persistido do change stream
COL_AUDIT = "demo_rvc_auditoria"           # trilha imutável de auditoria

# Latência simulada do Redis (in-process, sem rede). Melhor-caso otimista de
# propósito: mesmo assim o Redis não persiste. ~0.3ms representa um Redis local.
REDIS_SIM_LATENCY_S = float(os.getenv("REDIS_SIM_LATENCY_S", "0.0003"))

# SLA device-facing do cenário do cliente.
SLA_MS = 100

if not MONGO_URI:
    raise RuntimeError(
        "MONGO_URI não encontrada. Verifique backend/.env "
        f"(procurado em {_BACKEND_ENV})."
    )
