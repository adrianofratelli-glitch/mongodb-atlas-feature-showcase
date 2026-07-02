"""Utilitários compartilhados: geração de correlationId e payloads do cenário Inter."""
import uuid
import random

# Cenário device-facing do Banco Inter: um dispositivo (POS / app / caixa)
# dispara um processamento e segura a conexão esperando o resultado.
_TIPOS = ["pix", "ted", "boleto", "recarga", "cartao_credito"]
_ORIGENS = ["app_mobile", "internet_banking", "pos_maquininha", "caixa_eletronico"]


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def gerar_payload() -> dict:
    return {
        "tipo": random.choice(_TIPOS),
        "origem": random.choice(_ORIGENS),
        "valor": round(random.uniform(10.0, 5000.0), 2),
    }


def gerar_resultado(payload: dict) -> dict:
    """Resultado do worker: autorização da transação (o que o device espera)."""
    return {
        "autorizado": True,
        "codigo_autorizacao": uuid.uuid4().hex[:10].upper(),
        "tipo": payload.get("tipo"),
        "valor": payload.get("valor"),
    }


class CrashInjected(Exception):
    """Crash controlado injetado no worker para as demos de falha."""
