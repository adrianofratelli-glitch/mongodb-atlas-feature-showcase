from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from routers import transactions  # noqa: E402


def test_percentis_vazio_nao_inventa_numero():
    """Amostra vazia devolve None em tudo — nunca 0, que a UI leria como medição."""
    assert transactions._percentis([]) == {
        "p50": None, "p95": None, "p99": None, "min": None, "max": None
    }


def test_percentis_usa_medicao_real_e_nao_interpola():
    # p95 e p99 têm de cair sobre um valor efetivamente medido: interpolar
    # inventa um número que nenhuma execução produziu.
    amostras = [float(i) for i in range(1, 101)]
    p = transactions._percentis(amostras)
    assert p["min"] == 1.0
    assert p["max"] == 100.0
    assert p["p50"] == 50.5  # mediana de 1..100
    assert p["p95"] in amostras
    assert p["p99"] in amostras


def test_percentis_ordena_entrada_desordenada():
    p = transactions._percentis([9.0, 1.0, 5.0])
    assert p["min"] == 1.0 and p["max"] == 9.0


def test_limite_transacao_degrada_sem_permissao(monkeypatch):
    """
    Em vários tiers do Atlas o `getParameter` é negado ao usuário da aplicação.
    A resposta precisa dizer que não pôde ler, nunca cair no padrão de cabeça:
    um "60 s" inventado sobrevive à tela e morre na primeira pergunta.
    """
    from pymongo.errors import OperationFailure

    class AdminFalso:
        def command(self, *_args, **_kwargs):
            raise OperationFailure("not authorized")

    class ClienteFalso:
        admin = AdminFalso()

    monkeypatch.setattr(transactions, "client", ClienteFalso())
    resultado = transactions._limite_transacao()
    assert resultado["segundos"] is None
    assert resultado["fonte"] == "indisponível"
    assert resultado["motivo"] == "OperationFailure"


def test_limite_transacao_le_do_servidor(monkeypatch):
    class AdminFalso:
        def command(self, *_args, **_kwargs):
            return {"transactionLifetimeLimitSeconds": 42, "ok": 1}

    class ClienteFalso:
        admin = AdminFalso()

    monkeypatch.setattr(transactions, "client", ClienteFalso())
    assert transactions._limite_transacao() == {"segundos": 42, "fonte": "servidor"}


def test_rtt_base_mede_ping_sem_trabalho_de_banco(monkeypatch):
    """
    A linha de base tem de ser `ping` puro. Se algum dia ela passar a fazer
    trabalho de banco, o RTT reportado passa a incluir esse trabalho e o painel
    subestima quantas viagens a transação gastou.
    """
    chamadas = []

    class AdminFalso:
        def command(self, nome, *_args, **_kwargs):
            chamadas.append(nome)
            return {"ok": 1}

    class ClienteFalso:
        admin = AdminFalso()

    monkeypatch.setattr(transactions, "client", ClienteFalso())
    amostras = transactions._rtt_base_ms(n=5)
    assert len(amostras) == 5
    assert chamadas == ["ping"] * 5
    assert all(v >= 0 for v in amostras)


def test_min_amostra_protege_o_percentil():
    """
    Abaixo do piso um p95 é anedota: um GC do Python domina o percentil e o
    painel reportaria ruído como medição. O piso é validado pelo Query do
    FastAPI, então o valor precisa continuar batendo com o que a rota declara.
    """
    assert transactions.MIN_AMOSTRA >= 30
    campo = transactions.benchmark.__defaults__[0]
    pisos = [m.ge for m in campo.metadata if hasattr(m, "ge")]
    assert pisos == [transactions.MIN_AMOSTRA]


def test_colecoes_de_benchmark_nao_colidem_com_as_da_demo():
    """
    O benchmark escreve e limpa por `run_id`. Se alguma coleção dele coincidir
    com as da demonstração passo a passo, a limpeza de um apaga o outro.
    """
    assert not set(transactions.BENCH_COLLECTIONS) & set(transactions.DEMO_COLLECTIONS)
