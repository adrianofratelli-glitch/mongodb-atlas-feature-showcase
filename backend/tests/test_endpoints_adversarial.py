"""Suíte adversarial: entradas hostis, concorrência e isolamento de dado compartilhado.

Mongo stubado: nada aqui fala com cluster. Cada teste nomeia o achado da
revisão de 2026-10 que ele trava (ver _review/atlas-showcase-capacidades.md).
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from main import app  # noqa: E402
from routers import reindexacao, tese, transactions  # noqa: E402

client = TestClient(app)


class FakeIndexes:
    """`produtos` como no cluster de demo: seed + índices do marketplace de busca."""

    def __init__(self):
        self.names = ["_id_", "produto_id_1", "categoria_1", "marca_1", "subcategoria_1", "demo01_preco_1"]
        self.dropped = []

    def list_indexes(self):
        return [{"name": n, "key": {n.split("_")[0]: 1}} for n in self.names]

    def drop_index(self, name):
        self.dropped.append(name)
        self.names.remove(name)


@pytest.fixture
def produtos(monkeypatch):
    col = FakeIndexes()
    monkeypatch.setattr(reindexacao, "db", {reindexacao.COLLECTION: col})
    return col


# ── P1: "Remover" apagava índice de outra PoV no mesmo banco ─────────────────
@pytest.mark.parametrize("nome", ["marca_1", "subcategoria_1", "produto_id_1", "categoria_1"])
def test_drop_recusa_indice_que_nao_foi_criado_pela_demo(produtos, nome):
    r = client.delete(f"/reindexacao/drop/{nome}")
    assert r.status_code == 403
    assert produtos.dropped == []


def test_drop_aceita_indice_da_demo(produtos):
    r = client.delete("/reindexacao/drop/demo01_preco_1")
    assert r.status_code == 200
    assert produtos.dropped == ["demo01_preco_1"]


def test_lista_marca_quem_e_removivel(produtos):
    body = client.get("/reindexacao/indexes").json()
    removiveis = {i["name"] for i in body["indexes"] if i["removivel"]}
    assert removiveis == {"demo01_preco_1"}


@pytest.mark.parametrize("nome", ["..%2F..%2Fadmin", "a" * 300, "demo01_x;db.dropDatabase()", "%24where"])
def test_drop_com_nome_hostil_nao_chega_ao_banco(produtos, nome):
    r = client.delete(f"/reindexacao/drop/{nome}")
    assert r.status_code in (403, 404, 422)
    assert produtos.dropped == []


@pytest.mark.parametrize("campos", [
    ["{\"$gt\": \"\"}"],
    ["$where"],
    ["preco", "preco"],
    ["preco", "marca", "categoria", "created_at", "produto_id"],
    ["nome_inexistente"],
])
def test_create_index_recusa_campos_hostis(produtos, campos):
    qs = "&".join(f"fields={c}" for c in campos)
    r = client.post(f"/reindexacao/create?{qs}")
    assert r.status_code == 422


@pytest.mark.parametrize("filtro", [{"$where": "sleep(1000)"}, {"preco": {"$gt": 0}}, {"em_estoque": {"$ne": False}}])
def test_create_index_recusa_filtro_parcial_arbitrario(produtos, filtro):
    r = client.post("/reindexacao/create?fields=preco", json=filtro)
    assert r.status_code == 422


def test_json_malformado_vira_422_e_nao_500(produtos):
    r = client.post(
        "/reindexacao/create?fields=preco",
        content=b"{\"em_estoque\": tru",
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 422


def test_corpo_de_1mb_e_recusado_antes_do_router():
    r = client.post("/streaming/generator/start", content=b"x" * (1_048_576 + 1),
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 413


@pytest.mark.parametrize("params", [
    "tps=0", "tps=-5", "tps=999999999", "duration_s=1", "modo=%7B%22%24ne%22%3A1%7D", "modo=lote;drop",
])
def test_gerador_recusa_parametros_fora_do_contrato(params):
    chave, valor = params.split("=", 1)
    corpo = {chave: int(valor) if valor.lstrip("-").isdigit() else valor}
    r = client.post("/streaming/generator/start", json=corpo)
    assert r.status_code == 422


@pytest.mark.parametrize("run_id", ["a" * 65, "{\"$gt\":\"\"}", "pix 1", "../x"])
def test_reconciliacao_recusa_run_id_hostil(run_id):
    r = client.get("/streaming/reconciliacao", params={"run_id": run_id})
    assert r.status_code == 422


@pytest.mark.parametrize("categoria", ["x", "a" * 41])
def test_hotcold_categoria_fora_dos_limites(categoria):
    r = client.get("/hot-cold/query-transparent", params={"categoria": categoria})
    assert r.status_code == 422


@pytest.mark.parametrize("cenario", ["$where", "preco_negativo;drop", ""])
def test_schema_cenario_fora_da_lista(cenario):
    r = client.post("/schema/step4-insert-invalid", params={"scenario": cenario})
    assert r.status_code == 422


@pytest.mark.parametrize("op", ["drop", "$set", "insert;delete"])
def test_change_stream_trigger_so_aceita_tres_operacoes(op):
    r = client.post("/change-streams/trigger", params={"operacao": op})
    assert r.status_code == 422


@pytest.mark.parametrize("q", ["amostras=1", "amostras=100000", "concorrencia=0", "concorrencia=1000"])
def test_benchmark_limites_de_paginacao_e_carga(q):
    r = client.post(f"/transactions/benchmark?{q}")
    assert r.status_code == 422


@pytest.mark.parametrize("q", ["limit=0", "limit=21", "limit=-1", "limit=abc"])
def test_lookup_paginacao_hostil(q):
    r = client.get(f"/aggregations/lookup?{q}")
    assert r.status_code == 422


@pytest.mark.parametrize("q,esperado", [("limit=-50", 1), ("limit=999999", 100)])
def test_dlq_paginacao_hostil_e_grampeada(monkeypatch, q, esperado):
    from routers import streaming

    capturado = {}

    class Cursor:
        def limit(self, n):
            capturado["n"] = n
            return []

    class Col:
        def find(self, *_a, **_k):
            return Cursor()

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_DLQ: Col()})
    r = client.get(f"/streaming/asp/dlq?{q}")
    assert r.status_code == 200
    assert capturado["n"] == esperado


def test_change_stream_feed_last_event_id_hostil_nao_quebra(monkeypatch):
    from routers import change_streams

    class Req:
        headers = {"last-event-id": "'; DROP --"}

        async def is_disconnected(self):
            return True

    import asyncio

    resp = asyncio.run(change_streams.stream_events(Req()))
    assert resp.media_type == "text/event-stream"


# ── Concorrência: duplo clique / duas abas ──────────────────────────────────
def test_benchmark_concorrente_recebe_409(monkeypatch):
    liberar = threading.Event()
    entrou = threading.Event()

    def lento(*_a, **_k):
        entrou.set()
        liberar.wait(5)
        return {"ok": True}

    monkeypatch.setattr(transactions, "_benchmark", lento)
    resultado = {}
    t = threading.Thread(target=lambda: resultado.setdefault("a", client.post("/transactions/benchmark")))
    t.start()
    assert entrou.wait(5)
    segunda = client.post("/transactions/benchmark")
    liberar.set()
    t.join(5)
    assert segunda.status_code == 409
    assert resultado["a"].status_code == 200


def test_tese_medir_concorrente_recebe_409(monkeypatch):
    liberar = threading.Event()
    entrou = threading.Event()

    def lento():
        entrou.set()
        liberar.wait(5)
        return {"ok": True}

    monkeypatch.setattr(tese, "_executar", lento)
    resultado = {}
    t = threading.Thread(target=lambda: resultado.setdefault("a", client.post("/tese/medir")))
    t.start()
    assert entrou.wait(5)
    segunda = client.post("/tese/medir")
    liberar.set()
    t.join(5)
    assert segunda.status_code == 409
    assert resultado["a"].status_code == 200


def test_tese_falha_de_mongo_vira_linha_de_erro_e_nao_500(monkeypatch):
    from pymongo.errors import ServerSelectionTimeoutError

    def falha():
        raise ServerSelectionTimeoutError("sem cluster")

    monkeypatch.setattr(tese, "MEDICOES", (("ping", "Ida e volta", falha),))

    class FakeDb:
        name = "POC_test"

        def drop_collection(self, _):
            raise ServerSelectionTimeoutError("sem cluster")

    class FakeClient:
        nodes = frozenset()

    monkeypatch.setattr(tese, "db", FakeDb())
    monkeypatch.setattr(tese, "client", FakeClient())
    r = client.post("/tese/medir")
    assert r.status_code == 200
    linha = r.json()["resultados"][0]
    assert linha["ok"] is False and linha["erro"] == "ServerSelectionTimeoutError"


def test_tese_escreve_so_em_colecoes_tese_probe():
    fonte = (BACKEND / "routers" / "tese.py").read_text(encoding="utf-8")
    for col in (tese.COL_A, tese.COL_B, tese.COL_VALIDADO):
        assert col.startswith("tese_probe")
    assert "drop_collection(\"produtos\")" not in fonte


# ── Mutações de origem hostil ───────────────────────────────────────────────
@pytest.mark.parametrize("rota", ["/tese/medir", "/transactions/benchmark", "/streaming/reset"])
def test_mutacao_de_origem_desconhecida_e_recusada(rota):
    r = client.post(rota, headers={"Origin": "https://evil.example"})
    assert r.status_code == 403


# ── Dados ausentes: endpoints degradam com mensagem, não com 500 genérico ───
def test_preflight_sem_mongo_diz_o_que_falta(monkeypatch):
    import main

    monkeypatch.setattr(main, "readiness", lambda: (False, "MongoDB indisponível: ServerSelectionTimeoutError"))

    monkeypatch.setattr(main.streaming, "preflight_atlas_admin", lambda: {"ok": False, "message": "não configurada"})
    r = client.get("/preflight")
    assert r.status_code == 503
    assert r.json()["checks"]["mongodb"]["message"].startswith("MongoDB indisponível")


def test_nenhum_regex_em_query_de_app():
    for arquivo in (BACKEND / "routers").glob("*.py"):
        assert "$regex" not in arquivo.read_text(encoding="utf-8"), arquivo.name
