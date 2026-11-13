"""Módulo 08 · Busca — testes offline (sem Atlas).

O ponto central: `$search` com índice inexistente devolve vazio SEM erro, então a
API só executa a busca com o índice `queryable` e nunca rotula como executado o
que não rodou.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from routers import busca  # noqa: E402


class ColecaoFake:
    def __init__(self, indices=None, erro_listar=None):
        self.indices = list(indices or [])
        self.erro_listar = erro_listar
        self.criados = []
        self.pipelines = []

    def list_search_indexes(self, nome=None):
        if self.erro_listar:
            raise self.erro_listar
        return [i for i in self.indices if nome is None or i["name"] == nome]

    def create_search_index(self, modelo):
        doc = modelo.document
        self.criados.append(doc)
        self.indices.append({"name": doc["name"], "status": "PENDING", "queryable": False})
        return doc["name"]

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        if "$searchMeta" in pipeline[0]:
            return iter([{
                "count": {"total": 2},
                "facet": {
                    "categoria": {"buckets": [{"_id": "Eletrônicos", "count": 2}]},
                    "marca": {"buckets": [{"_id": "Alpha", "count": 1}, {"_id": "Beta", "count": 1}]},
                },
            }])
        return iter([
            {"nome": "Notebook Alpha Pro", "categoria": "Eletrônicos", "marca": "Alpha", "preco": 10.0,
             "em_estoque": True, "score": 1.23456},
        ])


class DbFake(dict):
    def __getitem__(self, nome):
        return dict.__getitem__(self, nome)


@pytest.fixture
def colecao(monkeypatch):
    def instalar(**kwargs):
        col = ColecaoFake(**kwargs)
        monkeypatch.setattr(busca, "db", DbFake(produtos=col))
        return col
    return instalar


PRONTO = {"name": busca.INDICE, "status": "READY", "queryable": True}


def test_indice_ausente_nao_executa_search(colecao):
    col = colecao()
    r = busca.buscar(q="notebok", fuzzy=True, so_em_estoque=False)
    assert r["executado"] is False
    assert r["indice"]["estado"] == "ausente"
    assert "pipeline" not in r  # a UI só mostra como executado o que rodou
    assert r["pipeline_nao_executado"]["resultados"][0]["$search"]["index"] == busca.INDICE
    assert col.pipelines == []


def test_indice_construindo_nao_executa(colecao):
    colecao(indices=[{"name": busca.INDICE, "status": "BUILDING", "queryable": False}])
    r = busca.buscar(q="fone", fuzzy=False, so_em_estoque=False)
    assert r["executado"] is False
    assert r["indice"]["estado"] == "construindo"


def test_indice_que_falhou_e_reportado(colecao):
    colecao(indices=[{"name": busca.INDICE, "status": "FAILED", "queryable": False}])
    assert busca.estado_indice()["estado"] == "falhou"


def test_outro_indice_de_busca_nao_conta(colecao):
    """`produtos_search` é da outra PoV no banco da demo: não serve a este módulo."""
    colecao(indices=[{"name": "produtos_search", "status": "READY", "queryable": True}])
    assert busca.estado_indice()["estado"] == "ausente"


@pytest.mark.parametrize("erro", [OperationFailure("no mongot"), ServerSelectionTimeoutError("sem cluster")])
def test_servidor_sem_search_degrada(colecao, erro):
    colecao(erro_listar=erro)
    info = busca.estado_indice()
    assert info["estado"] == "indisponivel"
    r = busca.buscar(q="x", fuzzy=True, so_em_estoque=False)
    assert r["executado"] is False
    criado = busca.criar_indice()
    assert criado["criado"] is False


def test_criar_indice_e_idempotente_e_so_cria_o_proprio(colecao):
    col = colecao(indices=[{"name": "produtos_search", "status": "READY", "queryable": True}])
    r = busca.criar_indice()
    assert r["criado"] is True
    assert r["estado"] == "construindo"
    assert [c["name"] for c in col.criados] == [busca.INDICE]
    assert col.criados[0]["definition"] == busca.DEFINICAO
    assert r["query"]["createSearchIndexes"] == "produtos"
    # Segunda chamada não cria outro.
    r2 = busca.criar_indice()
    assert r2["criado"] is False
    assert len(col.criados) == 1


def test_busca_pronta_devolve_pipeline_executado(colecao):
    col = colecao(indices=[PRONTO])
    r = busca.buscar(q="  notebok ", fuzzy=True, so_em_estoque=True)
    assert r["executado"] is True
    assert r["q"] == "notebok"
    assert r["total"] == 2
    assert r["resultados"][0]["score"] == 1.235
    assert r["facetas"]["marca"] == [{"valor": "Alpha", "n": 1}, {"valor": "Beta", "n": 1}]
    # O pipeline devolvido é o mesmo objeto que foi executado.
    assert col.pipelines == [r["pipeline"]["resultados"], r["pipeline"]["facetas"]]


def test_montar_pipelines_fuzzy_e_filtro():
    p = busca.montar_pipelines("fone", fuzzy=True, so_em_estoque=True)
    search = p["resultados"][0]["$search"]
    assert search["index"] == busca.INDICE
    texto = search["compound"]["must"][0]["text"]
    assert texto == {"query": "fone", "path": "nome", "fuzzy": {"maxEdits": 1}}
    assert search["compound"]["filter"] == [{"equals": {"path": "em_estoque", "value": True}}]
    assert p["facetas"][0]["$searchMeta"]["facet"]["operator"] == {"compound": search["compound"]}

    sem = busca.montar_pipelines("fone", fuzzy=False, so_em_estoque=False)
    comp = sem["resultados"][0]["$search"]["compound"]
    assert "fuzzy" not in comp["must"][0]["text"]
    assert "filter" not in comp


def test_definicao_e_estatica_e_sem_regex():
    assert busca.DEFINICAO["mappings"]["dynamic"] is False
    assert "$regex" not in repr(busca.montar_pipelines("a", True, True))


def test_rota_valida_tamanho_da_consulta(colecao):
    colecao(indices=[PRONTO])
    cliente = TestClient(main.app)
    assert cliente.get("/busca", params={"q": ""}).status_code == 422
    assert cliente.get("/busca", params={"q": "x" * 81}).status_code == 422
    assert cliente.get("/busca", params={"q": "fone"}).json()["executado"] is True


def test_criar_indice_passa_pela_guarda_de_mutacao(colecao):
    colecao()
    cliente = TestClient(main.app)
    r = cliente.post("/busca/indice", headers={"Origin": "https://evil.example"})
    assert r.status_code == 403


def test_preflight_indice_ausente_nao_reprova(monkeypatch, colecao):
    colecao()

    class FakeDb:
        @staticmethod
        def list_collection_names():
            return ["produtos", "avaliacoes"]

    async def _vazio():
        return {}

    monkeypatch.setattr(main, "readiness", lambda: (True, "MongoDB conectado"))
    monkeypatch.setattr(main, "db", FakeDb())
    monkeypatch.setattr(main.streaming, "preflight_checks", _vazio)
    monkeypatch.setattr(main.streaming, "preflight_atlas_admin", lambda: {"ok": True, "message": "ok"})
    r = TestClient(main.app).get("/preflight")
    assert r.status_code == 200
    assert r.json()["checks"]["busca_indice"]["ok"] is False
    assert "ausente" in r.json()["checks"]["busca_indice"]["message"]


def test_falha_entre_checagem_e_aggregate_nao_vira_500(colecao):
    col = colecao(indices=[PRONTO])

    def recusa(_pipeline):
        raise OperationFailure("index not found", code=8)
    col.aggregate = recusa
    r = busca.buscar(q="fone", fuzzy=True, so_em_estoque=False)
    assert r["executado"] is False
    assert r["indice"]["estado"] == "falhou_na_consulta"
    assert "OperationFailure" in r["aviso"] and "pipeline" not in r
    assert TestClient(main.app).get("/busca", params={"q": "fone"}).status_code == 200


def test_corrida_na_criacao_responde_criado_false(colecao):
    col = colecao()

    def ja_existe(_modelo):
        col.indices.append({"name": busca.INDICE, "status": "PENDING", "queryable": False})
        raise OperationFailure("Index already exists", code=68)
    col.create_search_index = ja_existe
    r = busca.criar_indice()
    assert r["criado"] is False
    assert r["estado"] == "construindo"
