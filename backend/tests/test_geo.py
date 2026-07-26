from __future__ import annotations

import importlib.util
import math
import os
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

BACKEND = Path(__file__).resolve().parents[1]
RAIZ = BACKEND.parent
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from routers import geo  # noqa: E402


def _carregar_seed():
    """O seed vive em scripts/, fora do pacote do backend."""
    caminho = RAIZ / "scripts" / "seed_geo.py"
    spec = importlib.util.spec_from_file_location("seed_geo", caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


seed_geo = _carregar_seed()


# ── Demo A ──────────────────────────────────────────────────────────────────
def test_resumo_plano_le_o_plano_aninhado_do_sbe():
    explain = {
        "queryPlanner": {"winningPlan": {"queryPlan": {
            "stage": "FETCH",
            "inputStage": {"stage": "IXSCAN", "indexName": "cliente_status_local_idx"},
        }}},
        "executionStats": {
            "nReturned": 3, "executionTimeMillis": 7,
            "totalKeysExamined": 12, "totalDocsExamined": 3,
        },
    }
    resumo = geo._resumo_plano(explain)
    assert resumo["estagios"] == ["FETCH", "IXSCAN"]
    assert resumo["estagio_vencedor"] == "FETCH"
    assert resumo["indice_usado"] == "cliente_status_local_idx"
    assert resumo["totalKeysExamined"] == 12


def test_resumo_plano_percorre_inputstages_de_um_or():
    explain = {"queryPlanner": {"winningPlan": {
        "stage": "OR",
        "inputStages": [{"stage": "IXSCAN", "indexName": "local_2dsphere_idx"}],
    }}}
    assert geo._resumo_plano(explain)["indice_usado"] == "local_2dsphere_idx"


def test_explain_compare_rejeita_centro_fora_do_intervalo(monkeypatch):
    monkeypatch.setattr(geo, "_explain_find", lambda *_a, **_k: {})
    pedido = geo.ExplainRequest(clienteId="CLI00000", centro=[-200.0, 0.0])
    with pytest.raises(HTTPException) as exc:
        geo.explain_compare(pedido)
    assert exc.value.status_code == 422


def test_explain_compare_usa_os_dois_hints(monkeypatch):
    usados = []

    def falso_explain(filtro, hint):
        usados.append(hint)
        assert "$geoWithin" in filtro["local"]
        return {"totalKeysExamined": 1}

    monkeypatch.setattr(geo, "_explain_find", falso_explain)
    resposta = geo.explain_compare(
        geo.ExplainRequest(clienteId="CLI00000", raioKm=50, centro=[-46.63, -23.55])
    )
    assert usados == [geo.INDICE_COMPOSTO, geo.INDICE_GEO_PURO]
    assert len(resposta["planos"]) == 2


# ── Demo B ──────────────────────────────────────────────────────────────────
def test_impossible_travel_monta_setwindowfields_e_haversine(monkeypatch):
    capturado = {}

    class FalsaColecao:
        def aggregate(self, pipeline, **_kwargs):
            capturado["pipeline"] = pipeline
            return iter([])

    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    resposta = geo.impossible_travel(limiteKmh=900, clienteId="CLI00007")
    pipeline = resposta["pipeline"]
    estagios = [chave for etapa in pipeline for chave in etapa]

    assert pipeline[0] == {"$match": {"clienteId": "CLI00007"}}
    assert "$setWindowFields" in estagios
    janela = next(e["$setWindowFields"] for e in pipeline if "$setWindowFields" in e)
    assert janela["partitionBy"] == "$clienteId"
    assert janela["sortBy"] == {"ts": 1}
    assert set(janela["output"]) == {"ts_ant", "coord_ant", "municipio_ant", "uf_ant"}
    # O cálculo tem de ficar no cluster: nada de $function.
    assert "$function" not in str(pipeline)
    assert "$degreesToRadians" in str(pipeline) and "$asin" in str(pipeline)
    assert resposta["limite_kmh"] == 900


def test_haversine_mql_espelha_a_formula_do_seed():
    """A fórmula usada nos stages é a mesma que o seed usa para plantar as fraudes."""
    stages = geo._haversine_stages("km", 1, 2, 3, 4)
    assert stages[-1] == {"$unset": ["_phi1", "_phi2", "_dphi", "_dlmb", "_a"]}
    assert stages[-2]["$addFields"]["km"]["$multiply"][0] == pytest.approx(2 * geo.RAIO_TERRA_KM)


# ── Demo C ──────────────────────────────────────────────────────────────────
def test_search_degrada_sem_index(monkeypatch):
    monkeypatch.setattr(geo, "_search_disponivel", lambda: (False, "índice ausente"))
    resposta = geo.geo_search(geo.SearchRequest(termo="padaria", centro=[-46.63, -23.55]))
    assert resposta["estado"] == "nao_configurado"
    assert "resultados" not in resposta


def test_search_filtra_por_geowithin_em_metros(monkeypatch):
    capturado = []

    class FalsaColecao:
        def aggregate(self, pipeline, **_kwargs):
            capturado.append(pipeline)
            return iter([])

    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    resposta = geo.geo_search(geo.SearchRequest(
        termo="padaria", centro=[-46.63, -23.55], raioKm=25, categorias=["alimentação"],
    ))

    search = resposta["pipeline"][0]["$search"]
    filtros = search["compound"]["filter"]
    circulo = filtros[0]["geoWithin"]["circle"]
    assert circulo["radius"] == 25_000  # o operador usa metros, a UI fala em km
    assert circulo["center"]["coordinates"] == [-46.63, -23.55]
    assert filtros[1]["in"]["value"] == ["alimentação"]
    assert search["compound"]["must"][0]["text"]["fuzzy"] == {"maxEdits": 1}
    # A distância volta calculada para o raio ser verificável sem confiar no operador.
    assert "km_do_centro" in str(resposta["pipeline"])
    assert "$searchMeta" in resposta["pipeline_meta"][0]


def test_search_rejeita_centro_invalido(monkeypatch):
    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    with pytest.raises(HTTPException) as exc:
        geo.geo_search(geo.SearchRequest(termo="x", centro=[0.0, 999.0]))
    assert exc.value.status_code == 422


# ── Seed ────────────────────────────────────────────────────────────────────
def test_seed_e_deterministico():
    """Idempotência depende disso: mesmo endToEndId a cada execução."""
    a, _ = seed_geo.gerar(clientes=5, por_cliente=8, fraudes=2)
    b, _ = seed_geo.gerar(clientes=5, por_cliente=8, fraudes=2)
    assert [d["endToEndId"] for d in a] == [d["endToEndId"] for d in b]
    assert len({d["endToEndId"] for d in a}) == len(a) == 40


def test_seed_planta_pares_acima_do_limite():
    documentos, plantados = seed_geo.gerar(clientes=40, por_cliente=10, fraudes=40)
    assert len(plantados) == 40

    por_cliente: dict[str, list[dict]] = {}
    for doc in documentos:
        por_cliente.setdefault(doc["clienteId"], []).append(doc)

    for cliente in plantados:
        transacoes = sorted(por_cliente[cliente], key=lambda d: d["ts"])
        velocidades = []
        for anterior, atual in zip(transacoes, transacoes[1:]):
            horas = (atual["ts"] - anterior["ts"]).total_seconds() / 3600
            (lng1, lat1) = anterior["local"]["coordinates"]
            (lng2, lat2) = atual["local"]["coordinates"]
            km = seed_geo.haversine_km(lat1, lng1, lat2, lng2)
            velocidades.append(km / horas if horas > 0 else 0)
        assert max(velocidades) > 900, f"{cliente} não tem par acima de 900 km/h"


def test_seed_mantem_pontos_dentro_dos_clusters():
    """Coordenada uniforme no bounding box do país destruiria a credibilidade."""
    documentos, _ = seed_geo.gerar(clientes=30, por_cliente=10, fraudes=0)
    centros = {nome: (lat, lng) for nome, _uf, lat, lng, _peso in seed_geo.MUNICIPIOS}
    for doc in documentos:
        lat_c, lng_c = centros[doc["municipio"]]
        lng, lat = doc["local"]["coordinates"]
        assert seed_geo.haversine_km(lat_c, lng_c, lat, lng) < 40


def test_municipio_distante_respeita_o_minimo():
    for origem in range(len(seed_geo.MUNICIPIOS)):
        destino = seed_geo.municipio_distante(origem)
        _, _, lat1, lng1, _ = seed_geo.MUNICIPIOS[origem]
        _, _, lat2, lng2, _ = seed_geo.MUNICIPIOS[destino]
        assert seed_geo.haversine_km(lat1, lng1, lat2, lng2) >= 700


def test_haversine_km_bate_com_distancia_conhecida():
    # São Paulo ↔ Rio de Janeiro: ~357 km em linha reta.
    distancia = seed_geo.haversine_km(-23.5505, -46.6333, -22.9068, -43.1729)
    assert 350 < distancia < 365
    assert math.isclose(seed_geo.haversine_km(0, 0, 0, 0), 0, abs_tol=1e-9)
