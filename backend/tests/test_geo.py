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

        def estimated_document_count(self):
            return 150_000

    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    resposta = geo.impossible_travel(limiteKmh=900, clienteId="CLI00007")
    pipeline = resposta["pipeline"]
    estagios = [chave for etapa in pipeline for chave in etapa]

    assert pipeline[0] == {"$match": {"clienteId": "CLI00007"}}
    assert "$setWindowFields" in estagios
    janela = next(e["$setWindowFields"] for e in pipeline if "$setWindowFields" in e)
    assert janela["partitionBy"] == "$clienteId"
    assert janela["sortBy"] == {"ts": 1}
    assert set(janela["output"]) == {
        "ts_ant", "coord_ant", "municipio_ant", "uf_ant", "dispositivo_ant", "localizacao_meta_ant",
    }
    # O cálculo tem de ficar no cluster: nada de $function.
    assert "$function" not in str(pipeline)
    assert "$degreesToRadians" in str(pipeline) and "$asin" in str(pipeline)
    assert resposta["limite_kmh"] == 900
    assert resposta["natureza"] == "sinal_de_risco_retrospectivo"
    assert resposta["decisao_fraude"] is False


def test_haversine_cabe_em_uma_unica_stage():
    """Quatro $addFields encadeados custavam quatro passadas sobre a janela."""
    stages = geo._haversine_stages("km", 1, 2, 3, 4)
    assert len(stages) == 1
    externo = stages[0]["$addFields"]["km"]["$let"]
    assert set(externo["vars"]) == {"phi1", "phi2", "dphi", "dlmb"}
    interno = externo["in"]["$let"]
    assert interno["in"]["$multiply"][0] == pytest.approx(2 * geo.RAIO_TERRA_KM)
    # $min contra 1 evita que arredondamento estoure o domínio do $asin.
    assert interno["in"]["$multiply"][1]["$asin"]["$sqrt"] == {"$min": ["$$a", 1]}


def test_impossible_travel_corta_pelo_limite_geometrico(monkeypatch):
    """Nenhum par na Terra dista mais que meia circunferência: intervalos longos
    não podem violar o limite, e são descartados antes do haversine."""

    class FalsaColecao:
        def aggregate(self, pipeline, **_kwargs):
            return iter([])

        def estimated_document_count(self):
            return 150_000

    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    pipeline = geo.impossible_travel(limiteKmh=900, clienteId=None)["pipeline"]

    # O corte vive no ramo de sinais do $facet: o ramo `avaliados` precisa contar
    # os pares ANTES dele, senão o denominador da taxa de sinalização seria o
    # subconjunto que já passou pelo filtro — e a taxa daria sempre alta.
    facet = next(e["$facet"] for e in pipeline if "$facet" in e)
    assert facet["avaliados"] == [{"$count": "pares"}]
    ramo = facet["sinais"]

    corte = next(e["$match"]["minutos"] for e in ramo
                 if "$match" in e and "minutos" in e["$match"])
    esperado = (math.pi * geo.RAIO_TERRA_KM / 900) * 60
    assert corte["$gt"] == 0
    assert corte["$lt"] == pytest.approx(esperado)
    # O corte precisa vir antes da parte cara.
    indice_corte = next(i for i, e in enumerate(ramo)
                        if "$match" in e and "minutos" in e["$match"])
    indice_haversine = next(i for i, e in enumerate(ramo)
                            if "$addFields" in e and "km" in e["$addFields"])
    assert indice_corte < indice_haversine
    # O $setWindowFields, que é a parte cara, roda uma vez só — antes do $facet.
    assert any("$setWindowFields" in e for e in pipeline)
    assert not any("$setWindowFields" in e for e in ramo)


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
    grupo = next(e["$group"] for e in resposta["pipeline"] if "$group" in e)
    assert grupo["_id"] == "$dispositivo.id"
    assert next(i for i, e in enumerate(resposta["pipeline"]) if "$group" in e) < next(
        i for i, e in enumerate(resposta["pipeline"]) if "$limit" in e
    )
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
    assert all(d["datasetVersion"] == seed_geo.VERSAO_DATASET for d in a)
    # A coordenada é do terminal do adquirente, não do aparelho: é isso que
    # torna o impossible travel um sinal forte em vez de um palpite sobre GPS.
    assert all(d["dispositivo"]["canal"] == "POS_PRESENCIAL" for d in a)
    assert all(d["localizacaoMeta"]["origem"] == "TERMINAL_ADQUIRENTE" for d in a)
    assert all(d["localizacaoMeta"]["qualidade"] == "CADASTRAL" for d in a)
    assert all("precisaoMetros" not in d["localizacaoMeta"] for d in a)
    assert all(d["tipo"].startswith("CARTAO_") for d in a)
    assert all(d["localizacaoMeta"]["capturadaEm"] <= d["ts"] for d in a)
    # Um terminal cadastrado é entidade estável: quando reaparece, localização
    # e estabelecimento não podem mudar de uma compra para outra.
    terminais = {}
    for documento in a:
        chave = documento["dispositivo"]["id"]
        identidade = (documento["estabelecimento"]["nome"], documento["local"]["coordinates"])
        assert terminais.setdefault(chave, identidade) == identidade


def test_dataset_atual_exige_volume_e_versao(monkeypatch):
    class FalsaColecao:
        def __init__(self, total, divergente):
            self.total = total
            self.divergente = divergente

        def count_documents(self, _filtro):
            return self.total

        def find_one(self, filtro, projecao):
            assert filtro == {"datasetVersion": {"$ne": seed_geo.VERSAO_DATASET}}
            assert projecao == {"_id": 1}
            return self.divergente

    class ManifestoPresente:
        @staticmethod
        def exists():
            return True

        @staticmethod
        def read_text(encoding):
            assert encoding == "utf-8"
            return ('{"dataset_version": %d, "semente": 20260726}'
                        % seed_geo.VERSAO_DATASET)

    monkeypatch.setattr(seed_geo, "ARQUIVO_FRAUDES", ManifestoPresente())
    assert seed_geo.dataset_atual(FalsaColecao(40, None), 40)[0] is True
    assert seed_geo.dataset_atual(FalsaColecao(39, None), 40)[0] is False
    assert seed_geo.dataset_atual(FalsaColecao(40, {"_id": 1}), 40)[0] is False


def test_preflight_geo_e_read_only_e_exige_search_ready(monkeypatch):
    class Metadata:
        def find_one(self, filtro):
            assert filtro == {"_id": seed_geo.ID_METADATA}
            return {
                "datasetVersion": seed_geo.VERSAO_DATASET,
                "semente": seed_geo.SEMENTE,
                "documentos": 40,
            }

    class Banco:
        def __getitem__(self, nome):
            assert nome == "demo_metadata"
            return Metadata()

    class Colecao:
        def estimated_document_count(self):
            return 40

        def list_indexes(self):
            return [{"name": nome} for nome in seed_geo.INDICES_OBRIGATORIOS]

        def list_search_indexes(self, nome):
            assert nome == "idx_geo_estabelecimento"
            return [{"name": nome, "status": "READY", "queryable": True}]

    monkeypatch.delenv("GEO_SEARCH_INDEX", raising=False)
    pronto, problemas = seed_geo.verificar_prontidao(Banco(), Colecao(), 40)
    assert pronto is True
    assert problemas == []


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


def test_impossible_travel_marca_pares_plantados_e_calcula_seletividade(monkeypatch):
    """Contar sinais sem denominador não responde "quantos alertas por dia".

    E apresentar um par plantado sem dizer que é plantado transforma a garantia
    da demo em prova — o mesmo erro que o painel em event time já evita.
    """
    class FalsaColecao:
        def aggregate(self, _pipeline, **_kwargs):
            return iter([{
                "avaliados": [{"pares": 148_000}],
                "sinais": [
                    {"clienteId": "CLI00001", "km": 2691.6, "minutos": 5.0, "kmh": 32299.0},
                    {"clienteId": "CLI99999", "km": 1200.0, "minutos": 12.0, "kmh": 6000.0},
                ],
            }])

        def estimated_document_count(self):
            return 150_000

    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    monkeypatch.setattr(geo, "_clientes_plantados", lambda: {"CLI00001"})

    # `clienteId=None` explícito: chamada direta não passa pelo FastAPI, e o
    # default é um objeto `Query`, que é truthy.
    resposta = geo.impossible_travel(limiteKmh=900, clienteId=None)

    origens = {r["clienteId"]: r["origem"] for r in resposta["resultados"]}
    assert origens == {"CLI00001": "plantado", "CLI99999": "emergente"}
    assert resposta["origem"] == {"plantados": 1, "emergentes": 1, "nota": resposta["origem"]["nota"]}

    sel = resposta["seletividade"]
    assert sel["pares_avaliados"] == 148_000
    assert sel["sinalizados"] == 2
    assert sel["taxa_pct"] == pytest.approx(0.0014, abs=1e-4)
    # Arredondado para duas casas: a tela mostra "alertas por dia", não fração.
    assert sel["alertas_por_dia"] == round(2 / geo.DIAS_DATASET, 2)


def test_seletividade_por_cliente_nao_reporta_alertas_por_dia(monkeypatch):
    """Um recorte de um cliente não é base para volume operacional diário."""
    class FalsaColecao:
        def aggregate(self, _pipeline, **_kwargs):
            return iter([{"avaliados": [{"pares": 74}], "sinais": []}])

        def estimated_document_count(self):
            return 150_000

    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    monkeypatch.setattr(geo, "_clientes_plantados", set)

    sel = geo.impossible_travel(limiteKmh=900, clienteId="CLI00007")["seletividade"]

    assert sel["pares_avaliados"] == 74
    assert sel["alertas_por_dia"] is None
    assert sel["janela_dias"] is None


def test_clientes_plantados_sem_arquivo_nao_quebra(monkeypatch, tmp_path):
    monkeypatch.setattr(geo, "ARQUIVO_FRAUDES", tmp_path / "inexistente.json")
    assert geo._clientes_plantados() == set()


def test_search_sem_termo_pergunta_o_entorno_e_ordena_por_distancia(monkeypatch):
    """A investigação começa numa compra, não num termo.

    Sem consulta textual todos os documentos empatam no score, então "os mais
    relevantes" seria uma ordem arbitrária — a pergunta, aí, é geográfica.
    """
    class FalsaColecao:
        def aggregate(self, _pipeline, **_kwargs):
            return iter([])

    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    monkeypatch.setattr(geo, "colecao", FalsaColecao())

    resposta = geo.geo_search(geo.SearchRequest(termo="", centro=[-46.63, -23.55], raioKm=10))

    compound = resposta["pipeline"][0]["$search"]["compound"]
    # Um compound só de `filter` devolveria tudo com score zero: precisa de ao
    # menos uma cláusula pontuável.
    assert compound["must"] == [{"exists": {"path": "estabelecimento.nome"}}]
    ordens = [e["$sort"] for e in resposta["pipeline"] if "$sort" in e]
    assert all("km_do_centro" in o for o in ordens)
    assert resposta["ordenacao"] == "distância do terminal"


def test_search_com_termo_mantem_fuzzy_e_ordena_por_relevancia(monkeypatch):
    class FalsaColecao:
        def aggregate(self, _pipeline, **_kwargs):
            return iter([])

    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    monkeypatch.setattr(geo, "colecao", FalsaColecao())

    resposta = geo.geo_search(geo.SearchRequest(termo="padaria", centro=[-46.63, -23.55]))

    assert resposta["pipeline"][0]["$search"]["compound"]["must"][0]["text"]["fuzzy"] == {"maxEdits": 1}
    assert resposta["ordenacao"] == "relevância textual"


def test_search_ancorado_na_compra_usa_a_coordenada_do_terminal(monkeypatch):
    """O centro sai do terminal da compra contestada, não de um município."""
    ancora = {
        "endToEndId": "E1", "estabelecimento": {"nome": "Auto Posto", "categoria": "combustível"},
        "municipio": "Manaus", "uf": "AM", "local": {"type": "Point", "coordinates": [-60.02, -3.10]},
        "dispositivo": {"id": "POS060107"}, "valor": "40.31",
    }

    class FalsaColecao:
        def find_one(self, filtro, _projecao):
            assert filtro == {"endToEndId": "E1"}
            return dict(ancora)

        def aggregate(self, _pipeline, **_kwargs):
            return iter([{"terminalId": "POS060107", "estabelecimento": {"nome": "Auto Posto"}},
                         {"terminalId": "POS999999", "estabelecimento": {"nome": "Vizinho"}}])

    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    monkeypatch.setattr(geo, "colecao", FalsaColecao())

    resposta = geo.geo_search(geo.SearchRequest(endToEndId="E1", raioKm=25))

    circulo = resposta["pipeline"][0]["$search"]["compound"]["filter"][0]["geoWithin"]["circle"]
    assert circulo["center"]["coordinates"] == [-60.02, -3.10]
    assert resposta["centro"] == [-60.02, -3.10]
    # O terminal da própria compra aparece na vizinhança e vem marcado.
    assert [r["e_a_ancora"] for r in resposta["resultados"]] == [True, False]


def test_search_sem_ancora_nem_centro_e_rejeitado(monkeypatch):
    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    with pytest.raises(HTTPException) as erro:
        geo.geo_search(geo.SearchRequest(termo="padaria"))
    assert erro.value.status_code == 422


def test_ancora_inexistente_devolve_404(monkeypatch):
    class FalsaColecao:
        def find_one(self, *_a, **_k):
            return None

    monkeypatch.setattr(geo, "_search_disponivel", lambda: (True, "ok"))
    monkeypatch.setattr(geo, "colecao", FalsaColecao())
    with pytest.raises(HTTPException) as erro:
        geo.geo_search(geo.SearchRequest(endToEndId="NAO-EXISTE"))
    assert erro.value.status_code == 404
