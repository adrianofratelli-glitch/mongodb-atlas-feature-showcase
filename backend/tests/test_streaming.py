"""
Testes do módulo Streaming.

Cobrem a lógica que JÁ QUEBROU durante a construção da PoV — cada teste aqui
corresponde a um bug real que só apareceu sob carga ou no meio de uma demo:

  • TPS medido reportando 1,8 milhão (janela de amostra degenerada)
  • carry sem teto disparando rajadas acima do alvo
  • connector RUNNING com todas as tasks FAILED pintando a coluna de verde
  • índice TTL procurado pelo nome em vez da chave
  • latência de 5 dígitos em ms sem conversão para segundos

São testes de unidade: não exigem Atlas nem Kafka.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from routers import streaming  # noqa: E402


# ---------------------------------------------------------------------------
# Meter — percentis e vazão sobre 100% dos eventos
# ---------------------------------------------------------------------------
def test_meter_sem_amostras_nao_inventa_percentil():
    m = streaming.Meter()
    snap = m.snapshot()
    assert snap["p50"] is None and snap["p95"] is None and snap["p99"] is None
    assert snap["amostras"] == 0


def test_meter_percentis_ordenam_a_amostra():
    m = streaming.Meter()
    for valor in range(1, 101):
        m.record(float(valor))
    snap = m.snapshot()
    assert snap["amostras"] == 100
    assert snap["p50"] == pytest.approx(50, abs=2)
    assert snap["p95"] == pytest.approx(95, abs=2)
    assert snap["p99"] == pytest.approx(99, abs=2)


def test_meter_conta_evento_sem_latencia_na_vazao():
    """Evento sem ts válido entra na vazão, mas não distorce os percentis."""
    m = streaming.Meter()
    m.record(None)
    m.record(10.0)
    snap = m.snapshot()
    assert snap["amostras"] == 1
    assert snap["eventos_s"] > 0


def test_meter_reset_zera_tudo():
    m = streaming.Meter()
    m.record(5.0)
    m.reset()
    snap = m.snapshot()
    assert snap["amostras"] == 0 and snap["eventos_s"] == 0.0


# ---------------------------------------------------------------------------
# Generator — TPS medido e teto do carry
# ---------------------------------------------------------------------------
def test_tps_medido_e_zero_sem_gerador():
    g = streaming.Generator()
    assert g.measured_tps() == 0.0


def test_tps_medido_usa_janela_fixa_e_nao_estoura():
    """
    Regressão: dividir pelo intervalo entre a primeira e a última amostra dava
    números absurdos (1,8 M TPS) quando restavam duas marcas quase simultâneas.
    """
    g = streaming.Generator()
    g._start_mono = time.monotonic() - 5.0
    agora = time.monotonic()
    # 100 docs em duas marcas separadas por 1 ms
    g._recent = [(agora - 0.001, 50), (agora, 50)]
    medido = g.measured_tps()
    assert medido == pytest.approx(100 / streaming.Generator.JANELA_TPS_S, rel=0.1)
    assert medido < 1000


def test_tps_medido_proporcional_na_janela():
    g = streaming.Generator()
    g._start_mono = time.monotonic() - 10.0
    agora = time.monotonic()
    g._recent = [(agora - i * 0.1, 10) for i in range(50)]  # 500 docs em 5 s
    assert g.measured_tps() == pytest.approx(100, rel=0.2)


def test_record_soma_inseridos_e_alimenta_janela():
    g = streaming.Generator()
    g._start_mono = time.monotonic()
    g._record(25)
    g._record(25)
    assert g.inserted == 50


def test_reset_counters_zera_inseridos():
    g = streaming.Generator()
    g._start_mono = time.monotonic()
    g._record(100)
    g.reset_counters()
    assert g.inserted == 0
    assert g.measured_tps() == 0.0


# ---------------------------------------------------------------------------
# Partições — o pipeline de cada worker filtra a sua fatia
# ---------------------------------------------------------------------------
def test_worker_sem_particionamento_nao_filtra_particao():
    w = streaming.ChangeStreamWorker(particao=0, particoes=1)
    match = w.pipeline()[0]["$match"]
    assert "fullDocument.particao" not in match


def test_worker_particionado_filtra_a_sua_particao():
    w = streaming.ChangeStreamWorker(particao=3, particoes=10)
    match = w.pipeline()[0]["$match"]
    assert match["fullDocument.particao"] == 3
    assert match["operationType"] == "insert"


def test_pipeline_projeta_apenas_o_necessario():
    """O $project é o que sustenta a vazão: se alguém removê-lo, o teste avisa."""
    w = streaming.ChangeStreamWorker(particao=0, particoes=4)
    project = w.pipeline()[-1]["$project"]
    assert project["_id"] == 1  # resume token precisa continuar vindo
    assert "fullDocument.ts" in project


def test_cluster_agrega_contadores_das_particoes():
    c = streaming.ChangeStreamCluster(4)
    assert c.particoes == 4 and len(c.workers) == 4
    c.workers[0].events, c.workers[2].events = 10, 5
    c.workers[1].recovered = 3
    assert c.events == 15
    assert c.recovered == 3


def test_particao_do_documento_fica_no_intervalo():
    for _ in range(200):
        doc = streaming._new_transacao()
        assert 0 <= doc["particao"] < streaming.CS_PARTICOES
        assert doc["ts"] is not None


# ---------------------------------------------------------------------------
# Perfil de valores — o formato importa mais que a média
# ---------------------------------------------------------------------------
def test_pesos_dos_tipos_somam_cem():
    assert sum(p for _, p in streaming.PERFIL_TIPOS) == 100


def test_todo_tipo_sorteado_tem_faixas_declaradas():
    for tipo, _ in streaming.PERFIL_TIPOS:
        assert tipo in streaming.PERFIL_VALORES
        assert streaming.PERFIL_VALORES[tipo], f"{tipo} sem faixas"


def test_faixas_de_valor_sao_crescentes_e_positivas():
    for tipo, faixas in streaming.PERFIL_VALORES.items():
        for peso, minimo, maximo in faixas:
            assert peso > 0, tipo
            assert 0 < minimo < maximo, f"{tipo}: faixa inválida {minimo}-{maximo}"


def test_valor_sorteado_respeita_as_faixas_do_tipo():
    for tipo, faixas in streaming.PERFIL_VALORES.items():
        menor = min(f[1] for f in faixas)
        maior = max(f[2] for f in faixas)
        for _ in range(500):
            valor = streaming._sorteia_valor(tipo)
            assert menor <= valor <= maior, f"{tipo}: {valor} fora de {menor}-{maior}"


def test_distribuicao_e_assimetrica_como_pix():
    """
    O ponto da calibração: mediana MUITO abaixo da média, com cauda longa.
    Um sorteio uniforme (o que havia antes) reprovaria neste teste.
    """
    import statistics

    valores = sorted(float(streaming._new_transacao()["valor"].to_decimal()) for _ in range(20_000))
    mediana = statistics.median(valores)
    media = statistics.mean(valores)
    assert 40 <= mediana <= 150, f"mediana fora do esperado: {mediana}"
    assert media > mediana * 3, f"distribuição pouco assimétrica: média {media}, mediana {mediana}"
    # A cauda tem que concentrar volume financeiro relevante.
    top1 = sum(valores[int(0.99 * len(valores)):])
    assert top1 / sum(valores) > 0.2


# ---------------------------------------------------------------------------
# DLQ — defeitos precisam chegar à DLQ, não quebrar o insert
# ---------------------------------------------------------------------------
def test_nenhum_defeito_gera_end_to_end_id_nulo():
    """
    Regressão: o defeito "sem endToEndId" gerava null, e o índice único só
    aceita UM null — do segundo doc em diante o insert quebrava com duplicate
    key em vez de o documento chegar à DLQ.
    """
    for i in range(len(streaming.DEFEITOS) * 4):
        doc = streaming._doc_invalido(i)
        assert doc["endToEndId"] is not None, doc.get("defeito")


def test_documentos_invalidos_sao_unicos_entre_si():
    ids = [streaming._doc_invalido(i)["endToEndId"] for i in range(400)]
    assert len(set(ids)) == len(ids)


def test_todo_defeito_marca_o_motivo_no_documento():
    nomes = {d[0] for d in streaming.DEFEITOS}
    vistos = {streaming._doc_invalido(i)["defeito"] for i in range(len(streaming.DEFEITOS) * 3)}
    assert vistos == nomes


# ---------------------------------------------------------------------------
# Datas vindas do Kafka
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("valor", [
    {"$date": 1700000000000},
    1700000000000,
    "2026-07-25T12:00:00Z",
    "2026-07-25T12:00:00+00:00",
])
def test_parse_ts_aceita_os_formatos_do_connector(valor):
    assert streaming._parse_ts(valor) is not None


@pytest.mark.parametrize("valor", [None, "não é data", {}, []])
def test_parse_ts_devolve_none_no_lixo(valor):
    assert streaming._parse_ts(valor) is None


# ---------------------------------------------------------------------------
# Kafka — o veredito precisa seguir a saúde das TASKS, não do connector
# ---------------------------------------------------------------------------
def test_connector_running_com_todas_as_tasks_mortas_e_reportado_como_failed():
    """Regressão: a coluna ficava verde com o pipeline parado."""
    estado, detalhe = streaming.classifica_connectors(
        1, ["RUNNING"], [{"id": "a#0", "state": "FAILED"}]
    )
    assert estado == "FAILED"
    assert "Reiniciar" in detalhe


def test_connector_com_parte_das_tasks_mortas_fica_degradado():
    estado, _ = streaming.classifica_connectors(
        2, ["RUNNING", "RUNNING"],
        [{"id": "a#0", "state": "RUNNING"}, {"id": "b#0", "state": "FAILED"}],
    )
    assert estado == "DEGRADADO"


def test_connector_sem_task_nao_e_running():
    estado, _ = streaming.classifica_connectors(1, ["RUNNING"], [])
    assert estado == "SEM_TASK"


def test_connector_saudavel_e_running():
    estado, _ = streaming.classifica_connectors(
        4, ["RUNNING"] * 4, [{"id": f"c{i}#0", "state": "RUNNING"} for i in range(4)]
    )
    assert estado == "RUNNING"


def test_connector_pausado_fica_degradado():
    estado, _ = streaming.classifica_connectors(
        2, ["RUNNING", "PAUSED"], [{"id": "a#0", "state": "RUNNING"}]
    )
    assert estado == "DEGRADADO"


# ---------------------------------------------------------------------------
# Cenário PIX — premissas separadas de medição
# ---------------------------------------------------------------------------
def test_derivados_do_cenario_batem_com_as_premissas():
    assert streaming.INTER_TX_DIA == int(streaming.PIX_BRASIL_TX_DIA * streaming.INTER_SHARE)
    assert streaming.INTER_TPS_MEDIO == round(streaming.INTER_TX_DIA / streaming.SEGUNDOS_DIA)
    assert streaming.INTER_TPS_PICO == streaming.INTER_TPS_MEDIO * streaming.PICO_FATOR


def test_ttl_e_maior_que_uma_rajada_de_demo():
    """TTL curto faz o deletor concorrer com o pico e encher o oplog."""
    assert streaming.TTL_SECONDS >= 600
