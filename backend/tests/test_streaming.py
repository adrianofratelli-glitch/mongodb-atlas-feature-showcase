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

import asyncio
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from bson import Decimal128
from datetime import timedelta
from fastapi import HTTPException
from pydantic import ValidationError

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


def test_stop_avanca_watermark_sem_contaminar_run_id(monkeypatch):
    class Colecao:
        def __init__(self):
            self.docs = []

        def insert_one(self, doc):
            self.docs.append(doc)

    async def sem_espera(_segundos):
        return None

    colecao = Colecao()
    monkeypatch.setattr(streaming, "ASP_ENABLED", True)
    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: colecao})
    monkeypatch.setattr(streaming.asyncio, "sleep", sem_espera)
    g = streaming.Generator()
    g.run_id = "run-da-demo"
    g.inserted = 10

    asyncio.run(g.stop(advance_watermark=True))

    assert len(colecao.docs) == 1
    assert colecao.docs[0]["run_id"] == streaming.ASP_WATERMARK_RUN_ID
    assert colecao.docs[0]["run_id"] != g.run_id
    assert colecao.docs[0]["controle_demo"] == "avancar_watermark"


def test_reset_counters_zera_inseridos():
    g = streaming.Generator()
    g._start_mono = time.monotonic()
    g._record(100)
    g.reset_counters()
    assert g.inserted == 0
    assert g.measured_tps() == 0.0


def test_stop_espera_batches_ja_em_voo():
    """Reset só pode limpar a coleção depois que os insert_many pendentes terminarem."""
    concluido = False

    async def scenario():
        nonlocal concluido
        g = streaming.Generator()

        async def batch_pendente():
            nonlocal concluido
            await asyncio.sleep(0.01)
            concluido = True

        tarefa = asyncio.create_task(batch_pendente())
        g._inflight.add(tarefa)
        tarefa.add_done_callback(g._inflight.discard)
        await g.stop()

    asyncio.run(scenario())
    assert concluido


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


def test_transacao_carrega_identidade_da_execucao():
    doc = streaming._new_transacao("pix-run-42", 7)
    assert doc["run_id"] == "pix-run-42"
    assert doc["sequencia"] == 7


def test_tracker_reconcilia_unicos_e_expoe_reentrega():
    tracker = streaming.RunTracker()
    tracker.record("change_streams", "run-1", "E1")
    tracker.record("change_streams", "run-1", "E1")
    tracker.record("change_streams", "run-1", "E2")
    snap = tracker.snapshot("run-1")["change_streams"]
    assert snap["unicos"] == 2
    assert snap["duplicados"] == 1


def test_so_history_lost_invalida_resume_token():
    assert streaming._resume_token_invalido(
        streaming.OperationFailure("histórico saiu do oplog", code=286)
    )
    assert not streaming._resume_token_invalido(streaming.PyMongoError("queda transitória"))
    assert not streaming._resume_token_invalido(
        streaming.OperationFailure("não primário", code=10107)
    )


def test_checkpoint_change_stream_e_persistido_e_recarregado(monkeypatch):
    class CheckpointsFake:
        def __init__(self):
            self.doc = {"_id": "change-stream-partition-2", "resume_token": {"_data": "abc"}}

        def find_one(self, _filter):
            return self.doc

        def replace_one(self, _filter, doc, upsert=False):
            assert upsert
            self.doc = doc

        def delete_one(self, _filter):
            self.doc = None

    checkpoints = CheckpointsFake()
    monkeypatch.setattr(streaming, "sdb", {streaming.COL_CHECKPOINTS: checkpoints})
    worker = streaming.ChangeStreamWorker(particao=2, particoes=3)
    worker._load_checkpoint()
    assert worker.token == {"_data": "abc"}
    worker.token = {"_data": "def"}
    worker._persist_checkpoint(force=True)
    assert checkpoints.doc["resume_token"] == {"_data": "def"}


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


class _CursorFake:
    def __init__(self, docs):
        self.docs = list(docs)

    def limit(self, limite):
        self.docs = self.docs[:limite]
        return self

    def sort(self, *_args):
        return self

    def __iter__(self):
        return iter(self.docs)


class _CollectionFake:
    def __init__(self, docs=(), erro_insert=None):
        self.docs = list(docs)
        self.erro_insert = erro_insert
        self.removidos = []
        self.filtro_find = None
        self.substituidos = []

    def find(self, filtro=None, *_args):
        self.filtro_find = filtro
        return _CursorFake(self.docs)

    def insert_one(self, _doc):
        if self.erro_insert:
            raise self.erro_insert

    def delete_many(self, filtro):
        self.removidos.extend(filtro["_id"]["$in"])

    def replace_one(self, filtro, documento, upsert=False):
        self.substituidos.append((filtro, documento, upsert))


def test_dlq_preserva_item_quando_insert_falha_transitoriamente(monkeypatch):
    dlq = _CollectionFake([{"_id": 1, "fullDocument": {"endToEndId": "E1"}}])
    tx = _CollectionFake(erro_insert=streaming.PyMongoError("eleição em andamento"))
    monkeypatch.setattr(streaming, "sdb", {streaming.COL_DLQ: dlq, streaming.COL_TX: tx})

    resultado = streaming._reprocessa_dlq(10)

    assert resultado["falharam"] == 1
    assert resultado["removidos_da_dlq"] == 0
    assert dlq.removidos == []


def test_dlq_remove_item_quando_indice_confirma_duplicidade(monkeypatch):
    dlq = _CollectionFake([{"_id": 7, "fullDocument": {"endToEndId": "E7"}}])
    tx = _CollectionFake(erro_insert=streaming.DuplicateKeyError("duplicado"))
    audit = _CollectionFake()
    monkeypatch.setattr(streaming, "sdb", {
        streaming.COL_DLQ: dlq,
        streaming.COL_TX: tx,
        streaming.COL_DLQ_AUDIT: audit,
    })

    resultado = streaming._reprocessa_dlq(10)

    assert resultado["ja_existiam"] == 1
    assert resultado["removidos_da_dlq"] == 1
    assert dlq.removidos == [7]
    assert audit.substituidos[0][1]["resultado"] == "ja_existia"


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
# Cenário PIX — prova conceitual, não sizing
# ---------------------------------------------------------------------------
def test_carga_conceitual_cabe_no_limite_da_api():
    assert 10 <= streaming.CONCEPT_TPS <= streaming.TPS_MAX


def test_teto_de_tps_limita_a_carga_de_palco_sem_virar_claim_de_capacidade():
    """
    O teto permite calibrar acima do pico Brasil, mas continua limitado.
    Nao afirma que M20 sustenta essa carga em regime de producao.
    """
    assert streaming.PIX_BRASIL_PICO_TPS <= streaming.TPS_MAX <= 15_000
    body = streaming.GeneratorStart(tps=streaming.TPS_MAX)
    assert body.tps == streaming.TPS_MAX
    with pytest.raises(ValidationError):
        streaming.GeneratorStart(tps=streaming.TPS_MAX + 1)


def test_presets_do_cenario_respeitam_o_teto():
    cenario = asyncio.run(streaming.cenario())
    assert all(1 <= preset["tps"] <= streaming.TPS_MAX for preset in cenario["presets"])
    assert cenario["default_duration_s"] == 30
    # O default segue o modo de escrita: no individual o alvo é o marco Inter,
    # porque 1 insert = 1 PIX satura o cliente perto de 1.000 TPS com os três
    # consumidores ativos. Prometer 8.000 ali mostraria "medido 1.000" na tela.
    if cenario["modo_escrita"] == "individual":
        assert cenario["default_tps"] == streaming.DEMO_TPS_INDIVIDUAL
        assert streaming.PIX_FATIA_PICO_TPS in {p["tps"] for p in cenario["presets"]}
    else:
        assert cenario["default_tps"] == 8_000
        assert 10_000 in {p["tps"] for p in cenario["presets"]}


def test_write_ack_mede_o_tempo_ate_confirmacao_do_microbatch(monkeypatch):
    class Colecao:
        def insert_many(self, docs, ordered):
            assert ordered is False
            return object()

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: Colecao()})
    streaming.meter_write_ack.reset()
    g = streaming.Generator()
    g._insert_batch([{"ts": None}])

    ack = streaming.meter_write_ack.snapshot()
    assert ack["amostras"] == 1
    assert ack["p99"] is not None


def test_referencia_pix_separa_media_de_pico_e_aplica_a_participacao():
    """A referência vem de números públicos do BCB e a fatia é configurável:
    a PoV não pode ficar amarrada a uma instituição específica."""
    assert streaming.PIX_BRASIL_MEDIA_TPS == round(streaming.PIX_RECORDE_DIA / 86_400)
    assert streaming.PIX_FATIA_MEDIA_TPS == round(
        streaming.PIX_BRASIL_MEDIA_TPS * streaming.PIX_PARTICIPACAO)
    assert streaming.PIX_FATIA_PICO_TPS == round(
        streaming.PIX_BRASIL_PICO_TPS * streaming.PIX_PARTICIPACAO)
    assert streaming.PIX_FATIA_MEDIA_TPS < streaming.PIX_FATIA_PICO_TPS
    assert 0.01 <= streaming.PIX_PARTICIPACAO <= 1.0


def test_ttl_nao_concorre_com_a_rodada_finita_e_limita_residuo():
    """
    O TTL deve ser maior que rodada + drenagem; Reset continua sendo a limpeza.
    """
    assert 180 <= streaming.TTL_SECONDS <= 300
    assert streaming.TTL_SECONDS >= streaming.DEMO_DURATION_DEFAULT_S * 6


def _preflight_com_mongo_stub(monkeypatch):
    """Roda preflight_checks() sem cluster: só o que interessa a ASP/Kafka."""
    class ColecaoFake:
        def list_indexes(self):
            return iter([{"key": {"ts": 1}, "name": "ts_ttl",
                          "expireAfterSeconds": streaming.TTL_SECONDS},
                         {"key": {"endToEndId": 1}, "name": "endToEndId_unique"},
                         {"key": {"run_id": 1}, "name": "run_id_reconciliacao"}])

        def estimated_document_count(self):
            return 0

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: ColecaoFake()})
    monkeypatch.setattr(streaming, "_cluster_info_sync",
                        lambda: {"tier": "M20", "autoscaling": {"ativo": True, "min": "M20",
                                                                "max": "M30"}, "escalou": False})
    return asyncio.run(streaming.preflight_checks())


def test_fora_do_modo_ao_vivo_asp_e_kafka_nao_reprovam(monkeypatch):
    """
    ASP e Kafka são o equipamento de GRAVAÇÃO, não o de demonstração: a aba 07
    reproduz uma execução já medida contra eles. Desligados são o estado
    correto — pintá-los de vermelho faria o pré-voo parecer quebrado justamente
    quando está como deveria, e o operador tentaria consertar o que está bom.
    """
    monkeypatch.setattr(streaming, "AO_VIVO", False)

    def nao_consulte(*a, **k):
        raise AssertionError("não deve consultar ASP/Kafka fora do modo ao vivo")

    monkeypatch.setattr(streaming, "_asp_reachable", nao_consulte)
    monkeypatch.setattr(streaming, "_connector_status_sync", nao_consulte)

    checks = _preflight_com_mongo_stub(monkeypatch)
    assert checks["streaming_asp"]["ok"] is True
    assert checks["streaming_kafka"]["ok"] is True
    assert "não provisionado" in checks["streaming_asp"]["message"]


def test_no_modo_ao_vivo_asp_quebrado_volta_a_reprovar(monkeypatch):
    """No modo de gravação o diagnóstico precisa voltar a ser exigente."""
    monkeypatch.setattr(streaming, "AO_VIVO", True)
    monkeypatch.setattr(streaming, "_asp_reachable",
                        lambda: (False, "processor pixJanelas5s=FAILED", "SP10"))
    monkeypatch.setattr(streaming, "_asp_atraso_s", lambda: None)
    monkeypatch.setattr(streaming, "_connector_status_sync",
                        lambda: {"estado": "FAILED", "detalhe": "task morta"})

    checks = _preflight_com_mongo_stub(monkeypatch)
    assert checks["streaming_asp"]["ok"] is False
    assert checks["streaming_kafka"]["ok"] is False


def _cluster_atlas(monkeypatch, tier, minimo="M20", maximo="M30"):
    """Responde a Admin API com um cluster no tier pedido."""
    class RespostaFake:
        def raise_for_status(self):
            pass

        def json(self):
            return {"stateName": "IDLE", "replicationSpecs": [{"regionConfigs": [{
                "electableSpecs": {"instanceSize": tier},
                "autoScaling": {"compute": {
                    "enabled": True, "minInstanceSize": minimo, "maxInstanceSize": maximo}},
            }]}]}

    import requests

    # dados=None invalida o cache de 60 s para o teste enxergar a resposta nova.
    streaming._cluster_cache.update(ts=0.0, dados=None)
    # settings é uma dataclass congelada: troca-se o objeto inteiro, não o campo.
    monkeypatch.setattr(streaming, "settings", SimpleNamespace(
        atlas_configured=True, atlas_project_id="p", atlas_cluster="c",
        atlas_public_key="k", atlas_private_key="s"))
    monkeypatch.setattr(requests, "get", lambda *a, **k: RespostaFake())
    return streaming._cluster_info_sync()


def test_tier_de_entrada_nao_e_tratado_como_pendencia(monkeypatch):
    """
    O preflight antigo reprovava M20 e mandava 'rodar carga para subir antes da
    demo' — escalar estava codificado como pré-requisito. A PoV é calibrada para
    o tier de entrada, então estar nele é o estado correto.
    """
    info = _cluster_atlas(monkeypatch, "M20")
    assert info["tier"] == "M20"
    assert info["escalou"] is False


def test_cluster_acima_do_tier_de_entrada_e_sinalizado(monkeypatch):
    info = _cluster_atlas(monkeypatch, "M30")
    assert info["escalou"] is True


def test_preflight_aceita_m30_dentro_do_autoscaling(monkeypatch):
    monkeypatch.setattr(streaming, "AO_VIVO", False)
    _preflight_com_mongo_stub(monkeypatch)
    monkeypatch.setattr(
        streaming,
        "_cluster_info_sync",
        lambda: {"tier": "M30", "autoscaling": {"ativo": True, "min": "M20", "max": "M30"}, "escalou": True},
    )

    checks = asyncio.run(streaming.preflight_checks())

    assert checks["cluster_tier"]["ok"] is True
    assert "M20→M30" in checks["cluster_tier"]["message"]


def test_ensure_indexes_cobre_a_contagem_da_reconciliacao(monkeypatch):
    """
    /streaming/reconciliacao conta a fonte por run_id em laço durante a demo.
    Sem índice esse count_documents é COLLSCAN e puxa a coleção viva inteira
    pelo cache a cada poll — foi o que fazia o cluster sair do M20.
    """
    criados: list[tuple] = []

    class ColecaoFake:
        def create_index(self, chave, **kwargs):
            criados.append((chave, kwargs))

        def list_indexes(self):
            return iter([{"key": {"ts": 1}, "name": streaming.TTL_INDEX_NAME,
                          "expireAfterSeconds": streaming.TTL_SECONDS}])

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: ColecaoFake()})
    streaming._ensure_indexes()

    assert any(chave == "run_id" for chave, _ in criados), criados


def test_purge_reutiliza_cliente_ja_conectado_sem_nova_resolucao_srv(monkeypatch):
    """Reset não pode depender de resolver o SRV novamente para cada coleção."""
    class ColecaoFake:
        def __init__(self):
            self.tentativas = 0

        def delete_many(self, filtro):
            assert filtro == {}
            self.tentativas += 1
            return SimpleNamespace(deleted_count=7)

        def count_documents(self, filtro, limit=None):
            assert filtro == {}
            assert limit == 1
            return 0

        def estimated_document_count(self):
            raise AssertionError("metadado defasado não pode decidir se a coleção ficou vazia")

    alvo = ColecaoFake()
    monkeypatch.setattr(streaming, "sdb", {"transacoes": alvo})

    removidos, restantes = streaming._purge("transacoes")

    assert (removidos, restantes) == (7, 0)
    assert alvo.tentativas == 1


def test_purge_confirma_vazio_com_contagem_exata_e_nao_com_estimativa(monkeypatch):
    """`estimated_document_count` ainda devolve o total antigo logo após o delete.

    Confiar nele fazia o Reset responder 503 com a coleção já vazia, e o Play
    era abortado sem mensagem no meio da apresentação.
    """
    class ColecaoDefasada:
        def delete_many(self, _filtro):
            return SimpleNamespace(deleted_count=2_000)

        def count_documents(self, _filtro, limit=None):
            return 0            # a verdade

        def estimated_document_count(self):
            return 2_000        # o metadado que ainda não atualizou

    monkeypatch.setattr(streaming, "sdb", {"transacoes": ColecaoDefasada()})

    removidos, restantes = streaming._purge("transacoes")

    assert removidos == 2_000
    assert restantes == 0


def test_diagnostico_de_entrega_atribui_o_limite_a_rede_quando_o_ack_domina(monkeypatch):
    """TPS entregue muito abaixo do alvo precisa dizer de quem é o limite.

    Sem isto, apresentar por VPN mostra "medido 64 · alvo 2.000" e a plateia lê
    capacidade do Atlas onde o gargalo é o round-trip do notebook.
    """
    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"running": True, "tps_alvo": 2_000})())

    lento = streaming._diagnostico_entrega(64.0, {"p50": 366.0})
    assert lento["estado"] == "abaixo_do_alvo"
    assert lento["limitador"] == "rede_do_apresentador"

    local = streaming._diagnostico_entrega(64.0, {"p50": 3.0})
    assert local["limitador"] == "cliente_local"

    no_alvo = streaming._diagnostico_entrega(1_900.0, {"p50": 3.0})
    assert no_alvo["estado"] == "no_alvo"

    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"running": False, "tps_alvo": 0})())
    assert streaming._diagnostico_entrega(0.0, {"p50": None})["estado"] == "sem_execucao"


def test_preflight_reprova_indice_de_reconciliacao_ausente_ou_ttl_divergente(monkeypatch):
    class ColecaoFake:
        def list_indexes(self):
            return iter([
                {"key": {"ts": 1}, "name": "ts_ttl", "expireAfterSeconds": 1_800},
                {"key": {"endToEndId": 1}, "name": "endToEndId_unique"},
            ])

        def estimated_document_count(self):
            return 0

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: ColecaoFake()})
    monkeypatch.setattr(streaming, "AO_VIVO", False)
    monkeypatch.setattr(
        streaming,
        "_cluster_info_sync",
        lambda: {"tier": "M20", "autoscaling": None, "escalou": False},
    )

    check = asyncio.run(streaming.preflight_checks())["streaming_indices"]
    assert check["ok"] is False
    assert "run_id" in check["message"]
    assert str(streaming.TTL_SECONDS) in check["message"]


def test_pipeline_asp_usa_bordas_oficiais_e_configuracao_dinamica():
    assert 'stream.window.start' in streaming.ASP_PIPELINE_SNIPPET
    assert 'stream.window.end' in streaming.ASP_PIPELINE_SNIPPET
    assert 'boundary: "eventTime"' in streaming.ASP_PIPELINE_SNIPPET
    assert "allowedLateness" in streaming.ASP_PIPELINE_SNIPPET
    assert 'config: { fullDocument: "updateLookup" }' in streaming.ASP_PIPELINE_SNIPPET
    assert "$jsonSchema" in streaming.ASP_PIPELINE_SNIPPET
    assert 'interval: { size: 5, unit: "second" }' in streaming.ASP_PIPELINE_SNIPPET
    assert 'required: ["endToEndId","run_id","valor","tipo","uf"]' in streaming.ASP_PIPELINE_SNIPPET
    assert "fullDocument.run_id" in streaming.ASP_PIPELINE_SNIPPET
    assert "fullDocument.uf" in streaming.ASP_PIPELINE_SNIPPET
    assert "alertas_valor_alto" in streaming.ASP_PIPELINE_SNIPPET
    assert 'fullDocument: "required"' not in streaming.ASP_PIPELINE_SNIPPET
    assert f'db: "{streaming.STREAM_DB}"' in streaming.ASP_PIPELINE_SNIPPET


def test_asp_reachable_nao_aceita_outro_processor_started(monkeypatch):
    class AdminFake:
        @staticmethod
        def command(_command):
            return {"streamProcessors": [{"name": "outro", "state": "STARTED", "tier": "SP10"}]}

    class ClientFake:
        admin = AdminFake()

        def close(self):
            pass

    monkeypatch.setattr(streaming, "ASP_ENABLED", True)
    monkeypatch.setattr(streaming, "ASP_CONNECTION_STRING", "mongodb://spi")
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: ClientFake())

    ok, detalhe, tier = streaming._asp_reachable()

    assert not ok
    assert streaming.ASP_PROCESSOR_NAME in detalhe
    assert tier is None


def test_asp_runtime_stats_expoe_checkpoint_lag_e_estado(monkeypatch):
    class AdminFake:
        @staticmethod
        def command(command):
            assert command["getStreamProcessorStats"] == streaming.ASP_PROCESSOR_NAME
            assert command["options"]["verbose"]
            return {
                "stats": {
                    "inputMessageCount": 100,
                    "outputMessageCount": 10,
                    "dlqMessageCount": 2,
                    "changeStreamTimeDifferenceSecs": 3,
                    "stateSize": 4096,
                    "watermark": streaming._now(),
                    "operatorStats": [
                        {"maxMemoryUsage": 1000},
                        {"maxMemoryUsage": 2500},
                    ],
                }
            }

    class ClientFake:
        admin = AdminFake()

        def close(self):
            pass

    monkeypatch.setattr(streaming, "ASP_ENABLED", True)
    monkeypatch.setattr(streaming, "ASP_CONNECTION_STRING", "mongodb://spi")
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: ClientFake())

    stats = streaming._asp_runtime_stats()

    assert stats["disponivel"]
    assert stats["input"] == 100
    assert stats["lag_oplog_s"] == 3
    assert stats["state_bytes"] == 4096
    assert stats["max_memory_bytes"] == 2500


def test_asp_stop_espera_estado_terminal(monkeypatch):
    class AdminFake:
        def __init__(self):
            self.stopped = False

        def command(self, command):
            if "stopStreamProcessor" in command:
                self.stopped = True
                return {"ok": 1}
            return {
                "streamProcessors": [{
                    "name": streaming.ASP_PROCESSOR_NAME,
                    "state": "STOPPED" if self.stopped else "STARTED",
                }]
            }

    class ClientFake:
        admin = AdminFake()

        def close(self):
            pass

    monkeypatch.setattr(streaming, "ASP_ENABLED", True)
    monkeypatch.setattr(streaming, "ASP_CONNECTION_STRING", "mongodb://spi")
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: ClientFake())

    assert streaming._asp_stop_wait(timeout_s=1)


def test_janelas_asp_filtram_duas_horas_e_ordenam(monkeypatch):
    windows = _CollectionFake([{"window_end": streaming._now()}])
    monkeypatch.setattr(streaming, "sdb", {streaming.COL_WINDOWS: windows})

    resultado = asyncio.run(streaming.asp_janelas(limit=30))

    assert "window_end" in windows.filtro_find
    assert "$gte" in windows.filtro_find["window_end"]
    assert resultado["total"] == 1


class CountCollection:
    """Coleção mínima para a reconciliação: contagem, agregação e find."""

    def __init__(self, count=0, aggregate_result=None, docs=None):
        self.count = count
        self.aggregate_result = aggregate_result or []
        self.docs = docs or []

    def count_documents(self, _filter):
        return self.count

    def aggregate(self, _pipeline):
        return iter(self.aggregate_result)

    def find(self, _filter, _projection=None):
        return iter(self.docs)


def _sdb_reconciliacao(monkeypatch, *, fonte_centavos=30_000, asp_volume=200.0,
                       docs=None, processadas=2, dlq=1):
    docs = docs if docs is not None else [{"endToEndId": e} for e in ("E1", "E2", "E3")]
    monkeypatch.setattr(streaming, "sdb", {
        streaming.COL_TX: CountCollection(
            count=3,
            aggregate_result=[{
                "documentos": 3, "centavos": fonte_centavos, "nao_numericos": 0,
            }],
            docs=docs,
        ),
        streaming.COL_WINDOWS: CountCollection(
            aggregate_result=[{
                "processadas": processadas, "alertas_valor_alto": 1,
                "volume": asp_volume, "janelas": 2,
            }]
        ),
        streaming.COL_DLQ: CountCollection(count=dlq, docs=[
            {"doc": {"fullDocument": {"valor": Decimal128("100.00")}}}
        ] * dlq),
        streaming.COL_DLQ_AUDIT: CountCollection(count=0),
    })


def test_reconciliacao_fecha_quando_todos_os_caminhos_contabilizam(monkeypatch):
    tracker = streaming.RunTracker()
    for channel in ("change_streams", "kafka"):
        for e2e in ("E1", "E2", "E3"):
            tracker.record(channel, "run-ok", e2e, Decimal128("100.00"))

    generator = type("GeneratorFake", (), {"running": False, "run_id": "run-ok"})()
    monkeypatch.setattr(streaming, "generator", generator)
    monkeypatch.setattr(streaming, "run_tracker", tracker)
    _sdb_reconciliacao(monkeypatch)

    result = streaming._reconcile_run("run-ok")

    assert result["final"] == "reconciliado"
    assert result["change_streams"]["reconciliado"]
    assert result["kafka"]["reconciliado"]
    assert result["asp"]["contabilizadas"] == 3
    # Contagem, valor e conjunto conferidos — não só a quantidade.
    assert result["change_streams"]["valor_confere"] is True
    assert result["change_streams"]["digest_confere"] is True
    assert result["fonte"]["valor"] == 300.00


def test_reconciliacao_acusa_valor_divergente_com_contagem_igual(monkeypatch):
    """Contagem igual e valor diferente é transformação errada, não perda.

    É o caso que a versão anterior — só contagem — deixava passar como verde.
    """
    tracker = streaming.RunTracker()
    for channel in ("change_streams", "kafka"):
        for e2e in ("E1", "E2", "E3"):
            tracker.record(channel, "run-x", e2e, Decimal128("99.00"))

    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"running": False, "run_id": "run-x"})())
    monkeypatch.setattr(streaming, "run_tracker", tracker)
    _sdb_reconciliacao(monkeypatch)

    result = streaming._reconcile_run("run-x")

    assert result["change_streams"]["contagem_confere"] is True
    assert result["change_streams"]["valor_confere"] is False
    assert result["final"] == "em_processamento"


def test_reconciliacao_acusa_documento_trocado_pelo_digest(monkeypatch):
    """Mesma contagem, mesmo valor, um documento trocado por outro.

    Só o digest do conjunto pega este caso.
    """
    tracker = streaming.RunTracker()
    for e2e in ("E1", "E2", "E3"):
        tracker.record("change_streams", "run-y", e2e, Decimal128("100.00"))
    # O Kafka viu um documento diferente, com o mesmo valor.
    for e2e in ("E1", "E2", "E9"):
        tracker.record("kafka", "run-y", e2e, Decimal128("100.00"))

    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"running": False, "run_id": "run-y"})())
    monkeypatch.setattr(streaming, "run_tracker", tracker)
    _sdb_reconciliacao(monkeypatch)

    result = streaming._reconcile_run("run-y")

    assert result["kafka"]["contagem_confere"] is True
    assert result["kafka"]["valor_confere"] is True
    assert result["kafka"]["digest_confere"] is False
    assert result["change_streams"]["digest_confere"] is True
    assert result["final"] == "em_processamento"


def test_centavos_de_ignora_valor_nao_numerico():
    """O evento inválido injetado de propósito conta como documento, não como valor."""
    assert streaming.centavos_de(Decimal128("10.55")) == 1055
    assert streaming.centavos_de(10.55) == 1055
    assert streaming.centavos_de(3) == 300
    assert streaming.centavos_de("isto-nao-e-um-numero") is None
    assert streaming.centavos_de(None) is None
    # JSON estendido vindo do connector.
    assert streaming.centavos_de(streaming._valor_json({"$numberDecimal": "12.30"})) == 1230
    assert streaming.centavos_de(streaming._valor_json("12.30")) == 1230
    assert streaming.centavos_de(streaming._valor_json("nao-numero")) is None


def test_api_rejeita_carga_acima_do_teto_da_poc():
    with pytest.raises(ValueError):
        streaming.GeneratorStart(tps=streaming.TPS_MAX + 1)


# ---------------------------------------------------------------------------
# Parada: o sinal `stopping` vale para os dois caminhos
# ---------------------------------------------------------------------------
def test_stop_manual_sinaliza_stopping_durante_o_watermark(monkeypatch):
    """Parar pelo botão passa pela mesma espera de watermark do stop automático.

    Antes, `stopping` só era marcado no timer: o Parar manual pendurava a
    requisição pelo tempo do flush sem a UI ter como mostrar "fechando janelas".
    """
    gen = streaming.Generator()
    visto: list[bool] = []

    async def _stop_interno(**_kwargs):
        visto.append(gen.stopping)

    monkeypatch.setattr(gen, "_stop", _stop_interno)
    asyncio.run(gen.stop(advance_watermark=True))

    assert visto == [True]
    assert gen.stopping is False


def test_stop_aninhado_nao_apaga_o_sinal_antes_da_hora(monkeypatch):
    """O Parar manual cancela e aguarda o timer; o sinal é do stop mais externo."""
    gen = streaming.Generator()

    chamadas = 0

    async def _stop_interno(**_kwargs):
        nonlocal chamadas
        chamadas += 1
        if chamadas == 1:
            # Simula o stop do timer sendo aguardado pelo stop manual.
            await gen.stop(advance_watermark=False)
            assert gen.stopping is True           # o externo ainda está fechando

    monkeypatch.setattr(gen, "_stop", _stop_interno)
    asyncio.run(gen.stop(advance_watermark=True))

    assert gen.stopping is False
    assert gen._stop_depth == 0


# ---------------------------------------------------------------------------
# Ingestão medida no servidor — separar banco de rede
# ---------------------------------------------------------------------------
def test_percentis_do_histograma_usa_a_fronteira_do_bucket():
    buckets = [(64, 10), (512, 80), (3072, 10)]
    p = streaming._percentis_do_histograma(buckets)
    assert p["p50"] == 0.51      # 512 us -> ms
    assert p["p99"] == 3.07


def test_percentis_do_histograma_sem_amostra_nao_inventa():
    assert streaming._percentis_do_histograma([]) == {"p50": None, "p95": None, "p99": None}


def test_ingestao_servidor_usa_delta_e_nao_o_acumulado_do_boot(monkeypatch):
    """O histograma é acumulado desde o boot; a demo mostra só a execução."""
    leituras = iter([
        {"ops": 1000, "latency_us": 50_000_000, "hist": [(512, 900), (3072, 100)]},
        {"ops": 1100, "latency_us": 52_000_000, "hist": [(512, 900), (3072, 200)]},
    ])
    monkeypatch.setattr(streaming, "_oplatencies_writes", lambda: next(leituras))

    ing = streaming.IngestaoServidor()
    ing.marcar_inicio()
    r = ing.medir()

    assert r["disponivel"] is True
    assert r["comandos"] == 100                     # 1100 - 1000, não 1100
    assert r["media_ms_por_comando"] == 20.0        # 2 s / 100 comandos
    assert r["p50"] == 3.07                         # só o bucket que cresceu


def test_ingestao_servidor_sem_baseline_nao_reporta_numero(monkeypatch):
    monkeypatch.setattr(streaming, "_oplatencies_writes",
                        lambda: {"ops": 5, "latency_us": 10, "hist": []})
    assert streaming.IngestaoServidor().medir()["disponivel"] is False


def test_ingestao_servidor_degrada_quando_serverstatus_nao_e_permitido(monkeypatch):
    monkeypatch.setattr(streaming, "_oplatencies_writes", lambda: None)
    r = streaming.IngestaoServidor().medir()
    assert r["disponivel"] is False and "serverStatus" in r["motivo"]


# ---------------------------------------------------------------------------
# Modo individual — 1 insert = 1 PIX
# ---------------------------------------------------------------------------
def test_modo_individual_dimensiona_workers_pelo_custo_da_escrita():
    """Dimensionar pelo RTT puro entregava 70% do alvo: cada worker só faz
    1/custo escritas por segundo, e o custo real (ACK) é o dobro do ping."""
    g = streaming.Generator()
    assert g._workers_para(1000) == 21          # ceil(1000*0.020)+1
    assert g._workers_para(50) == 2             # piso
    assert g._workers_para(100_000) == g.WORKERS_MAX  # teto de contenção (async: 200)


def test_modo_individual_nunca_passa_do_ponto_de_contencao():
    """Acima de ~50 threads a vazão CAI e o p95 explode (medido)."""
    g = streaming.Generator()
    assert g._workers_para(10_000) <= g.WORKERS_MAX


def test_api_aceita_apenas_modos_conhecidos():
    assert streaming.GeneratorStart(modo="individual").modo == "individual"
    assert streaming.GeneratorStart(modo="lote").modo == "lote"
    with pytest.raises(ValidationError):
        streaming.GeneratorStart(modo="turbo")


def test_insert_um_grava_uma_transacao_e_mede_o_ack(monkeypatch):
    """No modo individual um comando é uma transação — é isso que faz
    opLatencies medir um PIX em vez de um micro-batch. A escrita usa o driver
    assíncrono: com to_thread o GIL limitava o gerador a ~1.000 TPS."""
    class ColecaoAsync:
        def __init__(self):
            self.docs = []

        async def insert_one(self, doc):
            self.docs.append(doc)

    colecao = ColecaoAsync()
    monkeypatch.setattr(streaming, "acol_tx", lambda: colecao)
    streaming.meter_write_ack.reset()

    g = streaming.Generator()
    g._start_mono = time.monotonic()
    asyncio.run(g._insert_um(streaming._new_transacao("run-x", 1)))

    assert len(colecao.docs) == 1
    assert colecao.docs[0]["ts"] is not None
    assert g.inserted == 1
    assert streaming.meter_write_ack.snapshot()["amostras"] == 1


# ---------------------------------------------------------------------------
# Failover de primary e contrato do evento
# ---------------------------------------------------------------------------
def test_failover_exige_carga_em_andamento(monkeypatch):
    """Uma eleição sem carga rodando não demonstra retomada de nada."""
    # `settings` é um dataclass congelado: troca-se o objeto inteiro.
    monkeypatch.setattr(streaming, "settings", SimpleNamespace(atlas_configured=True))
    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"running": False, "run_id": None})())

    with pytest.raises(HTTPException) as erro:
        asyncio.run(streaming.falha_failover())

    assert erro.value.status_code == 409


def test_failover_exige_credenciais_do_atlas(monkeypatch):
    monkeypatch.setattr(streaming, "settings", SimpleNamespace(atlas_configured=False))

    with pytest.raises(HTTPException) as erro:
        asyncio.run(streaming.falha_failover())

    assert erro.value.status_code == 503


def test_estender_empurra_o_fim_da_execucao():
    """A eleição dura mais que a janela de 30 s; sem esticar, o stop automático
    fecharia a rodada no meio do evento."""
    g = streaming.Generator()
    g.run_id = "run-f"
    g.started_at = streaming._now()
    g.duration_s = 30
    g.ends_at = g.started_at + timedelta(seconds=30)
    g.task = SimpleNamespace(done=lambda: False)

    async def cenario():
        return g.estender(150)

    novo_fim = asyncio.run(cenario())

    assert g.duration_s == 180
    assert (novo_fim - g.started_at).total_seconds() == pytest.approx(180, abs=1)


def test_estender_ignora_execucao_parada():
    g = streaming.Generator()
    assert g.estender(150) is None


def test_evento_fora_do_contrato_perde_o_campo_obrigatorio(monkeypatch):
    """A mudança incompatível é o campo obrigatório renomeado, não um lixo
    qualquer: é o que um Schema Registry recusaria no registro."""
    class ColecaoAsync:
        def __init__(self):
            self.docs = []

        async def insert_one(self, doc):
            self.docs.append(doc)

    colecao = ColecaoAsync()
    monkeypatch.setattr(streaming, "acol_tx", lambda: colecao)
    monkeypatch.setattr(streaming, "generator",
                        type("G", (), {"run_id": "run-c"})())

    resposta = asyncio.run(streaming.falha_schema_incompativel())
    doc = colecao.docs[0]

    assert resposta["injetado"] is True
    assert "valor" not in doc and doc["amount"] == 123.45
    # Continua sendo um documento legítimo da coleção: entra na contagem da
    # fonte e a reconciliação tem de fechar mesmo assim, via DLQ.
    assert doc["run_id"] == "run-c" and doc["endToEndId"].startswith("S")
    # E fora da soma de valores, como o outro evento inválido.
    assert streaming.centavos_de(doc.get("valor")) is None


def test_contrato_publicado_lista_os_campos_obrigatorios_do_validate():
    contrato = asyncio.run(streaming.contrato())
    obrigatorios = {c["campo"] for c in contrato["campos"] if c["obrigatorio"]}

    assert obrigatorios == {"endToEndId", "run_id", "valor", "tipo", "uf"}
    assert "dlq" in contrato["fonte"]


# --- Folga do cluster --------------------------------------------------------
#
# Estes testes existem por causa de um erro cometido durante a construção: o
# painel concluiu "cluster ocioso" lendo métricas publicadas ANTES da carga. As
# métricas de processo do Atlas saem com um a dois minutos de atraso, então o
# recorte na janela da execução é a única coisa que separa medição de retórica.

def _medicao(nome, pontos, unidade="PERCENT"):
    return {"name": nome, "units": unidade,
            "dataPoints": [{"timestamp": ts, "value": v} for ts, v in pontos]}


def test_serie_resumo_ignora_buracos_e_recorta_na_janela_da_execucao():
    from datetime import datetime, timezone

    medicao = _medicao("SYSTEM_NORMALIZED_CPU_USER", [
        ("2026-01-01T10:00:00Z", 7.0),     # antes do run: tem de sair da conta
        ("2026-01-01T10:01:00Z", None),    # buraco: max() ingênuo estourava aqui
        ("2026-01-01T10:05:00Z", 43.0),    # durante o run
        ("2026-01-01T10:06:00Z", 12.0),
    ])
    inicio = datetime(2026, 1, 1, 10, 4, tzinfo=timezone.utc)

    recortado = streaming._serie_resumo(medicao, desde=inicio)
    assert recortado["max"] == 43.0 and recortado["amostras"] == 2

    # Sem recorte, os dez minutos diluem o pico e o 7.0 anterior entra na conta.
    assert streaming._serie_resumo(medicao)["amostras"] == 3


def test_serie_resumo_sem_ponto_na_janela_devolve_none():
    from datetime import datetime, timezone

    medicao = _medicao("SYSTEM_NORMALIZED_CPU_USER", [("2026-01-01T10:00:00Z", 7.0)])
    assert streaming._serie_resumo(medicao, desde=datetime(2026, 1, 1, 10, 4, tzinfo=timezone.utc)) is None


def _folga_com(monkeypatch, medicoes, generator_fake):
    # settings é dataclass congelada: troca-se o objeto, não o campo.
    monkeypatch.setattr(streaming, "settings",
                        SimpleNamespace(atlas_configured=True, atlas_project_id="p",
                                        atlas_public_key="k", atlas_private_key="s",
                                        atlas_cluster="demo"))
    monkeypatch.setattr(streaming, "_folga_cache", {"ts": 0.0, "dados": None})
    monkeypatch.setattr(streaming, "_cluster_info_sync", lambda: {"tier": "M20"})
    monkeypatch.setattr(streaming, "generator", generator_fake)

    def atlas_get(caminho, params=None):
        if caminho == "/processes":
            return {"results": [{"id": "host:27017", "typeName": "REPLICA_PRIMARY",
                                 "userAlias": "cliente-shard-00-01.mongodb.net"}]}
        return {"measurements": medicoes}

    monkeypatch.setattr(streaming, "_atlas_get", atlas_get)
    return streaming._folga_cluster_sync()


def test_folga_nao_conclui_enquanto_as_metricas_do_run_nao_foram_publicadas(monkeypatch):
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc)
    anterior = (inicio - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=2382.2, duration_s=180,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER", [(anterior, 7.0)])], gen)

    # O bug original: com a série anterior à carga, o painel dizia "ocioso".
    assert dados["veredito"] == "metricas_pendentes"
    assert dados["cpu_max_pct"] is None


def test_folga_atribui_o_custo_quando_a_serie_cobre_a_execucao(monkeypatch):
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc) - timedelta(minutes=2)
    durante = (inicio + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=2382.2, duration_s=180,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER", [(durante, 43.0)])], gen)

    assert dados["veredito"] == "cluster_participando"
    assert dados["cpu_max_pct"] == 43.0
    # O pico sobrevive ao fim do run: sem isso o painel compara 43% com 0 TPS.
    assert dados["tps_pico"] == 2382.2
    assert "M20" in dados["detalhe"]


def test_folga_sem_execucao_nao_transforma_ociosidade_em_folga(monkeypatch):
    gen = SimpleNamespace(started_at=None, tps_pico=0.0, duration_s=None,
                          measured_tps=lambda: 0.0, tps_alvo=0)
    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER",
                                              [("2026-01-01T10:00:00Z", 3.0)])], gen)

    assert dados["veredito"] == "sem_execucao"


def test_folga_nunca_expoe_o_hostname_do_cluster(monkeypatch):
    """O host do Atlas carrega o nome do cluster, que costuma ser o do cliente.

    Este campo vai para a tela e para prints de um repositório público.
    """
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc) - timedelta(minutes=2)
    durante = (inicio + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=100.0, duration_s=180,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER", [(durante, 5.0)])], gen)

    assert "cliente" not in str(dados) and "mongodb.net" not in str(dados)


def test_folga_degrada_sem_admin_api(monkeypatch):
    monkeypatch.setattr(streaming, "settings", SimpleNamespace(atlas_configured=False))
    monkeypatch.setattr(streaming, "_folga_cache", {"ts": 0.0, "dados": None})

    assert streaming._folga_cluster_sync()["estado"] == "nao_configurado"


def test_folga_marca_piso_quando_a_execucao_e_menor_que_o_balde_de_publicacao(monkeypatch):
    """Run de 30 s cai partido entre dois intervalos de 1 min e sai diluído.

    Medido com o mesmo perfil de carga: 43% num run alinhado ao balde, 15% num
    run partido. Sem esta marcação, o segundo número vira "sobra folga".
    """
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc) - timedelta(minutes=2)
    durante = (inicio + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=2163.8, duration_s=30,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER", [(durante, 15.0)])], gen)

    assert dados["cpu_subestimada"] is True
    assert "PISO" in dados["detalhe"]
    assert "folga de sobra" not in dados["detalhe"]


def test_folga_nao_marca_piso_em_execucao_longa(monkeypatch):
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc) - timedelta(minutes=4)
    durante = (inicio + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=2400.0, duration_s=180,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    dados = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER", [(durante, 43.0)])], gen)

    assert dados["cpu_subestimada"] is False
    assert "PISO" not in dados["detalhe"]


def test_pico_de_tps_e_apurado_na_escrita_e_nao_depende_de_alguem_consultar():
    """Sem espectador, o pico ficava 0 e o painel comparava CPU com TPS nenhum."""
    gen = streaming.Generator()
    gen._start_mono = time.monotonic()

    for _ in range(10):
        gen._record(200)

    # measured_tps() NUNCA foi chamado — é justamente o cenário do bug.
    assert gen.tps_pico > 0
    assert gen.inserted == 2000


def test_folga_nao_troca_a_pontuacao_da_frase_pelo_separador_de_milhar(monkeypatch):
    """O replace na frase pronta comia as vírgulas das orações.

    Saía "trabalho real. ainda com folga. e a alavanca" na tela. O mesmo erro já
    havia sido cometido e documentado em routers/geo.py.
    """
    from datetime import datetime, timedelta, timezone

    inicio = datetime.now(timezone.utc) - timedelta(minutes=3)
    durante = (inicio + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gen = SimpleNamespace(started_at=inicio, tps_pico=1613.8, duration_s=120,
                          measured_tps=lambda: 0.0, tps_alvo=0)

    detalhe = _folga_com(monkeypatch, [_medicao("SYSTEM_NORMALIZED_CPU_USER",
                                                [(durante, 53.0)])], gen)["detalhe"]

    assert "1.614 TPS" in detalhe            # separador aplicado ao número
    assert "trabalho real, ainda com folga," in detalhe   # e só a ele
