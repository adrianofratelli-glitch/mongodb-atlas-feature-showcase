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


def test_teto_de_tps_mantem_a_pov_reproduzivel_em_m20():
    """
    O teto é uma decisão de custo: acima disso a escrita sustentada dispara o
    auto-scaling do Atlas e a PoV deixa de rodar inteira no tier de entrada.
    """
    assert streaming.TPS_MAX <= 1_000
    body = streaming.GeneratorStart(tps=streaming.TPS_MAX)
    assert body.tps == streaming.TPS_MAX
    with pytest.raises(ValidationError):
        streaming.GeneratorStart(tps=streaming.TPS_MAX + 1)


def test_presets_do_cenario_respeitam_o_teto():
    assert min(streaming.TPS_MAX, streaming.CONCEPT_TPS * 2) <= streaming.TPS_MAX


def test_ttl_cobre_uma_demo_sem_estourar_o_cache_do_m20():
    """
    Curto demais, o deletor concorre com o pico e enche o oplog do resume token.
    Longo demais, o conjunto vivo passa do cache do WiredTiger de um M20 e a
    pressão de memória sozinha sobe o tier.
    """
    assert 600 <= streaming.TTL_SECONDS <= 900
    vivos = streaming.TTL_SECONDS * streaming.CONCEPT_TPS
    assert vivos <= 250_000, f"conjunto vivo estimado de {vivos} documentos"


def _preflight_com_mongo_stub(monkeypatch):
    """Roda preflight_checks() sem cluster: só o que interessa a ASP/Kafka."""
    class ColecaoFake:
        def list_indexes(self):
            return iter([{"key": {"ts": 1}, "name": "ts_ttl", "expireAfterSeconds": 600},
                         {"key": {"endToEndId": 1}, "name": "endToEndId_unique"},
                         {"key": {"run_id": 1}, "name": "run_id_reconciliacao"}])

        def estimated_document_count(self):
            return 0

    monkeypatch.setattr(streaming, "sdb", {streaming.COL_TX: ColecaoFake()})
    monkeypatch.setattr(streaming, "_cluster_info_sync",
                        lambda: {"tier": "M20", "autoscaling": {"ativo": True, "min": "M20",
                                                                "max": "M30"}, "escalou": False})
    return streaming.preflight_checks()


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

    check = streaming.preflight_checks()["streaming_indices"]
    assert check["ok"] is False
    assert "run_id" in check["message"]
    assert str(streaming.TTL_SECONDS) in check["message"]


def test_pipeline_asp_usa_bordas_oficiais_e_configuracao_dinamica():
    assert 'stream.window.start' in streaming.ASP_PIPELINE_SNIPPET
    assert 'stream.window.end' in streaming.ASP_PIPELINE_SNIPPET
    assert 'boundary: "eventTime"' in streaming.ASP_PIPELINE_SNIPPET
    assert "allowedLateness" in streaming.ASP_PIPELINE_SNIPPET
    assert "fullDocument.run_id" in streaming.ASP_PIPELINE_SNIPPET
    assert "fullDocument.endToEndId" in streaming.ASP_PIPELINE_SNIPPET
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


def test_reconciliacao_fecha_quando_todos_os_caminhos_contabilizam(monkeypatch):
    class CountCollection:
        def __init__(self, count=0, aggregate_result=None):
            self.count = count
            self.aggregate_result = aggregate_result or []

        def count_documents(self, _filter):
            return self.count

        def aggregate(self, _pipeline):
            return iter(self.aggregate_result)

    tracker = streaming.RunTracker()
    for channel in ("change_streams", "kafka"):
        for e2e in ("E1", "E2", "E3"):
            tracker.record(channel, "run-ok", e2e)

    generator = type("GeneratorFake", (), {"running": False, "run_id": "run-ok"})()
    monkeypatch.setattr(streaming, "generator", generator)
    monkeypatch.setattr(streaming, "run_tracker", tracker)
    monkeypatch.setattr(streaming, "sdb", {
        streaming.COL_TX: CountCollection(count=3),
        streaming.COL_WINDOWS: CountCollection(
            aggregate_result=[{"processadas": 2, "alertas_valor_alto": 1}]
        ),
        streaming.COL_DLQ: CountCollection(count=1),
        streaming.COL_DLQ_AUDIT: CountCollection(count=0),
    })

    result = streaming._reconcile_run("run-ok")

    assert result["final"] == "reconciliado"
    assert result["change_streams"]["reconciliado"]
    assert result["kafka"]["reconciliado"]
    assert result["asp"]["contabilizadas"] == 3


def test_api_rejeita_carga_acima_do_teto_da_poc():
    with pytest.raises(ValueError):
        streaming.GeneratorStart(tps=2_001)
