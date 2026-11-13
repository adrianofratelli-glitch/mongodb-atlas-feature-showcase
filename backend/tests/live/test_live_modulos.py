"""
Testes live: os módulos 00–08 contra o cluster real, por HTTP, num backend próprio.

Opt-in. Só rodam com `LIVE_TESTS=1` e com `MONGO_DB`, `STREAMING_DB` e `GEO_DB`
no formato `showcase_*_test`: bancos só desta PoV. Um `*_test` genérico pode ser de
outra PoV (o `/streaming/reset` apaga `<GEO_DB>.sinais_ao_vivo` inteiro, e `geo_test`
é o banco live do geo-showcase), então o teste recusa qualquer outro nome. Pré-requisito: dados
do `scripts/reset_demo.py` no mesmo `MONGO_DB`. Ver docs/adr/0001-testes-live-opt-in.md.

    LIVE_TESTS=1 MONGO_DB=showcase_poc_test STREAMING_DB=showcase_pix_test GEO_DB=showcase_geo_test \\
        backend/venv/bin/python -m pytest -m live backend/tests/live

O backend sobe numa porta livre com `ASP_ENABLED=false` e Kafka apontado para uma
porta morta: o teste não liga processor (custo) nem disputa o consumer group de uma
demo aberta. `LIVE_BASE_URL` usa um backend já no ar em vez de subir outro.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

httpx = pytest.importorskip("httpx")

pytestmark = pytest.mark.live

BACKEND = Path(__file__).resolve().parents[2]
BANCOS = ("MONGO_DB", "STREAMING_DB", "GEO_DB")


def _banco_proprio(nome: str | None) -> bool:
    return bool(nome) and nome.startswith("showcase_") and nome.endswith("_test")


def _motivo_para_pular() -> str | None:
    if os.environ.get("LIVE_TESTS") != "1":
        return "testes live desligados (LIVE_TESTS=1 liga)"
    fora = [f"{v}={os.environ.get(v)!r}" for v in BANCOS if not _banco_proprio(os.environ.get(v))]
    if fora:
        return "testes live só rodam em bancos próprios showcase_*_test: " + ", ".join(fora)
    return None


_motivo = _motivo_para_pular()
if _motivo:
    pytest.skip(_motivo, allow_module_level=True)


def _porta_livre() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def api():
    base = os.environ.get("LIVE_BASE_URL")
    proc = None
    if not base:
        porta = _porta_livre()
        env = {**os.environ, "ASP_ENABLED": "false", "KAFKA_BROKERS": "127.0.0.1:1",
               "CONNECT_URL": "http://127.0.0.1:1"}
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(porta)],
            cwd=BACKEND, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        base = f"http://127.0.0.1:{porta}"
    cliente = httpx.Client(base_url=base, timeout=120)
    try:
        for _ in range(60):
            try:
                if cliente.get("/health/live").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.5)
        else:
            pytest.fail("backend de teste não respondeu /health/live em 30 s")
        checks = cliente.get("/preflight").json()["checks"]
        if not checks["mongodb"]["ok"]:
            pytest.skip(f"Atlas indisponível (cluster pausado?): {checks['mongodb']['message']}")
        if not checks["collection_produtos"]["ok"]:
            pytest.skip(f"{os.environ['MONGO_DB']} sem dados: rode scripts/reset_demo.py com os mesmos bancos")
        yield cliente
    finally:
        cliente.close()
        if proc:
            proc.terminate()
            proc.wait(timeout=15)


def _ok(resposta):
    assert resposta.status_code == 200, resposta.text[:300]
    return resposta.json()


def test_00_tese_mede_as_seis_capacidades(api):
    medida = _ok(api.post("/tese/medir"))
    assert len(medida["resultados"]) >= 6
    falhas = [r for r in medida["resultados"] if not r["ok"]]
    assert not falhas, falhas
    assert all(r["p50_ms"] > 0 for r in medida["resultados"])


def test_01_indice_constroi_com_leitura_e_some_no_drop(api):
    explain = _ok(api.get("/reindexacao/explain"))
    assert explain["scan"] == "IXSCAN"

    criado = _ok(api.post("/reindexacao/create", params={"fields": ["marca", "-preco"]}))
    nome = criado["index_name"]
    assert nome.startswith("demo01_")
    try:
        for _ in range(120):
            assert _ok(api.get("/reindexacao/read-probe"))["ok"] is True
            estado = _ok(api.get("/reindexacao/build-status", params={"name": nome}))
            if estado["status"] in ("done", "exists"):
                break
            assert estado["status"] == "building", estado
            time.sleep(1)
        else:
            pytest.fail("build do índice não terminou em 2 min")
        assert nome in {i["name"] for i in _ok(api.get("/reindexacao/indexes"))["indexes"]}
    finally:
        api.delete(f"/reindexacao/drop/{nome}")
    assert nome not in {i["name"] for i in _ok(api.get("/reindexacao/indexes"))["indexes"]}


def test_01_indice_fora_do_prefixo_nao_cai(api):
    assert api.delete("/reindexacao/drop/categoria_1").status_code in (403, 409)


def test_02_hot_e_cold_somam_o_total(api):
    simulacao = _ok(api.get("/hot-cold/archive-simulation"))
    distribuicao = _ok(api.get("/hot-cold/distribution"))["distribution"]
    total = sum(linha["count"] for linha in distribuicao)
    assert simulacao["hot"]["count"] + simulacao["cold"]["count"] == total
    assert _ok(api.get("/hot-cold/query-transparent"))["cutoff_dias"] == simulacao["cutoff_dias"]


@pytest.mark.parametrize("estagio", ["lookup", "facet", "union-with", "group-advanced",
                                     "window-functions", "bucket-auto"])
def test_03_agregacoes_devolvem_dados(api, estagio):
    corpo = _ok(api.get(f"/aggregations/{estagio}"))
    dados = corpo.get("results") or corpo.get("data")
    assert dados, corpo


def test_04_strict_rejeita_e_constraint_exige_legado_corrigido(api):
    _ok(api.delete("/schema/reset"))
    _ok(api.post("/schema/step1-create-collection"))
    assert _ok(api.post("/schema/step2-insert-without-schema"))["inserted"] == 5
    _ok(api.post("/schema/step3-activate-schema"))
    assert _ok(api.post("/schema/step4-insert-invalid"))["status"] == "rejeitado"
    assert _ok(api.post("/schema/insert-valid"))["status"] == "aceito"

    status = _ok(api.get("/schema/status"))
    if not status["constraint_supported"]:
        pytest.skip("cluster abaixo do 9.0: constraint indisponível")
    bloqueado = _ok(api.post("/schema/step4-try-constraint"))
    assert bloqueado["status"] == "bloqueado_por_legado"
    assert bloqueado["legacy_invalid_count"] == 4
    assert _ok(api.post("/schema/step5-repair-legacy"))["restantes"] == 0
    assert _ok(api.post("/schema/step4-try-constraint"))["status"] == "constraint_ativo"
    assert _ok(api.get("/schema/status"))["validation_level"] == "constraint"


def test_05_change_stream_entrega_pre_image_no_update_e_no_delete(api):
    _ok(api.delete("/change-streams/clear"))
    _ok(api.post("/change-streams/start"))
    try:
        for operacao in ("insert", "update", "delete"):
            _ok(api.post("/change-streams/trigger", params={"operacao": operacao}))
        for _ in range(40):
            eventos = _ok(api.get("/change-streams/events"))["events"]
            if len(eventos) >= 3:
                break
            time.sleep(0.5)
    finally:
        _ok(api.post("/change-streams/stop"))
    por_tipo = {e["operation"]: e for e in eventos}
    assert set(por_tipo) >= {"insert", "update", "delete"}, eventos
    assert "→" in por_tipo["update"]["detalhe"]          # status antigo → novo, da pre-image
    assert por_tipo["delete"]["detalhe"].startswith("pre-image: R$ ")


def test_06_commit_grava_tres_colecoes_e_rollback_nao_grava_nada(api):
    _ok(api.post("/transactions/reset"))
    antes = _ok(api.get("/transactions/status"))
    assert _ok(api.post("/transactions/executar"))["success"] is True
    depois_commit = _ok(api.get("/transactions/status"))
    assert all(depois_commit[k] == antes[k] + 1 for k in antes), (antes, depois_commit)

    falha = _ok(api.post("/transactions/executar", params={"simular_falha": "true"}))
    assert falha["success"] is False
    assert any(passo["step"] == "ROLLBACK" for passo in falha["steps"])
    assert _ok(api.get("/transactions/status")) == depois_commit


def _abrir_sse(base_url: str, segundos: float) -> threading.Thread:
    """Abre o SSE de Change Streams do 07 (é ele que liga o consumidor) por alguns segundos."""
    def ler():
        try:
            with httpx.Client(base_url=base_url, timeout=None) as c:
                with c.stream("GET", "/streaming/changestream") as resposta:
                    fim = time.monotonic() + segundos
                    for _ in resposta.iter_lines():
                        if time.monotonic() > fim:
                            break
        except httpx.HTTPError:
            pass
    t = threading.Thread(target=ler, daemon=True)
    t.start()
    return t


def test_07_change_streams_reconcilia_e_inseridos_bate_com_a_fonte(api):
    _ok(api.post("/streaming/reset"))
    sse = _abrir_sse(str(api.base_url), segundos=5)
    for _ in range(20):
        if _ok(api.get("/streaming/changestream/status"))["aberto"]:
            break
        time.sleep(0.5)
    inicio = _ok(api.post("/streaming/generator/start", json={"tps": 30, "duration_s": 10, "modo": "individual"}))
    run_id = inicio["run_id"]
    for _ in range(60):
        if not _ok(api.get("/streaming/generator/status"))["running"]:
            break
        time.sleep(1)
    status = _ok(api.get("/streaming/generator/status"))
    assert status["escritas_rejeitadas"] == 0

    for _ in range(90):
        rec = _ok(api.get("/streaming/reconciliacao", params={"run_id": run_id}))
        if rec["change_streams"]["reconciliado"]:
            break
        time.sleep(1)
    sse.join(timeout=1)

    fonte = rec["fonte"]["inseridas"]
    assert fonte > 0
    # O contador da tela é o mesmo número que está no banco (antes: 87 × 92).
    assert status["inseridos"] == fonte, (status["inseridos"], fonte)
    cs = rec["change_streams"]
    assert cs["contagem_confere"] and cs["valor_confere"] and cs["digest_confere"], cs
    assert cs["duplicados"] == 0


def test_08_busca_fuzzy_acha_o_que_o_termo_exato_nao_acha(api):
    """Cria o índice próprio se faltar (fica no showcase_poc_test: recriar custa um build)."""
    _ok(api.post("/busca/indice"))
    for _ in range(60):
        estado = _ok(api.get("/busca/indice"))["estado"]
        if estado in ("pronto", "falhou", "indisponivel"):
            break
        time.sleep(5)
    assert estado == "pronto", estado
    fuzzy = _ok(api.get("/busca", params={"q": "notebok", "fuzzy": "true"}))
    assert fuzzy["executado"] is True and fuzzy["total"] > 0
    assert all("Notebook" in r["nome"] for r in fuzzy["resultados"])
    assert sum(b["n"] for b in fuzzy["facetas"]["categoria"]) == fuzzy["total"]
    exato = _ok(api.get("/busca", params={"q": "notebok", "fuzzy": "false"}))
    assert exato["executado"] is True and exato["total"] == 0
    estoque = _ok(api.get("/busca", params={"q": "notebook", "so_em_estoque": "true"}))
    assert estoque["resultados"] and all(r["em_estoque"] for r in estoque["resultados"])
