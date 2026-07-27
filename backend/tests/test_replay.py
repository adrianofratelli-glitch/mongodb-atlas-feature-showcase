"""
Testes do modo replay.

O que importa provar aqui não é só que a reprodução funciona, mas que ela não
mente: todo payload se declara `replay`, o manifesto expõe a origem, e nenhuma
escrita chega ao MongoDB. Testes de unidade — o router lê um arquivo, não o
banco, então nada aqui precisa de cluster.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from routers import replay  # noqa: E402

GRAVACAO = {
    "versao": 1,
    "gravado_em": "2026-07-27T18:00:00Z",
    "run_id": "pix-teste",
    "tps_alvo": 200,
    "segundos_de_escrita": 60,
    "parou_em_s": 60.0,
    "reconciliado_em_s": 95.0,
    "duracao_s": 100.0,
    "estatico": {"/streaming/cenario": {"premissas": {"sizing": False}, "tps_max": 1000}},
    "eventos": [
        {"t": 0.0, "canal": "/streaming/generator/status", "tipo": "snapshot",
         "dado": {"inseridos": 0, "run_id": "pix-teste"}},
        {"t": 10.0, "canal": "/streaming/generator/status", "tipo": "snapshot",
         "dado": {"inseridos": 2000, "run_id": "pix-teste"}},
        {"t": 30.0, "canal": "/streaming/generator/status", "tipo": "snapshot",
         "dado": {"inseridos": 6000, "run_id": "pix-teste"}},
        {"t": 5.0, "canal": "/streaming/changestream", "tipo": "sse",
         "dado": {"type": "evento", "endToEndId": "E1"}},
        {"t": 15.0, "canal": "/streaming/changestream", "tipo": "sse",
         "dado": {"type": "evento", "endToEndId": "E2"}},
    ],
}


@pytest.fixture
def gravado(tmp_path, monkeypatch):
    arquivo = tmp_path / "replay_streaming.json"
    arquivo.write_text(json.dumps(GRAVACAO))
    monkeypatch.setattr(replay, "ARQUIVO", arquivo)
    monkeypatch.setattr(replay, "gravacao", replay.Gravacao())
    monkeypatch.setattr(replay, "relogio", replay.Relogio())
    return replay


def test_snapshot_devolve_o_ultimo_valor_ate_o_instante(gravado):
    g = gravado.gravacao
    canal = "/streaming/generator/status"
    assert g.snapshot_em(canal, 0.0)["inseridos"] == 0
    assert g.snapshot_em(canal, 9.9)["inseridos"] == 0
    assert g.snapshot_em(canal, 10.0)["inseridos"] == 2000
    assert g.snapshot_em(canal, 29.0)["inseridos"] == 2000
    assert g.snapshot_em(canal, 999.0)["inseridos"] == 6000


def test_eventos_entre_nao_reentrega_o_mesmo_evento(gravado):
    """O SSE avança por janelas semiabertas; repetir evento inflaria contador."""
    g = gravado.gravacao
    canal = "/streaming/changestream"
    primeiro = g.eventos_entre(canal, 0.0, 10.0)
    segundo = g.eventos_entre(canal, 10.0, 20.0)
    assert [e["dado"]["endToEndId"] for e in primeiro] == ["E1"]
    assert [e["dado"]["endToEndId"] for e in segundo] == ["E2"]


def test_relogio_parado_nao_avanca(gravado):
    r = gravado.relogio
    assert r.posicao() == 0.0
    assert r.rodando is False
    r.play()
    assert r.rodando is True
    r.pause()
    parado = r.posicao()
    assert r.posicao() == parado


def test_stop_volta_para_o_inicio(gravado):
    r = gravado.relogio
    r.play()
    r.stop()
    assert r.rodando is False
    assert r.posicao() == 0.0


def test_todo_payload_se_declara_replay(gravado):
    """A tela precisa poder distinguir origem sem confiar no operador."""
    assert gravado._marca({"inseridos": 10})["replay"] is True
    assert gravado._marca({"replay": False})["replay"] is True


def test_manifesto_declara_a_origem_da_gravacao(gravado):
    import asyncio

    m = asyncio.run(gravado.manifest())
    assert m["run_id"] == "pix-teste"
    assert m["gravado_em"] == "2026-07-27T18:00:00Z"
    assert "medições" in m["origem"] and "Nenhuma escrita" in m["origem"]


def test_sem_gravacao_o_manifesto_responde_indisponivel_sem_erro(tmp_path, monkeypatch):
    """
    Sem arquivo o modo se declara indisponível, mas em 200: a tela sonda este
    endpoint no carregamento, e um 5xx viraria toast de erro global em toda
    instalação que nunca gravou nada.
    """
    import asyncio

    monkeypatch.setattr(replay, "ARQUIVO", tmp_path / "ausente.json")
    monkeypatch.setattr(replay, "gravacao", replay.Gravacao())
    m = asyncio.run(replay.manifest())
    assert m["disponivel"] is False
    assert "capture_replay" in m["motivo"]


def test_sem_gravacao_os_dados_recusam(tmp_path, monkeypatch):
    """Já os endpoints de dado recusam: melhor 503 do que execução inventada."""
    import asyncio

    from fastapi import HTTPException

    monkeypatch.setattr(replay, "ARQUIVO", tmp_path / "ausente.json")
    monkeypatch.setattr(replay, "gravacao", replay.Gravacao())
    with pytest.raises(HTTPException) as exc:
        asyncio.run(replay.cenario())
    assert exc.value.status_code == 503


def test_router_nao_toca_no_mongodb():
    """
    O replay existe para rodar com o cluster PAUSADO. Uma referência a coleção
    aqui derrubaria isso silenciosamente na primeira demo offline.
    """
    fonte = (BACKEND / "routers" / "replay.py").read_text()
    for proibido in ("pymongo", "client[", "sdb[", "insert_", "find(", "aggregate("):
        assert proibido not in fonte, f"replay.py não deve falar com o banco: {proibido}"
