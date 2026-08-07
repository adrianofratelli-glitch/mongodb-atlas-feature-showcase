"""
Testes do módulo Change Streams.

O feed da demo virou SSE e o `id` de cada frame é o que o navegador devolve em
`Last-Event-ID` ao reconectar. O buffer em memória é truncado, então o `id`
precisa ser uma sequência absoluta — um índice de lista muda de significado a
cada descarte e faria a retomada pular ou repetir eventos.

Teste de unidade: não exige Atlas.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from routers import change_streams as cs  # noqa: E402


@pytest.fixture(autouse=True)
def estado_limpo():
    cs._state["events"] = []
    cs._state["seq_base"] = 0
    yield
    cs._state["events"] = []
    cs._state["seq_base"] = 0


def test_buffer_mantem_no_maximo_250_eventos():
    for i in range(300):
        cs._append_event({"n": i})
    assert len(cs._state["events"]) == 250


def test_seq_base_acompanha_o_que_foi_descartado():
    """A sequência absoluta do primeiro evento retido não pode ficar em zero."""
    for i in range(300):
        cs._append_event({"n": i})

    base = cs._state["seq_base"]
    assert base == 50
    # O evento na posição 0 da lista é o de sequência absoluta `base`.
    assert cs._state["events"][0] == {"n": 50}


def test_sequencia_absoluta_identifica_o_mesmo_evento_apos_descarte():
    """Regressão: com índice de lista, o id 260 apontaria para outro evento.

    Reproduz o que o SSE faz — guarda o id emitido para um evento e, depois de
    o buffer truncar, resolve esse id de novo pela sequência absoluta.
    """
    for i in range(260):
        cs._append_event({"n": i})

    def resolve(cursor: int):
        base = cs._state["seq_base"]
        eventos = cs._state["events"]
        if cursor < base or cursor >= base + len(eventos):
            return None
        return eventos[cursor - base]

    assert resolve(259) == {"n": 259}

    for i in range(260, 400):
        cs._append_event({"n": i})

    # Mesmo id, mesmo evento: o truncamento moveu a posição na lista (de 259
    # para 109), mas não o significado do id. Com índice de lista, `resolve(259)`
    # devolveria {"n": 409} — um evento que o cliente já teria visto pulado.
    assert cs._state["seq_base"] == 150
    assert resolve(259) == {"n": 259}
    assert resolve(399) == {"n": 399}
    assert resolve(149) is None          # fora do buffer, e isso é explícito
    assert resolve(400) is None          # ainda não existe


def test_generation_antiga_nao_polui_o_feed():
    cs._state["generation"] = 7
    cs._append_event({"n": "atual"}, generation=7)
    cs._append_event({"n": "obsoleto"}, generation=6)
    assert cs._state["events"] == [{"n": "atual"}]
