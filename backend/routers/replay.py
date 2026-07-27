"""
Módulo 07 — reprodução de UMA execução real gravada.

Motivo de existir: M20/M30 são instâncias burstable e o auto-scaling do Atlas
dispara por CPU RELATIVA (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`), não
absoluta. Medido neste projeto: 17,6% de CPU absoluta correspondeu a 88%
relativa e escalou o cluster — com o gerador PARADO, só com o dashboard aberto.
Reproduzir uma execução gravada permite demonstrar as três capacidades com o
cluster pausado, sem custo de compute.

Contrato honesto do modo:

- Nada aqui é sintetizado. Cada número devolvido foi medido contra o Atlas
  durante a captura (`scripts/capture_replay.py`) e é devolvido sem alteração.
- Este router NÃO toca no MongoDB. Ele lê um arquivo e o relê no tempo.
- A origem é sempre declarada: todo payload sai com `replay: true`, o manifesto
  expõe `run_id` e `gravado_em`, e a tela marca o modo de forma visível. O modo
  não deve ser apresentado como execução ao vivo.

Espelha os caminhos de `/streaming` sob `/replay`, para o frontend só trocar o
prefixo e manter uma única implementação de tela.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/replay", tags=["Replay"])

ARQUIVO = Path(__file__).resolve().parents[1] / "data" / "replay_streaming.json"

# Canais SSE gravados, na nomenclatura de /streaming.
CANAIS_SSE = {
    "changestream": "/streaming/changestream",
    "kafka": "/streaming/kafka",
    "asp": "/streaming/asp",
}


class Gravacao:
    """Carrega o arquivo uma vez e indexa por canal para busca por tempo."""

    def __init__(self) -> None:
        self._dados: dict[str, Any] | None = None
        self._por_canal: dict[str, list[dict]] = {}
        self._tempos: dict[str, list[float]] = {}

    def carrega(self) -> dict[str, Any]:
        if self._dados is not None:
            return self._dados
        if not ARQUIVO.exists():
            raise HTTPException(
                status_code=503,
                detail=("Nenhuma execução gravada. Rode `python scripts/capture_replay.py` "
                        "com o ambiente ligado para gravar uma."),
            )
        try:
            dados = json.loads(ARQUIVO.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=503,
                                detail=f"Gravação ilegível: {type(exc).__name__}") from exc

        por_canal: dict[str, list[dict]] = {}
        for evento in dados.get("eventos", []):
            por_canal.setdefault(evento["canal"], []).append(evento)
        for canal, eventos in por_canal.items():
            eventos.sort(key=lambda e: e["t"])
            self._tempos[canal] = [e["t"] for e in eventos]

        self._por_canal = por_canal
        self._dados = dados
        return dados

    def snapshot_em(self, canal: str, t: float) -> Any:
        """Último snapshot do canal com instante <= t (o que a tela veria)."""
        self.carrega()
        eventos = self._por_canal.get(canal)
        if not eventos:
            return None
        idx = bisect.bisect_right(self._tempos[canal], t) - 1
        if idx < 0:
            # Antes do primeiro registro: devolve o primeiro para a tela não
            # ficar vazia enquanto o relógio ainda não alcançou a gravação.
            idx = 0
        return eventos[idx].get("dado")

    def eventos_entre(self, canal: str, inicio: float, fim: float) -> list[dict]:
        self.carrega()
        eventos = self._por_canal.get(canal)
        if not eventos:
            return []
        tempos = self._tempos[canal]
        return eventos[bisect.bisect_right(tempos, inicio):bisect.bisect_right(tempos, fim)]

    @property
    def duracao(self) -> float:
        return float(self.carrega().get("duracao_s") or 0.0)


gravacao = Gravacao()


class Relogio:
    """
    Relógio de reprodução. Um por processo — a demo é de uma tela só.

    `play` reinicia do zero; `pause` congela a posição; `stop` volta ao início e
    desliga. Sem laço em segundo plano: a posição é derivada do relógio
    monotônico na hora da consulta, então um replay parado não custa nada.
    """

    def __init__(self) -> None:
        self.rodando = False
        self._inicio = 0.0
        self._pausado_em = 0.0
        self.repetir = True

    def play(self, do_zero: bool = True) -> None:
        base = 0.0 if do_zero else self._pausado_em
        self._inicio = time.monotonic() - base
        self.rodando = True

    def pause(self) -> None:
        if self.rodando:
            self._pausado_em = self.posicao()
            self.rodando = False

    def stop(self) -> None:
        self.rodando = False
        self._pausado_em = 0.0

    def posicao(self) -> float:
        if not self.rodando:
            return self._pausado_em
        decorrido = time.monotonic() - self._inicio
        duracao = gravacao.duracao
        if duracao <= 0:
            return decorrido
        if decorrido <= duracao:
            return decorrido
        if self.repetir:
            return decorrido % duracao
        self.rodando = False
        self._pausado_em = duracao
        return duracao

    def estado(self) -> dict[str, Any]:
        return {
            "rodando": self.rodando,
            "posicao_s": round(self.posicao(), 2),
            "duracao_s": round(gravacao.duracao, 2),
            "repetir": self.repetir,
        }


relogio = Relogio()


def _marca(payload: Any) -> Any:
    """Carimba a origem: nenhum payload sai daqui sem se declarar replay."""
    if isinstance(payload, dict):
        return {**payload, "replay": True}
    return payload


# ---------------------------------------------------------------------------
# Controle
# ---------------------------------------------------------------------------
@router.get("/manifest")
async def manifest():
    """
    Metadados da gravação — o que a tela precisa para se rotular.

    Responde 200 mesmo sem gravação: a tela chama isto no carregamento só para
    saber se deve oferecer o modo. Um 5xx aqui viraria um toast de erro global
    em toda instalação que nunca gravou nada, que não é erro nenhum.
    """
    try:
        dados = gravacao.carrega()
    except HTTPException as exc:
        return {"disponivel": False, "motivo": exc.detail}
    return {
        "disponivel": True,
        "run_id": dados.get("run_id"),
        "gravado_em": dados.get("gravado_em"),
        "tps_alvo": dados.get("tps_alvo"),
        "segundos_de_escrita": dados.get("segundos_de_escrita"),
        "parou_em_s": dados.get("parou_em_s"),
        "reconciliado_em_s": dados.get("reconciliado_em_s"),
        "duracao_s": dados.get("duracao_s"),
        "eventos": len(dados.get("eventos", [])),
        "origem": ("Execução real gravada contra o Atlas. Os números são medições, "
                   "não simulação. Nenhuma escrita é feita no banco durante o replay."),
        "estado": relogio.estado(),
    }


@router.post("/play")
async def play(retomar: bool = False):
    gravacao.carrega()
    relogio.play(do_zero=not retomar)
    return relogio.estado()


@router.post("/pause")
async def pause():
    relogio.pause()
    return relogio.estado()


@router.post("/stop")
async def stop():
    relogio.stop()
    return relogio.estado()


@router.get("/estado")
async def estado():
    return relogio.estado()


# ---------------------------------------------------------------------------
# Snapshots — espelham os caminhos de /streaming
# ---------------------------------------------------------------------------
def _snapshot(canal: str) -> Any:
    return _marca(gravacao.snapshot_em(canal, relogio.posicao()))


@router.get("/streaming/cenario")
async def cenario():
    dados = gravacao.carrega()
    return _marca(dados.get("estatico", {}).get("/streaming/cenario") or {})


@router.get("/streaming/rede")
async def rede():
    dados = gravacao.carrega()
    return _marca(dados.get("estatico", {}).get("/streaming/rede") or {})


@router.get("/streaming/cluster")
async def cluster():
    dados = gravacao.carrega()
    return _marca(dados.get("estatico", {}).get("/streaming/cluster") or {})


@router.get("/streaming/generator/status")
async def generator_status():
    base = _snapshot("/streaming/generator/status") or {}
    # `running` do replay é o relógio, não o gerador real — que está parado.
    return {**base, "running": relogio.rodando, "replay": True}


@router.get("/streaming/kafka/status")
async def kafka_status():
    return _snapshot("/streaming/kafka/status")


@router.get("/streaming/asp/status")
async def asp_status():
    return _snapshot("/streaming/asp/status")


@router.get("/streaming/oplog")
async def oplog():
    return _snapshot("/streaming/oplog")


@router.get("/streaming/leitura")
async def leitura():
    return _snapshot("/streaming/leitura")


@router.get("/streaming/asp/dlq/resumo")
async def dlq_resumo():
    return _snapshot("/streaming/asp/dlq/resumo")


@router.get("/streaming/reconciliacao")
async def reconciliacao(run_id: str | None = None):  # noqa: ARG001 - assinatura da tela
    return _snapshot("/streaming/reconciliacao")


# ---------------------------------------------------------------------------
# SSE — reemite os eventos gravados conforme o relógio avança
# ---------------------------------------------------------------------------
def _sse(payload: Any) -> str:
    return f"data: {json.dumps(payload, default=str)}\n\n"


async def _stream(request: Request, canal: str):
    gravacao.carrega()
    yield _sse({"type": "hello", "replay": True, "canal": canal})
    anterior = relogio.posicao()
    try:
        while True:
            if await request.is_disconnected():
                return
            await asyncio.sleep(0.2)
            atual = relogio.posicao()
            if not relogio.rodando:
                anterior = atual
                continue
            if atual < anterior:
                # O laço recomeçou: a tela zera e reproduz de novo.
                yield _sse({"type": "reset", "replay": True})
                anterior = 0.0
            for evento in gravacao.eventos_entre(canal, anterior, atual):
                yield _sse(_marca(evento.get("dado")))
            anterior = atual
    except asyncio.CancelledError:  # pragma: no cover - desconexão do cliente
        raise


def _resposta(request: Request, canal: str) -> StreamingResponse:
    return StreamingResponse(
        _stream(request, canal),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/streaming/changestream")
async def sse_changestream(request: Request):
    return _resposta(request, CANAIS_SSE["changestream"])


@router.get("/streaming/kafka")
async def sse_kafka(request: Request):
    return _resposta(request, CANAIS_SSE["kafka"])


@router.get("/streaming/asp")
async def sse_asp(request: Request):
    return _resposta(request, CANAIS_SSE["asp"])
