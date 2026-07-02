"""
CAMINHO B — Dispatcher do MongoDB Change Stream (single-write).

Ponto arquitetural CRÍTICO: NÃO abrimos um change stream por request (isso não
escala — seriam milhares de cursores no oplog). Abrimos UM ÚNICO change stream
compartilhado na coleção `jobs`, filtrando apenas as conclusões
(operationType=update, updatedFields.status=done), e um dispatcher em memória
roteia cada evento pelo correlationId para resolver a Future que está segurando
a request device-facing.

Durabilidade/resiliência: o resumeToken é persistido continuamente. Num restart,
retomamos com resume_after e recuperamos EM ORDEM as conclusões que ocorreram
enquanto estávamos fora (resolvendo Futures pendentes ou logando p/ reconciliação).
"""
import asyncio
import threading

from pymongo.errors import PyMongoError

import config
from shared import mongo

# Filtro do change stream: só nos interessam CONCLUSÕES de job.
PIPELINE = [{
    "$match": {
        "operationType": "update",
        "updateDescription.updatedFields.status": "done",
    }
}]


class ChangeStreamDispatcher:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.pending: dict[str, asyncio.Future] = {}   # correlationId -> Future
        self.reconciliados: list[dict] = []            # eventos sem Future pendente (recuperação)
        self._thread: threading.Thread | None = None
        self._active = False
        self._ready = threading.Event()
        self._erro: str | None = None
        self.eventos_processados = 0   # observabilidade: eventos entregues pelo stream

    # ----- ciclo de vida -----------------------------------------------------
    def start(self, resume: bool = True):
        self._active = True
        self._ready.clear()
        self._erro = None
        self._thread = threading.Thread(
            target=self._run, args=(resume,), daemon=True, name="cs-dispatcher"
        )
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError(f"Change stream não abriu a tempo. erro={self._erro}")
        if self._erro:
            raise RuntimeError(self._erro)

    def stop(self):
        self._active = False
        if self._thread:
            self._thread.join(timeout=8)
            self._thread = None

    # ----- API para o serviço request-reply ---------------------------------
    def register(self, cid: str) -> asyncio.Future:
        fut = self.loop.create_future()
        self.pending[cid] = fut
        return fut

    def unregister(self, cid: str):
        self.pending.pop(cid, None)

    # ----- loop do change stream (roda em thread própria) --------------------
    def _run(self, resume: bool):
        token = mongo.load_resume_token() if resume else None
        kwargs = dict(full_document="updateLookup", max_await_time_ms=300)
        if token:
            kwargs["resume_after"] = token
        try:
            with mongo.get_db()[config.COL_JOBS].watch(PIPELINE, **kwargs) as stream:
                self._ready.set()
                while self._active:
                    try:
                        change = stream.try_next()
                    except PyMongoError as e:
                        self._erro = f"erro no change stream: {e}"
                        break
                    if change is None:
                        # Ocioso. IMPORTANTE: NÃO persistimos o postBatchResumeToken
                        # (high-water mark) aqui. Ele reflete o clusterTime atual e, num
                        # restart, pode "cobrir" um update que só é gravado depois no
                        # mesmo segundo (ordinal menor) — o resume_after PULARIA esse
                        # evento. Persistir apenas fronteiras de evento REAIS garante a
                        # entrega at-least-once que a demo de resiliência prova.
                        continue
                    self.eventos_processados += 1
                    token = change["_id"]
                    mongo.save_resume_token(token)   # persiste continuamente, por evento entregue
                    doc = change.get("fullDocument") or {}
                    cid = doc.get("correlationId")
                    result = doc.get("result")
                    self.loop.call_soon_threadsafe(self._resolve, cid, result)
        except PyMongoError as e:
            self._erro = f"falha ao abrir change stream: {e}"
            self._ready.set()
        finally:
            self._ready.set()

    def _resolve(self, cid: str, result):
        """Executa no event loop: resolve a Future pendente ou registra reconciliação."""
        fut = self.pending.pop(cid, None)
        if fut and not fut.done():
            fut.set_result(result)
        else:
            # Conclusão chegou sem ninguém esperando (ex.: completou enquanto o
            # dispatcher estava fora e a request original já caiu). O evento NÃO
            # se perde: veio do write já commitado e foi recuperado via resumeToken.
            self.reconciliados.append({"correlationId": cid, "result": result})
