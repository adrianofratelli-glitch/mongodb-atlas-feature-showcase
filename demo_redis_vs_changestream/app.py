"""
Camada interativa (opcional) — FastAPI na porta 8003.

Expõe o MESMO fluxo request-reply pelos dois caminhos como endpoints HTTP, útil
para uma demo ao vivo (curl / Postman). O change stream compartilhado sobe uma
única vez no lifespan e serve todas as requests (não há um cursor por request).

Não interfere no backend da POC (porta 8002): módulo e processo separados.
"""
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI

from shared import mongo
from shared.models import gerar_payload
from shared.fake_redis import FakeRedis
from caminho_a_redis import service as redis_svc
from caminho_b_changestream.dispatcher import ChangeStreamDispatcher
from caminho_b_changestream import service as mongo_svc

_estado: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    disp = ChangeStreamDispatcher(loop)
    disp.start(resume=True)                # UM único change stream para toda a app
    _estado["disp"] = disp
    _estado["redis"] = FakeRedis()
    try:
        yield
    finally:
        disp.stop()


app = FastAPI(title="Demo Redis vs MongoDB Change Streams", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root():
    return {
        "modulo": "demo_redis_vs_changestream",
        "cenario": "request-reply device-facing (Banco Inter) · SLA 100ms",
        "caminhos": {
            "B_mongo_change_stream": "POST /mongo/request-reply  (single-write, durável)",
            "A_redis_dual_write": "POST /redis/request-reply?variante=pubsub|blpop  (dual-write)",
        },
        "pendentes_no_dispatcher": len(_estado["disp"].pending),
        "reconciliados": len(_estado["disp"].reconciliados),
    }


@app.post("/mongo/request-reply")
async def mongo_request_reply(payload: dict | None = None):
    r = await mongo_svc.request_reply(_estado["disp"], payload or gerar_payload())
    return r


@app.post("/redis/request-reply")
async def redis_request_reply(variante: str = "pubsub", payload: dict | None = None):
    r = await redis_svc.request_reply(_estado["redis"], payload or gerar_payload(), variante=variante)
    return {**r, "nota": "dual-write: exigiu persistência + sinal + auditoria (3 escritas)"}


@app.get("/jobs/{cid}")
async def get_job(cid: str):
    job = await asyncio.to_thread(mongo.get_job, cid)
    return job or {"erro": "job não encontrado"}
