"""
Simulador in-process do Redis (sem subir nenhum container).

O objetivo NÃO é reimplementar o Redis, e sim reproduzir com fidelidade os dois
comportamentos que importam para a discussão arquitetural:

  1. Pub/Sub é FIRE-AND-FORGET: se ninguém está inscrito no instante do PUBLISH,
     a mensagem simplesmente evapora (não há durabilidade, não há replay).
  2. Redis NÃO persiste o resultado de negócio: para ter durabilidade + auditoria
     o worker é OBRIGADO ao dual-write (grava no banco durável + sinaliza no Redis).

Por ser em memória e sem rede, este Redis simulado é OTIMISTA de propósito
(latência ~0.3ms, melhor caso). Ou seja: mesmo dando ao Redis a vantagem de
latência, o argumento de durabilidade/consistência do MongoDB se mantém.
"""
import asyncio
from collections import defaultdict, deque

import config


class FakeRedis:
    def __init__(self, latency_s: float = config.REDIS_SIM_LATENCY_S):
        self.latency = latency_s
        self._channels: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lists: dict[str, deque] = defaultdict(deque)
        self._waiters: dict[str, deque[asyncio.Future]] = defaultdict(deque)
        # métricas para as demos
        self.publicados_entregues = 0
        self.publicados_perdidos = 0   # PUBLISH sem subscriber = notificação perdida

    async def _tick(self):
        """Latência simulada de uma operação Redis (melhor caso, sem rede)."""
        if self.latency:
            await asyncio.sleep(self.latency)

    # ----- Pub/Sub -----------------------------------------------------------
    async def subscribe(self, channel: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._channels[channel].add(q)
        return q

    async def unsubscribe(self, channel: str, q: asyncio.Queue):
        self._channels[channel].discard(q)
        if not self._channels[channel]:
            self._channels.pop(channel, None)

    async def publish(self, channel: str, message) -> int:
        await self._tick()
        subs = self._channels.get(channel)
        if subs:
            for q in subs:
                q.put_nowait(message)
            self.publicados_entregues += 1
            return len(subs)
        # Ninguém ouvindo → mensagem perdida para sempre (fire-and-forget).
        self.publicados_perdidos += 1
        return 0

    # ----- Lista (BLPOP / LPUSH) --------------------------------------------
    async def lpush(self, key: str, value) -> None:
        await self._tick()
        # Se há um BLPOP bloqueado esperando, entrega direto.
        waiters = self._waiters.get(key)
        while waiters:
            fut = waiters.popleft()
            if not fut.done():
                fut.set_result(value)
                return
        self._lists[key].appendleft(value)

    async def blpop(self, key: str, timeout: float):
        await self._tick()
        if self._lists[key]:
            return self._lists[key].pop()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._waiters[key].append(fut)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            try:
                self._waiters[key].remove(fut)
            except ValueError:
                pass
            return None
