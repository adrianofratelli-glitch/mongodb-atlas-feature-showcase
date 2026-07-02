# Redis vs MongoDB Change Streams — request-reply device-facing

Módulo de demonstração comparativa para o **Banco Inter**. Prova, com números e
cenários de falha reais contra o **Atlas** da POC, por que **MongoDB Change
Streams** é a arquitetura ideal para o padrão *request-reply / job-done
notification* quando **o resultado precisa ser durável e auditado**.

> Módulo **isolado**: venv próprio, coleções próprias (`demo_rvc_*`), processo
> próprio (porta 8003). Não altera nada da POC existente (backend na 8002).

## O cenário do cliente

Um request síncrono *device-facing* entra na API → dispara um worker assíncrono →
a API precisa saber que o processamento terminou para responder o dispositivo
(que segura a conexão). **SLA device-facing: 100 ms.** O resultado **precisa ser
persistido e auditado**.

| | Caminho A — Redis | Caminho B — MongoDB Change Stream |
|---|---|---|
| Escritas na conclusão | **2** (persistir no banco + sinalizar) + auditoria | **1** (update no doc) |
| Fonte de verdade | banco **e** Redis (dois sistemas) | o doc `jobs` (**uma** fonte) |
| Notificação | Pub/Sub fire-and-forget **ou** BLPOP | change stream derivado do commit |
| Recuperação pós-falha | nenhuma (Pub/Sub) / manual | **resumeToken** (replay em ordem) |
| Auditoria | escrita **EXTRA** (3º passo) | o próprio doc + stream (**sem** extra) |
| Inconsistência possível? | **sim** (dual-write) | **não** (persist = sinal = mesmo commit) |

> **O Redis não é o vilão.** Ele é excelente e, em latência bruta, **mais
> rápido** (sinal em memória). O ponto é arquitetural: como o Redis **não
> persiste** o resultado de negócio, o cenário obriga ao **dual-write**, e é daí
> que nascem perda de notificação e inconsistência.

## Como rodar (um comando)

Pré-requisito único: o `backend/.env` da POC preenchido (a `MONGO_URI` do Atlas
é reaproveitada). **Não precisa de Docker nem de Redis** — o Redis é simulado
in-process (`shared/fake_redis.py`).

```bash
cd demo_redis_vs_changestream
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

./demos.sh            # roda as 3 demonstrações em sequência
```

### Os 3 comandos de demonstração

```bash
./demos.sh 1     # LATÊNCIA ponta-a-ponta (p50/p95/p99, prova o SLA de 100ms)
./demos.sh 2     # RESILIÊNCIA (Pub/Sub perde a notificação, Change Stream recupera)
./demos.sh 3     # DUAL-WRITE INCONSISTENCY (o ponto principal)
```

Camada interativa opcional (HTTP, para demo ao vivo com `curl`):

```bash
./venv/bin/uvicorn app:app --port 8003
curl -X POST localhost:8003/mongo/request-reply
curl -X POST "localhost:8003/redis/request-reply?variante=pubsub"
```

## As três demonstrações

### 1. Latência device-facing

Roda N jobs no mesmo fluxo por três variantes e mede p50/p95/p99.

**Honestidade sobre os números.** Este ambiente (laptop) está a **~150 ms de RTT
do cluster Atlas** (cross-region). Esse RTT é de **rede pura** e é pago pelos
**dois** caminhos, pois ambos escrevem no mesmo Atlas. Por isso a demo separa os
números **absolutos** (dominados pela rede) da **projeção co-localizada** (app e
cluster na mesma região, RTT ~2 ms) — que é a topologia de produção e onde o SLA
de 100 ms se aplica.

Exemplo de saída real (12 jobs):

| Variante | p50 | Durável? | Round-trips Atlas |
|---|---|---|---|
| Redis Pub/Sub (sinal isolado) | ~0 ms | ✗ não | 0 |
| Redis dual-write (persist+sinal) | ~303 ms | ⚠ sim* | 2 |
| MongoDB Change Stream | ~453 ms | ✓ sim | 3 |

\* durável só se **as duas** escritas sucederem (ver demo 3).

**Projeção co-localizada (RTT ~2 ms):** Redis dual-write ~14 ms · MongoDB Change
Stream ~19 ms — **ambos dentro dos 100 ms**. O change stream adiciona ~1
round-trip de propagação sobre o write durável; co-localizado isso é
desprezível. **Conclusão: latência não é o fator decisivo** — durabilidade e
fonte única de verdade são.

### 2. Resiliência (contra Pub/Sub)

O consumidor cai no meio do fluxo, o job é concluído, ele religa.

- **Redis Pub/Sub**: `PUBLISH` sem subscriber → **notificação perdida para
  sempre**. O job fica durável no banco, mas o device nunca é avisado e não há
  replay. Recuperar exige um mecanismo de reconciliação que o Pub/Sub não dá.
- **MongoDB Change Stream**: o **resumeToken** persistido permite retomar com
  `resume_after` e **reprocessar em ordem** a conclusão perdida. O evento nunca
  se perde, porque deriva de um write já commitado.

### 3. Dual-write inconsistency (o ponto principal)

Injeta um crash do worker **entre** a persistência durável e o sinal (e
vice-versa).

- **Redis, cenário 1** (crash após persistir): resultado **durável**, mas device
  **nunca avisado** e **sem auditoria** → inconsistente.
- **Redis, cenário 2** (crash após sinalizar): device recebe "autorizado", mas
  **nada foi persistido** e **sem auditoria** → inconsistente (pior caso: mentiu
  para o device).
- **MongoDB**: o **mesmo** crash é incapaz de gerar inconsistência. Persistência
  e sinal são o **mesmo commit**; o sinal deriva do write já commitado. Ou o
  commit aconteceu (durável + no oplog + entregável), ou não aconteceu. **Não
  existe** o estado "sinalizado mas não persistido".

## Talking points de SA

- **Dual-write eliminado.** Redis obriga a duas escritas não-atômicas contra
  dois sistemas → janela de inconsistência sempre presente. Mitigar pede outbox,
  sagas, reconciliação — complexidade que você passa a **operar**. O change
  stream elimina a classe inteira do problema.
- **Single source of truth.** No caminho Mongo, o doc `jobs` é a única fonte. No
  Redis, a verdade está espalhada entre banco e broker, e elas podem divergir.
- **Um sistema a menos para operar.** Sem cluster Redis para provisionar,
  monitorar, escalar, pagar e manter consistente com o banco.
- **resumeToken vs fire-and-forget.** Change stream = entrega ao menos uma vez,
  recuperável e em ordem. Pub/Sub = rápido, porém sem garantia de entrega.
- **Auditoria nativa.** No Redis a trilha imutável é uma escrita extra do
  dual-write. No Mongo, o próprio doc + o change stream já são a trilha.
- **Latência, com honestidade.** O Redis é mais rápido no sinal (memória). Mas
  co-localizado os dois caminhos ficam muito abaixo dos 100 ms — então o
  diferencial real é **durabilidade e consistência**, não latência. O change
  stream entrega a notificação **derivada de um write já durável, dentro do SLA**.

## Arquitetura (Caminho B)

Um **único** change stream compartilhado em `demo_rvc_jobs`, filtrando só
conclusões:

```python
[{"$match": {"operationType": "update",
             "updateDescription.updatedFields.status": "done"}}]
```

Um **dispatcher em memória** roteia cada evento pelo `correlationId` e resolve a
`asyncio.Future` que segura a request. **Não** há um cursor por request (não
escalaria — milhares de cursores no oplog). O `resumeToken` é persistido
continuamente em `demo_rvc_resume_tokens`; no restart, `resume_after` recupera as
conclusões perdidas em ordem.

Dois cuidados de produção que a demo aplica no checkpoint do `resumeToken` (e que
valem como talking point):

- **Só persistir fronteiras de evento reais**, nunca o `postBatchResumeToken`
  ocioso (high-water mark). O high-water reflete o `clusterTime` atual e, num
  restart, poderia "cobrir" um update gravado logo depois no mesmo segundo,
  fazendo o `resume_after` pular a conclusão. Persistir só o token de eventos
  entregues garante a entrega **at-least-once**.
- **Checkpoint durável e consistente**: o token é gravado com `w=majority` e lido
  com `readConcern=majority` no primário (read-your-own-writes). Sem isso, uma
  leitura de um secundário com lag poderia devolver um token à frente do último
  evento e pular a recuperação.

## Estrutura

```
demo_redis_vs_changestream/
├── config.py                     # reaproveita backend/.env (MONGO_URI do Atlas)
├── shared/
│   ├── mongo.py                  # writes duráveis + resumeToken (PyMongo, como a POC)
│   ├── fake_redis.py             # Redis simulado in-process (Pub/Sub + BLPOP)
│   └── models.py                 # correlationId, payloads do cenário Inter
├── caminho_a_redis/service.py    # dual-write + injeção de crash + auditoria extra
├── caminho_b_changestream/
│   ├── dispatcher.py             # UM change stream compartilhado + resumeToken
│   └── service.py                # request-reply single-write
├── demo_1_latencia.py · demo_2_resiliencia.py · demo_3_dualwrite.py
├── app.py                        # camada HTTP interativa (porta 8003)
└── demos.sh                      # roda as 3 demos com um comando
```

## Notas

- **Redis simulado.** É um simulador in-process, **otimista de propósito**
  (memória, sem rede). Ou seja: mesmo dando ao Redis a melhor latência possível,
  o argumento de durabilidade/consistência do MongoDB se mantém.
- As coleções `demo_rvc_*` são exclusivas desta demo e limpas ao início/fim de
  cada execução — nenhuma coleção da POC é tocada.
- Change streams exigem replica set (Atlas M10+) — o cluster da POC atende.
