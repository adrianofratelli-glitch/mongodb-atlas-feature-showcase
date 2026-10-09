# Queries, pipelines e índices — referência rápida

> Todo pipeline/query/índice real do projeto, extraído do código (`backend/routers/*.py`, `backend/seed_data.py`). Sem dados sensíveis reais — os exemplos usam nomes fictícios já presentes no código (dados sintéticos de demo). Se um pipeline não aparece aqui, é porque não foi encontrado no código atual.

---

## Índices

### `POC.produtos` — garantidos por `ensure_indexes()` em `backend/seed_data.py`

| Nome | Campos | Tipo | Por que existe |
|---|---|---|---|
| `em_estoque_1` (auto) | `em_estoque: 1` | simples | Suporta o `$match` inicial de `/aggregations/facet`, `/group-advanced`, `/bucket-auto` (todos filtram por `em_estoque: true` antes de agregar) |
| `categoria_1` (auto) | `categoria: 1` | simples | Suporta `/reindexacao/explain?scenario=simples` (filtro `categoria: "Eletrônicos"`) |
| `total_av_idx` | `total_avaliacoes: -1` | simples | Ordenação descendente por popularidade |
| `cat_total_av_idx` | `categoria: 1, total_avaliacoes: -1` | composto | Atende o `$match(categoria)` + `$sort(total_avaliacoes desc)` de `/aggregations/window-functions` sem blocking sort em memória |
| `produto_id_1` (unique) | `produto_id: 1` | único | Chave de negócio — usada pelo `$lookup` de `/aggregations/lookup` |
| `destaque_idx` | `em_estoque: 1, total_avaliacoes: -1, avaliacao_media: -1` | composto | Atende o `$match` + `$sort` do lado "produtos" de `/aggregations/union-with` |

### `POC.avaliacoes` — garantidos por `ensure_indexes()` em `backend/seed_data.py`

| Nome | Campos | Tipo | Por que existe |
|---|---|---|---|
| `produto_id_1` (auto) | `produto_id: 1` | simples | Suporta o `$lookup` de `/aggregations/lookup` do lado `avaliacoes → produtos` |
| `recent_nota_idx` | `data: -1, nota: -1` | composto | Suporta o `$sort(data desc)` + `$match(nota >= 4)` do lado "avaliacoes" de `/aggregations/union-with` |

**Seed idempotente:** `seed()` grava por `ReplaceOne(..., upsert=True)` com chave natural determinística (`produto_id` = uuid5 do índice; `_id = "seed-av-<n>"` nas avaliações), via `bulk_write(ordered=False)` em lotes de 10 mil. O índice único `produto_id_1` é criado **antes** dos upserts. `produtos`/`avaliacoes` podem ser compartilhadas com outra PoV no mesmo banco (no cluster de demo, o marketplace de busca tem `marca_1`, `subcategoria_1` e o índice Atlas Search `produtos_search` em `POC.produtos`); nem o seed nem o `scripts/reset_demo.py` dropam essas coleções.

**Nota importante:** o `$group` inicial de `/aggregations/lookup` **não tem índice cobrindo** a chave de agrupamento (`produto_id`) sobre a coleção inteira — em `--full` (1M avaliações) isso é um COLLSCAN completo. Por isso o endpoint cacheia o resultado por `limit` durante 45s (`LOOKUP_CACHE_TTL_S` em `backend/routers/aggregations.py:17`), para refresh de tela na demo não repetir o scan.

### `pix.transacoes` — criados em `backend/routers/streaming.py:730-751` (função `_ensure_indexes()`)

| Nome | Campos | Tipo | Por que existe |
|---|---|---|---|
| `endToEndId_unique` | `endToEndId: 1` | único | Chave de negócio do PIX — garante idempotência: reprocessar a DLQ duas vezes não duplica, o índice barra |
| `run_id_reconciliacao` | `run_id: 1` | simples | A tela de reconciliação faz `count_documents({run_id})` a cada poucos segundos; sem esse índice era um COLLSCAN repetido em loop, o maior consumidor de CPU/memória medido no projeto, e o que empurrava o auto-scaling para fora do M20 |
| `ts_ttl` | `ts: 1`, `expireAfterSeconds` configurável (`TTL_SECONDS`) | TTL | Expira transações antigas da coleção de demo automaticamente. O índice é localizado pela **chave** (`{ts: 1}`), não pelo nome, porque `create_index` recusa um segundo índice sobre o mesmo campo com `expireAfterSeconds` diferente — versões antigas da PoV usaram outro nome, e o código faz `collMod` para ajustar a janela sem recriar o índice |

A limpeza (`/streaming/reset`) precisa recriar os três índices sempre que dropa a coleção acima de `STREAMING_DROP_ACIMA_DE`.

---

## Módulo Reindexação — `backend/routers/reindexacao.py`

Índices criados/removidos **dinamicamente pela própria demo** (não fixos), sobre `POC.produtos`, sempre com o prefixo `demo01_` (`DEMO_INDEX_PREFIX`; ex.: `demo01_categoria_1_preco_-1`). `DELETE /reindexacao/drop/{index_name}` recusa com 403 qualquer nome sem esse prefixo, e `GET /indexes` devolve `removivel` por índice. Campos restritos a `ALLOWED_INDEX_FIELDS` (whitelist: `categoria`, `preco`, `em_estoque`, `total_avaliacoes`, `avaliacao_media`, `marca`, `created_at`, `produto_id`).

```python
db["produtos"].create_index(key, name=name, sparse=sparse, partialFilterExpression=partial_filter)
```

- **`POST /reindexacao/create`** — cria índice em background (thread separada), até 4 campos, com suporte a `sparse` e a um único filtro parcial permitido (`{"em_estoque": True}`). Prova que o build é *hybrid* (MongoDB 4.2+): não bloqueia leitura/escrita na maior parte do processo.
- **`GET /reindexacao/explain`** — roda `db.command("explain", find_cmd, verbosity="executionStats")` sobre três cenários fixos:

```python
EXPLAIN_SCENARIOS = {
    "simples":  {"filter": {"categoria": "Eletrônicos"}, "limit": 100},
    "composto": {"filter": {"categoria": "Eletrônicos"}, "sort": {"preco": -1}, "limit": 100},
    "parcial":  {"filter": {"em_estoque": True, "preco": {"$lt": 100}}, "limit": 100},
}
```
Onde está: `reindexacao.py:9-13, 207-236`. O que faz: compara COLLSCAN vs IXSCAN, extraindo `totalDocsExamined`/`totalKeysExamined`/`executionTimeMillis` do plano vencedor. Por que existe: prova objetiva de ganho de índice, ao vivo.

---

## Módulo Hot/Cold (Online Archive) — `backend/routers/hot_cold.py`

Todas sobre `POC.produtos`.

### `GET /hot-cold/distribution` — linha 124-129

```python
pipeline = [
    {"$sample": {"size": 5000}},
    {"$group": {"_id": {"$year": "$created_at"}, "count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}}},
    {"$sort": {"_id": -1}},
    {"$limit": 10},
]
```
O que faz: distribuição de documentos por ano de criação, para classificar em Hot/Misto/Cold. Por que amostra em vez de contar tudo: resposta instantânea mesmo em 5M documentos — o `count` é extrapolado da amostra, e cada linha carrega `margem_erro_estimada` (1 desvio-padrão do erro binomial) para não fingir precisão que a amostra não tem.

### `GET /hot-cold/archive-simulation` — linha 172-179

```python
pipeline = [
    {"$sample": {"size": 5000}},
    {"$group": {
        "_id": None,
        "hot":  {"$sum": {"$cond": [{"$gte": ["$created_at", cutoff]}, 1, 0]}},
        "cold": {"$sum": {"$cond": [{"$lt":  ["$created_at", cutoff]}, 1, 0]}},
    }},
]
```
O que faz: split hot/cold pelo cutoff real (`expire_after_days` do último Online Archive criado, ou 365 por padrão), também extrapolado de amostra.

### `GET /hot-cold/query-transparent` — linha 214-215

```python
db.produtos.find({categoria, created_at: {"$gte": cutoff}}, {nome:1, preco:1, created_at:1, _id:0}).limit(3)
db.produtos.find({categoria, created_at: {"$lt":  cutoff}}, {nome:1, preco:1, created_at:1, _id:0}).limit(3)
```
Duas queries `find` simples (não federadas de verdade) que simulam o que o Online Archive faz de forma transparente: mesma "cara" de query para dado quente e frio.

### Online Archive — via Atlas Admin API, não MongoDB direto

`GET /hot-cold/online-archive/list`, `POST /hot-cold/online-archive/create`, `DELETE /hot-cold/online-archive/{id}` chamam `https://cloud.mongodb.com/api/atlas/v2/groups/{project}/clusters/{cluster}/onlineArchives`, autenticados com `HTTPDigestAuth`. A criação usa `partitionFields: [categoria, created_at]` e **nunca** define `dataExpirationRule` (expiração apagaria dados do archive — perigoso na coleção compartilhada com outros módulos).

---

## Módulo Aggregations — `backend/routers/aggregations.py`

Cinco pipelines, todos com `QueryBlock` visível na UI.

### `GET /aggregations/lookup` — `$lookup` com sub-pipeline (linha 38-75)

```python
pipeline = [
    {"$group": {
        "_id": "$produto_id",
        "total_reviews": {"$sum": 1},
        "avg_nota": {"$avg": "$nota"},
        "top_reviews": {"$topN": {
            "output": {"usuario": "$usuario", "nota": "$nota", "titulo": "$titulo"},
            "sortBy": {"nota": -1}, "n": 3,
        }},
    }},
    {"$sort": {"total_reviews": -1}},
    {"$limit": limit},
    {"$lookup": {
        "from": "produtos", "localField": "_id", "foreignField": "produto_id", "as": "produto",
        "pipeline": [{"$project": {"nome": 1, "categoria": 1, "preco": 1, "marca": 1, "_id": 0}}],
    }},
    {"$unwind": "$produto"},
    {"$project": {...}},
]
```
Roda sobre `avaliacoes`. O que faz: top produtos por número de avaliações, com as 3 melhores reviews de cada (via `$topN`, memória limitada — sem `$push` do array inteiro + slice depois). Por que `$topN`: evita carregar todas as reviews de um produto para depois cortar em Python. Cacheado 45s por `limit` (ver seção de índices).

### `GET /aggregations/facet` — `$facet` (linha 84-112)

```python
pipeline = [
    {"$match": {"em_estoque": True}},
    {"$facet": {
        "por_categoria": [{"$group": {...}}, {"$sort": ...}, {"$limit": 6}],
        "por_faixa_preco": [{"$bucket": {"groupBy": "$preco", "boundaries": [0,100,500,1000,5000,999999], ...}}],
        "por_avaliacao":   [{"$bucket": {"groupBy": "$avaliacao_media", "boundaries": [0,2,3,4,5], ...}}],
        "top_marcas":      [{"$group": {...}}, {"$sort": ...}, {"$limit": 5}],
    }},
]
```
Roda sobre `produtos`. O que faz: quatro agregações independentes em paralelo numa única ida ao servidor — sem trazer dado bruto para a aplicação.

### `GET /aggregations/union-with` — `$unionWith` (linha 121-165)

Combina reviews recentes com nota alta (`avaliacoes`, ordenadas por `recent_nota_idx`) com produtos em destaque (`produtos`, via `destaque_idx`), num resultado único ordenado por `source`/`valor`. Usa `$lookup` sub-pipeline para compatibilidade com datasets antigos sem categoria denormalizada nas avaliações.

### `GET /aggregations/group-advanced` — `$group` + `$addFields` (linha 172-187)

```python
pipeline = [
    {"$match": {"em_estoque": True}},
    {"$group": {"_id": "$categoria", "total_produtos": {"$sum": 1}, "preco_medio": {"$avg": "$preco"},
                "preco_max": {"$max": "$preco"}, "preco_min": {"$min": "$preco"}, "avaliacao_media": {"$avg": "$avaliacao_media"}}},
    {"$addFields": {"amplitude_preco": {"$subtract": ["$preco_max", "$preco_min"]}}},
    {"$sort": {"total_produtos": -1}}, {"$limit": 8},
]
```
Métricas agregadas por categoria, com campo derivado (`amplitude_preco`) calculado no servidor.

### `GET /aggregations/window-functions` — `$setWindowFields` (linha 201-231)

```python
pipeline = [
    {"$match": {"categoria": "Eletrônicos", "em_estoque": True}},
    {"$sort": {"total_avaliacoes": -1}}, {"$limit": 100},
    {"$setWindowFields": {
        "partitionBy": "$marca", "sortBy": {"total_avaliacoes": -1},
        "output": {
            "rank_marca": {"$rank": {}},
            "acumulado_avaliacoes": {"$sum": "$total_avaliacoes", "window": {"documents": ["unbounded", "current"]}},
            "media_movel_preco": {"$avg": "$preco", "window": {"documents": [-2, 2]}},
        },
    }},
    {"$sort": {"marca": 1, "rank_marca": 1}}, {"$limit": 20},
]
```
Rank, soma acumulada e média móvel por marca. O `$match`+`$sort`+`$limit` antes do `$setWindowFields` reduz o working set a 100 docs (via `cat_total_av_idx`) antes da parte cara.

### `GET /aggregations/bucket-auto` — `$bucketAuto` (linha 246-254)

```python
pipeline = [
    {"$match": {"em_estoque": True}},
    {"$bucketAuto": {"groupBy": "$preco", "buckets": 6, "output": {"count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}, "avg_avaliacao": {"$avg": "$avaliacao_media"}}}},
]
```
Faixas de preço automáticas e balanceadas (sem definir boundaries manualmente como no `$bucket` do `/facet`).

---

## Módulo Schema Validation — `backend/routers/schema_validation.py`

Coleção `POC.schema_demo`. Não é índice, é validador `$jsonSchema` ativado via `collMod`:

```python
SCHEMA = {"$jsonSchema": {
    "bsonType": "object",
    "required": ["nome", "preco", "categoria", "em_estoque"],
    "properties": {
        "nome":       {"bsonType": "string", "minLength": 2},
        "preco":      {"bsonType": "number", "minimum": 0},
        "categoria":  {"bsonType": "string", "enum": ["Eletrônicos","Moda","Casa","Esportes","Livros","Brinquedos"]},
        "em_estoque": {"bsonType": "bool"},
        "sku":        {"bsonType": "string", "pattern": "^[A-Z]{2}-[0-9]{4}$"},
    },
}}
db.command("collMod", COL, validator=SCHEMA, validationLevel="strict", validationAction="error")
```
Onde: `schema_validation.py`. O que faz: ativa validação **em uma coleção já existente**, sem recriar — o banco passa a recusar documentos fora do contrato, incluindo erro estruturado (`errInfo`) sobre qual regra falhou. Por que existe: diferencial vs validação só na aplicação — o contrato vive no banco.

Com `strict`, dados inválidos já armazenados podem permanecer. No MongoDB 9.0+, a demo também prepara a coleção (`prepareConstraintValidationLevel: true`) e tenta `validationLevel: "constraint"`. O banco examina todos os documentos existentes e bloqueia a promoção enquanto encontrar violações. A tela corrige quatro exemplos sintéticos marcados e repete a promoção, que garante que os dados atuais e futuros atendam ao validador. A verificação examina a coleção; considere o custo em coleções grandes. Para mudar as regras após a promoção, retorne a `strict`, altere o validador e promova novamente.

---

## Módulo Change Streams — `backend/routers/change_streams.py`

Coleção `POC.transacoes_cs_demo`, criada com `changeStreamPreAndPostImages={"enabled": True}` (linha 165-168) para o stream entregar o before-image real em updates.

```python
pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "delete"]}}}]
db["transacoes_cs_demo"].watch(
    pipeline, full_document="updateLookup",
    full_document_before_change="whenAvailable", max_await_time_ms=400,
)
```
Onde: linha 83-91. O que faz: observa inserts/updates/deletes em tempo real, entregando o feed via SSE (`GET /change-streams/feed`) sem polling no MongoDB. Por que `full_document_before_change`: permite mostrar `status: pendente → aprovada` como trilha de auditoria, não só o pós-estado.

`GET /change-streams/collection` (linha 294): `col.find({}, {"_id": 0}).sort("created_at", -1).limit(50)` — prova que os eventos também estão persistidos, não só na UI.

---

## Módulo Transactions — `backend/routers/transactions.py`

Sem aggregation pipeline — transações ACID multi-documento via `session.with_transaction()`.

- **`POST /transactions/executar`** — 4 steps numa transação: lê `produtos` (find), insere `pedidos_demo`, `$inc` em `estoque_demo` (upsert), insere `pagamentos_demo`. Com `simular_falha=True`, o step 4 lança exceção e o driver reverte automaticamente os writes dos steps 2 e 3 (rollback real, visível no payload).
- **`POST /transactions/benchmark`** (linha 260-424) — não é uma query de produto, é medição: compara **A)** transação multi-documento sobre 3 coleções, **B)** a mesma intenção como documento único, **C)** a mesma transação sob contenção (N sessões na mesma chave via `$inc` em `estoque_demo`), sempre com `WriteConcern("majority")` nos dois lados. Mede também o RTT puro (`ping` no admin) antes de tudo, para não atribuir ao produto um limite que é da rede.

---

## Módulo Streaming — `backend/routers/streaming.py` (o maior do projeto)

Todas sobre `pix.*` (`sdb` = handle do database, `STREAMING_DB`, padrão `pix`).

### `_perfil_medido()` — `$percentile` (linha 1429-1441)

```python
pipeline = [
    {"$sample": {"size": 20000}},
    {"$group": {
        "_id": None, "n": {"$sum": 1}, "media": {"$avg": {"$toDouble": "$valor"}}, "total": {"$sum": {"$toDouble": "$valor"}},
        "percentis": {"$percentile": {"input": {"$toDouble": "$valor"}, "p": [0.5, 0.9, 0.99], "method": "approximate"}},
    }},
]
```
Exposto em `GET /streaming/perfil-valores`. O que faz: mede mediana/p90/p99 real do valor das transações via `$percentile` nativo (MongoDB 7+), para não confundir ticket médio com distribuição — um fluxo assimétrico (PIX real) tem cauda longa que a média esconde.

### `_asp_totais()` — soma de janelas do ASP (linha 3529)

```python
[{"$group": {"_id": None, "qtd": {"$sum": "$qtd"}, "volume": {"$sum": "$volume"}}}]
```
Sobre `pix.metricas_janela` — soma o que o Atlas Stream Processing já agregou.

### `_dlq_resumo()` — DLQ agrupada por motivo (linha 3721-3729)

```python
[
    {"$group": {"_id": {"$ifNull": ["$errInfo.reason", "motivo não informado"]}, "qtd": {"$sum": 1},
                "primeiro": {"$min": "$_stream_meta.source.ts"}, "ultimo": {"$max": "$_stream_meta.source.ts"}}},
    {"$sort": {"qtd": -1}}, {"$limit": 10},
]
```
Exposto em `GET /streaming/asp/dlq/resumo`. Agrupa mensagens rejeitadas pelo `$validate` do processor, por motivo — é assim que se opera uma fila de rejeitados de verdade, não só "N erros".

### `_fonte_conferivel()` — reconciliação, contagem + valor (linha 3880-3895)

```python
[
    {"$match": {"run_id": run_id}},
    {"$group": {
        "_id": None,
        "documentos": {"$sum": 1},
        "centavos": {"$sum": {"$cond": [
            {"$isNumber": "$valor"},
            {"$round": [{"$multiply": [{"$toDecimal": "$valor"}, 100]}, 0]},
            0,
        ]}},
        "nao_numericos": {"$sum": {"$cond": [{"$isNumber": "$valor"}, 0, 1]}},
    }},
]
```
Onde: `GET /streaming/reconciliacao`. O que faz: um único `$group` conta e soma em centavos inteiros (nunca ponto flutuante — três somas independentes de `double` deixam resíduo indistinguível de divergência real). `$isNumber` isola o evento inválido injetado de propósito (`valor` como string) para não contaminar a soma. Complementado por um **digest de conteúdo** (`_digest_da_fonte`: `find({"run_id": run_id})` sem projeção, SHA-256 da forma canônica de cada documento, agregado em ordem de `endToEndId`), calculado só depois que o gerador para e pulado acima de `MAX_DOCS_DIGEST = 200.000`. Change Streams e Kafka calculam o mesmo hash sobre o documento que recebem; só bate quando todos os campos de todos os documentos são iguais aos da fonte. A soma sozinha não detecta mutações que se compensam.

### `_reconcile_run()` — soma das janelas do ASP para o mesmo run (linha 3920-3928)

```python
[
    {"$match": {"run_id": run_id}},
    {"$group": {"_id": None, "processadas": {"$sum": "$qtd"}, "alertas_valor_alto": {"$sum": "$alertas_valor_alto"},
                "volume": {"$sum": "$volume"}, "janelas": {"$sum": 1}}},
]
```
Sobre `pix.metricas_janela`, comparado contra `_fonte_conferivel()`. O caminho do ASP é só agregado (sem conjunto de ids), então reconcilia por valor dentro de tolerância declarada (±R$0,01 por janela fechada).

### Falhas injetadas (não são queries, mas operam sobre os mesmos dados)

`POST /streaming/falha/connector`, `/falha/evento-invalido`, `/falha/schema-incompativel`, `/falha/failover` — ver `architecture.md`/README para o comportamento; tecnicamente escrevem documentos malformados de propósito (`insert_many` com `ordered=False`) para observar como a DLQ e a reconciliação absorvem o defeito.

### Oplog — não é find comum

```python
oplog = client["local"]["oplog.rs"]
primeiro = next(oplog.find({}, {"ts": 1}).sort("$natural", 1).limit(1), None)
ultimo   = next(oplog.find({}, {"ts": 1}).sort("$natural", -1).limit(1), None)
```
Onde: linha 1495-1497, exposto em `GET /streaming/oplog`. O que faz: mede a janela de retenção real do oplog em minutos — é o limite operacional da garantia de resume token de um Change Stream.

---

## Coleções auxiliares do Streaming (sem pipeline complexo, mas relevantes)

- `pix.transacoes` — origem, com os 3 índices listados acima.
- `pix.metricas_janela` — sink do processor ASP `pixJanelas5s`, alimentado via `$merge` (definido em `scripts/setup-asp.js`, fora do backend Python).
- `pix.dlq` / `pix.dlq_audit` — dead-letter queue do processor e trilha de auditoria de reprocessamento.
- `pix.consumer_checkpoints` — resume tokens dos cursores de Change Streams (coluna 1 do Streaming), um por partição de demonstração.

---

## Tese — `backend/routers/tese.py`

`POST /tese/medir` (lock: uma medição por vez, 409 na segunda) roda 5 repetições de cada operação pelo `client`/`db` de `database.py` e devolve p50/máx:

```python
client.admin.command("ping")
db["produtos"].find({"categoria": "Eletrônicos"}, {"_id": 0, "nome": 1}).sort("total_avaliacoes", -1).limit(10)  # + explain() → indexName
db["produtos"].aggregate([{"$match": {"categoria": "Eletrônicos"}},
                          {"$group": {"_id": "$marca", "produtos": {"$sum": 1}, "preco_medio": {"$avg": "$preco"}}},
                          {"$sort": {"produtos": -1}}, {"$limit": 5}])
db.create_collection("tese_probe_validado", validator={"$jsonSchema": {"bsonType": "object", "required": ["valor"],
                     "properties": {"valor": {"bsonType": "number", "minimum": 0}}}}, validationAction="error")
db["tese_probe_validado"].insert_one({"valor": -1})          # espera WriteError code 121
db["tese_probe"].watch(full_document="updateLookup")          # insert → evento no cursor
session.with_transaction(lambda s: (db["tese_probe"].insert_one(..., session=s), db["tese_probe_b"].insert_one(..., session=s)))
```

As coleções `tese_probe*` são dropadas ao fim da medição e pelo `reset_demo.py`.

## O que NÃO foi encontrado / está fora deste repositório

- **Nenhum índice `2dsphere`, `$geoNear`, `$search` ou `$vectorSearch`** no código atual — o módulo de risco geográfico (que os usava) foi extraído para `mongodb-atlas-geo-showcase` em 2026-09-11. Se precisar dessas queries, consulte aquele repositório.
- Não há Vector Search nem Atlas Search configurados neste projeto (confirmado por grep em todo `backend/`).
