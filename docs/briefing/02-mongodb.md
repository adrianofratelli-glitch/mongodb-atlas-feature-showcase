# Atlas Feature Showcase — MongoDB: databases, índices e pipelines

> Segunda parte do briefing. Tudo que roda contra o cluster: separação de databases, índices por módulo, os pipelines do Geo, o contrato de streaming e as três checagens da reconciliação.

---

## Conexão

`database.py` com **um único `MongoClient`**, `connect=False`, `appname` setado e timeouts explícitos. Mais uma função `readiness()`.

Faz o import cair pra uma URI de localhost quando `MONGO_URI` estiver ausente. Motivo: eu quero que a falta da variável apareça em `/health/ready`, como problema de configuração, e não como erro de import derrubando o processo inteiro na subida.

## Separação de databases

| Database | Quem usa |
|---|---|
| `POC` | os módulos 01–06 (`produtos`, `avaliacoes`) |
| `pix` | módulo 07, Streaming |
| `geo` | módulo 08, risco geográfico |

**O módulo Geo nunca escreve em `POC` nem em `pix`** — e o inverso também vale, com uma exceção declarada: o sinal ao vivo grava em `geo.sinais_ao_vivo`, que é dado de corrida, limpo pelo reset. O dataset versionado (`geo.transacoes`) nunca é tocado por limpeza nenhuma.

## Índices do módulo Geo — `scripts/seed_geo.py`

```python
colecao.create_index([("endToEndId", ASCENDING)], name="e2e_unq_idx", unique=True)

# Demo A: igualdade primeiro, geo por último. O campo geo NÃO precisa ser
# prefixo para $geoWithin/$geoIntersects.
colecao.create_index([("clienteId", 1), ("status", 1), ("local", GEOSPHERE)],
                     name="cliente_status_local_idx")

# Demo A: geo puro, o outro lado do comparativo de explain.
colecao.create_index([("local", GEOSPHERE)], name="local_2dsphere_idx")

# Demo B: $setWindowFields particionado por cliente e ordenado por ts.
colecao.create_index([("clienteId", 1), ("ts", 1)], name="cliente_ts_idx")
colecao.create_index([("uf", 1), ("ts", -1)], name="uf_ts_idx")
colecao.create_index([("estabelecimento.categoria", 1), ("local", GEOSPHERE)],
                     name="categoria_local_idx")
```

Os seis nomes ficam num `INDICES_OBRIGATORIOS` e o preflight cobra os seis. Índice faltando muda o plano de execução e a demo de `explain` deixa de fazer sentido.

O fato de o campo geoespacial **não precisar ser prefixo** do índice composto é justamente o que a Demo A prova — e é contraintuitivo pra quem vem de outros bancos.

## Os três pipelines do Geo

### A — `explain-compare`

O **mesmo** `$geoWithin` com dois `hint` diferentes (`cliente_status_local_idx` e `local_2dsphere_idx`), comparando os planos lado a lado:

```python
{"local": {"$geoWithin": {"$centerSphere": [[lng, lat], raioKm / RAIO_TERRA_KM]}}}
```

O número medido é que decide qual ganha, não a intuição — e é isso que eu mostro.

### B — `impossible-travel`, em MQL puro

`$setWindowFields` + `$shift` + haversine **no servidor**. Nada de pós-processamento em Python; o ponto é exatamente que isso roda no banco.

```python
{"$setWindowFields": {
    "partitionBy": "$clienteId",
    "sortBy": {"ts": 1},
    "output": {
        "ts_ant":        {"$shift": {"output": "$ts", "by": -1}},
        "coord_ant":     {"$shift": {"output": "$local.coordinates", "by": -1}},
        "municipio_ant": {"$shift": {"output": "$municipio", "by": -1}},
        # … uf, dispositivo, localizacaoMeta
    },
}},
{"$match": {"ts_ant": {"$ne": None}}},                       # 1ª transação não tem anterior
{"$addFields": {"minutos": {"$divide": [{"$subtract": ["$ts", "$ts_ant"]}, 60_000]}}},
```

Antes do haversine vem um **corte geométrico**, e ele vale explicar: nenhum par de pontos na Terra dista mais que meia circunferência, então um intervalo maior que `(π·R / limite)` horas não pode violar o limite, seja qual for a geografia. Isso descarta a maior parte dos documentos antes da parte cara **sem depender de nada específico deste dataset**:

```python
{"$match": {"minutos": {"$gt": 0, "$lt": (math.pi * RAIO_TERRA_KM / limiteKmh) * 60}}}
```

Depois: haversine em operadores nativos, `kmh = km / (minutos/60)`, `$match` no limite, `$sort` decrescente, `$limit`, `$project`.

O pipeline fecha num `$facet` sobre o **mesmo fluxo já particionado** — o `$setWindowFields`, que é a parte cara, roda uma vez só:

```python
{"$facet": {
    "avaliados": [{"$count": "pares"}],   # ANTES do corte geométrico
    "sinais": ramo_sinais,
}}
```

O ramo `avaliados` conta os pares **antes** do corte, porque a pergunta de um time de risco não é "quantos sinais saíram" e sim "de quantas oportunidades". Sem denominador, "40 pares" não diz se a regra é seletiva ou se inunda a fila de alertas.

O `$facet` custa uns 5% sobre o pipeline simples **medido quente**; cache frio do WiredTiger logo depois de um reseed parece uma regressão de 2,6× e não é.

### C — `search`

Um único `$search` combinando texto + `geoWithin` + facetas. Degrada pra `nao_configurado` sem o índice. Ele é ancorado na **compra contestada**: passa o `endToEndId` e o centro vira a coordenada cadastrada daquele terminal. O `termo` é opcional — sem ele a cláusula de score é um `exists` (um compound só de `filter` devolve tudo com score zero) e o resultado ordena por distância, com o desempate aplicado **antes** do dedup por terminal.

## Custo nos dois escopos

Varredura completa contra corte por cliente: medido **2,5s contra 371 ms, 6,8×**. Esse número vai na tela junto com os sinais — capacidade sem custo é meia informação.

## Seed do Geo

`scripts/seed_geo.py` gera **150k transações georreferenciadas**, em clusters gaussianos em torno de 40 municípios reais. A localização é a coordenada **cadastrada do terminal adquirente**, e o terminal mantém identidade, estabelecimento e coordenada estáveis. **Idempotente** por seed fixa de RNG + índice único em `endToEndId`.

Uma regra que eu descobri errando: **o par plantado deriva o intervalo a partir de uma velocidade-alvo, nunca o contrário** (1.100–9.000 km/h). Sortear os minutos direto deu 16.000–42.000 km/h — vinte vezes qualquer padrão real de cartão clonado — e um intervalo fixo de 5 minutos com destino fixo deixou as 40 linhas idênticas na tela. O dataset v5 também sorteia a posição do par dentro da sequência do cliente, pra os casos se espalharem pelos 90 dias.

A procedência (`plantado`/`emergente`) sai do `fraud_seeds.json` e é reportada na tela. Hoje todo resultado retrospectivo é plantado, e **dizer isso é o ponto**.

## Dois instantes que não podem ser confundidos

`ts` é a **chegada** (campo de TTL, ordenação do stream) e `compradaEm` é a **compra no terminal**, possivelmente minutos antes.

Retrodatar o `ts` fez o TTL apagar a metade mais velha de um par **antes da reconciliação**, e a origem contou menos que os três consumidores — expiração que parecia exatamente perda de dado.

## Streaming — o que o cluster faz

- **Coluna 1**: change streams sobre a coleção de PIX, com N workers particionados.
- **Coluna 2**: MongoDB Kafka Connector lendo o mesmo change stream, connector `atlas-pix-source`.
- **Coluna 3**: Atlas Stream Processing, processor `pixJanelas5s` (janela de 5s).
- **Ponte com o Geo**: processor `geoSinais30s`, janela deslizante de 30s por cartão, haversine em MQL, `$merge` em `geo.sinais_ao_vivo`.

Preflight de Streaming exige três índices: `endToEndId` único, o TTL e `run_id_reconciliacao`. A limpeza precisa recriar os três. O rótulo laranja é `Pré-voo pendente`, não um pedido genérico de "verificar".

## A reconciliação — três checagens, não uma

Confere **contagem**, **soma em centavos inteiros** e um **digest XOR do conjunto de `endToEndId`**.

Contagem sozinha passa quando um documento é trocado por outro ou quando um valor é transformado em trânsito — e aí você tem um painel verde mentindo.

O caminho de ASP é só agregado, então reconcilia por valor dentro de uma tolerância de arredondamento **declarada** (±R$ 0,01 por janela fechada) e não tem conjunto de id pra digerir.

## Contrato de schema

`/contrato` publica o contrato que espelha o `$validate` do processor. É o que permite a injeção `/falha/schema-incompativel` (renomear `valor` → `amount`) provar a conversa de contrato **sem infraestrutura nova**.

E o módulo 04 é a mesma ideia no banco: `$jsonSchema` + `collMod`, com o Atlas recusando documento fora do contrato ao vivo.

## Limpeza e resets

- `/streaming/reset` confirma vazio com `count_documents({}, limit=1)`, **nunca** com `estimated_document_count()` — a metadata ainda reporta o total anterior logo depois de um `delete_many`, e o Reset respondia 503 com a coleção já vazia, abortando o Play sem mensagem nenhuma. E ele reaproveita os clientes Mongo existentes: criar um SRV novo por coleção reintroduz atraso de DNS.
- `scripts/cleanup-streaming-data.py` tem que continuar **escopado às coleções de demo conhecidas**. Ele roda como segunda camada de limpeza quando a API está fora — um cleanup genérico ali seria um estrago silencioso.
