# Mapa do código — mongodb-atlas-feature-showcase

> Também conhecida como: **Atlas Feature Showcase (8 módulos)** (pasta antiga `atlas-showcase-capacidades`). Portas: frontend **5174** / backend **8002**. Em uma frase: uma vitrine interativa que roda, contra um cluster Atlas real, sete capacidades da plataforma (reindexação online, Online Archive, aggregation, schema validation, change streams, transações e uma comparação de três formas de streaming), sempre mostrando a query que rodou e o número medido.

Atalhos para o que já existe: [architecture](briefing/architecture.md) · [queries](briefing/queries.md) · [ui-flows](briefing/ui-flows.md). Este mapa não repete esses textos; ele aponta o arquivo e a linha.

Convenção: `arquivo:linha` é a linha da definição da função (conferida no código atual). O frontend chama o backend por `/api/...`; o Vite remove o prefixo `/api` e repassa para `localhost:8002` (`frontend/vite.config.js`). Por isso, na coluna "Chamada HTTP" abaixo, a rota é a do backend (sem `/api`).

## 1. Árvore anotada

```
.
├── bin/overview               Launcher único: sobe backend+frontend (+ASP/Kafka), preflight, `down`, `status`, `logs`, desligamento automático em 45 min
├── start.sh                   Sobe só backend (8002) e frontend (5174), sem ASP/Kafka
├── scripts/
│   ├── ambiente.sh            Liga/desliga ASP e Kafka local (chamado pelo overview)
│   ├── reset_demo.py          RESET ÚNICO da demo (`--check` só verifica); recusa banco sem `_test`
│   ├── live_monitor.py        Monitor de leitura/escrita para rodar ao lado enquanto um índice é criado
│   ├── capture_replay.py      Grava uma corrida real do Streaming em backend/data/replay_streaming.json
│   ├── cleanup-streaming-data.py  Limpeza dos dados da rodada de streaming (usado pelo reset; guarda showcase_*_test)
│   ├── setup-asp.js / setup-kafka-connector.sh / kafka-local.sh   Provisionamento do processor ASP e do Kafka/connector
│   ├── setup-asp-geo.js       Segundo processor ASP (sinais geográficos; ver nota na seção 4)
│   ├── prepare-demo.sh, generate-streaming-guide.py, lib/expand_srv.py   Apoio de preparação/guia
├── backend/
│   ├── main.py                App FastAPI: CORS, request-id, handlers de erro, liga os routers, /health/*, /preflight, /stats
│   ├── settings.py            Lê as variáveis de ambiente uma vez (dataclass `Settings`)
│   ├── database.py            Único `MongoClient` + `readiness()`
│   ├── security.py            Middlewares: `MutationGuardMiddleware` (bloqueia mutação fora de localhost sem token) e `ApiHardeningMiddleware`
│   ├── seed_data.py           Popula `produtos`/`avaliacoes` (idempotente) + `ensure_indexes()`
│   ├── data/                  `municipios.json` (gerador do streaming) e `replay_streaming.json` (corrida gravada)
│   ├── routers/               UM arquivo por módulo da UI
│   │   ├── tese.py            Aba 00: `POST /tese/medir`
│   │   ├── reindexacao.py     Aba 01: criar/derrubar índice com cluster servindo tráfego
│   │   ├── hot_cold.py        Aba 02: Online Archive (Atlas Admin API) + distribuição por data
│   │   ├── aggregations.py    Aba 03: 6 pipelines
│   │   ├── schema_validation.py  Aba 04: `$jsonSchema` strict × constraint
│   │   ├── change_streams.py  Aba 05: watch no backend + SSE
│   │   ├── transactions.py    Aba 06: transação multi-coleção + benchmark
│   │   ├── busca.py           Aba 08: MongoDB Search (índice próprio `showcase_busca`, fuzzy, facetas)
│   │   ├── streaming.py       Aba 07 (4.212 linhas): gerador, 3 colunas, falhas, reconciliação
│   │   └── replay.py          Mesmo contrato do streaming, servindo o JSON gravado (nunca toca o Mongo)
│   └── tests/                 Testes unitários (Mongo stubado); tests/live/ opt-in contra *_test
├── frontend/
│   ├── vite.config.js         Porta 5174 + proxy `/api` -> 8002
│   └── src/
│       ├── App.jsx            Casca, seletor de módulo (array `MODULES`, linha 14), navegação por hash
│       ├── pages/             Uma tela por módulo (Tese, Reindexacao, HotCold, Aggregations, SchemaValidation, ChangeStreams, Transactions, Streaming, Busca)
│       ├── components/        `QueryBlock.jsx` (mostra a query) e `Limites.jsx` (limite declarado)
│       ├── hooks/useApi.js    Wrapper de fetch (timeout, token, tradução de erro)
│       ├── hooks/usePolling.js  Timers que param com a aba escondida
│       └── pov-signature.css  Visual comum do portfólio (cópia, ver seção 4)
└── docs/                      briefing/, reference.md, roteiro e setup do streaming, screenshots
```

## 2. "Apertei o botão X, o que roda?"

Os componentes chamam o backend pela função `call(...)` do hook `useApi` (`frontend/src/hooks/useApi.js`). Todos os botões abaixo passam por ela.

### Casca (`frontend/src/App.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| Abrir a página (selo de prontidão) | `App.jsx:98` | `GET /preflight` | `backend/main.py:113` | `readiness()` (`database.py`) + `streaming.preflight_atlas_admin` (`streaming.py:3203`) | `ping` (`database.py`) |
| Contadores do topo | `App.jsx:124` | `GET /stats` | `main.py:156` | `stats()` | `produtos`, `avaliacoes`: `estimated_document_count` |
| Info de cluster/tier no topo | `App.jsx:134` | `GET /streaming/cluster` | `streaming.py:1776` | `_cluster_info_sync` (`streaming.py:1545`) | Atlas Admin API (sem query Mongo) |
| Trocar de módulo | `App.jsx:194` (select) + `hashchange` (`App.jsx:116`) | nenhuma | - | só troca o componente | - |

### 00 A tese (`frontend/src/pages/Tese.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| **Medir agora** | `Tese.jsx:18` (`medir`) | `POST /tese/medir` | `routers/tese.py:195` | `_executar` (`tese.py:165`): `_medir_ping` 60, `_medir_consulta_indexada` 69, `_medir_agregacao` 89, `_medir_schema` 104, `_medir_change_stream` 122, `_medir_transacao` 140 | `produtos` (find+explain, aggregate), coleções temporárias de schema/change stream/transação |
| Tabela de componentes (antes de medir) | `Tese.jsx:130` (texto fixo, `COMPONENTES_FALLBACK`) | `GET /tese/componentes` existe, mas a tela só usa o texto local | `tese.py:205` | `componentes` | nenhuma |

### 01 Reindexação Online (`pages/Reindexacao.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| Lista de índices / **↻ Atualizar** | `Reindexacao.jsx:51` (`fetchIndexes`) | `GET /reindexacao/indexes` | `reindexacao.py:68` | `list_indexes` | `produtos`: `list_indexes()` |
| Cenário de índice (**Criar**) | `Reindexacao.jsx:63` (`handleCreate`) | `POST /reindexacao/create` | `reindexacao.py:93` | `create_index` valida e dispara `_build_worker` (`reindexacao.py:81`) em thread | `produtos`: `create_index` (`reindexacao.py:84`) |
| Sondagem de leitura durante o build | `Reindexacao.jsx:94` | `GET /reindexacao/read-probe` | `reindexacao.py:200` | `read_probe` | `produtos`: `find_one` |
| Acompanhar build | `Reindexacao.jsx:107` | `GET /reindexacao/build-status` | `reindexacao.py:156` | `build_status` | `produtos`: `list_indexes` |
| **Rodar explain** (simples/composto/parcial) | `Reindexacao.jsx:130` (`runExplain`) | `GET /reindexacao/explain` | `reindexacao.py:227` | `explain` (usa `_walk_stages` 212) | comando `explain` executionStats em `produtos` |
| **Remover** índice | `Reindexacao.jsx:137` (`handleDrop`) | `DELETE /reindexacao/drop/{index_name}` | `reindexacao.py:177` | `drop_index` (só índices `demo01_`, `is_demo_index` 46) | `produtos`: `drop_index` |
| Cenários prontos | texto da tela | `GET /reindexacao/demo-scenarios` (existe; a tela monta os cenários localmente) | `reindexacao.py:259` | `demo_scenarios` | nenhuma |

### 02 Hot / Cold (`pages/HotCold.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB / externo |
|---|---|---|---|---|---|
| Distribuição por ano | `HotCold.jsx:31` (`fetchDistribution`) | `GET /hot-cold/distribution` | `hot_cold.py:114` | `data_distribution` | `produtos`: `aggregate` + `estimated_document_count` |
| Simulação de arquivamento | `HotCold.jsx:32` | `GET /hot-cold/archive-simulation` | `hot_cold.py:168` | `archive_simulation` | `produtos`: `aggregate` |
| Query transparente quente+frio | `HotCold.jsx:33` | `GET /hot-cold/query-transparent` | `hot_cold.py:205` | `query_transparent` | `produtos`: dois `find` (data >= e < corte) |
| Listar archives | `HotCold.jsx:18` | `GET /hot-cold/online-archive/list` | `hot_cold.py:251` | `list_online_archives` -> `_atlas_request` (`hot_cold.py:55`) | Atlas Admin API |
| **Criar Online Archive** | `HotCold.jsx:35` (`createArchive`) | `POST /hot-cold/online-archive/create` | `hot_cold.py:277` | `create_online_archive` | Atlas Admin API (`hot_cold.py:301`) |
| **Remover** archive | `HotCold.jsx:44` (`deleteArchive`) | `DELETE /hot-cold/online-archive/{archive_id}` | `hot_cold.py:319` | `delete_online_archive` | Atlas Admin API (`hot_cold.py:333`) |

### 03 Aggregation Pipeline (`pages/Aggregations.jsx`)

Abas `lookup/facet/union/group/window/bucket` (`Aggregations.jsx:352`) + botão **Executar** (`Aggregations.jsx:338`, `run`). O mapa de rotas está em `Aggregations.jsx:329` (`ENDPOINTS`).

| Aba | HTTP | Handler | MongoDB |
|---|---|---|---|
| `$lookup` | `GET /aggregations/lookup` | `aggregations.py:23` (`lookup_produtos_avaliacoes`) | `avaliacoes`: aggregate (`:77`) |
| `$facet` | `GET /aggregations/facet` | `aggregations.py:83` (`facet_analytics`) | `produtos`: aggregate (`:114`) |
| `$unionWith` | `GET /aggregations/union-with` | `aggregations.py:119` (`union_with`) | `avaliacoes`: aggregate (`:167`) |
| `$group` avançado | `GET /aggregations/group-advanced` | `aggregations.py:171` (`group_advanced`) | `produtos`: aggregate (`:190`) |
| `$setWindowFields` | `GET /aggregations/window-functions` | `aggregations.py:199` (`window_functions`) | `produtos`: aggregate (`:234`) |
| `$bucketAuto` | `GET /aggregations/bucket-auto` | `aggregations.py:243` (`bucket_auto`) | `produtos`: aggregate (`:257`) |

A função de negócio é o próprio handler: o pipeline é montado na função e devolvido junto com o resultado para o `QueryBlock` exibir.

### 04 Schema Validation (`pages/SchemaValidation.jsx`)

Passos numerados via `runStep` (`SchemaValidation.jsx:88`); lista de passos em `:115-145`.

| Ação | Componente | HTTP | Handler | MongoDB (coleção da demo `COL`) |
|---|---|---|---|---|
| Estado inicial / **↻ Atualizar** | `SchemaValidation.jsx:73,84` | `GET /schema/status`, `GET /schema/documents` | `schema_validation.py:62`, `:222` | `listCollections`, `count_documents`, `find` |
| Passo 1: criar coleção | `:115` | `POST /schema/step1-create-collection` | `schema_validation.py:81` (`step1_create`) | `create_collection` |
| Passo 2: inserir sem schema | `:121` | `POST /schema/step2-insert-without-schema` | `:90` (`step2_insert`) | `insert_many` |
| Passo 3: ativar schema | `:127` | `POST /schema/step3-activate-schema` | `:163` (`step3_activate`) | `collMod` com `$jsonSchema` |
| Passo 4: tentar `constraint` | `:133` | `POST /schema/step4-try-constraint` | `:114` (`step4_try_constraint`) | `collMod prepareConstraintValidationLevel` e `validationLevel="constraint"` |
| Passo 5: reparar legado | `:139` | `POST /schema/step5-repair-legacy` | `:141` (`step5_repair_legacy`) | `update_many` |
| Cenários de inserção inválida | `:236` | `POST /schema/step4-insert-invalid?scenario=` | `:182` (`step4_insert_invalid`) | `insert_one` (esperado: rejeição) |
| Inserção válida | (no fluxo da tela) | `POST /schema/insert-valid` | `:213` (`insert_valid`) | `insert_one` |
| **↺ Resetar demo** | `:103` (`reset`) | `DELETE /schema/reset` | `:231` (`reset`) | `drop` da coleção da demo |

### 05 Change Streams (`pages/ChangeStreams.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| **Iniciar demo** | `ChangeStreams.jsx:55` (`startDemo`) | `POST /change-streams/start` | `change_streams.py:159` | `start_watch` abre thread `_watch_worker` (`:93`) | `transacoes_cs_demo`: drop + `create_collection`, depois `watch` (`:96`) |
| Feed ao vivo | `ChangeStreams.jsx:64` (`EventSource`) | `GET /change-streams/feed` (SSE) | `change_streams.py:249` | `stream_events` | lê eventos em memória (`_append_event` `:32`) |
| Disparar insert/update/delete | `ChangeStreams.jsx:71` | `POST /change-streams/trigger?operacao=` | `change_streams.py:202` | `trigger_event` | `insert_one`, `update_one`, `delete_one` |
| Parar / **Reiniciar** | `ChangeStreams.jsx:77,85,92` | `POST /change-streams/stop` | `change_streams.py:318` | `stop_watch` | fecha o cursor |
| Ver coleção | `ChangeStreams.jsx:79` | `GET /change-streams/collection` | `change_streams.py:296` | `get_collection` | `count_documents`, `find` |
| **Limpar coleção** | `ChangeStreams.jsx:97` | `DELETE /change-streams/clear` | `change_streams.py:328` | `clear_collection` | `drop` |
| (existe, sem uso direto pela tela) | - | `GET /change-streams/events` | `change_streams.py:235` | `get_events` | - |

### 06 Transações (`pages/Transactions.jsx`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| Contagem das coleções / **↻ Atualizar** | `Transactions.jsx:117` | `GET /transactions/status` | `transactions.py:30` | `status` | `count_documents` |
| **Executar (commit)** | `Transactions.jsx:121` (`executar(false)`) | `POST /transactions/executar?simular_falha=false` | `transactions.py:39` | `executar_transacao` com callback `_compra` + `with_transaction` (`:135`) | `produtos` (find_one), `pedidos_demo` (insert), `estoque_demo` (update), `pagamentos_demo` (insert) |
| **Executar com falha (rollback)** | `Transactions.jsx:121` (`executar(true)`) | mesma rota com `simular_falha=true` | `transactions.py:39` | levanta `SimulatedPaymentError` (`:21`) no meio e a transação desfaz tudo | idem |
| **Resetar** | `Transactions.jsx:127` | `POST /transactions/reset` | `transactions.py:176` | `reset` | `drop` das coleções da demo |
| **Medir** (benchmark) | `Transactions.jsx:146` (`medir`) | `POST /transactions/benchmark?amostras=30&concorrencia=6` | `transactions.py:282` | `_benchmark` (`:294`), `_transacao_multi` (`:262`), `_rtt_base_ms` (`:230`), `_limite_transacao` (`:249`) | coleções `BENCH_COL_*`: insert/update em transação, `delete_many` por `run_id` (`:393`) |

### 07 Streaming (`pages/Streaming.jsx`, 1.403 linhas; backend `routers/streaming.py`)

Modo "ao vivo" usa `/streaming/*`; modo "replay" usa `/replay/*` com o mesmo contrato (`replay.py`, nunca toca o Mongo). A tela escolhe com a variável `base`.

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB / externo |
|---|---|---|---|---|---|
| Alternar **Ao vivo / Replay** | `Streaming.jsx:524-550` | só estado local; replay carrega `GET /replay/manifest` (`:174`) | `replay.py:180` | `manifest` | lê `data/replay_streaming.json` |
| **Iniciar** corrida | `Streaming.jsx:192` (`iniciar`) | `POST /streaming/reset` (`:236`) e `POST /streaming/generator/start` (`:259`); replay: `POST /replay/play` | `streaming.py:2318`, `:1438` | `reset` limpa a rodada; `generator_start` -> `Generator.start` (`:1090`) | `transacoes`: `insert_many`/`insert_one` (`Generator._insert_batch` `:1241`, `_insert_um` `:1305`) |
| **Parar** | `Streaming.jsx:255` (`stopGen`) | `POST /streaming/generator/stop` (ou `/replay/stop`) | `streaming.py:1453` | `Generator.stop` (`:1146`) | - |
| **Resetar execução** | `Streaming.jsx:303` (`resetExecucao`) | `POST /streaming/reset` | `streaming.py:2318` | `reset`: `_drop_and_recreate` 2183, `_purge` 2215, `_resto_exato` 2200, para o ASP via `_asp_stop_wait` 2150 | `transacoes`, `metricas_janela`, `dlq`, `dlq_audit`, `consumer_checkpoints`; `delete_many` em `geo.sinais_ao_vivo` (`:2401`) |
| Coluna 1: eventos Change Streams | `Streaming.jsx:321` (`useSse`) | `GET /streaming/changestream` (SSE) | `streaming.py:2778` | `ChangeStreamCluster` (`:2713`) / `ChangeStreamWorker` (`:2450`) | `transacoes`: `watch` (`:2615`); checkpoint em `consumer_checkpoints` |
| Derrubar e retomar o cursor | `Streaming.jsx:339` (`dropResume`) | `POST /streaming/changestream/drop-resume` | `streaming.py:2792` | `ChangeStreamCluster.request_drop` | resume token em `consumer_checkpoints` |
| Coluna 2: Kafka Connector | `Streaming.jsx:346` (`useSse`), status `:385` | `GET /streaming/kafka` (SSE), `GET /streaming/kafka/status` | `streaming.py:3534`, `:3487` | `KafkaConsumer` (`:2835`), `classifica_connectors` (`:2966`) | Kafka Connect REST + broker Kafka (sem query Mongo) |
| **Reiniciar Kafka** / consumidor | `Streaming.jsx:364`, `:399` | `POST /streaming/kafka/restart`, `POST /streaming/kafka/consumer/restart` | `streaming.py:3515`, `:3523` | `_connector_restart_sync` (`:3037`), `KafkaConsumer.restart` (`:2860`) | Kafka Connect REST |
| Coluna 3: Atlas Stream Processing | `Streaming.jsx:379` (`useSse`), status `:416` | `GET /streaming/asp` (SSE), `GET /streaming/asp/status` | `streaming.py:3841`, `:3736` | `AspWatcher` (`:3769`), `_asp_runtime_stats` (`:3625`), `_asp_totais` (`:3723`) | `metricas_janela`: `watch` (`:3793`), `find` (`:3844`), `aggregate` (`:3726`) |
| Injetar 1 / 1000 inválidos (ASP) | `Streaming.jsx:469` (`injectInvalid`) | `POST /streaming/asp/inject-invalid?quantidade=` | `streaming.py:3883` | `asp_inject_invalid` + `_doc_invalido` (`:3869`) | `transacoes`: `insert_many` (`:3898`) |
| **Reprocessar DLQ** | `Streaming.jsx:472` (`reprocessarDlq`) | `POST /streaming/asp/dlq/reprocessar` | `streaming.py:4030` | `_reprocessa_dlq` (`:3943`) | `dlq` (find/delete_many), `transacoes` (insert_one), `dlq_audit` (replace_one) |
| Resumo da DLQ | `Streaming.jsx:413` | `GET /streaming/asp/dlq/resumo` | `streaming.py:3939` | `_dlq_resumo` (`:3915`) | `dlq`: aggregate + `count_documents` |
| **Reiniciar ASP do checkpoint** | `Streaming.jsx:476` | `POST /streaming/asp/restart-checkpoint` | `streaming.py:3762` | `_asp_restart_from_checkpoint` (`:3660`) | comandos `stopStreamProcessor`/`startStreamProcessor` na instância ASP |
| Falha: **derrubar connector** | `Streaming.jsx:260` | `POST /streaming/falha/connector` | `streaming.py:3374` | `_connector_derrubar_sync` (`:3064`) | Kafka Connect REST |
| Falha: **evento inválido** | `Streaming.jsx:290` | `POST /streaming/falha/evento-invalido` | `streaming.py:3383` | `falha_evento_invalido` | `transacoes`: `insert_one` (`:3404`) |
| Falha: **schema incompatível** | `Streaming.jsx:281` | `POST /streaming/falha/schema-incompativel` | `streaming.py:3451` | `falha_schema_incompativel` | `transacoes`: `insert_one` (`:3474`) |
| Falha: **forçar failover** | `Streaming.jsx:272` | `POST /streaming/falha/failover` | `streaming.py:3316` | `_atlas_test_failover_sync` (`:3150`) + `_acompanha_failover` (`:3280`) | Atlas Admin API (testFailover) |
| Painel de reconciliação | `Streaming.jsx:428,437` | `GET /streaming/reconciliacao?run_id=` | `streaming.py:4282` | `_reconcile_run` (`:4140`), `_fonte_conferivel` (`:4084`), `_digest_da_fonte` (`:4075`) | `transacoes`, `metricas_janela`, `dlq`, `dlq_audit` |
| Folga de CPU do primário | `Streaming.jsx:408` | `GET /streaming/folga` | `streaming.py:2015` | `_folga_cluster_sync` (`:1859`) | Atlas Admin API (métricas) |
| Latência de leitura / oplog | `Streaming.jsx:413` | `GET /streaming/leitura`, `GET /streaming/oplog` | `streaming.py:1757`, `:1771` | `SondaLeitura` (`:1701`), `_oplog_janela` (`:1666`) | `transacoes` (`find_one`), `local.oplog.rs` |
| Cenário/preset de TPS | `Streaming.jsx:162,664` | `GET /streaming/cenario`, `GET /streaming/rede`, `GET /streaming/contrato` | `streaming.py:2139`, `:2040`, `:3431` | `cenario`, `_medir_rtt` (`:2019`), `contrato` | `ping` (rede) |

Rotas só de apoio, sem botão direto: `GET /streaming/perfil-valores` (`streaming.py:1644`), `GET /streaming/generator/status` (usada pelo polling, `Streaming.jsx:181`), `GET /streaming/asp/dlq` (`streaming.py:4036`), `GET /streaming/asp/janelas` (`streaming.py:4044`), `GET /streaming/changestream/status` (`streaming.py:2798`), `GET /streaming/falha/failover/status` (`streaming.py:3369`).

### 08 Busca (`pages/Busca.jsx`; backend `routers/busca.py`)

| Ação | Componente | HTTP | Handler | Função de negócio | MongoDB |
|---|---|---|---|---|---|
| Abrir a aba / estado do índice (repete a cada 5 s só enquanto constrói) | `Busca.jsx:35` (`carregarIndice`), timer `Busca.jsx:42` | `GET /busca/indice` | `busca.py:83` | `_info_indice` (`busca.py:53`) | `produtos`: `$listSearchIndexes` (`:60`) |
| **Criar índice** (só com o índice ausente) | `Busca.jsx:44` (`criar`) | `POST /busca/indice` | `busca.py:89` | `criar_indice`; comando em `comando_criacao` (`:77`) | `produtos`: `createSearchIndexes` (`:98`) |
| **Buscar** / sugestões | `Busca.jsx:49` (`buscar`), form `:87`, sugestões `:102` | `GET /busca?q=&fuzzy=&so_em_estoque=` | `busca.py:137` | `montar_pipelines` (`:107`); só executa com o índice `pronto` | `produtos`: `$search` (`:162`) e `$searchMeta` (`:165`); `OperationFailure` no meio vira `executado: false` |

## 3. Onde estão as queries MongoDB

Briefing com o texto completo de cada query: [queries.md](briefing/queries.md). Não há `$regex` no código. `$search`/`$searchMeta` só no módulo 08 (`busca.py`); não há `$vectorSearch` ([ADR 0002](adr/0002-modulo-busca-sem-vector.md)).

| O que faz | arquivo::função (linha) | Operador / pipeline principal | Collection(s) |
|---|---|---|---|
| Seed idempotente | `backend/seed_data.py` (`bulk_write` `:192`) | upsert determinístico em lote | `produtos`, `avaliacoes` |
| Índices base | `seed_data.py::ensure_indexes` (`:166-187`) | `create_index` (categoria, `total_av_idx`, `cat_total_av_idx`, `recent_nota_idx`...) | `produtos`, `avaliacoes` |
| Ping/readiness | `database.py` (`:23`), `main.py::preflight` | `admin.command("ping")` | - |
| Criar índice online | `reindexacao.py::_build_worker` (`:81`) | `create_index` (hybrid build) | `produtos` |
| Listar/derrubar índice | `reindexacao.py::list_indexes` (`:68`), `drop_index` (`:177`) | `list_indexes`, `drop_index` | `produtos` |
| Sonda de leitura | `reindexacao.py::read_probe` (`:200`) | `find_one` | `produtos` |
| Explain por cenário | `reindexacao.py::explain` (`:227`) | `command("explain", ..., "executionStats")` | `produtos` |
| Distribuição por ano | `hot_cold.py::data_distribution` (`:114`) | `aggregate` (`:130`) + `estimated_document_count` | `produtos` |
| Simulação de arquivamento | `hot_cold.py::archive_simulation` (`:168`) | `aggregate` (`:180`) | `produtos` |
| Query quente+fria | `hot_cold.py::query_transparent` (`:205`) | dois `find` com `created_at` (`:214-215`) | `produtos` |
| Online Archive | `hot_cold.py::_atlas_request` (`:55`) usado em `:255,:301,:324,:333` | Atlas Admin API (não é query Mongo) | - |
| `$lookup` | `aggregations.py::lookup_produtos_avaliacoes` (`:23`) | aggregate (`:77`) | `avaliacoes` -> `produtos` |
| `$facet` | `aggregations.py::facet_analytics` (`:83`) | aggregate (`:114`) | `produtos` |
| `$unionWith` | `aggregations.py::union_with` (`:119`) | aggregate (`:167`) | `avaliacoes` |
| `$group` avançado | `aggregations.py::group_advanced` (`:171`) | aggregate (`:190`) | `produtos` |
| `$setWindowFields` | `aggregations.py::window_functions` (`:199`) | aggregate (`:234`) | `produtos` |
| `$bucketAuto` | `aggregations.py::bucket_auto` (`:243`) | aggregate (`:257`) | `produtos` |
| Schema validation | `schema_validation.py` (`:84`-`:231`) | `create_collection`, `collMod` (`:123,124,166`), `insert_*`, `update_many`, `drop` | coleção da demo `COL` |
| Change stream da demo | `change_streams.py::_watch_worker` (`:93`, `watch` em `:96`) | `watch` | `transacoes_cs_demo` |
| Eventos da demo | `change_streams.py::start_watch/trigger_event/get_collection/clear_collection` | `create_collection`, `insert_one`, `update_one`, `delete_one`, `find`, `drop` | `transacoes_cs_demo` |
| Transação de compra | `transactions.py::executar_transacao` (`:39`, callback `with_transaction` `:135`) | `with_transaction` com 4 operações | `produtos`, `pedidos_demo`, `estoque_demo`, `pagamentos_demo` |
| Benchmark de transação | `transactions.py::_benchmark` (`:294`) e `_transacao_multi` (`:262`) | `with_transaction` (`:327,:371`), `getParameter transactionLifetimeLimitSeconds` (`:256`) | coleções `BENCH_COL_*` |
| Medição da tese | `tese.py::_medir_*` (`:60-140`) | `find`+`explain` (`:74,:76`), aggregate (`:99`), `create_collection` com `$jsonSchema` (`:106`), `watch` (`:125`), `with_transaction` (`:149`) | `produtos` + coleções temporárias |
| Gerador de carga | `streaming.py::Generator._insert_batch` (`:1241`, `:1257`), `_insert_um` (`:1305`) | `insert_many(ordered=False)`, `insert_one` (driver assíncrono `AsyncMongoClient`, `acol_tx` `:175`) | `pix.transacoes` |
| Índices do streaming | `streaming.py::_ensure_indexes` (`:851`) | `create_index` único em `endToEndId`, `run_id`, TTL em `ts` (`:867-877`), `command` (`:886`) | `transacoes` |
| Change stream por partição | `streaming.py::ChangeStreamWorker._run` (`:2597`, `watch` `:2615`) | `watch` com pipeline e resume token | `transacoes` |
| Checkpoint do resume token | `ChangeStreamWorker._load_checkpoint/_persist_checkpoint/discard_checkpoint` (`:2546-2591`) | `find_one`, `replace_one`, `delete_one` | `consumer_checkpoints` |
| Watch das janelas ASP | `streaming.py::AspWatcher._run` (`:3789`, `watch` `:3793`) | `watch` com `updateLookup` | `metricas_janela` (e demais) |
| Últimas janelas | `streaming.py::asp_sse` (`:3844`), `asp_janelas` (`:4044`) | `find` ordenado por `window_end` desc | `metricas_janela` |
| Totais do ASP | `streaming.py::_asp_totais` (`:3723`), `_asp_atraso_s` (`:3704`) | aggregate / `find` ordenado | `metricas_janela` |
| Perfil de valores | `streaming.py::_perfil_medido` (`:1607`, aggregate `:1629`) | aggregate | `transacoes` |
| Latência de leitura | `streaming.py::SondaLeitura._consulta` (`:1731`) | `find_one` por `endToEndId` | `transacoes` |
| Oplog / serverStatus | `streaming.py::_oplog_janela` (`:1666`), `_oplatencies_writes` (`:495`) | `find` em `$natural` no oplog, `serverStatus` | `local.oplog.rs` |
| DLQ | `streaming.py::_dlq_resumo` (`:3915`), `_reprocessa_dlq` (`:3943`), `asp_dlq` (`:4036`) | aggregate por motivo, `find`, `delete_many`, `replace_one` | `dlq`, `dlq_audit`, `transacoes` |
| Reconciliação | `streaming.py::_fonte_conferivel` (`:4084`), `_reconcile_run` (`:4140`), `reconciliacao` (`:4282`) | aggregate, `find`, `count_documents` | `transacoes`, `metricas_janela`, `dlq`, `dlq_audit` |
| Reset do streaming | `streaming.py::_drop_and_recreate` (`:2254`), `_resto_exato` (`:2271`), `_purge` (`:2286`), `reset` (`:2318`) | `drop`, `count_documents(limit=1)`, `delete_many` | coleções do streaming + `geo.sinais_ao_vivo` |
| Controle do ASP | `streaming.py::_asp_command` (`:2203`), `_asp_stop_wait` (`:2221`), `_asp_restart_from_checkpoint` (`:3660`) | `listStreamProcessors`, `stopStreamProcessor`, `startStreamProcessor` | instância ASP (outra connection string) |
| Busca textual | `busca.py::buscar` (`:137`), `criar_indice` (`:89`), `_info_indice` (`:53`) | `$search` (`compound`: `text` com `fuzzy`, `equals`), `$searchMeta` (`facet`), `$listSearchIndexes`, `createSearchIndexes` | `produtos` |
| Reset geral | `scripts/reset_demo.py::reset` (`:92`) | drop só das coleções dos módulos + seed | vários (ver script) |
| Monitor ao vivo | `scripts/live_monitor.py::do_read` (`:61`) / `do_write` (`:69`) | leitura e escrita em loop | coleção da demo |

## 4. Compartilhado × só desta PoV

**Vem de `_shared`:** nenhum módulo Python do `_shared` é importado. Verifiquei com `rg`: não há `import` de `_shared` nem `grove_client` no repo (a PoV não usa LLM). O que existe de compartilhado é por **cópia**: `frontend/src/pov-signature.css` é cópia de `_shared/ui/pov-signature.css` (o próprio arquivo diz na linha 3 que a fonte da verdade é lá e que se sincroniza com `sync-ui.sh`).

**Variáveis de ambiente**
- Comuns (podem vir do `.env.shared` via `_shared/sync_env.py`): a conexão do cluster, `MONGO_URI` (lida em `backend/settings.py`). Não confirmei no `sync_env.py` a lista exata de chaves; a regra do portfólio é que as do cluster e do gateway vêm de lá.
- Locais (`backend/.env.example`, só nomes): `MONGO_DB` (banco dos módulos 01-06, padrão `POC`), `STREAMING_DB` (padrão `pix`), `GEO_DB`, `ATLAS_PUBLIC_KEY/ATLAS_PRIVATE_KEY/ATLAS_PROJECT_ID/ATLAS_CLUSTER` (Admin API: Online Archive, failover, métricas), `ATLAS_TIER_INICIAL`, `ATLAS_MIN_TIER`, `KAFKA_BROKERS`, `CONNECT_URL`, `CONNECT_CONNECTOR_NAME`, `KAFKA_CONSUMER_GROUP`, `STREAMING_CS_PARTICOES`, `STREAMING_CS_CHECKPOINT_MAX_S` (só no código, padrão 600), `STREAMING_DEMO_TPS`, `STREAMING_DEMO_DURATION_S`, `STREAMING_TTL_SEGUNDOS`, `STREAMING_DROP_ACIMA_DE`, `STREAMING_CONCEPT_TPS`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`, `ASP_CONNECTION_NAME`, `ASP_PROCESSOR_NAME`, `ASP_GEO_PROCESSOR_NAME`, `ASP_TIER`, `STREAMING_CARTAO_PCT`, `STREAMING_SINAL_*`, `DEMO_ADMIN_TOKEN`, `ALLOWED_ORIGINS`, `MONGO_TIMEOUT_MS`, `MAX_REQUEST_BYTES`. No frontend: `VITE_DEMO_API_TOKEN` (`frontend/.env.example`) e `VITE_API_PROXY_TARGET` (`vite.config.js`).
- Porta: 8002 em `start.sh`/`bin/overview`; 5174 em `frontend/vite.config.js`.

**Bancos/coleções**
- Compartilhado com outras PoVs: o banco `POC`, onde `produtos` e `avaliacoes` podem pertencer também a outra PoV de busca (comentário em `seed_data.py:15`). Por isso o reset nunca derruba `produtos`/`avaliacoes`. No banco da demo, `POC.produtos` também tem o índice de busca `produtos_search`, da outra PoV; o módulo 08 usa e cria só o seu (`showcase_busca`).
- Exclusivo: `pix.*` (`transacoes`, `metricas_janela`, `dlq`, `dlq_audit`, `consumer_checkpoints`) e as coleções `*_demo` / `transacoes_cs_demo` / `BENCH_COL_*`.

**Chamadas externas** (todas sem LLM): Atlas Admin API (`hot_cold.py::_atlas_request`, `streaming.py::_atlas_get`/`_atlas_test_failover_sync`), Kafka Connect REST e broker Kafka (`streaming.py::_connector_*_sync`, `KafkaConsumer`), instância de Atlas Stream Processing (`streaming.py::_asp_command`). Não há Grove, Voyage nem Langfuse.

**Resto do módulo Geo:** o `docs/briefing/architecture.md` diz que não existe mais código de Geo (a tela de fato saiu), mas ainda há rastro no código: `GEO_DB_NOME`/`COL_SINAIS` (`streaming.py:213-214`), limpeza de `geo.sinais_ao_vivo` no `reset` (`streaming.py:2401`), `backend/data/municipios.json` lido pelo gerador (`streaming.py:220`), `scripts/setup-asp-geo.js` e `GEO_DB`/`ASP_GEO_PROCESSOR_NAME` no `.env.example`. O módulo de tela foi removido; o canal "cartão presencial" do gerador e o segundo processor ainda existem.

## 5. "Quero mexer em..." (atalhos)

- Texto/ordem das abas -> `frontend/src/App.jsx`, array `MODULES` (linha 14).
- Texto da tese e dos não-objetivos -> `frontend/src/pages/Tese.jsx`; o que é medido -> `backend/routers/tese.py::_executar` (`:165`).
- Mudar um pipeline de aggregation -> `backend/routers/aggregations.py` (a função da aba correspondente); nome da aba/rota -> `Aggregations.jsx:329`.
- Mudar os cenários de índice da reindexação -> `frontend/src/pages/Reindexacao.jsx` (constantes `SCENARIOS` linha 10 e `EXPLAIN_SCENARIOS` linha 32) e a lista de campos permitidos em `backend/routers/reindexacao.py` (`ALLOWED_INDEX_FIELDS`).
- Mudar o schema validado -> `backend/routers/schema_validation.py` (`step3_activate`, `:163`).
- Adicionar uma rota nova -> criar função no router do módulo; se for módulo novo, registrar em `backend/main.py:85-94` (`include_router`). Lembrar: SSE tem que ser `GET` (o `MutationGuardMiddleware` barra o resto).
- Mudar o que a transação faz -> `backend/routers/transactions.py::executar_transacao` (`:39`).
- Mudar taxa/duração do gerador de streaming -> `STREAMING_DEMO_TPS`/`STREAMING_DEMO_DURATION_S` no `.env`; presets na tela em `Streaming.jsx:664`; lógica em `streaming.py::Generator` (`:1012`).
- Mudar dados da demo (seed) -> `backend/seed_data.py`; reset -> `scripts/reset_demo.py`.
- Mudar a corrida gravada do modo replay -> regravar com `scripts/capture_replay.py`; leitura em `backend/routers/replay.py`.
- Mudar permissão de mutação/CORS -> `backend/security.py` e `ALLOWED_ORIGINS` / `DEMO_ADMIN_TOKEN` no `.env`.
- Mudar porta -> frontend `frontend/vite.config.js` (5174); backend em `start.sh` e `bin/overview` (8002); `ALLOWED_ORIGINS` default em `backend/settings.py`.
- Mudar visual -> `frontend/src/index.css` (tokens locais); `pov-signature.css` é cópia do portfólio (altere na origem em `_shared/ui`).

## 6. Testes

Comando (README): `pytest` na raiz (`pyproject.toml`: `testpaths = backend/tests`). Mongo é stubado; nenhum teste da CI precisa de cluster. A suíte live (`backend/tests/live/`, `LIVE_TESTS=1`, só bancos `showcase_*_test`) roda local; ver `docs/adr/0001-testes-live-opt-in.md`. Lint: `ruff check backend`. Frontend: `node --test tests/*.test.mjs` em `frontend/` (polling, `useApi`, formatadores de `src/lib/formato.js`) e `npm run build`, os dois na CI (`.github/workflows/ci.yml`).

| Arquivo | O que cobre |
|---|---|
| `backend/tests/test_endpoints_adversarial.py` | Entradas hostis (operadores Mongo em query/body, JSON malformado, corpo de 1 MB), 409 em concorrência, degradação sem Mongo |
| `backend/tests/test_features.py` | Comportamento dos módulos 01-06 |
| `backend/tests/test_streaming.py` | Módulo 07: gerador, contadores, digest, reconciliação, DLQ, falhas (o maior arquivo) |
| `backend/tests/test_busca.py` | Módulo 08: índice ausente/construindo/indisponível não executa `$search`; criação idempotente e só do índice próprio; pipeline devolvido = executado; recusa do servidor entre a checagem e o aggregate vira `executado: false`; corrida na criação devolve `criado: false`; preflight opcional |
| `backend/tests/test_replay.py` | Replay: contrato igual ao ao-vivo e que nunca toca o Mongo |
| `backend/tests/test_security.py` | `MutationGuardMiddleware` e `ApiHardeningMiddleware` |
| `backend/tests/test_change_streams.py` | Fila de eventos/geração do módulo 05 |
| `backend/tests/test_transactions_benchmark.py` | Benchmark de transação |
| `backend/tests/test_seed_data.py` | Seed idempotente e recusa de banco fora de `showcase_*_test` (ou do `backend/.env` com a flag) |
| `backend/tests/test_reset_demo.py` | `scripts/reset_demo.py` |
| `backend/tests/test_aggregation_evidence.py` | Evidência (pipeline devolvido) das aggregations |
| `frontend/tests/polling.test.mjs` | `usePolling`: nenhum timer com a aba escondida |
