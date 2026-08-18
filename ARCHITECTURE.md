# Arquitetura

```
React 18 + Vite (frontend/, :5174)
   │  fetch /api/*        (JSON)
   │  EventSource /api/streaming/*        (SSE — sessão ao vivo, modo principal)
   │  EventSource /api/replay/streaming/* (SSE — fallback gravado)
   ▼
FastAPI (backend/main.py, :8002)
   ├─ PyMongo ─────────────► MongoDB Atlas   (POC.*, pix.* e geo.*)
   ├─ requests ────────────► Atlas Admin API v2      (só Online Archive)
   ├─ requests ────────────► Kafka Connect REST      (:8083, modo ao vivo)
   ├─ aiokafka (opcional) ─► broker Kafka (:9092, KRaft via Homebrew, modo ao vivo)
   └─ arquivo ─────────────► backend/data/replay_streaming.json (playback do módulo 07)
```

O servidor de dev do Vite faz proxy de `/api` para `http://localhost:8002`, removendo o
prefixo. Toda chamada do navegador — SSE incluído — passa por esse proxy, então nenhum
host externo é contatado para renderizar uma página.

## Organização do backend

| Arquivo | Responsabilidade |
|---|---|
| `main.py` | App, CORS, middleware de request-id, handlers de exceção, ligação dos routers, `/`, `/health/live`, `/health/ready`, `/preflight`, `/stats` |
| `settings.py` | Dataclass congelada que lê as variáveis de ambiente uma vez; `settings.atlas_configured` habilita o Online Archive |
| `database.py` | Um único `MongoClient` (`connect=False`, timeouts explícitos) mais `readiness()` |
| `security.py` | `MutationGuardMiddleware` (loopback/token/Origin) e `ApiHardeningMiddleware` (teto de corpo + cabeçalhos) |
| `routers/*.py` | Um módulo por demo |

O `MutationGuardMiddleware` ignora métodos seguros, então os endpoints SSE (todos `GET`)
são alcançáveis pelo `EventSource`, que não consegue enviar o cabeçalho `X-Demo-Token`.

## Endpoints

### Operação

`GET /` · `GET /health/live` · `GET /health/ready` · `GET /preflight` · `GET /stats`

### Módulos

| Prefixo | Endpoints |
|---|---|
| `/reindexacao` | `GET /indexes`, `POST /create`, `GET /build-status`, `DELETE /drop/{index_name}`, `GET /read-probe`, `GET /explain`, `GET /demo-scenarios` |
| `/hot-cold` | `GET /distribution`, `GET /archive-simulation`, `GET /query-transparent`, `GET /online-archive/list`, `POST /online-archive/create`, `DELETE /online-archive/{archive_id}` |
| `/aggregations` | `GET /lookup`, `GET /facet`, `GET /union-with`, `GET /group-advanced`, `GET /window-functions`, `GET /bucket-auto` |
| `/schema` | `GET /status`, `POST /step1-create-collection`, `POST /step2-insert-without-schema`, `POST /step3-activate-schema`, `POST /step4-insert-invalid`, `POST /insert-valid`, `GET /documents`, `DELETE /reset` |
| `/change-streams` | `POST /start`, `POST /trigger`, `GET /feed` (SSE), `GET /events`, `GET /collection`, `POST /stop`, `DELETE /clear` |
| `/transactions` | `GET /status`, `POST /executar`, `POST /reset` |
| `/streaming` | veja abaixo |
| `/geo` | `GET /status`, `GET /municipios`, `GET /sinais-ao-vivo`, `POST /explain-compare`, `GET /impossible-travel`, `POST /search` |

### `/streaming` (módulo 07)

Um gerador de escrita alimenta quatro consumidores da mesma mudança. Os dados vivem em
`pix.transacoes`, `pix.metricas_janela`, `pix.dlq`, `pix.dlq_audit` e
`pix.consumer_checkpoints` (`STREAMING_DB` sobrescreve o nome do banco), mais
`geo.sinais_ao_vivo` para o quarto.

**Dois canais em um fluxo.** `canal: "PIX"` não carrega coordenada — uma transferência
PIX realmente não tem uma. `canal: "CARTAO_PRESENCIAL"`
(`STREAMING_CARTAO_PCT`, 18% por padrão) carrega `local` como o ponto cadastrado do
terminal do adquirente, a mesma modelagem do dataset do módulo 08, vinda do
mesmo `backend/data/municipios.json`. É isso que permite ao `geoSinais30s`
calcular risco geográfico em tempo de evento, em vez de o módulo 08 varrer o histórico
sob demanda.

O canal de cartão tem dois instantes distintos, e confundi-los é um bug real:
`ts` é a chegada ao fluxo e o campo do TTL; `compradaEm` é a compra
no terminal, que pode ser minutos antes, porque a captura do adquirente atrasa.
A velocidade é calculada a partir de `compradaEm`. Retrodatar o `ts` fazia o TTL apagar a
metade mais antiga de um par antes de a reconciliação rodar, então a origem contava menos que os
consumidores — expiração indistinguível de perda.

**Negócio e operação**

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/streaming/oplog` | Janela de retenção do oplog em minutos, lida de `local.oplog.rs`, mais a retenção mínima configurada. Este é o **limite operacional da garantia de resume token** — a recuperação só funciona enquanto o ponto de retomada ainda está no oplog. |
| `GET` | `/streaming/leitura` | Latência de uma busca pontual por `endToEndId`, amostrada a cada 250 ms **enquanto o gerador escreve**, com p50/p95/p99. Responde à pergunta operacional do dia a dia que os números de vazão não respondem. |
| `GET` | `/streaming/asp/dlq/resumo` | DLQ agrupada por motivo de rejeição, com primeira/última ocorrência. |
| `POST` | `/streaming/asp/dlq/reprocessar` | Corrige o defeito conhecido e reinsere, preservando o `endToEndId` original — rodar duas vezes não duplica, o índice único bloqueia. Idempotência por chave de negócio. |
| `GET` | `/streaming/reconciliacao` | Reconcilia um `run_id` finito em três níveis: **contagem** (documentos de origem, eventos únicos do Change Stream, mensagens únicas do Kafka, agregados do ASP e DLQ/auditoria), **valor** somado em centavos inteiros — nunca em ponto flutuante, porque três somas independentes de doubles deixam um resíduo indistinguível de divergência real — e um **digest** XOR do conjunto de `endToEndId`, que independe de ordem e só bate quando os caminhos viram o mesmo *conjunto*, não apenas a mesma quantidade. O caminho do ASP é só agregado: ele reconcilia por valor dentro de uma tolerância declarada de R$0,01 por janela fechada e não tem conjunto de ids para digerir. O digest é pulado acima de 200.000 documentos em uma execução, e a página diz isso. Ele só reporta `reconciliado` depois que a entrada para e todos os caminhos prestam contas da mesma execução. A contagem da origem depende do índice de `run_id` criado por `_ensure_indexes()`; a UI consulta isso a cada 5 s e para quando o resultado é final. |

**Cenário e rede**

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/streaming/cenario` | Retorna presets para o modo de escrita ativo. O caminho individual padrão expõe 1.000 TPS como referência do cliente, 2.000 TPS como alvo sustentado de palco e 12.000 TPS em modo lote como folga do tier. O endpoint também retorna `modo_escrita`, `default_tps` e o teto do modo individual. Isso é evidência de comparação e de palco, não sizing nem capacidade certificada. |
| `GET` | `/streaming/rede` | RTT mediano app ↔ cluster, medido com `ping`. Sem ele a latência das colunas é lida como custo de change stream, quando boa parte é distância. |
| `GET` | `/streaming/folga` | Quanto a execução custou ao cluster: CPU do primário lida pela Atlas Admin API, recortada à janela da própria execução, ao lado do TPS de pico que a produziu. Responde de quem foi o teto atingido — o gerador, a rede do apresentador ou o cluster. Vereditos: `sem_execucao`, `metricas_pendentes`, `cluster_com_folga`, `cluster_participando`, `cluster_no_limite`. Medido, não sizing. |

**Gerador**

| Método | Caminho | Descrição |
|---|---|---|
| `POST` | `/streaming/generator/start` | Corpo `{"tps": 1..TPS_MAX, "duration_s": 10..120, "modo": "individual|lote"}` (`TPS_MAX` = 15.000; padrão é modo individual a 2.000 TPS/30 s). O modo individual usa o driver assíncrono e um `insert_one` confirmado por PIX; o modo lote usa micro-lotes de `insert_many` para a história de volume maior. Cria um `run_id` e uma sequência, e para sozinho. O teto é uma proteção, não uma garantia do M20 nem um limite de produto. |
| `POST` | `/streaming/generator/stop` | Cancela a tarefa, espera 7,2 s (janela de 5 s + 2 s de atraso) e escreve um marcador técnico sob um `run_id` reservado para avançar a marca d'água do tempo de evento. O marcador fica fora da reconciliação da execução demonstrada e permite que a janela final dela feche. |
| `GET` | `/streaming/generator/status` | `run_id`, `running`, `stopping`, `duration_s`, `ends_at`, `tps_alvo`, **`tps_medido`**, `inseridos`, modo de escrita, `write_ack` p50/p95/p99 e estado da coleção. No modo individual o `write_ack` é o ACK ponta a ponta de um PIX; no modo lote ele descreve um micro-lote confirmado. As três colunas de consumidores medem propagação pós-commit. |
| `POST` | `/streaming/reset` | Para o gerador, garante os índices de chave de negócio única, TTL e `run_id_reconciliacao`, e então limpa origem, janelas, DLQ e auditoria em paralelo usando a topologia MongoDB já conectada da aplicação. Acima de `STREAMING_DROP_ACIMA_DE` (25 mil por padrão), ele para o ASP, dropa/recria a origem dedicada e os índices, e então recupera ASP e Kafka; uma exclusão de rotina não reinicia o Kafka. Dado residual **na coleção de origem** retorna 503 em vez de iniciar uma execução misturada; uma janela atrasada ou um documento na DLQ é o processor terminando a rodada anterior e não bloqueia, já que tudo a jusante é filtrado por `run_id`. O vazio é confirmado com uma contagem exata limitada, nunca com `estimated_document_count()`, cujos metadados ainda reportam o total anterior à exclusão. Com `?finalizar=true`, ele também remove os checkpoints da aplicação e deixa ASP/Kafka parados. |

**Coluna 1 — Change Streams**

A PoV consegue abrir vários cursores `watch()` com filtros disjuntos. Isto é uma
técnica de demonstração, não particionamento nativo nem recomendação de sizing.
Cada cursor persiste seu resume token em `pix.consumer_checkpoints`. Erros
transitórios preservam o checkpoint; só uma condição confirmada de
`ChangeStreamHistoryLost` o descarta.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/streaming/changestream` | **SSE.** Um cursor `collection.watch()` por partição de demonstração (`[{$match: {operationType: "insert"}}]`) transmite para todos os assinantes. Como o pipeline só aceita inserts, ele lê `fullDocument` do evento sem `updateLookup`. Tipos de evento: `hello`, `aberto`, `evento`, `derrubado`, `erro`, `reset`. Cada `evento` carrega `latency_ms` ponta a ponta, o resume token truncado e a flag `recuperado`. |
| `POST` | `/streaming/changestream/drop-resume` | Fecha o cursor, espera 3 s enquanto o gerador segue escrevendo, e reabre com `resume_after(<resume token persistido>)`. Eventos cujo `ts` precede a reabertura são marcados como `recuperado`; o endpoint de reconciliação então verifica a contabilidade, em vez de inferir "zero perda" só pela animação. |
| `GET` | `/streaming/changestream/status` | `aberto`, `eventos`, `recuperados`, **`duplicados`**, `token`, mais `eventos_s` e os percentis de latência p50/p95/p99. A entrega é at-least-once, então duplicatas são *medidas* sobre uma janela limitada de `endToEndId` recentes, e não descartadas por afirmação. |

**Coluna 2 — MongoDB Kafka Connector**

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/streaming/kafka` | **SSE** das mensagens consumidas de `atlas.pix.transacoes`, com partição, offset e a `latency_ms` de insert no Atlas → chegada ao tópico. O `aiokafka` é importado de forma preguiçosa: sem a dependência ou sem o broker, o fluxo emite `{"type": "status", "estado": "nao_configurado"}` e a UI mostra as instruções de setup. |
| `GET` | `/streaming/kafka/status` | Estado agregado de todo connector `atlas-pix-source*`, **rebaixado pela saúde das tasks**: um connector reportando `RUNNING` com todas as tasks `FAILED` é reportado como `FAILED` (`DEGRADADO` quando só algumas falharam), porque quem move dado é a task. Mais contagem de mensagens e offset atual. |
| `POST` | `/streaming/kafka/restart` | Reinicia connector e tasks. Uma task morta por uma oscilação de rede ou por um restart de cluster nunca se recupera sozinha enquanto o connector segue afirmando `RUNNING`. |
| `POST` | `/streaming/kafka/consumer/restart` | Reinicia o observador da UI com o mesmo `group.id`, demonstrando recuperação a partir dos offsets commitados e expondo reentregas à reconciliação. |

**Falha injetada**

| Método | Caminho | Descrição |
|---|---|---|
| `POST` | `/streaming/falha/connector` | Para todos os connectors do showcase, espera `segundos` (1–30, padrão 8) e os retoma. Parar não descarta o offset: o resume token fica em `connect-offsets`, então tudo o que foi escrito durante a queda é entregue depois, e a reconciliação tem que fechar mesmo assim. |
| `POST` | `/streaming/falha/evento-invalido` | Escreve uma transação cujo `valor` é uma string. É um documento válido para a coleção — passa pelo índice único e conta na origem — mas o `$validate` do processor o desvia para a DLQ enquanto o pipeline continua rodando. |
| `POST` | `/streaming/falha/schema-incompativel` | Publica uma "nova versão" do evento com o campo obrigatório `valor` renomeado para `amount` — a mudança incompatível que um Schema Registry recusaria no registro. Mesmo desfecho, causa diferente: DLQ com o motivo, pipeline ainda rodando, reconciliação ainda fechando. |
| `POST` | `/streaming/falha/failover` | **Test failover** do Atlas: uma eleição real de primário no cluster de demo, sob carga. A única falha injetada que atinge o MongoDB, e não um terceiro. Ela estende a execução em 150 s, porque uma eleição dura mais que a janela de 30 s e o auto-stop fecharia a execução no meio do evento. A evidência é `escritas_rejeitadas` ao lado de `escritas_confirmadas` — o `retryWrites` absorve o step-down, então o número honesto é zero. O recurso da Admin API mudou de forma entre versões, então a chamada tenta as formas conhecidas em ordem e só 404/405 avança; um erro de credencial ou de access list aparece sem máscara. |
| `GET` | `/streaming/contrato` | O contrato que o processor aplica, espelhando o `$validate` de `scripts/setup-asp.js`, mais a política em caso de violação. Mostrado na tela para que "foi para a DLQ" vire "foi para a DLQ porque violou *isto*, que você acabou de ler". |

Os dois existem porque uma execução em que nada falha só prova que nada falhou. Eles são a
contrapartida da reconciliação: o número só significa algo depois que o caminho
que o produziu foi quebrado e recuperado no palco.

**Coluna 3 — Atlas Stream Processing**

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/streaming/asp` | **SSE** de janelas fechadas e documentos da DLQ. O backend não consulta o SPI: ele observa `pix.metricas_janela` e `pix.dlq` com change streams, então o resultado do ASP chega à tela pela mecânica da coluna 1. |
| `GET` | `/streaming/asp/status` | Estado real mais `getStreamProcessorStats`: entrada/saída/DLQ, atraso do oplog, marca d'água, tamanho do estado, latência e memória máxima de operador quando disponível. |
| `POST` | `/streaming/asp/restart-checkpoint` | Para o processor nomeado, espera o `STOPPED` e então o inicia normalmente, para que o ASP retome do checkpoint gerenciado. Ele nunca dropa/recria o processor. |
| `POST` | `/streaming/asp/inject-invalid` | `?quantidade=N` (até 5.000) insere documentos que violam o schema esperado de quatro formas diferentes, para que a DLQ mostre motivos distintos; o `$validate` os roteia para a DLQ em vez de derrubar o processor. Falhas parciais são toleradas e reportadas. Retorna 409 quando o ASP não está configurado. |
| `GET` | `/streaming/asp/dlq` | Últimos documentos da DLQ. |
| `GET` | `/streaming/asp/janelas` | Últimas janelas fechadas, direto da coleção em que o processor escreve. |

**Amostragem.** Um navegador não deve renderizar todo evento de streaming. O feed SSE é, portanto, uma
*amostra* (um quadro a cada 120 ms), rotulada como tal na UI, enquanto contadores
e percentis são calculados no worker sobre **100% dos eventos**. Um
evento recuperado nunca é descartado pela amostragem: ele é a prova de que o drop/resume funciona.

### `/replay` (playback do módulo 07)

O replay é a contingência sem escrita, selecionada explicitamente na página ou com
`bin/overview --replay`. Ele não provisiona ASP nem Kafka e fala com
`/replay/*`, que espelha os caminhos de `/streaming/*`. Tudo é servido
a partir de `backend/data/replay_streaming.json`, gravado por
`scripts/capture_replay.py` contra o cluster real.

Este router nunca toca o MongoDB — um teste garante isso — então a página funciona com
o cluster pausado. A razão de existir é custo: M20/M30 são burstable, e o
auto-scaling de compute do Atlas dispara por CPU **relativa**
(`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`). Medido aqui, 17,6% absoluto foi lido
como 88% relativo e escalou o cluster com o gerador já parado, só pelo polling do dashboard.

Todo payload carrega `replay: true`, e a página mostra um selo permanente de uma linha
nomeando o `run_id` gravado e sua data. Os números são medições reais
daquela execução — apresentá-los como ao vivo seria a única coisa que
este modo não pode fazer.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/replay/manifest` | `run_id`, `gravado_em`, duração, contagem de eventos e o estado do relógio. Responde **200 com `disponivel: false`** quando não há gravação — a página sonda isso no carregamento, e um 5xx levantaria um toast global de erro em toda instalação que nunca gravou. |
| `POST` | `/replay/play` | Inicia o relógio de playback do zero (`retomar=true` retoma da posição pausada). É o que o único botão **▶ Play** chama. |
| `POST` | `/replay/pause` / `/replay/stop` | Congela na posição atual / rebobina e para. A posição é derivada de um relógio monotônico na leitura, então um replay parado não custa nada. |
| `GET` | `/replay/estado` | `rodando`, `posicao_s`, `duracao_s`, `repetir`. |
| `GET` | `/replay/streaming/{cenario,rede,cluster}` | Contexto estático capturado junto com a execução: descreve o ambiente em que a execução foi *medida*, não o atual. |
| `GET` | `/replay/streaming/{generator/status,kafka/status,asp/status,oplog,leitura,asp/dlq/resumo,reconciliacao}` | O snapshot gravado cujo timestamp é o último igual ou anterior à posição atual de playback. |
| `GET` | `/replay/streaming/{changestream,kafka,asp}` | **SSE.** Reemite os eventos gravados conforme o relógio avança, mais um `reset` quando a gravação dá a volta. Envia `: keepalive` a cada 10 s — sem isso um fluxo ocioso é derrubado pelo navegador e pelo proxy do Vite, o cliente reconecta, e o gerador do lado do servidor nunca descobre que o cliente sumiu (um gerador que nunca escreve nunca vê a desconexão). Esses fluxos vazados esgotam o orçamento de ~6 conexões por host do navegador, e fetches comuns começam a estourar 30 s de timeout enquanto o backend responde em milissegundos. |

### `/geo` (módulo 08)

Tem banco próprio (`geo`, sobrescreva com `GEO_DB`). Duas coleções:
`geo.transacoes`, o dataset versionado semeado por `scripts/seed_geo.py`, e
`geo.sinais_ao_vivo`, que é dado de execução escrito pelo processor de ASP e limpo
por `/streaming/reset` e `cleanup-streaming-data.py`. A coleção de dataset
nunca é tocada por nenhum dos dois caminhos de limpeza.

| Método | Caminho | Descrição |
|---|---|---|
| `GET` | `/geo/sinais-ao-vivo` | Lê `geo.sinais_ao_vivo`, materializada pelo stream processor `geoSinais30s` enquanto o módulo 07 roda. Nada é calculado aqui — a janela já fez isso. Retorna os pares recentes mais contagens separadas de `plantados` e `emergentes`, porque juntá-las transformaria o sinal garantido da demo em evidência. |
| `GET` | `/geo/status` | Contagem de documentos, a lista de índices lida da coleção e se o índice do Atlas Search existe. Nada é fixado no código da UI. |
| `GET` | `/geo/municipios` | Municípios presentes no dataset com um ponto representativo, para que a UI possa centralizar uma consulta sem enviar uma tabela de coordenadas ao navegador. Cacheado em memória; a lista só muda quando o seed roda de novo. |
| `POST` | `/geo/explain-compare` | A mesma consulta `$geoWithin` (`$centerSphere`) explicada duas vezes: com hint em `cliente_status_local_idx` (campos de igualdade primeiro, geo por último) e em `local_2dsphere_idx`. Retorna estágio vencedor, índice usado, `totalKeysExamined`, `totalDocsExamined`, `nReturned` e `executionTimeMillis` para cada um. Se a medição contradisser a nota didática, é a medição que aparece na tela. |
| `GET` | `/geo/impossible-travel` | Sinal de risco retrospectivo: `$setWindowFields` particionado por `clienteId`, ordenado por `ts`, `$shift` puxando o timestamp anterior, coordenadas, dispositivo e procedência da localização, e então haversine em MQL puro. Ele retorna explicitamente `decisao_fraude: false`; nenhum documento sai do cluster para o cálculo. Um `$facet` também conta os pares avaliados **antes** do corte geométrico, então a resposta carrega seletividade (taxa, alertas por dia) ao lado dos casos, cada um rotulado `plantado`/`emergente` a partir de `fraud_seeds.json`. Aceita `clienteId` para estreitar a varredura — o recorte, não o hardware, é o que mantém isso viável sobre histórico real. |
| `POST` | `/geo/search` | A vizinhança de uma **compra contestada**: passe `endToEndId` e o centro vira a coordenada cadastrada daquele terminal, com a âncora retornada junto dos resultados. Um `$search` com `geoWithin`, filtro opcional de categoria e facetas de `$searchMeta`; o `termo` é opcional e adiciona casamento fuzzy de nome para o caso de estabelecimento clonado. Sem termo, a cláusula de scoring é `exists` (um compound só de `filter` retorna tudo com score zero) e os resultados são ordenados por distância, com o desempate aplicado antes da deduplicação por terminal. Sem o índice, o endpoint retorna `estado: "nao_configurado"` em vez de resultados vazios. |

As checagens de geo entram no `/preflight`, mas nunca o reprovam: o módulo é opcional,
do mesmo jeito que Kafka e ASP.

O mapa é renderizado como SVG inline, com uma projeção linear escrita à mão sobre o
bounding box brasileiro — sem Leaflet, sem Mapbox, sem tiles, sem nova dependência
de frontend. Com a rede externa bloqueada o módulo ainda renderiza e todo
número ainda vem do cluster; a única requisição externa da aplicação é o
link do Google Fonts em `frontend/index.html`, que vale para o app inteiro e é pré-existente,
e a tipografia cai para fontes do sistema quando ele falha.

### Convenções de SSE

Todos os endpoints de streaming compartilham o `_sse_stream`: uma `asyncio.Queue` por assinante
alimentada por um `Hub` de broadcast, um quadro `hello` na conexão, um comentário `: keepalive`
a cada 15 s e detecção de desconexão via `request.is_disconnected()`. Produtores
rodando em threads (cursores do PyMongo) publicam por
`loop.call_soon_threadsafe`. Um assinante lento tem seu quadro mais antigo descartado,
em vez de bloquear o produtor.

## Frontend

- `src/App.jsx` — casca, sidebar, roteamento por hash (`/#agg`, `/#streams`, `/#tx`, `/#streaming`).
- `src/pages/` — um componente por módulo; `src/components/` — `DemoFlow`, `QueryBlock`.
- `src/hooks/useApi.js` — wrapper de fetch que adiciona `X-Demo-Token`. Aborts esperados
  causados pelo unmount de um módulo são silenciosos; timeouts e falhas reais ainda disparam
  um erro global. O `App.jsx` deduplica toasts de erro idênticos por oito
  segundos. O SSE usa `EventSource` diretamente (`useSse` em
  `pages/Streaming.jsx`).
- O estado vive apenas no estado do React — sem `localStorage`/`sessionStorage`.

A UI segue uma hierarquia de prova primeiro, documentada em
`docs/SESSION_HANDOFF.md`: o Streaming expõe os três caminhos no primeiro
viewport de notebook, o de agregações usa `Origem → Pipeline → Resultado`, e definições
grandes de código são reveladas progressivamente.

## Infraestrutura externa

O `scripts/kafka-local.sh` roda o broker Kafka (Homebrew, KRaft, `:9092`) e o
Kafka Connect (`:8083`) com o plugin `mongodb-kafka-connect` cacheado localmente
depois do primeiro download. As instruções passo a passo estão em
`docs/setup-streaming.md`. Nada aqui precisa de Docker — o caminho por container foi
removido de propósito, porque uma segunda forma de subir a mesma dependência só adicionava
superfície de setup, e um dos containers publicava a `9093` no host, que é
a porta em que o próprio controller KRaft do Kafka escuta.

Dois jobs de Atlas Stream Processing, não um, porque um pipeline implantado tem um
único sink terminal: `pixJanelas5s` (`scripts/setup-asp.js`) faz merge de janelas de
5 segundos em `pix.metricas_janela`, e `geoSinais30s`
(`scripts/setup-asp-geo.js`) faz merge dos sinais de risco geográfico em
`geo.sinais_ao_vivo`. São consumidores independentes do mesmo change stream.
O `scripts/ambiente.sh` provisiona e para os dois.

O `scripts/lib/expand_srv.py` reescreve a URI `mongodb+srv://` na forma padrão de
três hosts antes de o connector ser registrado. O source connector
reinterpreta o `connection.uri` a cada start de task, então uma URI SRV transforma cada restart
em uma consulta DNS SRV+TXT; um resolver instável deixava o connector em `RUNNING` com
sua única task em `FAILED`.
