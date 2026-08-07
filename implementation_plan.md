# Atlas Feature Showcase — prompt de construção

> Este é o prompt que uso para subir a PoV inteira numa única execução. Não é documentação do que existe: é o briefing que entrego antes de existir qualquer linha de código.

> **Nota para retomada:** este arquivo é histórico e preserva o briefing
> original. Para o estado implementado, comece por `docs/SESSION_HANDOFF.md` e
> `docs/SESSION_HANDOFF.md`; eles registram a migração para `sa-east-1`, o modo
> individual de PIX, o módulo de Risco geográfico e as correções operacionais
> posteriores.

---

Quero um showcase interativo que exercite **oito capacidades centrais do Atlas contra um cluster de verdade**. Não é simulação, não é slide animado: cada módulo dispara uma operação real e mostra o resultado medido.

**Sem LLM nenhum aqui.** Essa é a PoV que responde "o Atlas aguenta?" e "o Atlas faz?" com número, não com narrativa. É deliberado que não tenha IA — em metade das conversas o cliente já está cansado de ouvir sobre IA e quer ver o banco.

Os oito módulos:

| # | Módulo | O que ele tem que provar |
|---|---|---|
| 01 | Reindexação Online | índice sendo construído com o cluster servindo tráfego |
| 02 | Hot/Cold Tiering | Online Archive movendo dado frio pra storage barato, com a query continuando unificada |
| 03 | Aggregation Pipeline | transformação e análise no servidor, sem trazer dado pra aplicação |
| 04 | Schema Validation | `$jsonSchema` + `collMod` — o banco recusando documento fora do contrato |
| 05 | Change Streams | reação a mudança sem polling |
| 06 | Transações ACID | multi-documento, com commit e rollback visíveis |
| 07 | Streaming | três abordagens de captura lado a lado |
| 08 | Geo | `2dsphere` composto, viagem impossível, geo + Atlas Search |

## Arquitetura

```
React 18 + Vite (:5174) --fetch + EventSource--> FastAPI (:8002) --> PyMongo --> Atlas (M20)
                                                       |-> Atlas Admin API v2 (só Online Archive)
                                                       |-> Kafka Connect REST :8083
                                                       \-> aiokafka -> Redpanda/Kafka :19092
```

O dev server do Vite proxia `/api` pro backend **removendo o prefixo**, e toda chamada do browser passa por ali — SSE incluído.

Um router por demo, em `backend/routers/`. Não junte dois módulos no mesmo arquivo, mesmo que pareçam parecidos: eu preciso poder mexer no Streaming sem risco de quebrar o Geo cinco minutos antes de uma reunião.

## Segurança do backend

Dois middlewares, e os dois têm motivo prático:

- **`MutationGuardMiddleware`** — bloqueia mutação vinda de fora do loopback quando não há `DEMO_ADMIN_TOKEN` válido. Compare com `hmac.compare_digest`, não com `==`. Valide também o header `Origin` contra `ALLOWED_ORIGINS`.
- **`ApiHardeningMiddleware`** — limite de tamanho de corpo + headers `nosniff`, `DENY`, `no-referrer`, `no-store`.

O guard **ignora métodos seguros**, e é por isso que **todos os endpoints SSE têm que ser `GET`**: o `EventSource` do browser não consegue enviar o header `X-Demo-Token`. Se você fizer um SSE em POST, ele simplesmente não conecta e o erro não vai ser óbvio.

Vários endpoints aqui são **destrutivos por natureza** — derrubam índice, fazem `collMod` de schema, criam e apagam Online Archive. Deixe isso escrito em letras grandes no README: **nunca aponte esta PoV pra nada que não seja um cluster de demo descartável.**

## Cluster e conexão

`database.py` com **um único `MongoClient`**, `connect=False`, `appname` setado e timeouts explícitos. Mais uma função `readiness()`.

Faça o import cair pra uma URI de localhost quando `MONGO_URI` estiver ausente. Motivo: eu quero que a falta da variável apareça em `/health/ready`, como problema de configuração, e não como erro de import derrubando o processo inteiro na subida.

`settings.py` é uma dataclass congelada que lê todas as env vars uma vez. Um flag `settings.atlas_configured` libera o módulo de Online Archive — sem credencial da Admin API, o módulo aparece como não configurado em vez de estourar.

Database default `POC`, streaming em `pix`, geo em `geo`. **O módulo Geo nunca toca `POC` nem `pix`.**

## Módulo 07 — Streaming

É o maior do projeto e o que mais rende conversa. Três colunas lado a lado, comparando abordagens de captura sobre a mesma coleção:

1. **Change Streams** — `ChangeStreamWorker`/`ChangeStreamCluster`, com N workers particionados.
2. **MongoDB Kafka Connector** — via Kafka Connect REST, connector `atlas-pix-source`, com um `KafkaConsumer` observando o tópico.
3. **Atlas Stream Processing** — um processor de janela, `pixJanelas5s`.

Precisa de um gerador de transações PIX sintéticas com TPS configurável, um `Hub`/`Meter` de SSE alimentando a UI, e os endpoints de sonda de leitura, janela de oplog, cluster/rede/custo e preflight.

As colunas 2 e 3 **degradam pra um painel "não configurado"** quando as variáveis de ambiente não estão lá. A página nunca pode quebrar por falta de Kafka local — em cliente eu frequentemente rodo só a coluna 1.

### O modo replay

Faça também um `routers/replay.py` que **reproduz uma corrida real gravada**, sob `/replay/*`, espelhando os caminhos de `/streaming/*` pro frontend só precisar trocar o prefixo. Um botão ▶ Play move o relógio de reprodução.

O motivo é operacional e vale citar em cliente: M20 e M30 são *burstable*, e o auto-scaling do Atlas dispara em CPU **relativa** (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`). Já medi 17,6% de CPU absoluta sendo lida como 88% relativa, e o cluster escalou **com o gerador já parado**, só pelo polling do dashboard.

Regras do replay, todas obrigatórias:

- Ele **nunca toca o MongoDB**. Escreva um teste que garante isso.
- Todo payload carrega `replay: true`.
- `/replay/manifest` responde **200** com `disponivel: false` quando não há gravação — não 404.
- Os streams mandam `: keepalive` a cada 10s. Sem isso, stream vazado esgota o orçamento de ~6 conexões por host do browser e os fetches comuns começam a estourar em 30s.
- Botões que agem no ambiente ficam desabilitados, e a página exibe um badge permanente de origem.

A página **abre em modo ao vivo por padrão**; replay é contingência explícita, não o caminho principal. E o mais importante: **nunca apresente um replay como corrida ao vivo.** Os números são medição real gravada, mas a honestidade sobre isso é o que sustenta tudo o mais que eu digo na reunião.

Gravação nova sai de `scripts/capture_replay.py` → `backend/data/replay_streaming.json`.

## Módulo 08 — Geo

Database próprio (`geo`), três endpoints:

- **`explain-compare`** — dois `hint` diferentes no mesmo `$geoWithin`, comparando planos de execução lado a lado.
- **`impossible-travel`** — `$setWindowFields` + `$shift` + haversine **em MQL puro**. Nada de pós-processamento em Python. O ponto é justamente que isso roda no banco.
- **`search`** — um único `$search` combinando texto + `geoWithin` + facetas. Degrada pra `nao_configurado` sem o índice.

O seed (`scripts/seed_geo.py`) gera 150k transações georreferenciadas, em clusters gaussianos em torno de 40 municípios reais. **Idempotente** por seed fixa de RNG + índice único em `endToEndId` — rodar duas vezes não duplica a coleção. Grave em `data/fraud_seeds.json` os IDs de cliente cujos pares de viagem impossível foram plantados, pra eu ter resultado garantido em cena e não depender de sorte.

Trate localização como **telemetria sintética de app**, com dispositivo, fonte, acurácia e horário de captura. Viagem impossível é **sinal de risco, nunca decisão de fraude** — isso precisa estar escrito na própria página.

### O que o módulo Geo NÃO faz — e isso vai dito em voz alta na tela

Só **predicados** geoespaciais. Sem álgebra de geometria: nada de buffer, união, interseção, área. Só WGS84. Sem raster, sem topologia. `$geoNear` tem que ser o primeiro estágio do pipeline, e o `filter` do `$vectorSearch` rejeita operadores geoespaciais.

**Não amplie essa alegação em silêncio.** Prefiro perder um ponto na demo a ser corrigido por um especialista de GIS na frente do cliente.

O mapa é **SVG inline com projeção escrita à mão**. Sem Leaflet, sem Mapbox, sem tiles, sem dependência nova no front. Quero o módulo renderizando e funcionando com a rede externa bloqueada — já apresentei em cliente com egress fechado.

## Frontend

Aqui o produto **é** a tela. Oito capacidades, cada uma tendo que se provar sozinha na frente de alguém cético. Duas regras que valem pra todos os módulos:

1. **Nada de dado inventado.** Todo número na tela veio de uma chamada real ao cluster. Se o Atlas não responder, a tela diz isso — não preenche com placeholder.
2. **A query fica visível.** Todo módulo mostra o pipeline que rodou, num `QueryBlock`. Quem está assistindo tem que conseguir copiar e rodar no Compass.

Stack mínima de propósito: React 18 + Vite, JSX puro, CSS escrito à mão. A única dependência de UI é `react-syntax-highlighter`, pra realçar os pipelines. Sem router, sem biblioteca de estado, sem UI kit.

Navegação por **hash** (`/#agg`, `/#streams`, `/#tx`), lida no boot e reagindo a `hashchange`. É o suficiente pra deep-link funcionar — eu preciso conseguir abrir um módulo específico direto na URL durante a apresentação.

`src/index.css` com os tokens dark do MongoDB: `--bg-primary #001E2B`, `--accent #00ED64`, Outfit + JetBrains Mono. Mesma paleta das outras PoVs do meu portfólio.

Um `DemoFlow` dando o passo a passo de cada módulo na própria tela — é o roteiro, pra eu não depender de decorar oito sequências diferentes.

### `useApi` — o hook que segura a demo

Todo fetch passa por `src/hooks/useApi.js`, e ele carrega mais decisão do que o tamanho sugere:

- **Timeout de 30s por requisição**, configurável até 300s com `AbortController` — criar índice ou Online Archive demora.
- **Erro traduzido pra linguagem de operador.** `Failed to fetch` vira "API indisponível — verifique se o backend está rodando na porta 8002". Numa demo, "Failed to fetch" não ajuda ninguém, inclusive a mim.
- **Abort de navegação não vira erro.** Trocar de módulo cancela as requisições da tela anterior; isso é esperado e não pode pintar um toast vermelho falso.
- **Contador de pendentes** em vez de booleano de loading, senão duas chamadas concorrentes fazem o spinner piscar cedo demais.
- **`X-Demo-Token`** injetado de `VITE_DEMO_API_TOKEN`, casando com o hardening do backend.
- **Erro global via `CustomEvent('api-error')`** — o shell mostra o toast sem eu precisar passar callback por toda a árvore.

### Disciplina de polling

Oito módulos com relógio próprio viram uma rajada de requisições sem ninguém perceber. **Todo intervalo do app passa por `usePolling`:**

- `useVisivel()` — aba escondida, nada roda.
- `useIntervaloVisivel(fn, ms, ativo)` — o timer só existe enquanto a aba está visível **e** `ativo` é verdadeiro. Dispara uma vez ao reativar, pra tela não ficar com dado velho quando eu volto. **Guarde a função numa `ref`** — deixar a identidade dela nas dependências recria o timer a cada render, e é o erro clássico que transforma um poll de 5s em rajada.

Nada faz polling de dado que não pode mudar: o snapshot gravado só avança enquanto o relógio de reprodução está rodando.

O `useSse` fecha o `EventSource` quando a aba esconde, e **reconecta manualmente**. O `EventSource` só refaz a conexão sozinho em alguns casos, e cada conexão presa ocupa uma das ~6 por host do browser — as vazadas faziam fetch comum estourar em 30 segundos.

Quero isso medido e registrado no README. Minha baseline: **48 requisições/20s → 1 quando parado, 0 com a aba escondida.**

## Ferramental de operação

Um entrypoint `bin/overview` que faz preflight read-only rápido e sobe backend e frontend:

```bash
./bin/overview           # up
./bin/overview down      # para app, ASP e Kafka, limpa PIX
./bin/overview status
./bin/overview logs
./bin/overview --replay  # sem provisionar ASP nem Kafka
```

**`overview` nunca altera estado do cluster.** Não pausa, não retoma, não redimensiona, não mexe em auto-scaling. Ciclo de vida do cluster é decisão do operador, manual. Já me queimei com script que "ajudava" mexendo em tier.

Delegue a parte de ASP e Kafka pra `scripts/ambiente.sh {up,down,status}`.

Um `scripts/prepare-demo.sh` pra rodar **antes** da apresentação, materializando o dataset Geo versionado e deixando o índice do Atlas Search em READY. Índice de Search não fica pronto instantaneamente, e descobrir isso no meio da demo é constrangedor.

**Sempre `overview down` depois de demo ao vivo** — processor ASP rodando cobra por segundo, mesmo ocioso.

E um `live_monitor.py`: monitor de latência de leitura/escrita no terminal. É o que eu deixo rodando ao lado enquanto o índice constrói, pra provar que o cluster continuou respondendo.

## Testes

Todos unitários, com Mongo stubado ou monkeypatched. **Nenhum teste pode exigir cluster ao vivo** — CI não tem credencial e eu não quero suíte que só passa na minha máquina.

Cubra: comportamento dos guards, o router de Streaming (é a maior suíte), clamping de env em settings, o seed, o pipeline e o determinismo do Geo, e o replay — relógio de reprodução, snapshot no tempo, rotulagem de origem, e a asserção de que o replay **nunca fala com o Mongo**.

```bash
pytest                                  # testpaths = backend/tests
ruff check backend                      # py311, line-length 120
pip-audit -r backend/requirements.txt
```

CI rodando exatamente isso a cada push, mais `npm ci && npm run build && npm audit --audit-level=high` no front.

E um `/preflight` no backend, pra eu rodar antes de qualquer demo: verifica `MONGO_URI`, alcance do cluster, as coleções esperadas, as credenciais da Admin API e o modo do mutation guard.

## Duas regras do Atlas que é fácil errar

Ambas retornam HTTP 400 e a mensagem não é lá muito clara:

- `minInstanceSize` tem que ser **estritamente** menor que `maxInstanceSize`. Ou seja, "fixar em M20 com auto-scaling ligado" é inexprimível na API.
- Cada tier tem disco máximo. M10 vai até 128 GB, então um cluster de 150 GB não pode ter piso M10.

Deixe as duas documentadas no README. Já perdi tempo com as duas.

## Armadilhas que quero registradas

- Mudar a porta do frontend faz mutação começar a retornar 403, porque o guard valida `Origin` contra `ALLOWED_ORIGINS`.
- `scripts/ambiente.sh` parseia o `.env` com `grep`/`cut`. Valor entre aspas é tratado, mas valor com `#` ou quebra de linha quebra o script.
- Deixar o processor ASP rodando custa por segundo mesmo ocioso, e o cluster continua cobrando storage mesmo pausado.
- `scripts/cleanup-streaming-data.py` tem que continuar **escopado às coleções de demo conhecidas**. Ele roda como segunda camada de limpeza quando a API está fora — um cleanup genérico ali seria um estrago silencioso.

## Como quero que você trabalhe

- Textos de UI, docstrings do backend e mensagens de erro em **pt-BR** — o público é brasileiro. Identificadores de código, README e documentação de engenharia em **inglês**.
- Credenciais só em `backend/.env`, gitignored. O frontend só enxerga `VITE_DEMO_API_TOKEN`.
- Nenhum número na tela sem chamada real por trás. Se não deu pra medir, a tela diz que não deu.
- Toda alegação de capacidade tem que ter o contra-exemplo escrito junto. O módulo Geo é o modelo disso.

## Ordem de trabalho

1. Backend base: `settings`, `database`, os dois middlewares, health e preflight.
2. Seed de `produtos`/`avaliacoes` com os índices que os módulos assumem.
3. Os cinco módulos simples (03, 04, 05, 06, 01), um router de cada vez, cada um com sua página.
4. Módulo 02 (Online Archive), que é o único que precisa da Admin API.
5. Módulo 08 (Geo), com seed próprio e database isolado.
6. Módulo 07 (Streaming) — coluna 1 primeiro, funcionando sozinha; só depois Kafka e ASP.
7. Replay, e só então a gravação.
8. Disciplina de polling e a medição de requisições.

Não comece o Streaming antes do resto estar de pé. Ele é grande o bastante pra consumir o projeto inteiro se vier primeiro.

## O roteiro que eu preciso conseguir executar no fim

Abrir cada módulo direto pela URL, disparar a operação, e mostrar o número e a query lado a lado. Especificamente:

1. Subir um índice com o `live_monitor` rodando no terminal ao lado — leitura e escrita não param.
2. Mostrar o banco recusando um documento fora do `$jsonSchema`.
3. Escrever num terminal e ver o evento aparecer na tela pelo change stream.
4. Commit e rollback de transação, com o estado antes e depois.
5. Query atravessando dado quente e arquivado de forma transparente.
6. As três colunas de streaming rodando juntas, com latência, throughput e custo comparados.
7. `explain` do mesmo `$geoWithin` com dois hints diferentes, e a viagem impossível calculada em MQL puro.
8. `overview down` no fim, na frente do cliente, e dizer por quê.
