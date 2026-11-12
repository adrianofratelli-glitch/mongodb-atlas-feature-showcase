# Arquitetura — Atlas Feature Showcase

> Referência rápida de arquitetura. Para endpoints e queries detalhadas, veja `queries.md` e `ui-flows.md`. Fonte: código real em `backend/` e `frontend/`, mais `ARCHITECTURE.md` na raiz do projeto.

---

## O que é

Um showcase interativo que exercita **capacidades centrais do Atlas contra um cluster real** (não simulação): reindexação online, tiering quente/frio (Online Archive), Aggregation Pipeline, validação de schema, Change Streams, transações ACID, e um módulo de Streaming que compara três formas de capturar mudança (Change Streams / Kafka Connector / Atlas Stream Processing).

**Sem LLM nesta PoV.** Decisão deliberada — o objetivo é responder "o Atlas aguenta?" e "o Atlas faz?" com número medido, não com narrativa de IA.

**Nota de escopo:** o antigo módulo 08 (risco geográfico / Geo) foi extraído em 2026-09-11 para o repositório standalone `mongodb-atlas-geo-showcase`. Não existe mais o módulo/telas de Geo neste repositório (routers, frontend), mas restam resquícios ligados ao streaming: `GEO_DB`/`GEO_DB_NOME` e a coleção `sinais_ao_vivo` em `backend/routers/streaming.py` (sink do processor ASP `geoSinais30s`, limpo no reset de streaming), `backend/data/municipios.json` e `scripts/setup-asp-geo.js`. O que permanece deste histórico é só o canal `CARTAO_PRESENCIAL` dentro do gerador do módulo de Streaming (ver abaixo), que é história do módulo 07, não do antigo 08.

## Stack

- **Frontend**: React 19 + Vite, JSX puro, CSS escrito à mão. Única dependência de UI: `react-syntax-highlighter` (para realçar pipelines). Sem router de terceiro, sem biblioteca de estado, sem UI kit — navegação por hash lida no boot.
- **Backend**: FastAPI + PyMongo.
- **Dados**: MongoDB Atlas (M20).
- **Streaming opcional**: Kafka (Homebrew, KRaft, local) + Atlas Stream Processing.

## Diagrama de fluxo

```
React 19 + Vite (frontend/, :5174)
   │  fetch /api/*                          (JSON)
   │  EventSource /api/streaming/*          (SSE — sessão ao vivo, modo principal)
   │  EventSource /api/replay/streaming/*   (SSE — fallback gravado)
   ▼
FastAPI (backend/main.py, :8002)
   ├─ PyMongo ─────────────► MongoDB Atlas   (POC.*, pix.*)
   ├─ requests ────────────► Atlas Admin API v2      (só Online Archive + métricas de CPU)
   ├─ requests ────────────► Kafka Connect REST      (:8083, modo ao vivo)
   ├─ aiokafka (opcional) ─► broker Kafka (:9092, KRaft via Homebrew, modo ao vivo)
   └─ arquivo ─────────────► backend/data/replay_streaming.json (playback do módulo Streaming)
```

O dev server do Vite faz proxy de `/api` para `http://localhost:8002`, **removendo o prefixo**. Toda chamada do browser — SSE incluído — passa por esse proxy; nenhum host externo é contatado para renderizar a página (fontes servidas localmente pela assinatura visual).

## Componentes do backend

| Arquivo | Responsabilidade |
|---|---|
| `backend/main.py` | App FastAPI, CORS, middleware de request-id, handlers de exceção, ligação dos routers, `/`, `/health/live`, `/health/ready`, `/preflight`, `/stats` |
| `backend/settings.py` | Dataclass congelada que lê env vars uma vez; `settings.atlas_configured` habilita o módulo de Online Archive sem estourar quando a credencial falta |
| `backend/database.py` | Um único `MongoClient` (`connect=False`, `appname`, timeouts explícitos) + `readiness()`. Cai para URI de localhost no import — falta de `MONGO_URI` aparece em `/health/ready`, não derruba o processo |
| `backend/security.py` | `MutationGuardMiddleware` + `ApiHardeningMiddleware` (ver seção de segurança) |
| `backend/routers/*.py` | Um módulo por demo — reindexacao, hot_cold, aggregations, schema_validation, change_streams, transactions, streaming, replay, tese (`POST /tese/medir`) |
| `backend/seed_data.py` | Upsert determinístico (idempotente) de `produtos`/`avaliacoes` + `ensure_indexes()`; sem `ALLOW_DEMO_DB_WRITE=1`, recusa `MONGO_DB` fora de `showcase_*_test`; com a flag, aceita só o `MONGO_DB` do arquivo `backend/.env` (padrão `POC`, `mongo_db_da_demo()`). O `scripts/reset_demo.py` e o `scripts/cleanup-streaming-data.py` exigem ainda `STREAMING_DB`/`GEO_DB` no formato `showcase_*_test` sem a flag (a limpeza dropa coleções; `geo_test` é do geo-showcase). Com a flag, só passam os nomes do arquivo `backend/.env` (ou o padrão `pix`/`geo`), nunca um override exportado no shell (`banco_da_demo()` em `scripts/cleanup-streaming-data.py`). O `scripts/ambiente.sh` passa a flag porque o `overview` é dono do ciclo da demo |
| `scripts/reset_demo.py` | Reset único: dropa só coleções dos módulos, remove índices `demo01_*`, garante dados/índices e limpa o streaming; `--check` só verifica |

**Por que um router por demo:** permite mexer no Streaming sem risco de quebrar outro módulo minutos antes de uma reunião com cliente.

## Componentes do frontend

- `src/App.jsx` — casca, seletor compacto de módulo, roteamento por hash (`/#tese`, `/#agg`, `/#streams`, `/#tx`, `/#streaming`). Rota padrão é `/#tese`.
- `src/pages/` — um componente por módulo, mais `Tese.jsx` (a abertura: tese de convergência + não-objetivos, ~70 palavras).
- `src/components/` — `QueryBlock` (mostra o pipeline/query executado), `Limites` (bloco de limite declarado, `<details>` fechado, usado por todo módulo).
- `src/hooks/useApi.js` — wrapper de fetch com `X-Demo-Token`, timeout de 30s (configurável até 300s), erro traduzido para linguagem de operador, contador de pendentes em vez de booleano de loading.
- `src/hooks/usePolling.js` — `useVisivel()` / `useIntervaloVisivel(fn, ms, ativo)`: nenhum timer roda com a aba oculta; guarda a função numa `ref` para não recriar o timer a cada render.
- `src/index.css` — tokens dark do MongoDB (`--bg-primary #061621`, `--accent #00ED64`, Special Gothic + Source Code Pro).

## Fluxo de dados (visão de produto)

1. O usuário abre um módulo pela URL (hash) ou pelo seletor.
2. A página dispara um `fetch`/`EventSource` real contra o backend — **nunca há dado inventado na tela**; se o Atlas não responder, a UI diz isso em vez de preencher com placeholder.
3. O backend roda a operação real contra o Atlas (índice, pipeline, transação, change stream, etc.) via PyMongo, ou contra a Atlas Admin API para Online Archive/métricas de cluster.
4. O resultado medido volta para a UI junto com o pipeline/query que rodou (`QueryBlock`), para que quem está assistindo possa copiar e rodar no Compass.

## Separação de databases

| Database | Quem usa |
|---|---|
| `POC` (configurável via `MONGO_DB`) | módulos de Reindexação, Hot/Cold, Aggregations, Schema Validation, Change Streams, Transactions (`produtos`, `avaliacoes`, mais coleções de demo por módulo) |
| `pix` (configurável via `STREAMING_DB`) | módulo de Streaming |

## Segurança do backend

Dois middlewares em `backend/security.py`:

- **`MutationGuardMiddleware`** — bloqueia mutação vinda de fora do loopback quando não há `DEMO_ADMIN_TOKEN` válido, comparado com `hmac.compare_digest` (nunca `==`). Também valida o header `Origin` contra `ALLOWED_ORIGINS`.
- **`ApiHardeningMiddleware`** — teto de tamanho de corpo (`MAX_REQUEST_BYTES`) + headers `nosniff`, `DENY`, `no-referrer`, `no-store`.

O guard **ignora métodos seguros** (GET), e é por isso que **todo endpoint SSE tem que ser GET**: o `EventSource` do browser não consegue enviar `X-Demo-Token`. Um SSE em POST simplesmente não conecta, e o erro não é óbvio.

**Atenção:** vários endpoints são destrutivos por natureza (derrubam índice, fazem `collMod` de schema, criam/apagam Online Archive, disparam test failover). Nunca aponte esta PoV para um cluster que não seja descartável.

## Decisões de desenho

- **Cluster e workspace de ASP na mesma região** (`sa-east-1`). Região separada faz o módulo de Streaming medir salto transcontinental e parecer fraqueza de produto (medido: RTT caiu de 148,10 ms para 7,39 ms só com a mudança de região).
- **Modo replay do Streaming** (`backend/routers/replay.py`, `/replay/*`) — nunca toca o MongoDB (testado), serve `backend/data/replay_streaming.json` gravado por `scripts/capture_replay.py`. Existe porque M20/M30 são *burstable* e o auto-scaling dispara por CPU **relativa** — já mediu 17,6% CPU absoluta lida como 88% relativa, escalando o cluster com o gerador parado, só pelo polling do dashboard. A página abre em modo ao vivo por padrão; replay é contingência explícita, com badge permanente de origem, e **nunca é apresentado como corrida ao vivo**.
- **Disciplina de polling** — todo intervalo do app passa por `usePolling`; aba escondida não gera tráfego. Baseline medida: 48 requisições/20s parado → 1 quando parado, 0 com aba escondida.
- **Toda alegação de capacidade carrega o contra-exemplo escrito junto** — nenhum número na tela sem chamada real por trás.
- **Limites declarados, nunca inventados** — cada item de `Limites.jsx` precisa ser verificável na documentação do produto ou medido nesta PoV; limite inventado é pior que limite omitido.
- **Tela enxuta, o apresentador narra** — fica visível o que a narração não carrega (query, comando, resultado do cluster). O módulo 03 mostra o pipeline sempre aberto, não atrás de "Ver código": mostrar quão pouco se escreve é o argumento. Limites e não-objetivos ficam colapsados.
- **Benchmark de transações (`POST /transactions/benchmark`)** — sempre `writeConcern majority` nos três cenários (comparar contra write concern mais fraco compararia garantias diferentes). O RTT é medido antes com `ping` puro: num enlace ruim a transação mede ~1 s, e isso é afirmação sobre a rede, não sobre o produto (acima de 40 ms, veredito `limitado_pela_rede`, mesmo limiar do módulo 07). Os retries são contados pelas entradas no callback, porque `with_transaction` não os expõe. O `getParameter` de `transactionLifetimeLimitSeconds` é negado em vários tiers, então a resposta diz `indisponível` em vez de citar o padrão de memória. O número a levar é a razão (medido 4,1× a escrita única, ~4 viagens de rede), não o absoluto.
- **Sinal geográfico do streaming exige três limiares** (`km/h`, `MIN_KM`, `MIN_MIN`) — velocidade sozinha sinalizava duas compras a 20 km capturadas com segundos de diferença, que é captura simultânea, não deslocamento. Sinais carregam `origem: plantado | emergente` e a UI os conta separadamente: o par plantado garante um sinal no palco, e somá-lo ao emergente transformaria essa garantia em evidência.
- **Pares plantados derivam o intervalo de uma velocidade alvo** (1.100–9.000 km/h), nunca o contrário — sortear minutos diretamente dava 16.000–42.000 km/h, vinte vezes qualquer padrão real de cartão clonado.
- **`/streaming/generator/status` atribui o teto (`entrega`)** — quando o TPS entregue cai abaixo de 70% do alvo, o limite é atribuído à rede do apresentador (ACK p50 ≥ 40 ms) ou ao processo gerador local. Sob VPN a página diria "medido 64 · alvo 2.000" e a plateia culparia o cluster, com tempo de escrita no servidor de ~3 ms.
- **`/streaming/folga` nunca devolve o hostname do Atlas** — só o nome do cluster, que costuma identificar o cliente. A CPU é recortada à janela da execução; execuções abaixo de 120 s marcam `cpu_subestimada` (bucket de 1 min promediado com ocioso: 43% alinhada vs 15% atravessando, mesma carga), e o pico de TPS é registrado pelo gerador, não só quando alguém consulta o status.
- **Preflight da Admin API sonda, não só confere a credencial** — antes ficava verde com toda chamada recusada, e `/streaming/cluster` caía em silêncio para o tier do `.env`. O IP de saída muda com VPN; a correção fica em Organization → Access Manager → API Keys → Access List, que é uma lista **diferente** do Network Access do projeto (este governa o driver).
- **Failover estende a execução em 150 s** — uma eleição dura mais que a janela de 30 s, e o auto-stop fecharia a execução no meio do evento. O recurso da Admin API mudou de forma entre versões: a chamada tenta as formas conhecidas em ordem, e só 404/405 avança — erro de credencial ou de access list aparece sem máscara.
- **Uma única forma de subir o Kafka** (binários do Homebrew, KRaft) — o caminho Docker/Redpanda foi removido porque um container publicando a `9093` no host rouba a porta do controller KRaft, e o broker falha todo registro de um jeito que parece instalação corrompida.

## Ferramental de operação

```bash
./bin/overview           # sobe backend + frontend, preflight read-only
./bin/overview down      # para app, ASP e Kafka; cluster intocado
./bin/overview status
./bin/overview logs
./bin/overview --replay  # sem provisionar ASP nem Kafka
```

`overview` **nunca** altera estado do cluster (não pausa, não retoma, não redimensiona, não mexe em auto-scaling) — ciclo de vida do cluster é decisão manual do operador. Delega ASP/Kafka para `scripts/ambiente.sh {up,down,status}`. Agenda um `down` automático 45 min depois de `up` (processor de ASP cobra por segundo mesmo ocioso).

## Testes

A suíte offline (`pytest`, a mesma da CI) usa Mongo stubado/monkeypatchado e não exige cluster: 273 testes, mais 1 módulo live pulado. A suíte live, `backend/tests/live/test_live_modulos.py`, é opt-in (`LIVE_TESTS=1`) e só roda com `MONGO_DB`/`STREAMING_DB`/`GEO_DB` no formato `showcase_*_test` (bancos próprios: o reset do streaming apaga `<GEO_DB>.sinais_ao_vivo`). Ela sobe o próprio backend numa porta livre, com ASP desligado e Kafka numa porta morta, e exercita 00–07 por HTTP. No 07 ela confere a reconciliação por Change Streams e `inseridos == fonte` (decisão em `docs/adr/0001-testes-live-opt-in.md`). O frontend tem testes `node --test` (`frontend/tests/`: polling, `useApi`, formatadores de `src/lib/formato.js`), que rodam na CI. `backend/tests/test_endpoints_adversarial.py` cobre entradas hostis (operadores Mongo em query/body, nomes de índice alheios, JSON malformado, corpo de 1 MB, paginação fora do contrato), concorrência (409 em benchmark e medição simultâneos) e degradação sem Mongo.

```bash
pytest                                  # testpaths = backend/tests
ruff check backend                      # py311, line-length 120
pip-audit -r backend/requirements.txt
```

CI (`.github/workflows/ci.yml`) roda isso a cada push, mais `npm ci && npm run build && npm audit --audit-level=high` no frontend.

`/preflight` no backend confere `MONGO_URI`, alcance do cluster, coleções esperadas, credenciais da Admin API e modo do mutation guard — rodar antes de qualquer demo.
