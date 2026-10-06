# Arquitetura — Atlas Feature Showcase

> Referência rápida de arquitetura. Para endpoints e queries detalhadas, veja `queries.md` e `ui-flows.md`. Fonte: código real em `backend/` e `frontend/`, mais `ARCHITECTURE.md` na raiz do projeto.

---

## O que é

Um showcase interativo que exercita **capacidades centrais do Atlas contra um cluster real** (não simulação): reindexação online, tiering quente/frio (Online Archive), Aggregation Pipeline, validação de schema, Change Streams, transações ACID, e um módulo de Streaming que compara três formas de capturar mudança (Change Streams / Kafka Connector / Atlas Stream Processing).

**Sem LLM nesta PoV.** Decisão deliberada — o objetivo é responder "o Atlas aguenta?" e "o Atlas faz?" com número medido, não com narrativa de IA.

**Nota de escopo:** o antigo módulo 08 (risco geográfico / Geo) foi extraído em 2026-09-11 para o repositório standalone `mongodb-atlas-geo-showcase`. Não existe mais código de Geo neste repositório — o `docs/briefing/02-mongodb.md` anterior ainda descrevia esse módulo como presente; isso está desatualizado e foi corrigido aqui. O que permanece deste histórico é só o canal `CARTAO_PRESENCIAL` dentro do gerador do módulo de Streaming (ver abaixo), que é história do módulo 07, não do antigo 08.

## Stack

- **Frontend**: React 18 + Vite, JSX puro, CSS escrito à mão. Única dependência de UI: `react-syntax-highlighter` (para realçar pipelines). Sem router de terceiro, sem biblioteca de estado, sem UI kit — navegação por hash lida no boot.
- **Backend**: FastAPI + PyMongo.
- **Dados**: MongoDB Atlas (M20).
- **Streaming opcional**: Kafka (Homebrew, KRaft, local) + Atlas Stream Processing.

## Diagrama de fluxo

```
React 18 + Vite (frontend/, :5174)
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

O dev server do Vite faz proxy de `/api` para `http://localhost:8002`, **removendo o prefixo**. Toda chamada do browser — SSE incluído — passa por esse proxy; nenhum host externo é contatado para renderizar a página (exceto o link do Google Fonts, pré-existente, fallback silencioso para fonte de sistema).

## Componentes do backend

| Arquivo | Responsabilidade |
|---|---|
| `backend/main.py` | App FastAPI, CORS, middleware de request-id, handlers de exceção, ligação dos routers, `/`, `/health/live`, `/health/ready`, `/preflight`, `/stats` |
| `backend/settings.py` | Dataclass congelada que lê env vars uma vez; `settings.atlas_configured` habilita o módulo de Online Archive sem estourar quando a credencial falta |
| `backend/database.py` | Um único `MongoClient` (`connect=False`, `appname`, timeouts explícitos) + `readiness()`. Cai para URI de localhost no import — falta de `MONGO_URI` aparece em `/health/ready`, não derruba o processo |
| `backend/security.py` | `MutationGuardMiddleware` + `ApiHardeningMiddleware` (ver seção de segurança) |
| `backend/routers/*.py` | Um módulo por demo — reindexacao, hot_cold, aggregations, schema_validation, change_streams, transactions, streaming, replay, tese (`POST /tese/medir`) |
| `backend/seed_data.py` | Upsert determinístico (idempotente) de `produtos`/`avaliacoes` + `ensure_indexes()`; recusa banco sem `_test` sem `ALLOW_DEMO_DB_WRITE=1` |
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

## Decisões de design que valem registrar

- **Cluster e workspace de ASP na mesma região** (`sa-east-1`). Região separada faz o módulo de Streaming medir salto transcontinental e parecer fraqueza de produto (medido: RTT caiu de 148,10 ms para 7,39 ms só com a mudança de região).
- **Modo replay do Streaming** (`backend/routers/replay.py`, `/replay/*`) — nunca toca o MongoDB (testado), serve `backend/data/replay_streaming.json` gravado por `scripts/capture_replay.py`. Existe porque M20/M30 são *burstable* e o auto-scaling dispara por CPU **relativa** — já mediu 17,6% CPU absoluta lida como 88% relativa, escalando o cluster com o gerador parado, só pelo polling do dashboard. A página abre em modo ao vivo por padrão; replay é contingência explícita, com badge permanente de origem, e **nunca é apresentado como corrida ao vivo**.
- **Disciplina de polling** — todo intervalo do app passa por `usePolling`; aba escondida não gera tráfego. Baseline medida: 48 requisições/20s parado → 1 quando parado, 0 com aba escondida.
- **Toda alegação de capacidade carrega o contra-exemplo escrito junto** — nenhum número na tela sem chamada real por trás.

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

Todos unitários, Mongo stubado/monkeypatchado — **nenhum teste exige cluster ao vivo** (CI não tem credencial). `backend/tests/test_endpoints_adversarial.py` cobre entradas hostis (operadores Mongo em query/body, nomes de índice alheios, JSON malformado, corpo de 1 MB, paginação fora do contrato), concorrência (409 em benchmark e medição simultâneos) e degradação sem Mongo.

```bash
pytest                                  # testpaths = backend/tests
ruff check backend                      # py311, line-length 120
pip-audit -r backend/requirements.txt
```

CI (`.github/workflows/ci.yml`) roda isso a cada push, mais `npm ci && npm run build && npm audit --audit-level=high` no frontend.

`/preflight` no backend confere `MONGO_URI`, alcance do cluster, coleções esperadas, credenciais da Admin API e modo do mutation guard — rodar antes de qualquer demo.
