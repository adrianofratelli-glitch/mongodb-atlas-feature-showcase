# Interface, telas e fluxos — referência rápida

> Fonte: `frontend/src/App.jsx`, `frontend/src/pages/*.jsx`, `frontend/src/components/*.jsx`, `docs/screenshots/`. Módulo de risco geográfico (antigo 08) não existe mais neste frontend — foi extraído para `mongodb-atlas-geo-showcase` em 2026-09-11.

---

## Estrutura geral

Não há sidebar. Um **seletor compacto** no topo troca entre os módulos e preserva os hashes de deep-link. A abertura da aplicação é a página `Tese.jsx` (`/#tese`), não o módulo 01 — entrar direto pela feature 01 fazia a demo ser lida como catálogo item a item, e um time de dados compara com o que já opera e conclui "já temos isso". A tese declara convergência (não capacidade) e os não-objetivos explícitos. Sem número de economia estimado — é a primeira coisa desmontada numa sala técnica. O botão **Medir agora** chama `POST /tese/medir` e mostra uma tabela p50/máx por capacidade (ping, consulta indexada, agregação, rejeição de schema, entrega de change stream, transação), com a evidência de cada linha; antes do clique a tela diz que não há medição, e falha vira banner com a ação a tomar. Um `<details>` fechado traz a comparação de componentes (rotulada como desenho de arquitetura, não medição).

### As 8 entradas (`frontend/src/App.jsx`, array `MODULES`: a tese + 7 módulos)

| Hash | Nº | Título | Componente | O que prova |
|---|---|---|---|---|
| `#tese` | 00 | A tese | `Tese.jsx` | O que a demo prova e o que não prova |
| `#reindex` | 01 | Reindexação Online | `Reindexacao.jsx` | Índice sendo construído com o cluster servindo tráfego (hybrid build) |
| `#hotcold` | 02 | Hot / Cold Tiering | `HotCold.jsx` | Online Archive movendo dado frio, query continuando unificada |
| `#agg` | 03 | Aggregation Pipeline | `Aggregations.jsx` | Transformação e análise no servidor, sem trazer dado para a aplicação |
| `#schema` | 04 | Schema Validation | `SchemaValidation.jsx` | `strict` rejeita escritas inválidas; `constraint` verifica e garante o contrato para a coleção inteira (MongoDB 9.0+) |
| `#streams` | 05 | Change Streams | `ChangeStreams.jsx` | Reação a mudança sem polling |
| `#tx` | 06 | Transações ACID | `Transactions.jsx` | Multi-documento, com commit e rollback visíveis |
| `#streaming` | 07 | Streaming | `Streaming.jsx` | Três abordagens de captura lado a lado, e o que acontece quando uma falha |

Navegação por hash, lida no boot e reagindo a `hashchange` — suficiente para deep-link funcionar durante a apresentação (abrir um módulo específico direto pela URL).

## Contrato visual (assinatura MongoDB Dark)

`src/pov-signature.css` é uma cópia sincronizada entre os frontends de todo o portfólio de PoVs do autor, importada **depois** do stylesheet local. O contêiner raiz carrega `data-pov-shell`, existe `.pov-skip-link` para `#conteudo-principal`, e `index.html` declara pt-BR, dark color scheme, theme color e favicon comuns. Mudanças na assinatura precisam ser replicadas nas outras cópias do portfólio e validadas em 1440/768/360px.

Tokens em `src/index.css`: `--bg-primary: #061621`, `--accent: #00ED64`, tipografia Special Gothic (texto) + Special Gothic Condensed One (títulos) + Source Code Pro (código/dados).

## Duas regras que valem para todos os módulos

1. **Nada de dado inventado.** Todo número na tela veio de uma chamada real ao cluster. Se o Atlas não responder, a tela diz isso — não preenche com placeholder.
2. **A query fica visível.** Todo módulo mostra o pipeline que rodou, num `QueryBlock` (via `react-syntax-highlighter`). Quem está assistindo tem que conseguir copiar e rodar no Compass.

## `useApi` — o hook central de fetch (`src/hooks/useApi.js`)

- Timeout de 30s por requisição, configurável até 300s com `AbortController` (criar índice ou Online Archive demora).
- Erro traduzido para linguagem de operador — `Failed to fetch` vira "API indisponível — verifique se o backend está rodando na porta 8002".
- Abort de navegação (trocar de módulo cancela requisições da tela anterior) **não** vira toast de erro.
- Contador de pendentes em vez de booleano de loading — duas chamadas concorrentes não fazem o spinner piscar cedo demais.
- `X-Demo-Token` injetado de `VITE_DEMO_API_TOKEN`.
- Erro global via `CustomEvent('api-error')` — o shell mostra o toast sem callback manual em cada componente.

**Armadilha conhecida:** o `loading` é uma flag única para todas as chamadas daquela instância de `useApi()`. Uma página com polling **e** botão de ação precisa de um `useApi()` separado para o poll, com o "ocupado" de cada ação controlado localmente — senão os botões piscam desabilitados a cada tick do polling sem ninguém ter clicado.

## Disciplina de polling (`src/hooks/usePolling.js`)

- `useVisivel()` — aba escondida, nada roda.
- `useIntervaloVisivel(fn, ms, ativo)` — timer só existe com aba visível **e** `ativo` verdadeiro; dispara uma vez ao reativar. Guarda a função numa `ref` (identidade nas dependências recria o timer a cada render — o erro clássico que vira rajada).
- `useSse` fecha o `EventSource` quando a aba esconde e reconecta manualmente (o `EventSource` nem sempre reconecta sozinho, e cada conexão presa ocupa uma das ~6 por host do browser).

Baseline medida: **48 requisições/20s → 1 quando parado, 0 com a aba escondida.**

## Componentes compartilhados (`src/components/`)

- **`QueryBlock`** — mostra o pipeline/comando executado, sempre visível no módulo de Aggregations (não escondido atrás de "Ver código" — mostrar quão pouco se escreve é o argumento).
- **`Limites`** — bloco de limite declarado, em `<details>` fechado, usado pelos módulos; cada item precisa ser verificável na documentação do produto ou medido na própria PoV.

## Módulo Streaming (`#streaming`) — o único com layout próprio

Três colunas comparáveis lado a lado: latência, throughput e custo. Mais:

- Botões de injeção de falha (parar connector, evento inválido, schema incompatível, failover).
- Painel de reconciliação com as três checagens (contagem, valor em centavos, digest de conteúdo por documento).
- `/streaming/folga` mostrando o custo em CPU do primário.

As colunas 2 (Kafka Connector) e 3 (Atlas Stream Processing) aparecem como **"não configurado"** quando faltam variáveis de ambiente — nunca como coluna quebrada. Em modo replay, a página carrega um badge permanente de origem e desabilita botões que agem no ambiente. Abre em modo ao vivo por padrão.

## Screenshots (`docs/screenshots/`)

| Arquivo | Módulo/tela |
|---|---|
| `01-reindex.png` | Reindexação Online |
| `02-hotcold.png` | Hot/Cold Tiering |
| `03-aggregations.png` | Aggregation Pipeline |
| `04-schema.png` | Schema Validation |
| `05-changestreams.png` | Change Streams |
| `06-transactions.png` | Transações ACID |
| `07-streaming.png` | Streaming — visão geral das 3 colunas |
| `07b-streaming-colunas.png` | Streaming — detalhe das colunas |
| `07c-streaming-replay.png` | Streaming — modo replay |
| `07e-reconciliacao.png` | Streaming — painel de reconciliação |

Regra do README: qualquer screenshot usado dentro de tabela precisa ter exatamente 1440×900 (tamanhos mistos distorcem tabelas do GitHub). Screenshots avulsos (fora de tabela) são capturados em 1600×1000.

## O roteiro de apresentação (visão prática)

1. Subir um índice com `live_monitor.py` rodando ao lado — leitura e escrita não param.
2. Mostrar o banco recusando um documento fora do `$jsonSchema`.
3. Escrever num terminal e ver o evento aparecer na tela via change stream.
4. Commit e rollback de transação, com estado antes/depois.
5. Query atravessando dado quente e arquivado de forma transparente.
6. As três colunas de streaming rodando juntas, latência/throughput/custo comparados.
7. Quebrar de propósito — parar o connector, mandar evento inválido, mostrar a reconciliação fechando depois.
8. Disparar o failover do Atlas e mostrar escritas rejeitadas ao lado das confirmadas.
9. `/streaming/folga` — custo de CPU da corrida no primário.
10. `overview down` no fim, na frente do cliente, e dizer por quê.


## Antes de apresentar

- `/preflight` limpo — inclusive a sonda real da Admin API (o IP de saída muda com VPN).
- `pgrep -f "uvicorn main:app"` devolvendo **um** PID.
- Notebook fora de VPN, senão a coluna de latência mede a rota e não o cluster.
