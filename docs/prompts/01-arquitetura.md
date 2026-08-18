# Atlas Feature Showcase — arquitetura e princípios

> Primeiro dos três prompts que eu uso pra levantar essa PoV do zero. Os oito módulos, a arquitetura, a segurança do backend e a operação. Coleções, índices e pipelines em `02-mongodb.md`; tela e roteiro em `03-interface-fluxos.md`.

---

## O que eu quero construir

Um showcase interativo que exercite **oito capacidades centrais do Atlas contra um cluster de verdade**. Não é simulação, não é slide animado: cada módulo dispara uma operação real e mostra o resultado medido.

**Sem LLM nenhum aqui.** Essa é a PoV que responde "o Atlas aguenta?" e "o Atlas faz?" com número, não com narrativa. É deliberado que não tenha IA — em metade das conversas o cliente já está cansado de ouvir sobre IA e quer ver o banco.

| # | Módulo | O que ele tem que provar |
|---|---|---|
| 01 | Reindexação Online | índice sendo construído com o cluster servindo tráfego |
| 02 | Hot/Cold Tiering | Online Archive movendo dado frio pra storage barato, com a query continuando unificada |
| 03 | Aggregation Pipeline | transformação e análise no servidor, sem trazer dado pra aplicação |
| 04 | Schema Validation | `$jsonSchema` + `collMod` — o banco recusando documento fora do contrato |
| 05 | Change Streams | reação a mudança sem polling |
| 06 | Transações ACID | multi-documento, com commit e rollback visíveis |
| 07 | Streaming | três abordagens de captura lado a lado, e o que acontece quando uma falha |
| 08 | Risco geográfico | `2dsphere` composto, viagem impossível, geo + Atlas Search |

**Os módulos 07 e 08 são uma história só, não duas.** Essa é a decisão estrutural mais importante do projeto.

## Arquitetura

```
React 18 + Vite (:5174) --fetch + EventSource--> FastAPI (:8002) --> PyMongo --> Atlas (M20)
                                                       |-> Atlas Admin API v2 (Online Archive, CPU, failover)
                                                       |-> Kafka Connect REST :8083
                                                       \-> aiokafka -> Kafka :9092
```

O dev server do Vite proxia `/api` pro backend **removendo o prefixo**, e toda chamada do browser passa por ali — SSE incluído.

Um router por demo, em `backend/routers/`. Não junta dois módulos no mesmo arquivo, mesmo que pareçam parecidos: eu preciso poder mexer no Streaming sem risco de quebrar o Geo cinco minutos antes de uma reunião.

**Cluster e workspace de ASP têm que estar na mesma região.** Os dois em `sa-east-1`, e região de ASP **não muda no lugar** — se errar, é recriar. Região separada faz a coluna 3 medir um salto transcontinental e parecer fraqueza de produto: eu medi RTT puro caindo de 148,10 ms pra 7,39 ms, e o round-trip de um cliente PIX de 141,70 ms pra 11,54 ms, só com a mudança de região. E confere se o notebook da apresentação não está saindo por uma rota de VPN nos EUA antes de confiar em qualquer medição de latência.

## Segurança do backend

Dois middlewares, e os dois têm motivo prático:

- **`MutationGuardMiddleware`** — bloqueia mutação vinda de fora do loopback quando não há `DEMO_ADMIN_TOKEN` válido. Compara com `hmac.compare_digest`, não com `==`. Valida também o header `Origin` contra `ALLOWED_ORIGINS`.
- **`ApiHardeningMiddleware`** — limite de tamanho de corpo + headers `nosniff`, `DENY`, `no-referrer`, `no-store`.

O guard **ignora métodos seguros**, e é por isso que **todos os endpoints SSE têm que ser `GET`**: o `EventSource` do browser não consegue enviar o header `X-Demo-Token`. Se você fizer um SSE em POST, ele simplesmente não conecta e o erro não vai ser óbvio.

Vários endpoints aqui são **destrutivos por natureza** — derrubam índice, fazem `collMod` de schema, criam e apagam Online Archive, disparam failover de teste. Deixa isso escrito em letras grandes no README: **nunca aponta esta PoV pra nada que não seja um cluster de demo descartável.**

## Configuração

`settings.py` é uma dataclass congelada que lê todas as env vars uma vez. Um flag `settings.atlas_configured` libera o módulo de Online Archive — sem credencial da Admin API, o módulo aparece como não configurado em vez de estourar.

As variáveis do Streaming e do Geo são lidas **nos próprios routers**, não no `settings.py`. É o que permite os dois módulos degradarem sozinhos sem arrastar o resto.

## Módulo 07 — Streaming

É o maior do projeto (o router passa de 4 mil linhas) e o que mais rende conversa. Três colunas lado a lado, comparando abordagens de captura sobre a mesma coleção:

1. **Change Streams** — `ChangeStreamWorker`/`ChangeStreamCluster`, com N workers particionados.
2. **MongoDB Kafka Connector** — via Kafka Connect REST, connector `atlas-pix-source`, com um `KafkaConsumer` observando o tópico.
3. **Atlas Stream Processing** — um processor de janela, `pixJanelas5s`.

Precisa de um gerador de transações PIX sintéticas com TPS configurável, um `Hub`/`Meter` de SSE alimentando a UI, e os endpoints de sonda de leitura, janela de oplog, cluster/rede/custo e preflight.

As colunas 2 e 3 **degradam pra um painel "não configurado"** quando as variáveis de ambiente não estão lá. A página nunca pode quebrar por falta de Kafka local — em cliente eu frequentemente rodo só a coluna 1.

Modo de escrita **individual é o padrão**, não batch: ele modela um write confirmado por PIX, que é o que acontece de verdade. 2.000 TPS é o último alvo medido entregue por inteiro mantendo o caminho pós-commit abaixo de 35 ms. O modo batch continua disponível pra história de volume (8.000 TPS). O teto de 15.000 TPS da API **não é alegação de capacidade** — não apresenta como se fosse.

### O caminho feliz não prova nada

Essa é a parte que eu mais quero, e é o que separa essa PoV de um dashboard bonito. **Reconciliação fechando depois de uma falha vale mais que três painéis verdes.**

Quatro injeções de falha reais:

- **`/falha/connector`** — para o connector e retoma **do offset armazenado**.
- **`/falha/evento-invalido`** — manda um documento com `valor` string; ele é desviado pra DLQ e o processor continua rodando.
- **`/falha/schema-incompativel`** — renomeia o campo obrigatório `valor` pra `amount`. É a mudança incompatível que um Schema Registry recusaria, e existe um `/contrato` publicado que espelha o `$validate` do processor. Isso cobre a conversa de contrato **sem infraestrutura nova**: Schema Registry, Avro e TLS/SASL no broker ficaram de fora de propósito — provam configuração de terceiro, amarram a PoV a um desenho de stack e são a maior superfície de falha em cima do palco.
- **`/falha/failover`** — failover de teste do Atlas, a única falha injetada que atinge o MongoDB em si. Ela **estende a corrida em 150s**, porque uma eleição dura mais que a janela de 30s e o auto-stop fecharia a corrida no meio do evento. A evidência é `escritas_rejeitadas` ao lado de `escritas_confirmadas` — medido: **0 de 299.208** durante uma eleição de 145,6s, com 332.568 documentos reconciliados em R$ 104.486.759,65 nos três caminhos.

### `/streaming/folga` — o que a corrida custou

TPS sozinho nunca diz de quem era o teto. Então esse endpoint traz a CPU do primário lida da Admin API, **recortada na janela da própria corrida**, ao lado do pico de TPS que a produziu.

Três armadilhas aqui, todas achadas medindo e todas com teste:

- O Atlas publica métrica de processo com **1 a 2 minutos de atraso**. Então devolve `metricas_pendentes` em vez de concluir a partir do ponto que é anterior à carga.
- Corrida mais curta que o bucket de um minuto é **promediada com o tempo ocioso**: 43% alinhado contra 15% atravessando o bucket, mesma carga. Abaixo de 120s, marca `cpu_subestimada`.
- O pico era calculado dentro do `measured_tps()`, que só roda quando alguém consulta o status — então corrida sem ninguém olhando terminava com pico 0.

Medido: ~1.600 TPS por 120s num M20, a 28,6% e 53,0% de CPU em duas corridas idênticas.

E o endpoint **nunca** devolve o hostname do Atlas: ele carrega o nome do cluster, que normalmente é o nome do cliente.

### O modo replay

Faz também um `routers/replay.py` que **reproduz uma corrida real gravada**, sob `/replay/*`, espelhando os caminhos de `/streaming/*` pro frontend só precisar trocar o prefixo. Um botão ▶ Play move o relógio de reprodução.

O motivo é operacional e vale citar em cliente: M20 e M30 são *burstable*, e o auto-scaling do Atlas dispara em CPU **relativa** (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`). Já medi 17,6% de CPU absoluta sendo lida como 88% relativa, e o cluster escalou **com o gerador já parado**, só pelo polling do dashboard.

Regras do replay, todas obrigatórias:

- Ele **nunca toca o MongoDB**. Escreve um teste que garante isso.
- Todo payload carrega `replay: true`.
- `/replay/manifest` responde **200** com `disponivel: false` quando não há gravação — não 404.
- Os streams mandam `: keepalive` a cada 10s. Sem isso, stream vazado esgota o orçamento de ~6 conexões por host do browser e os fetches comuns começam a estourar em 30s.
- Botões que agem no ambiente ficam desabilitados, e a página exibe um badge permanente de origem.

A página **abre em modo ao vivo por padrão**; replay é contingência explícita. E o mais importante: **nunca apresenta um replay como corrida ao vivo.** Os números são medição real gravada, mas a honestidade sobre isso é o que sustenta tudo o mais que eu digo na reunião.

Gravação nova sai de `scripts/capture_replay.py` → `backend/data/replay_streaming.json`.

## Módulos 07 + 08 são a mesma história

O gerador emite **um stream só, com dois canais**: `PIX` (sem coordenada — essa alegação continua valendo) e `CARTAO_PRESENCIAL` (coordenada do terminal adquirente, mesma modelagem do seed do Geo, lendo o mesmo `municipios.json`).

Um segundo processor de ASP (`geoSinais30s`) lê **o mesmo change stream**, agrupa por cartão numa janela deslizante de 30s, calcula haversine em MQL e faz merge em `geo.sinais_ao_vivo`. O painel do topo do módulo 08 lê dali.

O ponto: **o sinal sai em tempo de evento, não de uma varredura sob demanda.** Os painéis retrospectivos continuam existindo, mas respondem outra pergunta.

Os sinais ao vivo carregam `origem`: **`plantado`** (o gerador injeta um par a cada ~6s pra o palco sempre ter sinal) ou **`emergente`** (encontrado no tráfego comum). A página conta os dois separadamente — **a garantia nunca pode ser apresentada como a evidência.**

E o sinal exige **os três limiares juntos** (`km/h`, `MIN_KM`, `MIN_MIN`): velocidade sozinha marcou duas compras a 20 km de distância capturadas com segundos de diferença, que é captura simultânea, não viagem.

## Módulo 08 — o que ele NÃO faz

Isso vai dito em voz alta na tela. Só **predicados** geoespaciais. Sem álgebra de geometria: nada de buffer, união, interseção, área. Só WGS84. Sem raster, sem topologia. `$geoNear` tem que ser o primeiro estágio do pipeline, e o `filter` do `$vectorSearch` rejeita operadores geoespaciais.

**Não amplia essa alegação em silêncio.** Prefiro perder um ponto na demo a ser corrigido por um especialista de GIS na frente do cliente.

Trata localização como **telemetria sintética de app**, com dispositivo, fonte, acurácia e horário de captura. Viagem impossível é **sinal de risco, nunca decisão de fraude** — isso precisa estar escrito na própria página.

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

Delega a parte de ASP e Kafka pra `scripts/ambiente.sh {up,down,status}`, que provisiona e para **os dois** processors (`pixJanelas5s` e `geoSinais30s`).

E existe **um jeito só** de subir o broker: Kafka via Homebrew, KRaft, na 9092. O caminho de Docker/Redpanda foi removido porque um container publicando a 9093 no host rouba a porta do próprio controller KRaft do Kafka, e aí o broker falha todo registro de um jeito que parece instalação corrompida. Nada nesta PoV precisa de Docker.

Um `scripts/prepare-demo.sh` pra rodar **antes** da apresentação, materializando o dataset Geo versionado e deixando o índice do Atlas Search em READY. Índice de Search não fica pronto instantaneamente, e descobrir isso no meio da demo é constrangedor.

**Sempre `overview down` depois de demo ao vivo** — processor ASP rodando cobra por segundo, mesmo ocioso, e o cluster continua cobrando storage mesmo pausado.

E um `live_monitor.py`: monitor de latência de leitura/escrita no terminal. É o que eu deixo rodando ao lado enquanto o índice constrói, pra provar que o cluster continuou respondendo.

## Testes

Todos unitários, com Mongo stubado ou monkeypatched. **Nenhum teste pode exigir cluster ao vivo** — CI não tem credencial e eu não quero suíte que só passa na minha máquina.

Cobre: comportamento dos guards, o router de Streaming (é a maior suíte), clamping de env em settings, o seed, o pipeline e o determinismo do Geo (inclusive os pares plantados), os change streams, e o replay — relógio de reprodução, snapshot no tempo, rotulagem de origem, e a asserção de que o replay **nunca fala com o Mongo**.

```bash
pytest                                  # testpaths = backend/tests
ruff check backend                      # py311, line-length 120
pip-audit -r backend/requirements.txt
```

CI rodando exatamente isso a cada push, mais `npm ci && npm run build && npm audit --audit-level=high` no front.

E um `/preflight` no backend, pra eu rodar antes de qualquer demo: verifica `MONGO_URI`, alcance do cluster, as coleções esperadas, as credenciais da Admin API e o modo do mutation guard.

O `preflight_atlas_admin()` **sonda a Admin API de verdade**, não checa se a credencial existe. Antes ele ficava verde enquanto toda chamada era recusada, e o `/streaming/cluster` caía no tier do `.env` em silêncio — o módulo 02 quebrava no palco depois de um preflight limpo. E o IP de saída muda a cada vez que eu ligo ou desligo VPN, então isso falha com frequência: o conserto fica em Organization → Access Manager → API Keys → Access List, que é uma **lista diferente** do Network Access do projeto.

## Duas regras do Atlas que é fácil errar

Ambas retornam HTTP 400 e a mensagem não é lá muito clara:

- `minInstanceSize` tem que ser **estritamente** menor que `maxInstanceSize`. Ou seja, "fixar em M20 com auto-scaling ligado" é inexprimível na API.
- Cada tier tem disco máximo. M10 vai até 128 GB, então um cluster de 150 GB não pode ter piso M10.

## Armadilhas que quero registradas

- Mudar a porta do frontend faz mutação começar a retornar 403, porque o guard valida `Origin` contra `ALLOWED_ORIGINS`.
- `scripts/ambiente.sh` parseia o `.env` com `grep`/`cut`. Valor entre aspas é tratado, mas valor com `#` ou quebra de linha quebra o script.
- **Exatamente um processo de backend pode rodar.** Um `uvicorn` esquecido mantém a participação no consumer group do Kafka e **zera a coluna do Kafka em silêncio** enquanto os offsets avançam normalmente. `pgrep -f "uvicorn main:app"` tem que devolver um PID só.
- A resposta de `/streaming/generator/start` carrega **só a identificação da corrida**. Faz merge no objeto de status existente em vez de substituir — substituir zerava `write_ack` e `ttl_segundos` até o próximo poll.
- `/streaming/generator/status` carrega `entrega`: quando o TPS entregue cai abaixo de 70% do alvo, ele **atribui o limite** à rede do apresentador (ACK p50 ≥ 40 ms) ou ao processo gerador local. Por VPN, a página diria "medido 64 · alvo 2.000" e a plateia culpa o cluster — enquanto o tempo de escrita no servidor está em ~3 ms.
- Orientação de apresentador (React/Distribute/Transform) foi tirada da UI do cliente de propósito. Ela vive em `docs/roteiro-apresentacao-streaming.md`.

## Como quero que você trabalhe

- Textos de UI, docstrings do backend e mensagens de erro em **pt-BR** — o público é brasileiro. Identificador de código, README e documentação de engenharia em **inglês**.
- Credenciais só em `backend/.env`, gitignored. O frontend só enxerga `VITE_DEMO_API_TOKEN`.
- Nenhum número na tela sem chamada real por trás. Se não deu pra medir, a tela diz que não deu.
- Toda alegação de capacidade tem que ter o contra-exemplo escrito junto. O módulo Geo é o modelo disso.
- Onde tem número medido, o comentário guarda a medição. Metade das decisões aqui só faz sentido com o número do lado.

## Ordem de trabalho

1. Backend base: `settings`, `database`, os dois middlewares, health e preflight.
2. Seed de `produtos`/`avaliacoes` com os índices que os módulos assumem.
3. Os cinco módulos simples (03, 04, 05, 06, 01), um router de cada vez, cada um com sua página.
4. Módulo 02 (Online Archive), que é o único que precisa da Admin API.
5. Módulo 08 (Geo) retrospectivo, com seed próprio e database isolado.
6. Módulo 07 (Streaming) — coluna 1 primeiro, funcionando sozinha; só depois Kafka e ASP.
7. Reconciliação com as três checagens, e **só então** as injeções de falha.
8. O canal de cartão no gerador + o processor `geoSinais30s`, ligando 07 e 08.
9. Replay, e só então a gravação.
10. Disciplina de polling e a medição de requisições.

Não começa o Streaming antes do resto estar de pé. Ele é grande o bastante pra consumir o projeto inteiro se vier primeiro. E não injeta falha antes da reconciliação existir — falha sem reconciliação é só uma tela quebrando.
