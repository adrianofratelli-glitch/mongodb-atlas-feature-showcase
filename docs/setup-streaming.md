# Módulo de streaming — setup

Tudo o que o módulo de streaming (`/#streaming`) precisa antes de uma execução ao vivo: o
broker Kafka local, o source connector do MongoDB e os dois jobs de Atlas Stream
Processing. O fallback gravado (`overview --replay`) não precisa de nada disso.

Voltar para o [README](../README.md).

## 1. Suba o Kafka localmente

Duas formas de fazer isso, escolha uma.

*Nativo (sem Docker, recomendado em notebook):*

```bash
brew install kafka          # uma vez
./scripts/kafka-local.sh up # broker (KRaft) + Kafka Connect + plugin do MongoDB
./scripts/kafka-local.sh status
./scripts/kafka-local.sh down
```

O plugin `mongodb-kafka-connect` é baixado só na primeira execução e fica
cacheado localmente, então execuções seguintes funcionam offline. O broker escuta em
`localhost:9092`, que é para onde o `KAFKA_BROKERS` do `backend/.env` precisa apontar.

Existe deliberadamente **uma** forma de rodar o broker. Havia um caminho por Docker e ele
foi removido: duas formas de subir a mesma dependência dobravam a superfície de setup sem
valor nenhum para a demo, e um container publicando a `9093` no host roubava em silêncio a
porta do próprio controller KRaft do Kafka — o broker então aceitava TCP na porta do
controller e estourava timeout em todo registro, o que parece exatamente uma
instalação corrompida do Kafka.

## 2. Registre os source connectors

Lê o `MONGO_URI` do `backend/.env`:

```bash
./scripts/setup-kafka-connector.sh      # um connector (padrão)
./scripts/setup-kafka-connector.sh 2    # experimento opcional com filtros disjuntos
```

Ele faz PUT de uma configuração de `MongoSourceConnector` na API REST do Connect
(`http://localhost:8083`) com `database=pix`, `collection=transacoes`,
`publish.full.document.only=true`, `startup.mode=latest`, heartbeats e
`topic.prefix=atlas`, produzindo o tópico `atlas.pix.transacoes`.
No caminho Docker, um console em http://localhost:8085 permite inspecionar o tópico
ao vivo; o caminho nativo não tem UI, então use `./scripts/kafka-local.sh status`.

Antes de registrar, o script para cada connector antigo, apaga os offsets dele
e só então apaga o connector. Apagar um connector **não** apaga
os offsets — o Connect guarda o resume token em `connect-offsets` chaveado pelo
nome do connector, e o `startup.mode=latest` só vale quando não há offset armazenado.
Como o `overview up` dropa `pix.transacoes`, esse token armazenado aponta para uma
posição do oplog que não existe mais, e a task falha com
`ChangeStreamHistoryLost` enquanto o connector ainda reporta `RUNNING`. Se você
algum dia vir `RUNNING · FAILED`, a causa é essa; o endpoint de offsets exige
Kafka Connect 3.6+.

## 3. Crie a Stream Processing Instance

Na UI do Atlas, em Stream Processing, crie uma SPI na mesma região
do cluster. O processor lê o change stream do cluster, então mantê-los juntos evita um salto entre regiões a
cada janela. Adicione uma conexão *Atlas Database* chamada `atlasCluster`, e então:

```bash
# em backend/.env: ASP_ENABLED=true e ASP_CONNECTION_STRING=<connection string da SPI>
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js
mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp-geo.js
```

São dois processors porque um pipeline implantado tem exatamente um sink
terminal. O `pixJanelas5s` termina em `pix.metricas_janela`; o `geoSinais30s` termina em
`geo.sinais_ao_vivo`. São dois consumidores independentes do mesmo change
stream, o que por acaso é o próprio argumento do módulo. O `scripts/ambiente.sh`
provisiona e para os dois.

O script preserva por padrão um processor existente e seu checkpoint. Para
substituir a definição de propósito, rode uma vez com `ASP_RECREATE=true`;
essa escolha destrutiva é impressa explicitamente.

O processor lê o change stream de `pix.transacoes`, manda documentos
malformados para uma DLQ, agrega janelas de tempo de evento de 5 segundos por `run_id`, `uf`
e `tipo` (contagem, volume, ticket e um sinal simples de alto valor), e faz `$merge` de cada janela fechada em
`pix.metricas_janela`. O backend expõe essas janelas observando aquela
coleção com um change stream, então o resultado do stream processing chega à
tela pelo mesmo mecanismo da coluna 1.

Derrube tudo com `./scripts/ambiente.sh down` (ou `./bin/overview down`,
que também para a aplicação e limpa as coleções do PIX).

**Limpeza entre execuções.** O `POST /streaming/reset` (o botão **Reset**)
limpa a origem atual, as janelas, a DLQ e a auditoria mantendo o ambiente
pronto para outra execução. O `POST /streaming/reset?finalizar=true`, usado pelo
`overview down`, primeiro para o processor e também remove os checkpoints da
aplicação. O `scripts/ambiente.sh down` faz uma segunda limpeza direta e com escopo
antes de parar os serviços locais de integração, para que uma chamada de API interrompida
não deixe dados de demo para trás. Ele nunca pausa nem redimensiona o cluster Atlas.

O Reset preserva os três contratos da origem: `endToEndId` único, TTL em `ts`
e `run_id_reconciliacao`. Ele reaproveita a topologia MongoDB já conectada da
aplicação e limpa coleções independentes em paralelo. Acima de
`STREAMING_DROP_ACIMA_DE` (25.000 documentos por padrão), ele usa um
drop/recreate controlado e então recupera ASP/Kafka em vez de gastar a pausa do palco com
`delete_many`; a preparação medida foi de 6,43 s depois de uma execução de 59 mil documentos e
1,33 s numa execução partindo do zero.
Uma limpeza de rotina não reinicia o Kafka.

O índice TTL de 5 minutos em `ts` (`STREAMING_TTL_SEGUNDOS`) é a rede de segurança para
quando você esquece de resetar, não o mecanismo principal. Em regime permanente, o
apagador de TTL remove na mesma taxa em que você insere, seja qual for a janela; o que a
janela de fato decide é o tamanho do **conjunto vivo**. A 1800 s a
coleção estabilizou perto de um milhão de documentos — dados mais índices maiores que o
cache do WiredTiger de um M20, o que por si só sustentou a pressão de memória que
dispara o auto-scaling. Um TTL de 60 segundos foi rejeitado de propósito para a
execução de palco: depois do primeiro minuto ele apagaria mais ou menos na taxa de ingestão, somaria
pressão no oplog e poderia remover documentos de origem antes da reconciliação. A
execução finita de 30 segundos mais o **Reset** são o caminho de limpeza; 300 segundos é só a
janela de reserva.

**A página de streaming usa o modo ao vivo por padrão.** Ela abre os três caminhos de observação apenas
quando o operador inicia uma sessão, mede o TPS pedido e o alcançado,
e para o polling contra o Atlas assim que uma execução finita reconcilia. O modo alternativo
**Replay de segurança** lê `backend/data/replay_streaming.json` por
`/replay/*`; ele nunca escreve no MongoDB e fica permanentemente rotulado.

O estado normal da casca é o verde `Pronto`. `Pré-voo pendente` significa que uma checagem
obrigatória de fato falhou; substituiu o selo ambíguo `Verificar`. O
painel de decisão React/Distribute/Transform, voltado ao apresentador, foi intencionalmente
removido da tela do cliente e movido para o guia de apresentação,
`docs/roteiro-apresentacao-streaming.md`.

A janela passou de 10 s para 5 s. A semântica não mudou — tumbling, sem
sobreposição — mas a 10 s a coluna 3 ficava muda por dez segundos seguidos, e uma
plateia assistindo vinte segundos da demo via no máximo duas rajadas. A
gravação também continua rodando por 25 s depois que a execução reconcilia: parar no
momento da reconciliação deixava o estado verde final vivo apenas nos últimos segundos
de um loop de ~106 s, então a recompensa sumia no rebobinar.

A página ao vivo mantém a lição anterior sobre CPU relativa: não a deixe observando
depois da execução, e sempre termine com `overview down`.

**Abra a página de captura ao vivo antes de iniciar o gerador.** Os consumidores de Change Stream
e de Kafka sobem de forma preguiçosa na primeira assinatura SSE. O observador
usa `auto.offset.reset=earliest`, enquanto o próprio source connector usa
`startup.mode=latest` quando não tem offset armazenado. Esperar a coluna do Kafka
reportar `consumindo` antes de escrever mantém a fronteira da captura explícita e
evita medir a subida do consumidor como backlog.

![Modo de replay com a execução reconciliada](screenshots/07c-streaming-replay.png)

O selo de origem na tela é permanente e nomeia o `run_id` gravado e o
timestamp. O card do gerador também diz que o playback não toca o
banco, e um aviso separado aparece quando o arquivo de gravação está ausente. Os
números são medições reais, não uma execução ao vivo. Na captura, os quatro caminhos
concordam em 12.200, com zero duplicatas observadas e DLQ vazia.

Ações que atuariam sobre um ambiente real (restart de connector, injeção na DLQ,
restart de checkpoint) permanecem visíveis, mas desabilitadas — a capacidade faz parte
da história, mas não há sobre o que agir durante um replay.

**ASP e Kafka são provisionados por padrão para a página ao vivo.** O `overview`
verifica os ativos materializados, sobe ASP/Kafka/backend/frontend e não
redimensiona o cluster. O processor cobra por segundo, então o `overview down` faz parte
do runbook da demo. Para capturar ou atualizar a gravação de reserva:

```bash
overview                      # preflight + ASP + Kafka + backend + frontend
python scripts/capture_replay.py
```

Grave uma execução com o ambiente no ar:

```bash
python scripts/capture_replay.py                    # 60 s de escritas reais a 200 TPS
python scripts/capture_replay.py --segundos 90 --tps 200
```

A captura assina os mesmos fluxos SSE e consulta os mesmos endpoints que a
página consome ao vivo, armazenando cada payload com seu timestamp. **Os números
reproduzidos são medições, não simulação** — o gravador não sintetiza
nada. Como essa distinção só se sustenta se a plateia puder vê-la, a
página mostra um selo permanente nomeando o `run_id` gravado e sua data, todo
payload de replay carrega `replay: true`, e ações que atuam sobre o ambiente
real (restart de connector, injeção na DLQ e restart de checkpoint) ficam
desabilitadas. Não apresente um replay como execução ao vivo.

Por que ele existe: M20/M30 são instâncias burstable, e o auto-scaling de compute do Atlas
dispara por CPU **relativa** (`NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75`), não
absoluta. Medido neste projeto, 17,6% de CPU absoluta registrou como 88%
relativa e escalou o cluster para M30 — com o gerador já parado, só pelo
polling do dashboard. O replay elimina esse custo por completo nas partes da
demo que só precisam mostrar a mecânica.

**Calibração de palco.** O caminho ao vivo é calibrado para uma execução finita de 30 segundos;
não é um benchmark de produção nem uma declaração de capacidade sustentada:

- O `run_id` é indexado. O painel de reconciliação conta a origem a cada poucos
  segundos; sem esse índice a contagem é uma varredura completa da coleção repetida em
  loop, e ela era de longe o maior consumidor de CPU e cache do cluster.
- O polling de reconciliação roda a cada 5 s e **para** quando a execução é final —
  ele não consulta mais o Atlas em busca de uma resposta que não pode mudar.
- O **Play** usa por padrão o modo individual a 2.000 TPS por 30 segundos. O driver
  assíncrono emite um `insert_one` confirmado por PIX, o que corresponde ao caminho
  bancário e torna a latência de ACK interpretável por transação. A 2.000 TPS a
  execução medida entregou 2.037 TPS com p50 de 3,07 ms no Atlas, p50 de ACK do cliente de
  17,6 ms, p50 de Change Streams de 21,9 ms e p50 de Kafka de 32,1 ms.
- O modo em lote continua disponível para a história de volume. O teto do gerador é
  15.000 TPS e quatro observadores disjuntos de Change Stream expõem folga de
  consumo; nenhum dos dois é alegação de capacidade de produção. A rampa completa de lote 4k→12k
  reconciliou em M20 + **SP10** com DLQ 0; o observador Kafka local degradou
  antes do Atlas Stream Processing.
- A execução de aceitação ao vivo de 2026-08-07 produziu 59.896 documentos e reconciliou
  Atlas, Change Streams, Kafka e ASP/DLQ com zero perda. O estado final chegou
  em 41,09 s, incluindo a janela de geração de 30 segundos.
- A checagem `cluster_tier` do `/preflight` agora **passa** no tier de entrada e falha
  quando o cluster escalou acima dele. Antes fazia o contrário: falhava
  em M20 e mandava o operador rodar carga até o cluster escalar,
  o que codificava "escale antes de demonstrar" como pré-requisito.
- Estado e tier do cluster pertencem ao operador. O `overview` reporta prontidão da
  aplicação, mas nunca pausa, retoma, redimensiona ou muda o auto-scaling.

Duas regras do Atlas vale conhecer antes de mexer nisso, porque as duas falham com
HTTP 400: o auto-scaling de compute exige `minInstanceSize` **estritamente** menor que
`maxInstanceSize` — então "fixar o cluster em M20 mantendo o auto-scaling ligado" não é
exprimível — e cada tier tem um tamanho máximo de disco, então um cluster de 150 GB
não pode ter piso M10 (o M10 vai até 128 GB).

Variáveis de ambiente relevantes: `STREAMING_DB`, `KAFKA_BROKERS`, `CONNECT_URL`,
`CONNECT_CONNECTOR_NAME`, `ASP_ENABLED`, `ASP_CONNECTION_STRING`,
`ASP_CONNECTION_NAME`, `ASP_PROCESSOR_NAME`, `ASP_GEO_PROCESSOR_NAME`
(`geoSinais30s`), `ASP_TIER` (padrão de palco `SP10`),
`STREAMING_CARTAO_PCT` (18 — fatia do fluxo no canal de cartão; em 0 o
fluxo é só PIX e o painel de tempo de evento do módulo 08 fica vazio),
`STREAMING_SINAL_KMH` (900), `STREAMING_SINAL_MIN_KM` (200),
`STREAMING_SINAL_MIN_MIN` (1),
`STREAMING_MODO_ESCRITA` (`individual`), `STREAMING_DEMO_TPS_INDIVIDUAL`
(2.000), `STREAMING_DEMO_TPS` (8.000 para o modo em lote),
`STREAMING_CS_PARTICOES` (padrão 4), `STREAMING_TTL_SEGUNDOS` (padrão 300),
`STREAMING_DROP_ACIMA_DE` (padrão 25.000), `STREAMING_DEMO_DURATION_S` (30),
`STREAMING_CONCEPT_TPS` (200; teto de API 15.000),
`ATLAS_TIER_INICIAL` (só a expectativa do preflight), `ATLAS_MIN_TIER`
(apenas documentação) e `KAFKA_CONSUMER_GROUP`.

## Valores das transações

O tráfego real de pagamentos é desbalanceado: muitas transferências pequenas e algumas grandes que
carregam a maior parte do dinheiro. Sortear valores de forma uniforme perde isso, e o ticket
médio sai errado. O `PERFIS_VALORES` declara faixas de valor ponderadas por tipo de
transação. Escolha uma com `STREAMING_PERFIL_VALORES`:

| Perfil | Mediana | Média | Média ÷ mediana | Top 1% do volume |
|---|---|---|---|---|
| `varejo` (padrão) | R$ 91 | R$ 559 | 6,2× | 36% |
| `corpo_medio` | R$ 500 | R$ 1.252 | 2,5× | 18% |

Os dois mantêm a cauda longa. Tentamos um sorteio uniforme entre R$ 100 e R$ 2.000: a
média cai quase em cima da mediana (1,4×) e o top 1% acaba carregando 3% do
volume, o que não é a cara de um fluxo de pagamentos.

O `GET /streaming/perfil-valores` retorna as faixas declaradas ao lado dos percentis
medidos com `$percentile` sobre a coleção ao vivo — uma boa forma de ver um
operador de agregação responder a uma pergunta sobre dados que estão sendo escritos enquanto você
pergunta.

## Como ler os números

A vazão e a latência na tela descrevem esta execução, neste cluster, a partir de
onde você está sentado. Não são um número de capacidade do MongoDB, e os
presets padrão são modestos de propósito, para que tudo rode em um tier pequeno
e barato.

Duas coisas que vale saber antes de ler um número de latência:

- Ele inclui a ida e volta até o cluster, impressa acima das colunas. Rodar
  do Brasil contra um cluster nos EUA soma cerca de 200 ms de distância pura a
  tudo.
- Os feeds por evento se redesenham a cada 120 ms, porque nenhuma aba de navegador desenha
  milhares de linhas por segundo. Os contadores e percentis por trás deles ainda
  contam todo evento.

O número em que de fato confiar é a reconciliação descrita acima. A vazão
varia com o seu notebook, a sua região e o seu tier; se os eventos todos
chegaram, não.

### De quem era o teto?

O `GET /streaming/folga` lê a CPU do primário pela Atlas Admin API e a coloca
ao lado do TPS de pico da mesma execução, para que o número de vazão deixe de ser
ambíguo: um TPS modesto com o cluster quase ocioso significa que o limite foi o
gerador ou a rede, e o painel diz qual.

Três coisas sobre essa leitura, todas achadas medindo:

- **O Atlas publica métricas de processo com um a dois minutos de atraso.** Consultada logo
  depois de uma execução de 30 s, a série ainda descreve o cluster *antes* da carga.
  O endpoint corta a série no `started_at` da execução e responde
  `metricas_pendentes` em vez de concluir a partir de pontos velhos. Espere um minuto
  e atualize.
- **Execuções menores que o intervalo de publicação de um minuto leem baixo.** O resto
  daquele minuto — com o cluster ocioso — entra na média. A mesma carga mostrou
  43% quando a execução caiu dentro de um bucket e 15% quando ficou entre dois. Abaixo de
  120 s a resposta marca `cpu_subestimada` e o texto diz para ler o número
  como um piso. Use `duration_s: 120` para a conversa de capacidade.
- **Execuções repetidas variam.** Duas execuções idênticas de 120 s a ~1.600 TPS leram 28,6% e
  53,0%. Cite a faixa, ou cite a execução na tela — não um número lembrado.

Nada disso é sizing. Isso responde quem atingiu o teto *nesta* execução; um
volume de produção exige medição em volume de produção.
