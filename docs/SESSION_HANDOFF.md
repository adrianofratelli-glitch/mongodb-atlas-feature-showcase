# Handoff da implementação atual

Última revisão: 2026-08-11

## De quem era o teto? O custo da execução no cluster (2026-08-11)

O número de vazão era o mais fraco do módulo 07 — não por ser baixo, mas porque
sozinho ele nunca diz **de quem** é o limite. Um arquiteto de pagamentos
lê "2.000 TPS" como uma alegação de capacidade do Atlas; o gerador é um processo CPython
no notebook do apresentador e normalmente satura primeiro.

O `GET /streaming/folga` lê a CPU do primário pela Atlas Admin API, recortada à
janela da própria execução, e a reporta ao lado do TPS de pico que a produziu.
Vereditos: `sem_execucao`, `metricas_pendentes`, `cluster_com_folga`,
`cluster_participando`, `cluster_no_limite`.

Medido em M20, sa-east-1, WARP desligado (RTT 7,5 ms): ~1.600 TPS sustentados por
120 s a **28,6% e 53,0% de CPU em duas execuções idênticas**. Execuções em rajada de 30 s chegam a
~2.400 TPS.

Quatro defeitos foram achados medindo, e cada um virou teste:

1. **O Atlas publica métricas de processo com um a dois minutos de atraso.** A primeira
   versão consultava logo depois de uma execução de 30 s e concluía "cluster ocioso" a partir de
   pontos anteriores à carga — a resposta certa pela evidência errada.
   A série é cortada em `started_at`; sem nenhum ponto cobrindo a execução, a resposta
   é `metricas_pendentes`, nunca um número.
2. **Uma execução mais curta que o intervalo de publicação é diluída.** O resto daquele
   minuto (cluster ocioso) entra na média: a mesma carga leu 43% quando
   caiu dentro de um bucket e 15% quando ficou entre dois. Abaixo de 120 s a
   resposta marca `cpu_subestimada` e abandona a alegação de "muita folga".
   O `duration_s` vai até 120, que é exatamente o limiar limpo.
3. **O pico só existia se alguém estivesse olhando.** O `tps_pico` era atualizado
   dentro de `measured_tps()`, que só roda quando a UI consulta o status. Uma execução com
   ninguém na página terminava com pico 0, e o painel comparava CPU do cluster com
   TPS nenhum. Agora é calculado em `_record()`, no caminho de escrita.
4. **O `.replace(",", ".")` sobre a frase pronta comia as vírgulas da prosa** —
   "trabalho real. ainda com folga." na tela. O mesmo erro já havia sido
   cometido e documentado em `routers/geo.py`. O número é formatado isoladamente.

O endpoint nunca retorna o hostname do Atlas: ele carrega o nome do cluster,
que costuma ser o nome do cliente, e esse campo chega à tela e aos
screenshots de um repositório público. Um teste garante isso.

## O que sai do desenho (2026-08-11)

A segunda lacuna era que a PoV provava que a tecnologia funciona sem dizer o que ela
**substitui** — um time que já roda Kafka não compra mais um lugar por onde
o dado passa. Um novo painel no módulo 07 compara os componentes que cada
caminho exige, quem os opera e onde a reconciliação acontece. Ele é
deliberadamente factual e não estima economia nenhuma: um número de custo inventado é a
primeira coisa a ser desmontada na sala, e a linha do Kafka continua correta
sempre que o evento precisa chegar a sistemas fora do Atlas — que é por isso que ele está na
demo, funcionando.

## O painel 02 deixou de ser busca de catálogo (2026-08-10)

Ele perguntava "encontre uma padaria". Ninguém num time de disputas pergunta isso. A investigação
começa na **compra contestada** e pergunta o que existe em volta *daquele* terminal.

A alegação técnica não mudou — um estágio `$search` com `geoWithin`, filtro de categoria
e facetas de `$searchMeta` — mas o ponto de entrada mudou:

- O `POST /geo/search` aceita `endToEndId`. Quando presente, o centro é a
  coordenada cadastrada do terminal daquela compra, e a resposta carrega a
  âncora (estabelecimento, terminal, valor, status, procedência) para que a tela mostre
  o que está sendo investigado.
- **O `termo` agora é opcional.** Sem ele, a pergunta é "o que há aqui", e a
  cláusula de scoring vira `exists` sobre o nome do estabelecimento — um compound só com
  cláusulas de `filter` retornaria tudo com score zero. Os resultados são então
  ordenados por distância, porque sem consulta textual todos os documentos empatam em score
  e "mais relevante" seria uma ordem arbitrária. O desempate é aplicado
  **antes** da deduplicação por terminal, então cada terminal contribui com o documento certo.
  Com um termo, o casamento fuzzy e a ordenação por relevância seguem iguais —
  é o caso de "nome de estabelecimento clonado", mantido como refinamento e não como
  ponto de partida.
- O próprio terminal da âncora aparece na vizinhança e é sinalizado, para que não seja
  confundido com um estabelecimento vizinho.
- O painel 01 ganhou um botão "ver o entorno desta compra" que leva o caso sinalizado
  para o painel 02, então o caminho do analista é um clique.

Verificado ao vivo: caso `ECLI000270025` → âncora *Auto Posto Estrela*, Manaus/AM,
terminal `POS060107`, vizinhos ordenados de 0 a 2,78 km, facetas retornando 5.431
documentos no AM em quatro categorias.

## Módulo 08: fechando os flancos em que um arquiteto ainda apertaria (2026-08-10)

Quatro mudanças, todas baratas, depois de reler a aba como um arquiteto de banco faria.

- **O painel prometia detecção e entregava investigação.** O título era
  "o mesmo cálculo sobre 90 dias de histórico", o que se lê como descoberta; a
  evidência é um analista investigando sem mover dado. Agora ele diz
  *"Investigar 90 dias sem tirar o histórico do banco"*. A promessa maior estava
  minando a menor, que era verdadeira.
- **Posicionamento dito em voz alta.** Um novo banner diz que isto **não** é um motor
  antifraude e não substitui um — um emissor tem regras, scoring comportamental e
  modelos treinados em fraude confirmada, nada disso está aqui. O que muda é
  *onde o cálculo acontece*: sem cópia do histórico em um motor separado
  com CDC, contrato e operação próprios.
- **A taxa de seletividade é uma propriedade do seed.** 0,027% só reflete quantos
  casos foram plantados. A tela já dizia "não é acurácia"; agora diz também
  que o próprio denominador é construído, porque um analista comparando esse
  número com o dele encerraria a conversa mal.
- **Memória do `$setWindowFields`, respondida antes de ser perguntada.** Cada partição é
  ordenada em memória sob o teto de 100 MB por estágio; a consulta roda com
  `allowDiskUse`, então uma partição grande vai para disco em vez de falhar, ao custo de
  I/O. O que mantém as partições pequenas em produção é o recorte (um cliente, uma janela
  de datas), não uma máquina maior.

### Um defeito que só clicando se acha

O `useApi()` expõe **uma** flag `loading` para todas as chamadas de um componente, e a
aba de Geo consulta `/geo/sinais-ao-vivo` a cada 4 s na mesma instância. Os três
botões de ação, portanto, iam para "carregando" e ficavam **desabilitados por cerca de
um segundo a cada poll**, sem ninguém ter clicado. Medido antes: 4 de 24
amostras em 12 s desabilitadas; depois: 0 de 24. O poll ao vivo agora usa a própria
instância de `useApi()` e cada ação controla o próprio estado de ocupado, então rodar
a detecção não desabilita mais o painel de busca ao lado. Qualquer página que misture
polling com botões de ação em um `useApi()` compartilhado tem esse bug.

## Módulo 08 revisado como o 07 (2026-08-10)

Quatro problemas, todos achados rodando a aba em vez de lê-la.

**Os 40 pares plantados eram clones.** Todos usavam exatamente `5 minutos`, a
mesma posição na sequência do cliente, e o `municipio_distante` retornava o
primeiro candidato — então a maioria dos pares terminava na mesma cidade. A tabela mostrava 40 linhas
repetindo "5 min / ~30.000 km/h / → São Paulo", o que anuncia dado sintético
antes de alguém perguntar. Pior, derivar os minutos diretamente produzia velocidades implícitas de
16.000–42.000 km/h, vinte vezes além de qualquer padrão real de cartão clonado.

O seed (v5) agora escolhe a **velocidade alvo primeiro** — uniforme entre 1.100 e 9.000 km/h
— e deriva o intervalo da distância real entre as duas cidades, com
a posição e o destino do par aleatorizados dentro da mesma semente fixa de RNG.
Medido depois da regeneração: velocidades de **1.352 a 8.936 km/h**, intervalos de
5,5 a 115,7 minutos, rotas espalhadas pelo país e casos limítrofes
logo acima do limiar de 900 km/h — que são justamente os que forçam a
conversa sobre política de risco em vez de certeza. O `routers/streaming.py`
recebeu o mesmo tratamento, então o painel ao vivo em tempo de evento parou de reportar 20.815
km/h e agora fica entre 2.522 e 7.523.

**O painel retrospectivo não tinha procedência.** Todos os 40 resultados eram os plantados,
enquanto o texto ("o mesmo cálculo sobre 90 dias de histórico") sugeria
descoberta. Agora ele marca cada linha como `plantado`/`emergente` a partir de
`backend/data/fraud_seeds.json`, exatamente como o painel em tempo de evento — a
garantia nunca pode ser apresentada como a evidência.

**Sem denominador.** Um time de risco não pergunta "detectou?", pergunta "quantos
alertas por dia isso coloca na minha fila?". O `$facet` agora conta os pares
avaliados na mesma passada (o caro `$setWindowFields` roda uma vez, e a
contagem é tirada **antes** do corte geométrico, ou a taxa sempre pareceria
alta). Medido: **148.000 pares avaliados, 40 sinalizados, 0,027%, ~0,44 alertas por
dia** em 90 dias. A tela diz explicitamente que isso é volume operacional,
não acurácia: sem rótulos de fraude confirmada não há precisão nem recall, e
esta PoV não tem esse rótulo.

Custo do `$facet`, medido a quente para não confundir cache frio com
regressão: 2.532 ms com ele contra 2.394 ms sem — cerca de 5%. Os 5.641 ms
vistos logo depois de recriar a coleção eram o WiredTiger aquecendo.

**O caminho barato era inalcançável.** O `clienteId` existia na API e não tinha
controle na UI, então o apresentador não conseguia responder à objeção de escala. O
painel agora tem um recorte por cliente ao lado da varredura completa e mantém as duas medições
na tela: **2,5 s sobre a coleção inteira contra 371 ms para um cliente, 6,8×
mais barato**. Essa é a resposta honesta para "e sobre 90 dias de histórico real?" — o
estreitamento é o que mantém o pipeline viável, não o hardware.

Deixado de propósito como estava: o painel 02 ainda busca um catálogo ("padaria") em vez de
uma pergunta de investigação, e a nota de LGPD ainda vive dentro de um checklist recolhido.

## Uma forma de rodar o broker, e cold start medido (2026-08-10)

O caminho Docker/Redpanda foi removido: `docker-compose.streaming.yml` e
`scripts/teardown-streaming.sh` sumiram, e a documentação, a nota de arquitetura e
o painel "Kafka não configurado" na UI agora apontam para `scripts/kafka-local.sh`
(ou simplesmente `./bin/overview`). Duas formas de subir a mesma dependência dobravam a
superfície de setup sem valor para a demo — e um dos containers publicava a `9093` no
host, que é a porta em que o próprio controller KRaft do Kafka escuta. O broker
então aceitava TCP na porta do controller e estourava timeout em todo registro,
o que parece exatamente uma instalação corrompida do Kafka e custou meia hora de
depuração. **Nada nesta PoV precisa de Docker.**

Cold start medido, VPN ligada (RTT 256 ms — o pior caso realista):

| Passo | Tempo |
|---|---:|
| `./bin/overview` → "Pronto" (ASP recriado, broker, Connect, connector, backend, frontend) | **77 s** |
| Play → gerador escrevendo (reset 3,5 s + pausa deliberada de 0,8 s + start 1,9 s) | **6,2 s** |

Os 6,2 s são mais ou menos 24 idas e voltas sequenciais: na rede da apresentação
(cluster na mesma região, sem VPN) isso fica abaixo de um segundo. Nada aqui depende de
aquecimento — o primeiro Play depois do `overview` funciona.

Volume de requisições com a aba aberta, medido no log de acesso do backend:

| Estado | Requisições / 20 s |
|---|---:|
| Execução em andamento, aba visível | 52 (~2,6/s, majoritariamente o status do gerador a cada 1 s) |
| Execução parada, aba visível | **0** |
| Aba oculta | **0** |

Dois comportamentos que vale conhecer antes de culpar um painel: a página só acompanha execuções
**que ela iniciou** (uma execução disparada por curl ou por outra aba deliberadamente não é
consultada), e todo intervalo e EventSource suspendem quando a aba está oculta.

### Uma regressão introduzida e removida na mesma sessão

Fazer o `atlas_admin_api` sondar de verdade levou o `/preflight` de ~1 s para ~6 s a
256 ms de RTT, porque a autenticação digest custa duas idas e voltas e o cabeçalho da aplicação chama
o preflight. A sonda agora é cacheada por 60 s com timeout de 5 s. A checagem que
existe para a demo não quebrar não pode ser o que a deixa lenta.

## Failover de primário e contrato de schema, ambos exercitados ao vivo (2026-08-10)

Duas falhas foram adicionadas ao módulo 07, e as duas foram executadas contra o cluster
real, não apenas testadas em unidade.

**Failover de primário.** O `POST /streaming/falha/failover` dispara o test
failover do Atlas no cluster de demo. É a única falha desta PoV que atinge o
próprio MongoDB, e não um terceiro. Medido, em uma execução:

| | |
|---|---:|
| Documentos na execução | **332.568** |
| Valor, idêntico nos três caminhos | **R$ 104.486.759,65** |
| Duração da eleição (até o cluster voltar a IDLE) | **145,6 s** |
| Escritas rejeitadas após retry do driver | **0** |
| Duplicatas | **0** |
| p50 de ACK do cliente / p50 do lado do servidor | 18,5 ms / 8,2 ms |
| Final | **reconciliado** |

Três detalhes que fazem isso funcionar e não podem ser desfeitos:

- A execução é **estendida em 150 s** quando o failover é injetado. Uma eleição
  dura mais que a janela de 30 s da demo, então sem a extensão o auto-stop fechava
  a execução no meio do evento e a recuperação seria demonstrada com o gerador
  já parado.
- A evidência não é a eleição; são as **escritas rejeitadas ao lado das escritas
  confirmadas**. O `retryWrites` absorve o step-down, então o número honesto é zero
  — e zero só significa algo quando está ao lado de 299.208 confirmadas.
- O recurso da Admin API mudou de forma entre versões, então a chamada tenta
  as formas conhecidas em ordem. Só 404/405 avança para a próxima: um erro de credencial ou de
  access list é final e precisa chegar à tela sem máscara.

Com 332 mil documentos a execução ultrapassou o `MAX_DOCS_DIGEST` (200.000), então o digest do conjunto
foi pulado e a página disse isso, enquanto contagem e valor continuaram reconciliando. Esse
caminho, portanto, também é exercitado, não apenas teorizado.

**Contrato de schema, sem infraestrutura nova.** O `GET /streaming/contrato`
publica o contrato que o processor aplica, espelhando o `$validate` de
`scripts/setup-asp.js`, e o `POST /streaming/falha/schema-incompativel` publica
um evento com o campo obrigatório `valor` renomeado para `amount` — a mudança incompatível
que um Schema Registry rejeitaria no registro. Medido: 49.683 documentos,
valor idêntico nos três caminhos, DLQ 1 com o motivo `Input document found to be
invalid in $validate stage`, pipeline nunca parou, `final: reconciliado`.

A decisão deliberada foi *não* adicionar Schema Registry, Avro ou TLS/SASL ao
broker local: isso provaria configuração de um produto de terceiro, prenderia a
PoV ao desenho de stack de um banco e adicionaria a maior superfície de falha das três
opções a algo que não pode quebrar no palco. O que convence é o
comportamento sob violação, e isso não precisou de container novo.

### O preflight reportava um sinal verde que não conseguia sustentar

O `atlas_admin_api` só checava se a credencial existia no `.env`. Com a
access list da chave sem o IP de saída atual, toda chamada à Admin API era
recusada enquanto o preflight seguia verde — e o `/streaming/cluster` caía para o
tier do `.env` em silêncio (`"fonte": "env (HTTPError)"`), então a página anunciava M20
sem nunca alcançar o Atlas. O módulo 02 (Online Archive) teria quebrado no
palco depois de um preflight limpo. Agora ele sonda a API e retorna o IP recusado
com o caminho para corrigir. **O IP de saída muda sempre que uma VPN é ligada
ou desligada, então esta é a checagem que mais falha na vida real.** Repare nas duas
listas distintas: a lista de Network Access do projeto governa as conexões do driver
(que seguiam funcionando), enquanto a Admin API precisa da entrada em Organization →
Access Manager → API Keys → Access List.

## A reconciliação agora confere valor e conjunto, não só contagem (2026-08-10)

Uma reconciliação só de contagem fica verde em dois casos em que o dado está errado: um
valor transformado em algum ponto do caminho, e um documento trocado por outro.
Os dois são exatamente o que um time de pagamentos pergunta, então o `/streaming/reconciliacao`
agora reporta três níveis por caminho:

| Nível | Como | Onde se aplica |
|---|---|---|
| Contagem | documentos por caminho | origem, CS, Kafka, ASP+DLQ |
| Valor | soma em **centavos inteiros** (nunca ponto flutuante) | os quatro |
| Conjunto | XOR de `blake2b(endToEndId)` — independe de ordem | origem, CS, Kafka |

O ASP é só agregado: ele reconcilia por valor dentro de uma tolerância declarada de
±R$0,01 por janela fechada (cada janela arredonda o volume para 2 casas) e não tem
conjunto de identificadores para digerir. O digest é pulado acima de 200.000 documentos numa
execução, e a página diz isso em vez de mostrar um espaço em branco.

O evento deliberadamente inválido (`valor` em string) segue contado como documento e
fora das somas, em todos os caminhos — é isso que torna a história da DLQ e a checagem
de valor consistentes em vez de contraditórias.

Medido sob VPN, execução com as duas falhas injetadas: 1.960 documentos,
R$ 726.718,22 idênticos nos três caminhos, mesmo digest, 1 não numérico,
DLQ 1, `final: reconciliado`.

### Dois impedimentos de demo corrigidos na mesma passada

- **O Reset respondia 503 com a coleção já vazia.** O `_purge` confirmava
  o vazio com `estimated_document_count()`, que lê metadados da coleção
  ainda reportando o total anterior à exclusão. O Play era abortado em silêncio. Agora ele
  usa `count_documents({}, limit=1)`, e só resíduo em `pix.transacoes`
  bloqueia uma execução — uma janela atrasada escrita pelo processor é normal e tudo
  é filtrado por `run_id`. A UI também diz o motivo quando o Play não inicia.
- **Um selo vermelho de "backend desatualizado" durante uma demo saudável.** O selo
  testava `write_ack` em qualquer payload que estivesse no estado, e a
  resposta de `/generator/start` não o carrega. O payload de start agora é mesclado
  no objeto de status em vez de substituí-lo.

Também adicionado: o `entrega` em `/streaming/generator/status` atribui um
TPS abaixo do alvo à rede do apresentador ou ao processo gerador local,
então apresentar sob VPN não mostra mais "medido 64 · alvo 2.000" sem
explicação; e o módulo 08 reporta o custo medido do pipeline retrospectivo
(ms + documentos varridos) ao lado do resultado, com o painel de tempo de evento
nomeado como a resposta para decisões em fluxo.

## Mudança de região: feita e medida (2026-08-07)

O cluster e o workspace de Stream Processing rodam ambos em **sa-east-1 (São
Paulo)**. Medido antes e depois, mesmo notebook, mesmo harness:

| Medida | us-east-1 | **sa-east-1** | Ganho |
|---|---:|---:|---:|
| RTT puro (`ping`, sem escrita) | 148,10 ms | **7,39 ms** | 20× |
| Um PIX, ida e volta do cliente (p50) | 141,70 ms | **11,54 ms** | 12× |
| Um PIX, dentro do mongod (`opLatencies`) | 3,06 ms | 4,19 ms | — |
| Commit → evento de change stream | 0,10 ms | 0,09 ms | — |
| Teto de insert individual | 260 TPS | **2.376 TPS** | 9× |

Três conclusões que devem moldar como a PoV é apresentada:

1. **Uma ida e volta de PIX agora é de 11,5 ms ponta a ponta**, bem abaixo dos 100 ms que a
   conversa com o cliente final exige. Os dois números que *não* mudaram são os
   que nunca foram sobre distância: o tempo dentro do mongod e a propagação
   commit → CDC. O Atlas nunca foi a latência; a geografia era.
2. **`1 insert = 1 PIX` agora é viável.** 2.376 TPS com 50 threads supera a
   marca de 1.000 TPS do Inter com folga, então o micro-lote de ~800 documentos não é
   mais necessário para atingir o volume da demo. O lote existia só para amortizar uma
   ida e volta de 148 ms.
3. **50 threads é o ponto ideal, e mais é pior**: 50 → 2.376 TPS com p95
   de 33,9 ms, enquanto 600 → 2.041 TPS com p95 de 2.038 ms. Passando de ~50 o gargalo é
   o CPython (GIL mais codificação BSON), não o Atlas. Não "otimize" isso subindo
   a contagem de threads.

**Cuidado com a rota de saída ao ler qualquer número de latência.** Esses números só
apareceram depois de desligar um proxy tipo Cloudflare WARP no notebook da apresentação.
Com ele ligado, o tráfego saía de **Nova York**, tornando São Paulo *mais longe* que a
Virgínia (conexão s3: sa-east-1 308 ms vs us-east-1 226 ms), e o RTT pós-mudança
lia 254 ms — pior que antes da mudança. Confirme a saída com
`curl -s https://ipinfo.io/json` antes de confiar em um número de latência ou depurá-lo.
O cliente está no Brasil, em rota direta; o notebook da demo precisa estar também, ou ele
não consegue mostrar a latência que o cliente de fato teria.

Reproduza com as quatro medidas: RTT puro, ida e volta do cliente por PIX, o
`serverStatus().opLatencies.writes` por PIX e o teto de insert individual com
50/150/300/600 threads. O harness viveu no scratchpad da sessão e não está
commitado.

### O modo individual agora é o padrão (2026-08-07)

`STREAMING_MODO_ESCRITA=individual` faz o gerador escrever **um `insert_one`
por PIX**. É assim que o fluxo de um banco realmente é, e é isso que faz o
`opLatencies` medir uma transação em vez de um micro-lote de 800 documentos — a
objeção que motivou a mudança.

O caminho de escrita usa **`AsyncMongoClient`**. Com `asyncio.to_thread(insert_one)`
cada PIX segurava uma thread do pool, e como o mesmo processo também roda quatro cursores de
change stream, o consumidor Kafka e os polls da UI, o GIL travava a vazão em
~1.000 TPS. O assíncrono dobrou isso com semântica idêntica.

Medido em sa-east-1 com os três consumidores ativos (alvo → medido):

| Alvo | Medido | p50 Atlas | p50 ACK | p50 CS | p50 Kafka |
|---:|---:|---:|---:|---:|---:|
| 1.000 | 1.018 | 3,07 ms | 14,6 ms | 20,6 ms | 28,2 ms |
| **2.000** | **2.037** | **3,07 ms** | **17,6 ms** | **21,9 ms** | **32,1 ms** |
| 2.500 | 2.277 | 8,19 ms | 21,2 ms | 23,7 ms | 99,8 ms |
| 3.000 | 2.544 | 8,19 ms | 23,4 ms | 32,8 ms | 262,9 ms |
| 4.000 | 3.015 | 8,19 ms | 25,7 ms | 40,4 ms | 1.003 ms |

Toda linha reconciliou nos três caminhos com DLQ 0 e zero pendente.

**2.000 TPS é o padrão** — o último alvo que é de fato entregue e
mantém todo o caminho pós-commit abaixo de 35 ms. Os três presets de palco são 363
(média do Inter), 1.000 (pico do Inter, a marca do cliente) e 2.000 (2× a marca).

**O que satura primeiro é o consumidor Kafka local, não o Atlas.** Durante uma
execução a 1.000 TPS o servidor ingeriu cada PIX em 3,07 ms usando 157 de 3.000
conexões, com o cache do WiredTiger em 67%. Subir o alvo além de 2.000 só
degrada o consumidor do notebook — não leia isso como limite do Atlas, e não
"conserte" subindo a contagem de threads.

O `modo: "lote"` continua disponível em `/streaming/generator/start` para a história de volume
de 8.000 TPS; o lote existe unicamente para amortizar custo do lado do cliente.

**A região do workspace de ASP não pode ser trocada no lugar.** O `PATCH /streams/{name}` e
o `atlas streams instances update --region` retornam ambos
`400 INVALID_JSON_ATTRIBUTE`. O workspace precisa ser apagado e recriado na
nova região, o que também destrói suas conexões e processors.

**O identificador da região de São Paulo é `SAOPAULO_BRA`** — sem underline entre
SAO e PAULO. `SA_EAST_1` (o que a tabela da documentação lista), `SAO_PAULO_BRA` e
`sa-east-1` são todos rejeitados com `400 INVALID_JSON_ATTRIBUTE`, o que parece
"região não suportada" e é só um identificador errado. Verifique um valor novo
fazendo POST de um workspace descartável antes de concluir que uma região está indisponível.

Receita de reconstrução, feita em 2026-08-06 e verificada (`activeRegion: sa-east-1`):

```bash
# 1. workspace (o tier vai em streamConfig; a CLI não tem flag --tier)
POST /api/atlas/v2/groups/{proj}/streams
  {"name":"spi-inter-pix",
   "dataProcessRegion":{"cloudProvider":"AWS","region":"SAOPAULO_BRA"},
   "streamConfig":{"tier":"SP10"}}

# 2. conexão
POST /api/atlas/v2/groups/{proj}/streams/spi-inter-pix/connections
  {"name":"atlasCluster","type":"Cluster","clusterName":"inter",
   "dbRoleToExecute":{"role":"readWriteAnyDatabase","type":"BUILT_IN"}}

# 3. processor — troque antes só o host em ASP_CONNECTION_STRING
ASP_RECREATE=true ASP_TIER=SP10 mongosh "$ASP_CONNECTION_STRING" \
  --file scripts/setup-asp.js
```

**O workspace novo recebe um hostname diferente, então o `ASP_CONNECTION_STRING` em
`backend/.env` precisa ser atualizado** — deixar o host antigo `virginia-usa` ali é
o modo de falha que faz a coluna 3 ler em silêncio um cluster do outro lado do
continente. Um backup do arquivo anterior à mudança está em `backend/.env.bak-regiao`.

Mantenha cluster e workspace de ASP na **mesma região**. Separados, a coluna do ASP
mede um salto transcontinental e parece fraqueza do produto quando é
topologia.

Este é o ponto de entrada confiável mais curto ao retomar o projeto. Ele
registra as decisões por trás da PoV atual; use o `ARCHITECTURE.md` para detalhe de endpoint.

## Posicionamento do produto

A PoV prova que o MongoDB Atlas pode ser uma plataforma confiável de dados e eventos
para uma carga com o formato do PIX. Ela deliberadamente **não** é um benchmark, um exercício de
sizing nem uma recomendação de topologia de produção.

- A carga e os valores são sintéticos; Atlas, Change Streams, Kafka,
  checkpoints, Stream Processing e a DLQ são reais.
- TPS e latência descrevem apenas a execução atual notebook → Atlas.
- Um source connector Kafka e quatro cursores filtrados de Change Stream são usados na
  demonstração padrão. Os cursores expõem paralelismo de consumo nesta
  PoV; não são partições nativas do Kafka nem prescrição de sizing de produção.
- A confiabilidade é demonstrada por reconciliação de execução finita, retomabilidade,
  idempotência, backlog observável e estados de falha explícitos — não por um número
  grande de vazão.

## Mudanças e racional

### Módulo 08 reenquadrado: risco em cartão presencial, não "geo" (2026-08-07)

A aba era uma vitrine de recursos geoespaciais; agora é uma aba de **risco**. Renomeada
para "Risco geográfico" na navegação, e as três demos foram reordenadas pelo que um banco
de fato pergunta:

| Antes | Depois |
|---|---|
| Demo A: comparação de planos de índice (primeira) | movida para um `<details>` — ela responde "o índice está certo?", não "que problema isso resolve?" |
| Demo B: viagem impossível | **01 · Sinal de risco** — agora a demo de abertura |
| Demo C: geo + Atlas Search | **02 · Contexto para investigação**, ancorada em triagem de disputa/alerta |

**O dataset agora modela compras de cartão presencial (`VERSAO_DATASET = 4`).** O seed antigo
era internamente inconsistente: tinha um `estabelecimento` físico, mas tirava a
coordenada de `APP_MOBILE` / `GPS_APP_SIMULADO`. Um analista atento pergunta
por que uma compra numa padaria é localizada pelo celular do cliente.

Por que isso importa para o argumento, e não só para a arrumação:

- **O PIX não carrega coordenada.** O arranjo do BACEN não tem geolocalização, então qualquer
  enquadramento de "geo do PIX" convida uma correção da sala e custa credibilidade.
  O PIX também é o caso geo *mais fraco* — é online, sem terminal.
- **O cartão presencial resolve exatamente isso.** A coordenada é o terminal do adquirente:
  fixa e cadastrada de forma independente da telemetria do aparelho. É mais difícil
  de o cliente manipular, mas o cadastro do adquirente ainda pode estar desatualizado ou
  incorreto. Viagem impossível sobre transações de cartão presencial é o caso canônico do setor.
- **Dá continuidade à narrativa sem mentir.** O módulo 07 é PIX
  (transferência, online); o módulo 08 é cartão (compra, presencial). Duas frentes
  transacionais de um banco digital, um cluster. Relevante porque o cliente aqui é um
  banco digital **sem agências** — então casos geo de rede de agências não se aplicam.

Mudanças no documento: `dispositivo.canal` → `POS_PRESENCIAL`, `localizacaoMeta.origem`
→ `TERMINAL_ADQUIRENTE`, `qualidade` → `CADASTRAL`, `tipo` → `CARTAO_DEBITO`/
`CARTAO_CREDITO`. Estabelecimentos e terminais são entidades estáveis de catálogo: o
mesmo terminal mantém a mesma coordenada cadastrada entre compras. Ressemeie com
`python scripts/seed_geo.py --drop`.

A ressalva de GPS na tela foi reescrita de acordo: a captura no terminal é bem mais
confiável que o GPS do aparelho, mas o sinal ainda não decide sozinho —
cartões adicionais, uso autorizado por terceiros e atraso de captura todos produzem falsos
positivos.

### Streaming ao vivo para o time de PIX (2026-08-05; padrões atualizados em 2026-08-07)

- O módulo 07 volta a usar por padrão uma sessão real ao vivo; a execução gravada segue como
  contingência visivelmente rotulada, selecionada com `overview --replay`.
- A UI abre seus três observadores SSE e os polls contra o Atlas somente depois que o
  operador inicia uma sessão ao vivo, e os fecha depois da reconciliação. Isso
  preserva a lição de CPU relativa que originalmente motivou o modo só-replay.
- O `Parar e reconciliar` espera uma janela mais o atraso permitido e então insere um
  evento técnico sob `__demo_watermark__`. Ele avança o tempo de evento sem
  contaminar o `run_id` demonstrado, para que a última janela do ASP possa fechar.
- A comparação de negócio agora é explícita e vem do BCB: 313.339.828 PIX
  no dia recorde de 2025-12-05 dão 3.627 TPS médios, ou ~363 TPS sob a
  premissa de 10% de participação do cliente. O alvo de impacto de palco é 1.000 TPS, igual a
  10% do pico sustentado de 10 mil planejado pelo BCB. São marcas de comparação, não
  alegação de capacidade de produção ou sizing.
- O `overview` agora sobe ASP e Kafka por padrão, e o `overview --replay` é o
  caminho sem escrita. O `overview down` é obrigatório porque o ASP cobra por segundo.
- Todo Play ao vivo agora faz um reset de PIX com escopo antes de abrir os observadores,
  e então inicia um `run_id` novo. Respostas de status/reconciliação em voo são
  protegidas por esse `run_id`, então uma execução anterior concluída não pode fechar a nova sessão
  SSE nem sobrescrever a reconciliação dela. A UI também nomeia o namespace exato do Data
  Explorer (`pix.transacoes`) e detecta um contrato antigo de backend.
- M20 e M30 são ambos estados saudáveis dentro da faixa configurada de auto-scaling de
  compute M20→M30. O cabeçalho ainda expõe o tier real, mas o preflight
  não falha mais só porque o Atlas legitimamente foi para M30.

### Endurecimento da apresentação do PIX (2026-08-04)

- O módulo 05 agora entrega seu feed de UI por SSE e não sugere mais que sua
  animação introdutória prova retomada durável. O módulo 07 continua sendo a evidência
  de tokens persistidos, reentrega, idempotência e recuperação limitada pelo oplog.
- O selo de gravação do módulo 07 voltou a ser permanente. Aviso falado não é
  substituto de procedência visível de medições gravadas.
- O Kafka agora explicita seu contrato atual de chave/JSON do documento e separa
  offsets observados de decisões de produção sobre ordenação, chave de partição,
  compatibilidade de schema, alta disponibilidade e segurança.
- O trecho do ASP corresponde à política implantada de tempo de evento 5 s / 2 s e expõe
  ociosidade da marca d'água, comportamento de evento atrasado na DLQ e a restrição de sink
  terminal único. Parar a entrada não força o fechamento de uma janela de tempo de evento.
- **Substituído pelo modelo de cartão presencial acima:** o Geo originalmente rotulava
  coordenadas como telemetria sintética de app. O dataset v4 agora usa coordenadas
  cadastradas de terminal de adquirente e identidades estáveis de terminal/estabelecimento.
  Viagem impossível segue como sinal de risco retrospectivo, não veredito de fraude.

### Backend de streaming: evidência em vez de alegações

Arquivos principais: `backend/routers/streaming.py`,
`backend/tests/test_streaming.py`, `scripts/setup-asp.js`.

| Mudança | Por quê |
|---|---|
| Todo start de gerador cria um `run_id`; as transações também carregam sequência e `endToEndId` estável. | Uma execução finita pode ser contada independentemente de demonstrações anteriores. |
| O `RunTracker` registra os IDs únicos vistos por Change Streams e Kafka; a reconciliação também lê a origem, as janelas do ASP, a DLQ e as coleções de auditoria. | "Nada foi perdido" passa a ser resultado contábil, não animação ou comparação de contadores. |
| Os workers de Change Stream persistem resume tokens em `pix.consumer_checkpoints`. | Restarts da API e falhas transitórias de cursor podem retomar a partir de estado durável. |
| Um checkpoint só é descartado quando o MongoDB confirma `ChangeStreamHistoryLost`; outros erros o preservam. | Manter um token antigo pode reentregar, enquanto começar em silêncio a partir de "agora" poderia perder eventos. |
| Entregas duplicadas são medidas por `endToEndId`. | Change Streams e Kafka são caminhos at-least-once; a idempotência é explícita. |
| A saúde do Kafka é rebaixada do estado do connector para o estado da task, e endpoints de restart de connector/consumidor foram adicionados. | Um connector pode reportar `RUNNING` enquanto sua única task está `FAILED`; quem move o dado é a task. |
| O status do ASP inclui estatísticas de runtime do processor quando disponíveis, e o restart usa stop/start controlado e preserva o checkpoint gerenciado. | A UI mostra estado operacional e recuperação sem recriar o processor. |
| O pipeline de ASP valida `run_id`, tipo de PIX e valor numérico, usa janelas tumbling de tempo de evento com atraso permitido, e faz merge com um `_id` determinístico de execução/janela/UF/tipo. | Entrada ruim fica auditável na DLQ, eventos atrasados têm política definida, e o replay substitui em vez de contar duas vezes uma janela. |
| Resumo da DLQ, injeção e reprocessamento idempotente preservam a chave de negócio. | O tratamento de erro vira um caminho de recuperação demonstrável, e não um contador sem saída. |
| Janela do oplog, RTT de rede e latência de leitura pontual são medidos separadamente. | Retenção de resume token, distância de rede e leituras operacionais respondem a perguntas diferentes e não podem ser confundidas. |
| Presets moderados e um teto de gerador seguro para notebook substituíram alegações "impressionantes" de capacidade. | A PoV prova mecânica em um ambiente de baixo custo; o sizing do cluster está explicitamente fora de escopo. |

### Postura de custo do streaming: a PoV precisa caber em um M20

Arquivos principais: `backend/routers/streaming.py`, `scripts/ambiente.sh`,
`frontend/src/pages/Streaming.jsx`.

O cluster estava escalando para M30 em toda demo de streaming. A causa dominante
não era a carga de escrita: o `/streaming/reconciliacao` contava a origem com
`count_documents({"run_id": ...})` contra uma coleção sem índice de
`run_id`, e a UI consultava isso a cada 2 s. Essa varredura de coleção puxava o conjunto
vivo inteiro pelo cache do WiredTiger, em loop.

| Mudança | Por quê |
|---|---|
| O `_ensure_indexes()` cria um índice de `run_id`. | Transforma a contagem da reconciliação de um COLLSCAN repetido em varredura de índice. Maior ganho isolado. |
| Poll de reconciliação de 2 s → 5 s, e o loop para quando a execução é final. | Ele seguia consultando o Atlas depois que a resposta já não podia mudar. |
| **Play individual** = 2.000 TPS por 30 s; modo lote = 8.000 TPS; `TPS_MAX` = 15.000; 4 partições de Change Stream; tier de palco do ASP SP10. | O modo individual é o padrão voltado ao cliente depois da mudança de região. A execução em lote de 8.000 TPS segue sendo uma história de volume medida, não sizing de produção. |
| `STREAMING_TTL_SEGUNDOS` 1800 → 300. | O Reset é a limpeza principal. Um TTL de 60 s começaria a apagar perto da taxa de ingestão em execuções repetidas, somaria pressão no oplog e arriscaria correr contra a reconciliação. |
| A normalização de cluster foi removida do `scripts/ambiente.sh`; tier/estado pertencem ao operador. | A subida da demo não pode pausar, retomar ou redimensionar o cluster Atlas do usuário. |
| A checagem `cluster_tier` do `/preflight` foi invertida. Antes ela falhava em M20 e mandava o operador "rodar carga por alguns minutos para escalar antes da demo"; agora passa no tier de entrada e falha quando o cluster escalou **acima** dele. O `_cluster_info_sync()` expõe `escalou` no lugar de `aquecido`, e o selo do cabeçalho fica amarelo quando escala. | Escalar estava codificado no produto como pré-requisito de demo. Essa premissa era o que fazia o M30 parecer normal; a checagem agora declara a expectativa oposta. |
| O `scripts/kafka-local.sh down` lê a lista de connectors para uma variável e a interpreta de forma defensiva, e agora também para cada connector e apaga os offsets antes de removê-lo. | O teardown imprimia um traceback cru de `json.load` (`Extra data: line 1 column 7`) quando o Connect retornava algo diferente do array JSON esperado durante o desligamento. Em um caminho de limpeza esse ruído é indistinguível de uma falha genuína. Agora ele avisa em uma linha e imprime os primeiros 200 bytes do corpo, que é o que um diagnóstico futuro precisa. Resetar offsets na descida complementa a mesma correção na subida. |
| O `scripts/setup-kafka-connector.sh` para cada connector antigo, apaga os offsets dele e então apaga o connector. | Apagar um connector não apaga os offsets — o Connect guarda o resume token em `connect-offsets` sob o nome do connector, e o `startup.mode: latest` só vale quando não há offset armazenado. Como o `up` dropa `pix.transacoes`, o token armazenado apontava para uma posição sumida do oplog e a task morria com `ChangeStreamHistoryLost`, deixando o connector `RUNNING` com sua única task `FAILED`. |

**Calibração de palco ao vivo, M20 + SP10 (2026-08-06).** Cada linha é uma execução finita de
20 segundos, reconciliada entre origem, Change Streams, Kafka e ASP com zero
duplicatas, DLQ 0 e zero pendente nos três caminhos. São observações da
PoV, não sizing de produção:

| Alvo | Documentos | p99 ACK | p99 Change Stream | p99 Kafka | Reconciliado em | Resultado |
|---:|---:|---:|---:|---:|---:|---|
| 4.000 TPS | 80.400 | 0,61 s | 0,51 s | 0,48 s | 4,1 s | Confortável. |
| 6.000 TPS | 120.600 | 0,95 s | 0,60 s | 0,59 s | 4,1 s | Confortável. |
| 8.000 TPS | 160.800 | 0,67 s | 0,76 s | 3,61 s | 4,2 s | Confortável; padrão de palco. |
| 10.000 TPS | 201.000 | 0,99 s | 2,67 s | 12,5 s | 4,3 s | Inflexão: a latência pós-commit sobe forte. |
| 12.000 TPS | 241.200 | 0,83 s | 11,6 s | 16,0 s | 12,0 s | Reconciliou, mas o observador acumulou backlog visível; fronteira de estresse. |

**O tier do ASP é SP10, não SP30.** A versão anterior desta tabela alegava
SP30. O `sp.pixJanelas5s.stats()` reporta `tier` e `effectiveTier` ambos SP10,
e a linha de 2026-08-05 para 4.000 TPS registra os mesmos 80.400 documentos medidos
aqui — aquela calibração quase certamente já rodava em SP10 e só
o rótulo estava errado. Não provisione SP30 de novo com base na nota antiga.

Por que o SP10 basta para este pipeline, medido em vez de presumido:

- **O estado da janela é trivial.** A chave do `$group` é `(run_id, uf, tipo)` sobre 10
  UFs, então uma janela guarda dezenas de chaves. O `stateSize` é 0 e a memória ficou entre
  186 e 221 MB dos 2 GB do tier — cerca de 11% do teto de 80% que causa
  OOM. Estado de janela grande é a razão usual para exigir SP30, e não se
  aplica aqui.
- **A banda não chega perto.** 605 bytes/evento medidos (1,42 GB em 2,35 milhões de
  eventos). A 8.000 TPS isso dá ~39 Mbps contra os 200 Mbps do tier.
- **O paralelismo é 0.** Todo estágio roda no padrão 1, que está incluído no
  tier. Nada no pipeline precisa do teto maior de paralelismo do SP30.

O gargalo a partir de 10.000 não é o processor. O p99 do ASP fica estável (9,4 s → 11,5
s, e ele mede o fechamento da janela, não o atraso por evento) enquanto o **observador Kafka
local** degrada primeiro — 3,6 s a 8.000 e 12,5 s a 10.000. Acima de 8.000 o
número que se move é o do consumidor, não o do Atlas.

O cluster ficou em **M20 durante a rampa inteira**, inclusive a 12.000 TPS; ele não
escalou para M30. Isso sustenta apenas uma alegação de demo curta; não
estabelece capacidade sustentada de M20. O processor cobra por segundo e precisa ser
parado depois da execução.

**Medido contra o cluster M20 real (2026-07-27).** Uma execução de 25 minutos a 200
TPS, gerador escrevendo continuamente, um connector de Change Stream e o processor de ASP
ativos. A fase A rodou o código corrigido; a fase B reverteu apenas o índice de `run_id`
e voltou o poll para 2 s, reproduzindo a regressão no lugar.
Números por minuto vindos da Atlas Admin API, nó primário:

| Métrica | A — indexado, poll de 5 s | B — COLLSCAN, poll de 2 s |
|---|---|---|
| `QUERY_EXECUTOR_SCANNED_OBJECTS` (docs/s) | 751 | 50.176 (**67×**) |
| `QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED` | 1,68 | 89,23 (**53×**) |
| `PROCESS_CPU_USER` (média) | 8,24% | 12,55% |

O `QUERY_EXECUTOR_SCANNED` se move na direção *contrária* (8.785 → 3.035) porque conta
chaves de índice, e uma varredura de coleção não lê nenhuma — é o par de métricas junto que
identifica a mudança de plano.

O cluster ficou em M20 o tempo todo, com pico de 16,4% de CPU e 2,27 GB de 4 GB.
**Não leia esses 16,4% como folga** — veja "O auto-scaling dispara por CPU RELATIVA"
abaixo, que corrige isso. O limiar é relativo à linha de base de uma
instância burstable, e ~17% absoluto é ~88% relativo. O cluster escalou para
M30 mais tarde no mesmo dia.

Mais dois limites desta evidência: 25 minutos é curto demais para disparar o
auto-scaling de compute do Atlas, que faz média sobre cerca de uma hora, então "não escalou"
prova pouco sozinho; e a fase B é uma reprodução
*enfraquecida* — ela reverteu o índice e o intervalo de poll, mas manteve o
TTL em 600 s e 200 TPS, então seu conjunto vivo era de ~131 mil documentos contra os
~900 mil que a configuração original de 1800 s / 500 TPS produzia.

A latência do lado do cliente mal separou as fases (p50 579 ms vs 613 ms) porque
é dominada pela ida e volta notebook → Atlas; só as métricas do lado do servidor
resolvem a diferença.

**Execução ponta a ponta pela UI (navegador, mesmo dia).** Gerador iniciado pela página a
200 TPS e parado pela página: origem 14.420 = Change Streams 14.420 =
Kafka 14.420 = ASP 14.420, zero duplicatas, DLQ 0, `final: reconciliado`.
Poll de reconciliação medido no navegador: intervalo médio de 5,0 s. A única
mensagem no console é o 404 pré-existente de `favicon.ico`.

Um comportamento para conhecer antes de demonstrar: a janela do ASP usa
`boundary: "eventTime"`, então a janela final só fecha quando um evento mais novo
avança a marca d'água. O caminho de parada agora espera a janela de 5 s + os 2 s de
atraso e escreve um marcador técnico sob o run id reservado
`__demo_watermark__`. O marcador fica fora da contabilidade da execução
demonstrada, fecha a última janela dela e permite que a reconciliação seja final
sem exigir uma segunda rajada de negócio.

A normalização só roda depois que o cluster chega a `IDLE` — o Atlas rejeita mudanças de
spec em cluster pausado ou em transição. Um PATCH rejeitado avisa e
continua, em vez de bloquear a demo.

Duas restrições do Atlas foram descobertas na marra, ambas HTTP 400, e ambas estão agora
codificadas no script:

- O `minInstanceSize` precisa ser **estritamente** menor que o `maxInstanceSize` quando
  o auto-scaling de compute está habilitado. "Fixar o cluster em M20 com auto-scaling ligado"
  é, portanto, inexprimível: a faixa colapsaria em um único tier.
- Cada tier tem um tamanho máximo de disco. Este cluster tem 150 GB, e o M10 vai até
  128 GB, então um piso M10 é rejeitado. O `ATLAS_MIN_TIER` fica vazio por padrão
  (não mexa no piso) por essa razão.

### O auto-scaling dispara por CPU RELATIVA — e o modo replay

As correções acima não bastaram. O cluster escalou para M30 de novo no mesmo dia,
às 15:36Z, **com o gerador parado** desde 14:45Z. O payload do evento do Atlas
é inequívoco:

```
computeAutoScalingTriggers: "CPU_ABOVE"
threshold: NORMALIZED_AUTO_SCALE_SYSTEM_CPU > 0.75  (mode: AVERAGE)
absoluteCpuMetric: 0.176   cpuThresholdType: "RELATIVE"   relativeCpuMetric: 0.881
```

M20/M30 são instâncias burstable. O limiar se aplica à CPU **relativa** à
cota de base da instância, não à CPU absoluta. 17,6% absoluto registrou
como 88% relativo. Um evento de escala anterior (2026-07-26, antes das correções) mostra
27,0% absoluto com 100% relativo — então as correções cortaram a CPU em cerca de um terço, mas
não abaixo da linha.

Isso corrige uma conclusão anterior registrada aqui: "16,4% de CPU está bem abaixo do
limiar de 75%" comparava a métrica certa com a régua errada.

A consequência que importa operacionalmente: com o gerador parado, o que
sustentava aquela CPU era **o próprio dashboard** — três cursores de change stream
mais polling (status do gerador a 1 s, oplog/sonda de leitura/DLQ a 4 s, reconciliação e
ASP a 5 s, Kafka a 4 s). O `/streaming/oplog` sozinho faz uma ordenação `$natural` sobre
`local.oplog.rs` a cada 4 s. Deixar a página aberta custa cluster.

**Decisão histórica, substituída em 2026-08-05:** o replay foi temporariamente o
único modo porque um dashboard sempre observando estressava o cluster mesmo depois de o
gerador parar. O modo ao vivo atual trata essa causa raiz abrindo observadores só
para uma sessão explícita e fechando-os depois da reconciliação; o replay continua sendo a
contingência sem escrita.

A palavra "replay" saiu da UI, mas o aviso não. O
selo é permanente e incondicional, a coluna de Change Streams diz
"reproduzindo" em vez de "ao vivo", e os botões que atuam no ambiente ficam
desabilitados. Renomear o controle é cosmético; remover o selo não seria.

A restrição de honestidade faz parte do design, não é decoração. A alegação inteira da PoV
é "evidência em vez de alegações", então um modo que *pareça* ao vivo enquanto nada
acontece a inverteria — e o que viraria falso é exatamente o que o módulo
vende (change streams, fan-out Kafka, janelas de ASP). Portanto: o gravador
não sintetiza nada, todo payload reproduzido carrega `replay: true`,
o `/replay/manifest` declara a origem e o `run_id` gravado, a página mostra um
selo permanente, ações que tocam o ambiente real ficam desabilitadas, e um
teste garante que o `replay.py` nunca importa nem chama o Mongo. Um replay nunca pode ser
apresentado como execução ao vivo.

**Ordem da captura:** os consumidores de Change Stream e Kafka sobem de forma preguiçosa na
primeira assinatura SSE. O observador agora usa `auto.offset.reset=earliest`; o
source connector mantém `startup.mode=latest` quando não existe offset de connector.
Abra a página de captura ao vivo primeiro e espere a coluna do Kafka reportar
`consumindo` antes de iniciar o gerador. O `scripts/capture_replay.py` codifica
essa ordem para que a subida do consumidor não seja medida como backlog da aplicação.

**Período histórico de só-replay:** o `scripts/ambiente.sh` e o `bin/overview`
temporariamente não provisionavam ASP nem Kafka a menos que `STREAMING_AO_VIVO=1` /
`overview --ao-vivo` fosse usado. Essa decisão foi substituída: o `overview` agora
usa por padrão a plataforma ao vivo e o `overview --replay` é o caminho explícito sem escrita.
O caminho `down` ainda para os dois incondicionalmente, já que um processor deixado rodando
cobra por segundo.

**Dois bugs achados ao montar isso, ambos dignos de nota:**

- O SSE de replay foi lançado sem keepalive (o ao vivo sempre teve um).
  Um fluxo ocioso — replay pausado, ou simplesmente entre eventos — é derrubado pelo
  navegador e pelo proxy do Vite; o `useSse` reconecta a cada 2 s, e o gerador abandonado
  do lado do servidor nunca percebe, porque um gerador que nunca escreve
  nunca vê a desconexão. Esses fluxos vazados esgotam o orçamento de ~6
  conexões por host do navegador, e fetches comuns ficam na fila até o timeout de 30 s
  enquanto o backend responde em ~2 ms. Corrigido com keepalive de 10 s; um
  teste cobre isso.
- O `porta_ativa()` em `bin/overview` e `scripts/kafka-local.sh` usava
  `lsof -ti:PORT` sem filtro de estado, então conexões ESTABLISHED (o proxy do Vite,
  o navegador) contavam como "serviço no ar". Com o backend morto, o `up`
  concluía "já estava de pé" e imprimia **✅ Pronto sem backend**; o `down`
  usava a mesma lista para escolher PIDs a matar, então podia matar um cliente em vez
  do servidor. Os dois agora filtram `-sTCP:LISTEN`.

**O polling ocioso acabou.** O `frontend/src/hooks/usePolling.js` adiciona
`useVisivel()` e `useIntervaloVisivel()`; todo intervalo da página de streaming
e o poll de cluster da casca passam por ele, e o `useSse` fecha seu
EventSource quando a aba está oculta. Duas regras: nada faz polling com a aba
oculta, e nada faz polling de dado que não pode mudar — os snapshots gravados
só se movem enquanto o relógio de playback roda.

Medido no navegador, com o replay parado: **48 requisições por 20 s → 1**. Aba
oculta: **0 requisições em 15 s**, retomando imediatamente ao voltar. Enquanto toca, ele
volta à cadência normal, que é o ponto.

Vale dizer com clareza, porque uma nota anterior aqui sugeria o contrário: isto
*não* foi o que parou o auto-scaling. A carga contra o Atlas sumiu quando o
módulo 07 virou replay (seus polls agora leem um arquivo, ~2 ms, sem Mongo) e quando
o ASP e o Kafka deixaram de ser provisionados, o que removeu três cursores de change
stream. A única chamada remota recorrente que sobrou é `/streaming/cluster`, e essa
é a Admin API — plano de controle, não CPU do cluster. Matar o polling ocioso compra
CPU de notebook e de backend, e remove as conexões seguradas que causavam os
timeouts de 30 s no fetch; não muda o tier. Pausar polls quando
`document.visibilityState !== 'visible'` e quando o gerador está parado, mais
revisitar a sonda `/streaming/oplog`, é o trabalho que resta para o modo ao vivo.

### Ciclo de vida do ambiente: um comando, fronteiras limpas

Arquivos principais: `bin/overview`, `scripts/ambiente.sh`,
`scripts/cleanup-streaming-data.py`, `scripts/kafka-local.sh`,
`scripts/setup-kafka-connector.sh`, `scripts/setup-asp.js`.

O `./bin/overview` agora trata a demo inteira como um ciclo de vida:

1. Rode o `scripts/prepare-demo.sh` com antecedência para materializar o dataset de Geo
   dedicado, os índices do MongoDB e um índice do Atlas Search consultável.
2. Na subida, faça apenas uma checagem rápida e somente leitura de prontidão; nunca altere estado
   do cluster, tier ou auto-scaling.
3. Pare qualquer processor deixado rodando e remova resíduo de PIX com escopo.
4. No modo ao vivo padrão, recrie a definição/checkpoint do ASP e suba
   Kafka/Connect com um connector limpo. O `overview --replay` mantém os dois desligados.
5. Suba backend e frontend somente depois que o ambiente selecionado estiver consistente.

Se a prontidão falhar, o `overview` aborta e direciona o operador ao
`scripts/prepare-demo.sh`; ele nunca começa uma reconstrução lenta durante a demo.

O `./bin/overview down` faz um desligamento em duas camadas:

1. A API para o gerador e o ASP, espera o `STOPPED`, remove a origem,
   as janelas, a DLQ, a auditoria e os checkpoints da aplicação.
2. O script de ambiente repete uma limpeza direta e com escopo, remove o
   connector/tópico/consumer group da demo e para o Kafka. O Atlas não é alterado.

A segunda camada cobre uma API indisponível ou interrompida. A limpeza é limitada
às coleções de demo conhecidas do `pix` e aos recursos Kafka; ela nunca pode virar um
delete no banco inteiro. O TTL de 30 minutos nos timestamps das transações é apenas uma
rede de segurança para uma execução abandonada.

### Frontend: narrativa de prova primeiro

Arquivos principais: `frontend/src/pages/Streaming.jsx`,
`Aggregations.jsx`, `Reindexacao.jsx`, `SchemaValidation.jsx`,
`frontend/src/hooks/useApi.js`, `frontend/src/App.jsx`,
`frontend/src/index.css`.

| Área | Decisão atual e razão |
|---|---|
| Streaming | O cenário, o ambiente observado, o gerador compacto e o começo das três colunas de capacidade cabem no primeiro viewport 1440×900. A tabela de comparação detalhada fica recolhida. Isso mantém a prova de confiabilidade acima do material de referência de apoio. |
| Agregações | As abas descrevem resultados, com o operador em segundo plano. Um fluxo permanente `Origem → Pipeline → Resultado` e um estado honesto pré-execução tornam a narrativa de negócio visível antes de uma consulta rodar. |
| Reindexação | Os cards mostram um comando de uma linha e recolhem a versão comentada. O painel de explain destaca `COLLSCAN → IXSCAN`; a lista de índices mostra por padrão os índices relevantes à demo. Isso enfatiza mudança de plano medida em vez de volume de código. |
| Validação de schema | A sequência guiada de escrita/rejeição continua sendo o principal; o JSON Schema completo fica recolhido em details. |
| Erros de API | Aborts esperados de requisição causados pelo unmount de um módulo são ignorados, e erros globais idênticos são deduplicados por oito segundos. A navegação não produz mais toasts falsos de falha; timeouts genuínos e falhas de API seguem visíveis. |
| Outros módulos | Hot/Cold, Change Streams e Transações mantiveram a estrutura porque suas narrativas de uma tela já eram fortes. |

A direção visual segue o sistema escuro MongoDB existente: Outfit,
JetBrains Mono, `#001E2B` e `#00ED64`. A revisão intencionalmente melhorou
hierarquia e revelação progressiva em vez de introduzir uma segunda linguagem de
design.

## Verdade operacional e limitações

- A reconciliação fica verde só depois que todos os caminhos prestam contas da execução finita.
  Parar a entrada não fecha por si só uma janela de tempo de evento; a marca d'água
  precisa avançar. "Pendente" é backlog observável ou estado de janela aberta, não
  evidência de perda.
- Os contadores únicos de Change Stream e Kafka pertencem ao processo de backend atual.
  As contagens de origem, ASP e DLQ são lidas do Atlas.
- O resume só funciona enquanto o token salvo permanecer dentro da janela do oplog.
- O Kafka local é de nó único e intencionalmente não tem TLS/SASL, ACLs nem Schema
  Registry. Esses são temas de produção, não alegações escondidas.
- Um connector por coleção é o padrão. O fan-out multi-connector filtrado é
  educativo e pode somar pressão no oplog.
- O processor de ASP precisa ser parado depois da PoV porque ele cobra mesmo ocioso.
  O armazenamento do Atlas pode seguir sendo cobrado enquanto o cluster está pausado.
- Nunca aponte endpoints destrutivos de demo ou scripts de limpeza para um banco que não
  seja descartável.

## Linha de base de validação

### Endurecimento da apresentação de streaming (2026-08-07)

- O estado amarelo ambíguo `Verificar` foi rastreado até o
  `cleanup-streaming-data.py`: ele recriava os índices único e de TTL, mas não o
  `run_id_reconciliacao`. A limpeza agora materializa os três contratos, então
  o estado normal é o verde `Pronto`; uma falha real aparece como `Pré-voo
  pendente` em vez de pedir à plateia para "verificar" alguma coisa.
- O `/streaming/reset` não cria mais um `MongoClient` SRV novo por coleção.
  Ele reaproveita a topologia da aplicação já conectada e limpa coleções
  independentes em paralelo. Uma queda de DNS, portanto, não consegue mais deixar o Play em
  preparação por 20 segundos enquanto a conexão saudável existente é ignorada.
- O Kafka Connect só é reiniciado depois de um drop de coleção, a operação que de fato
  invalida o cursor de origem dele. Uma execução limpa de rotina não perturba mais
  um connector saudável.
- O limiar de drop controlado agora é 25 mil documentos (configurável com
  `STREAMING_DROP_ACIMA_DE`). Apagar a execução de aceitação de 59.896 documentos levava
  9,92 s mesmo depois da correção de DNS; acima do limiar, o reset para o ASP, dropa a
  origem dedicada, recria seus índices e retoma ASP/Kafka.
  Medição ao vivo depois da mudança: **6,43 s** para preparar depois de uma execução de 59 mil
  documentos, contra **9,92 s** com `delete_many`; uma execução partindo do zero segue em
  **1,33 s**.
- O painel de decisão de arquitetura, voltado apenas ao apresentador, foi removido da UI do
  cliente. O roteiro de react/distribute/transform e os trade-offs agora vivem no
  editável `docs/roteiro-apresentacao-streaming.md`; regenere o PDF com
  `scripts/generate-streaming-guide.py`.
- Aceitação ao vivo depois da correção: preparação **1,33 s**, execução de 30 segundos,
  **59.896** documentos de origem reconciliados entre Atlas, Change Streams, Kafka
  e ASP + DLQ, zero perdidos, erros de HTTP/console zero, estado final em **41,09 s**.

Validado depois da implementação atual:

```bash
backend/venv/bin/python -m pytest -q backend/tests  # 165 passed
npm --prefix frontend run build                    # build do Vite passou
git diff --check                                   # passou
```

A validação no navegador usou um viewport de 1440×900. O Streaming expôs as três
colunas de capacidade acima da dobra, as telas revisadas de agregações/reindexação
renderizaram corretamente, o código de schema veio recolhido por padrão, e a navegação
rápida entre módulos produziu zero toasts de erro de API e zero erros de console.

## 2026-08-07 — módulos 07 e 08 unidos; injeção de falha; passada de densidade

Movido por uma leitura crítica da PoV do assento de um arquiteto de squad de PIX em um
banco que já roda Kafka e Elastic. Quatro objeções, quatro mudanças.

1. **"Fan-out sem ETL é argumento para Kafka, não para MongoDB."** O ganho real e
   indiscutível é remover a dual-write/outbox da aplicação. A reconciliação prova
   isso; o texto agora abre por aí.
2. **"É tudo caminho feliz."** O `POST /streaming/falha/connector` para o
   connector no meio do fluxo e o retoma a partir do offset armazenado; o
   `POST /streaming/falha/evento-invalido` escreve um documento com `valor` em string
   que o ASP desvia para a DLQ enquanto o processor segue rodando.
   Medido com as duas injetadas: origem 742, Change Streams 742, Kafka 742,
   ASP 742, duplicatas 0, DLQ 1, final `reconciliado`.
3. **"O módulo 08 não é o meu problema, e é retrospectivo."** O fluxo agora
   carrega dois canais (`PIX` sem coordenada, `CARTAO_PRESENCIAL` com a do
   terminal), e um segundo processor, `geoSinais30s`, calcula o sinal de risco
   dentro de uma janela hopping de 30 s em `geo.sinais_ao_vivo`. O módulo 08 abre com
   esse painel; os painéis sob demanda ficam, rotulados como investigação
   retrospectiva.
4. **"Suas únicas descobertas são as que você plantou."** Os sinais carregam
   `origem: plantado | emergente` e a página os conta separadamente. Uma execução a 1.000 TPS
   produziu 5 plantados e 4 emergentes.

Duas armadilhas achadas na construção, ambas dignas de lembrança:

- Retrodatar o `ts` para modelar o atraso de captura do adquirente colocava o campo do TTL no
  passado, então a metade mais antiga de um par expirava antes de a reconciliação rodar e a
  origem contava 610 contra 652 nos três consumidores. A chegada é `ts`;
  o instante da compra é `compradaEm`. Nunca os confunda.
- Um limiar de km/h sozinho é uma fábrica de falsos positivos: duas compras a 20 km
  capturadas com segundos de diferença dão 1.343 km/h. O sinal precisa também de uma distância
  mínima (200 km) e de um intervalo mínimo (1 min).

Também nesta passada: o connector Kafka não resolve mais DNS no start da task
(o `scripts/lib/expand_srv.py` reescreve a URI SRV para a forma padrão no setup
— a falha `Failed looking up TXT record` que matava a task sob carga era
um resolver instável, não vazão), o preset de 12.000 TPS é rotulado
`Volume em lote` com o gargalo declarado, a semântica de entrega
(at-least-once + `endToEndId` único + ordenação por partição) está na tela
em vez de em nota de rodapé, e a prosa narrativa do módulo 07 foi para `<details>`
para que as três colunas e a reconciliação ocupem a dobra.

Ressalva de rede que voltou a morder no meio da sessão: o RTT até o cluster mediu
**243,6 ms** e a vazão colapsou para ~68 TPS, com `write_ack` p50 em 324 ms.
Isso é a rota WARP/saída nos EUA, não o código. Cheque o `/streaming/rede` antes de
confiar em qualquer número de latência.

### Aceito com o WARP desligado

Tudo foi reexecutado ponta a ponta assim que a rota de VPN saiu do caminho, com RTT em
7,4 ms: 2.000 TPS individual por 30 s, as duas falhas injetadas na metade,
**47.377** documentos com a mesma contagem nos quatro caminhos, zero duplicatas, um
documento na DLQ, `write_ack` p50 19,8 ms / p95 40,5 ms / p99 48,8 ms. O módulo
08 na mesma execução: 40 pares retrospectivamente, 8 resultados de busca com facetas, e
a comparação de explain em 3 chaves / 3 ms contra 49.493 chaves / 234 ms.

Dois defeitos apareceram só em escala de demo e vale lembrar:

- Com 600 portadores a 2.000 TPS, cada cartão compra dezenas de vezes por janela,
  então os extremos do par plantado eram sufocados pelo tráfego comum: a execução
  reportou **0 plantados / 30 emergentes** e sinalizou 0,4% de todas as compras, uma taxa
  que nenhum detector real produz. O tráfego comum agora usa 12.000 portadores e os
  pares plantados uma faixa exclusiva de 400, com viagem legítima em 0,06%.
- Quatro processos `uvicorn` órfãos de restarts manuais. Um deles segurava a
  participação no grupo `showcase-pix-observer` e comia as mensagens, então a coluna 2
  lia zero enquanto os offsets avançavam normalmente. Não é bug de produto, mas o sintoma
  no palco é idêntico: o `pgrep -f "uvicorn main:app"` precisa retornar exatamente um
  PID.

### Documentação reestruturada

O README havia crescido para 771 linhas e misturava o pitch com o setup de
Kafka/ASP/Search. Agora tem 183 linhas — o que é, os oito módulos, início rápido, os dois
módulos que sustentam a demo, segurança — com o resto separado em
`docs/setup-streaming.md`, `docs/setup-geo.md` e `docs/reference.md`.

Regra de screenshot, porque tamanhos divergentes estavam distorcendo as tabelas do GitHub: toda
imagem usada dentro de uma tabela tem exatamente **1440×900**. Recortes de detalhe usados
avulsos podem diferir. O `07e-reconciliacao.png` (1010×300) e o `08b-geo-aovivo.png`
(800×530) são as duas exceções atuais e devem ser recapturados em 1440×900 na
próxima vez que o ambiente estiver no ar — o cluster estava pausado quando a reestruturação
aconteceu.

## Ordem rápida de leitura

1. Leia este arquivo.
2. Leia apenas a seção relevante do `ARCHITECTURE.md`.
3. Para trabalho de streaming, inspecione `backend/routers/streaming.py`, seus testes
   correspondentes e a coluna relevante do frontend, juntos.

## O módulo 06 passou a ser medido, e a demo passou a abrir por uma tese (2026-08-21)

Auditoria da PoV inteira para uma apresentação a time de dados de banco. O
diagnóstico não foi de acabamento: **os módulos 07 e 08 argumentam, os módulos
01 a 06 demonstravam.** Um grep confirmava — só `Geo.jsx` e `Streaming.jsx`
tinham limite declarado. E limite declarado é o que mais separa esta PoV de uma
demo de vendedor.

A frase que o módulo 06 exibia era *"o MongoDB reverte automaticamente —
nenhuma coleção fica com dados parciais"*. Um DBA de Postgres lê isso e pensa
"isso é uma transação, meu banco faz desde 1995". Demo de paridade de feature
convida à resposta **"já temos isso"** e deixa a objeção para a reunião
seguinte, onde ninguém do nosso lado está presente.

Quatro mudanças:

1. **`components/Limites.jsx`**, aplicado aos oito módulos. Cada item é
   verificável na documentação do produto ou medido aqui. Os desconfortáveis são
   os que valem: sem chave estrangeira; validador não valida o que já está
   gravado; a janela do oplog é o SLA real do Change Stream; `$lookup` não tem
   otimizador de join; transação não é o caminho barato.

2. **`POST /transactions/benchmark`** — o módulo 06 medido como o 07 foi.
   Transação multi-documento × a mesma intenção num documento só × contenção na
   mesma chave, com `majority` nas duas pontas. O número desconfortável é
   proposital: no MongoDB a escrita de um documento já é atômica, e precisar de
   transação multi-documento em toda escrita costuma ser modelo relacional
   transplantado. Esconder isso não sobrevive à primeira pergunta da sala.

   A armadilha que quase virou vergonha no palco: a primeira medição deu p50 de
   1.057 ms e teria dito a um banco que transação no MongoDB leva um segundo. O
   RTT puro até o cluster era 262 ms — a máquina estava numa rota ruim, o mesmo
   problema de WARP/VPN que o módulo 07 já documentava. O endpoint agora mede o
   RTT com `ping` puro **antes** de tudo, reporta `limitado_pela_rede` acima de
   40 ms e diz na tela que **o número a levar é a razão (4,1×) e a contagem de
   viagens (~4), não o absoluto** — esses não mudam com o enlace.

   Duas outras armadilhas viraram teste: `with_transaction` não expõe os
   retries, então eles são contados pelas entradas no callback; e o
   `getParameter` do `transactionLifetimeLimitSeconds` é negado ao usuário da
   aplicação em vários tiers, então a resposta diz `indisponível` em vez de citar
   o padrão de cabeça.

3. **`pages/Tese.jsx` (`/#tese`) virou a página de entrada.** Declara o
   argumento (convergência, não capacidade — nenhuma das oito é exclusiva) e os
   não-objetivos: não é benchmark competitivo, não estima economia, não
   substitui o warehouse, não é desenho de produção. Sem número de economia de
   propósito: custo estimado é a primeira coisa desmontada na sala.

4. **Módulos 03 e 04 reposicionados de "veja a feature" para "o que sai do
   desenho".** Cada agregação diz qual componente ela dispensa; o 04 diz o que
   de fato muda — validar na aplicação vale enquanto a aplicação for a única a
   escrever, e em base com alguns anos ela nunca é (job de carga, ETL, script de
   correção pelo shell, serviço legado, time vizinho).

Lacunas conhecidas e não fechadas: criptografia (a PoV de Queryable Encryption é
um repositório à parte e é provavelmente o diferencial mais forte para banco),
backup/PITR e RPO/RTO além de um failover, multi-região e residência de dado
para BACEN/LGPD. Nenhum desses tem módulo aqui.

## A tela emagreceu porque o apresentador narra (2026-08-21, mesma sessão)

Correção de rota sobre a mudança anterior. A PoV é apresentada com narração ao
vivo, e a tela estava repetindo o que sai da boca de quem apresenta. Princípio
aplicado às oito abas: **fica visível o que a narração não carrega** — a query,
o comando e o resultado do cluster. O que explica conceito saiu.

- Módulo 03: `what` + `why` + `substitui` viraram **uma linha** por agregação, e
  o pipeline passou a ficar **sempre visível** em vez de atrás de "Ver código" —
  mostrar quão pouco se escreve é o argumento. 675 → 152 palavras.
- Módulo 06: banners e notas encurtados. 73 → 27.
- Página de tese: 255 → 72 palavras.
- Módulos 04 e 08: os blocos de contexto viraram uma frase cada.
- Títulos dos blocos de limite perderam o sufixo "— dito antes da pergunta".

Os blocos de limite **continuam**, sempre em `<details>` fechado: custo zero de
tela e munição quando a pergunta vier. O módulo 07 ficou praticamente intacto
(~425 palavras) porque ali o texto já é uma linha por coluna — é comparação de
três caminhos lado a lado, densidade legítima, não prosa.

## Desligamento automático do ambiente (2026-08-21)

O `overview up` passou a agendar um `overview down` para **45 minutos** depois
(`OVERVIEW_AUTO_DOWN_MIN`; 0 desliga). O risco coberto não é técnico: é a demo
que termina, todo mundo fecha o notebook e o processor de ASP segue cobrando por
segundo. 45 min é a duração típica da apresentação — `overview adiar 30`
reagenda, `overview manter` cancela.

Duas decisões que o timer exigiu:

- **O `down` cancela o agendamento antes de qualquer outra coisa.** Sem isso, um
  timer órfão de uma sessão anterior derrubaria uma sessão nova.
- **`disown` no processo agendado**, senão cancelar imprime `Terminated: 15` no
  terminal do apresentador, que no meio da demo parece erro.

Achado no caminho: o symlink `/opt/homebrew/bin/overview` apontava para
`/Users/adriano.fratelli/Documents/PoVs/mdboverview/bin/overview`, pasta que não
existe mais — `overview` dava "command not found". Repontado para esta PoV.

### O `down` deixou de matar por porta às cegas

O `derrubar()` encerrava quem estivesse escutando em 8002/5174 sem checar de
quem era o processo. Enquanto o `down` era manual isso passava; com o
agendamento automático ele passa a disparar 45 min depois, sem ninguém olhando —
e a reserva no `PORTS.md` é convenção, não garantia.

Agora `dono_do_workspace()` confirma pelo comando ou pelo cwd que o PID pertence
a esta pasta, e preserva qualquer outro dono com aviso na tela. É a mesma regra
que o `reap-povs.sh` do workspace já aplicava (`SKIP` para quem não é nosso).

Salvaguarda que o teste revelou: se `BASE` degenerar para `/` ou para um caminho
curto, `grep -F "$BASE"` casaria com todo processo da máquina e a checagem
viraria decoração. Abaixo de 9 caracteres a função não reivindica nada.
