# Vitrine de capacidades do MongoDB Atlas

Oito capacidades do Atlas, uma página para cada, rodando contra um cluster real. Construa um índice e veja as leituras continuarem fluindo. Quebre um change stream e veja-o retomar pelo token. Faça rollback de uma transação e veja os documentos voltarem.

Nada é mockado. Se alguma peça não está configurada, a UI diz isso em vez de inventar um número.

FastAPI + React 18. UI em pt-BR. Sem LLM.

## Os oito módulos

A aplicação abre numa página de **tese**, não no módulo 01: nenhuma das oito capacidades é
exclusiva do MongoDB, e o argumento não é a capacidade — é a convergência de todas elas
sobre o mesmo dado, no mesmo cluster, na mesma linguagem. A página também diz o que a demo
*não* prova (não é benchmark competitivo, não estima economia, não substitui o warehouse).

| Módulo | O que o Atlas faz |
|---|---|
| **01** Reindexação online | Construção de índice em rolling, sem downtime. O `live_monitor.py` imprime a latência de leitura/escrita enquanto acontece. |
| **02** Tiering quente/frio | O Online Archive move documentos antigos para armazenamento barato, ainda consultáveis em um único namespace. |
| **03** Aggregation Pipeline | `$lookup`, `$facet`, `$unionWith`, `$setWindowFields`, `$bucketAuto`. |
| **04** Validação de schema | JSON Schema aplicado pelo banco, não pela aplicação. |
| **05** Change Streams | Feed ordenado de inserts/updates/deletes com pre/post-images. |
| **06** Transações ACID | Transações multi-documento percorridas passo a passo, rollback incluído — e medidas: latência com `majority`, o custo sobre a mesma escrita num documento só, e o comportamento sob contenção. |
| **07** Streaming | Change Streams, Kafka Connector e Atlas Stream Processing sobre as mesmas escritas. |
| **08** Risco geográfico | Viagem impossível em tempo de evento + `$search` com texto, filtro geográfico e facetas. |

Cada módulo declara onde a capacidade dele **não** vai, antes de alguém perguntar.

Todo módulo tem deep link: `/#tese`, `/#agg`, `/#streams`, `/#tx`, …

**Módulo 01 — o índice é construído enquanto as leituras seguem fluindo:**

![Módulo de reindexação online durante uma construção de índice em rolling](docs/screenshots/01-reindex.png)

## Início rápido

```bash
cd backend && python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # MONGO_URI, opcionalmente as chaves da Atlas API
python seed_data.py           # 100 mil produtos + 20 mil avaliações
cd .. && ./start.sh            # API :8002 + UI :5174
```

O launcher usa backend sem reload e build otimizado do frontend por padrão. Para desenvolver com reload/HMR, rode `POV_DEV=1 ./start.sh`; o build só é refeito quando fontes, lockfile ou configuração mudam.

Rode `curl http://localhost:8002/preflight` antes de apresentar — ele checa URI, cluster, coleções, chaves do Atlas e a guarda de mutação.

Uma vez configurado, o `bin/overview` substitui tudo isso:

```bash
./scripts/prepare-demo.sh   # com antecedência: dataset Geo + índice de Search
./bin/overview              # preflight, backend, frontend, Kafka, ASP
./bin/overview --replay     # fallback gravado, nada escrito no cluster
./bin/overview down         # para tudo; o cluster fica intocado
```

**Rode `overview down` ao terminar** — um stream processor é cobrado por segundo. Como rede de segurança, o `up` já
agenda esse `down` para 45 minutos depois; `overview manter` cancela e `overview adiar 30` reagenda.

Detalhes: [setup de Streaming](docs/setup-streaming.md) · [setup de Geo](docs/setup-geo.md) · [referência](docs/reference.md).

## Módulo 07 — três consumidores, uma escrita

Change Streams na aplicação, o Kafka Connector publicando em um broker real e o Atlas Stream Processing agregando janelas de 5s — tudo sobre as mesmas escritas. O fluxo tem dois canais: `PIX` (sem coordenada, porque uma transferência PIX não tem) e `CARTAO_PRESENCIAL` (coordenada do terminal do adquirente, que é o que torna o módulo 08 defensável).

![Módulo de streaming: três consumidores contando a mesma execução ao vivo](docs/screenshots/07-streaming.png)

**Quebre de propósito.** Quatro botões ficam ao lado do gerador: *derrubar o connector* (para no meio do fluxo e retoma pelo offset armazenado), *injetar um evento inválido* (um `valor` em string, desviado para a DLQ pelo `$validate` do processor), *publicar uma versão de schema incompatível* (o campo obrigatório `valor` renomeado para `amount` — a mudança que um Schema Registry recusaria) e *forçar um failover do primário* (o test failover do Atlas, no cluster, sob carga).

Depois veja a reconciliação fechar mesmo assim — e ela confere três coisas, não uma: **contagem** (nada faltando), **valor** somado em centavos inteiros (nada transformado no caminho) e um **digest** XOR do conjunto de `endToEndId` (os caminhos viram os mesmos documentos, não apenas a mesma quantidade). Medido através de uma eleição real: 332.568 documentos, R$ 104.486.759,65 idênticos nos três caminhos, 0 escritas rejeitadas após retry do driver, 0 duplicatas.

![Reconciliação fechando depois de uma queda do connector e de um evento envenenado](docs/screenshots/07e-reconciliacao.png)

A entrega é at-least-once, dito acima dos números e não em nota de rodapé: depois de um resume o mesmo evento pode chegar duas vezes, e é o índice único em `endToEndId` que torna isso seguro. A ordenação vale dentro de uma partição, não entre partições.

**Quanto custou?** Um número de vazão sozinho convida à leitura errada — *então é só isso que o Atlas faz?* — porque nunca diz de quem foi o teto atingido. A página lê a CPU do primário pela Atlas Admin API, recortada à janela da própria execução, e a coloca ao lado do TPS que a produziu: **~1.600 TPS sustentados por dois minutos em um M20, com 29–53% de CPU do primário entre execuções** (o painel sempre mostra a execução à sua frente, nunca um número guardado). Duas armadilhas moram aqui, e as duas foram achadas medindo, não supondo. O Atlas publica métricas de processo com um a dois minutos de atraso, então um painel consultado logo depois de uma execução de 30s descreve o cluster *antes* da carga — agora ele diz `metricas_pendentes` em vez de concluir. E uma execução menor que o intervalo de publicação de um minuto é promediada com o restante ocioso daquele minuto: a mesma carga leu 43% quando caiu dentro de um bucket e 15% quando ficou entre dois, então execuções curtas são rotuladas como piso.

O último painel responde à pergunta que vem depois de *funciona?* — **o que sai do desenho**. Ele compara os componentes que cada caminho exige, sem inventar economia: a linha do Kafka continua certa sempre que o evento precisa chegar a sistemas fora do Atlas, e é por isso que ele está na demo, funcionando.

## Módulo 08 — o sinal, enquanto acontece

Um segundo processor lê o mesmo change stream, agrupa o canal de cartão por portador em uma janela hopping de 30s e roda haversine em MQL. A viagem impossível aparece em tempo de evento, não em um scan que alguém lembra de rodar.

![Viagem impossível detectada em tempo de evento, plotada no mapa SVG embutido](docs/screenshots/08-geo.png)

Pares plantados e pares emergentes são contados separadamente — a garantia não pode virar a evidência. O mapa é SVG inline: malha estadual do IBGE embutida no bundle, projeção equiretangular corrigida por `cos(lat)`, sem tiles e sem requisição em runtime — funciona com a rede do local caída. Definir `VITE_GOOGLE_MAPS_KEY` acrescenta um alternador para o Google Maps; o padrão continua sendo a malha local.

O painel retrospectivo responde às duas perguntas que um time de operações faz antes de qualquer outra: **quantos alertas isto coloca na fila** (pares avaliados, sinalizados, taxa, alertas por dia) e **quanto custa a query** — medida nos dois escopos, varredura completa contra um recorte por cliente. A investigação começa na compra contestada, não em um nome de lugar: escolha um caso sinalizado e um `$search` retorna o que existe em volta *daquele terminal*, com casamento fuzzy de nome mantido como refinamento para o caso de estabelecimento clonado.

A saída é um **sinal de risco** para política, nunca uma decisão automática — e explicitamente não é um motor antifraude, que um emissor já tem.

## Mais screenshots

| Tiering quente/frio | Aggregation Pipeline |
|---|---|
| ![Tiering quente/frio: documentos arquivados ainda consultáveis](docs/screenshots/02-hotcold.png) | ![Estágios e resultados do aggregation pipeline](docs/screenshots/03-aggregations.png) |

| Validação de schema | Change Streams |
|---|---|
| ![Validação de schema rejeitando um documento inválido](docs/screenshots/04-schema.png) | ![Feed do change stream com pre/post images](docs/screenshots/05-changestreams.png) |

| Transações ACID | Módulo 07, três colunas |
|---|---|
| ![Custo medido da transação multi-documento contra a mesma escrita num documento só](docs/screenshots/06-transactions.png) | ![Change Streams, Kafka e ASP lado a lado](docs/screenshots/07b-streaming-colunas.png) |

## Segurança

Algumas dessas demos destroem coisas de verdade — elas removem índices, fazem `collMod` em regras de validação, criam e apagam Online Archives. Por isso o raio de impacto permanece local: o lançador escuta em `127.0.0.1`, mutações do navegador só são aceitas de origens configuradas, mutações remotas exigem `DEMO_ADMIN_TOKEN`, corpos são limitados, e erros retornam um id de requisição em vez de um trace.

A aplicação também fecha o cliente de streaming pelo lifespan do FastAPI e devolve cabeçalhos de endurecimento do navegador. O `/api/health` continua público; autorização de mutação não torna o laboratório Kafka embutido pronto para produção.

**Nunca aponte isto para nada além de um cluster de demonstração descartável.**

Em rede compartilhada, defina um `DEMO_ADMIN_TOKEN` longo e aleatório em `backend/.env` e espelhe-o como `VITE_DEMO_API_TOKEN`. A stack Kafka embutida é um laboratório de nó único — sem TLS, SASL, ACLs ou Schema Registry.

## Stack

Python 3.11 · FastAPI · PyMongo · React 18 · Vite · MongoDB Atlas. O módulo 07 opcionalmente precisa de um broker Kafka local e de uma instância de ASP.

```bash
pip install -r backend/requirements-dev.txt
pytest             # 165 testes unitários, Mongo stubado, sem necessidade de cluster
ruff check backend
```

[`ARCHITECTURE.md`](ARCHITECTURE.md) · [`docs/SESSION_HANDOFF.md`](docs/SESSION_HANDOFF.md)

## Licença

MIT
