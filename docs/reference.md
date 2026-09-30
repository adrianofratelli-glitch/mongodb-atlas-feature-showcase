# Referência

Dataset, organização do repositório, o monitor de latência avulso e notas diversas.

Voltar para o [README](../README.md).

## Dataset

As demos rodam contra duas coleções, ambas geradas pelo `backend/seed_data.py`:

| Coleção | Documentos (completo) | Descrição |
|---|---|---|
| `produtos` | ~5.000.000 | Produtos de e-commerce: preço, categoria, estoque, avaliações |
| `avaliacoes` | ~1.000.000 | Avaliações de produtos ligadas por `produto_id` |

O script de seed também cria os índices de que as demos dependem. A execução padrão
(100 mil/20 mil) leva alguns minutos e é suficiente para exercitar todos os módulos.
Use `--full` para reproduzir o dataset em larga escala.

## Estrutura do projeto

O `ARCHITECTURE.md` tem o diagrama completo do caminho da requisição e uma tabela de
responsabilidades por arquivo. A versão curta:

```
.
├── bin/overview                 # Um comando: ambiente cloud + backend + frontend
├── scripts/
│   ├── ambiente.sh              # preflight + ASP/Kafka; cluster intocado
│   ├── prepare-demo.sh          # materializa Geo/índices antes da demo
│   ├── cleanup-streaming-data.py # Remoção com escopo das coleções geradas pelo PIX
│   ├── kafka-local.sh           # Kafka nativo (KRaft) + Connect + plugin do Mongo
│   ├── setup-kafka-connector.sh # Registra o source connector
│   ├── setup-asp.js             # Cria o stream processor de janelas (mongosh)
│   ├── setup-asp-geo.js         # Cria o stream processor de risco geográfico (mongosh)
│   ├── lib/expand_srv.py        # Reescreve a URI SRV para o connector pular o DNS
│   ├── seed_geo.py              # Dataset Geo: 150 mil transações georreferenciadas
│   └── create_search_index_geo.sh # Índice do Atlas Search para o módulo Geo
├── docs/
├── backend/
│   ├── main.py                  # App FastAPI, CORS, health, /preflight, /stats
│   ├── database.py              # MongoClient, timeouts e prontidão
│   ├── security.py              # Guarda de mutação e cabeçalhos defensivos
│   ├── settings.py              # Configuração centralizada de ambiente
│   ├── requirements.txt
│   ├── .env.example             # Template de ambiente (copie para .env)
│   ├── routers/
│   │   ├── reindexacao.py       # Gestão de índices online
│   │   ├── hot_cold.py          # Online Archive (Atlas Admin API)
│   │   ├── aggregations.py      # Demos de aggregation pipeline
│   │   ├── schema_validation.py # Demo de JSON Schema com collMod
│   │   ├── change_streams.py    # Observador de change stream
│   │   ├── transactions.py      # Transações ACID multi-documento
│   │   ├── streaming.py         # Gerador + Change Streams / Kafka / ASP (SSE)
│   │   └── geo.py               # viagem impossível, os 5 operadores geo, explain, geo + $search
│   ├── data/                    # dados do módulo independentes de UF (fraud_seeds.json)
│   └── tests/                   # pytest; não exige cluster ao vivo
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Casca, seletor compacto, roteamento por hash
│   │   ├── index.css            # Tokens de design e estilos base
│   │   ├── hooks/useApi.js      # Wrapper de fetch
│   │   ├── components/          # DemoFlow, QueryBlock
│   │   └── pages/               # Um componente por módulo
│   └── vite.config.js           # Faz proxy de /api para :8002
├── live_monitor.py              # Monitor de latência no terminal
└── docs/screenshots/
```

## Live Monitor (opcional)

O `live_monitor.py` imprime a latência de leitura e escrita contra o cluster, ao vivo, em um
terminal. Rode-o em uma segunda janela enquanto o módulo de reindexação online constrói um
índice: a linha de latência simplesmente continua, que é justamente a alegação sobre construção
de índice em rolling, visível em vez de apenas afirmada.

```bash
python live_monitor.py
```

## Notas

- Change Streams exigem replica set ou cluster shardado (todo cluster Atlas atende, incluindo os tiers gratuito/Flex).
- Transações ACID exigem MongoDB 4.0 ou superior em um replica set.
- O Online Archive (tiering quente/frio) exige um cluster dedicado (M10+).
- O módulo de tiering quente/frio chama a Atlas Admin API, então `ATLAS_PUBLIC_KEY`,
  `ATLAS_PRIVATE_KEY`, `ATLAS_PROJECT_ID` e `ATLAS_CLUSTER` precisam estar definidos.
- Vários endpoints são deliberadamente destrutivos. Aponte isto apenas para um cluster
  de demonstração descartável.
- O `backend/.env` está no gitignore. Nunca commite credenciais reais.
- Testes: `pip install -r backend/requirements-dev.txt && pytest` (127 testes, todos
  unitários — o Mongo é stubado, então não é preciso cluster). Lint com `ruff check backend`.
- O GitHub Actions compila as duas aplicações, roda testes/lint e audita as dependências.


## Revisão de apresentação — setembro de 2026

A tela prioriza operação, consulta e resultado. A aba Geo agora se chama
**Consultas geoespaciais** e mantém a investigação retrospectiva como exemplo.
Os cinco operadores são selecionados individualmente após executar: query aberta,
contorno ilustrativo e amostra real. Inclusão/interseção usam polígono;
proximidade usa distância radial. O dataset continua composto de pontos.
O `$geoNear` mostra uma transação por terminal entre os terminais mais próximos,
mas seu total continua contando transações. A coleção de apoio aparece no resultado.

Na investigação, “não sinalizada” substitui “aprovada”. A amostra não sinalizada
não é uma contagem total. Taxa e alertas/dia contam todos os sinais antes do limite
da tabela; períodos não positivos não entram no cálculo de velocidade da amostra.
O contador do cabeçalho identifica o tamanho estimado da coleção, não documentos
examinados pelo plano de um recorte.

Hot/Cold distingue estimativas por amostragem, prévia por data no cluster e
configuração real de Online Archive. A prévia não acessa o endpoint federado.
As abas de índices, agregação, schema, eventos, transações e streaming mantêm
suas operações, com afirmações limitadas à evidência: leituras amostradas não
comprovam ausência de bloqueios; digest XOR admite colisões; o custo de uma
transação inclui rede e servidor. A tese continua sendo convergência funcional.

Referências usadas para revisar os limites:
- [Geospatial queries](https://www.mongodb.com/docs/manual/geospatial-queries/)
- [Aggregation pipeline limits](https://www.mongodb.com/docs/manual/core/aggregation-pipeline-limits/)
- [Validation level](https://www.mongodb.com/docs/manual/core/schema-validation/specify-validation-level/)
- [Change streams](https://www.mongodb.com/docs/manual/changeStreams/)


### Mapa detalhado dos operadores Geo

O painel 02 usa mapa próprio com zoom para os resultados ou a área completa,
marcadores numerados, agrupamento de coordenadas coincidentes e escala métrica.
O painel 02 usa apenas a camada de ruas online. O painel 01 mantém seu mapa anterior.

A camada **Ruas · online** carrega automaticamente somente os tiles visíveis do OpenStreetMap,
com atribuição e cache normal do navegador. Não há download ou prefetch offline.
Se um tile falhar, aparece um aviso com opção de tentar novamente. O serviço é externo;
esta camada requer internet. `VITE_MAP_TILES_URL` permite substituir o provedor
com URL no formato `{z}/{x}/{y}` (a atribuição deve ser adaptada ao provedor).
Consulte a [política de tiles do OSM](https://operations.osmfoundation.org/policies/tiles/).

### Inclusão × interseção de rotas

Nos cartões `$geoWithin` e `$geoIntersects`, o exemplo padrão é **Rotas · LineString**;
**Terminais · Point** mantém a consulta do dataset original. Centro e raio atualizam automaticamente as consultas de terminais e rotas.
Os percursos sintéticos são deslocados para a cidade selecionada e mantêm distâncias
fixas ao centro: mudar o raio altera a área consultada, não os percursos.

`GET /geo/rotas-comparar` envia três geometrias sintéticas via `$documents` e executa
os dois predicados no Atlas, em um `$facet`, sem persistência e sem índice. Com raio de 50 km, A fica dentro do polígono, B cruza a área com os extremos
fora e C permanece fora. Com 10 km, A também cruza; com 200 km, as três ficam
contidas. Esses resultados foram conferidos no Atlas em São Paulo, Fortaleza,
Manaus e Porto Alegre. A UI usa os
IDs retornados pelo banco para preencher a tabela e destacar as linhas; não decide
inclusão ou interseção no navegador. O filtro fica aberto e o pipeline com a entrada
completa fica disponível para inspeção. Este exemplo prova semântica, não desempenho.

Com GeoJSON e índice `2dsphere`, `$near` e `$nearSphere` mantêm a equivalência esperada.
`$geoNear` demonstra a distância como campo e composição com agregações (neste exemplo,
contagem de transações e amostra por terminal). Não se força diferença de resultados
entre operações equivalentes.
