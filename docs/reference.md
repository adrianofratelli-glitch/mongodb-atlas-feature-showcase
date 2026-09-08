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
│   ├── create_search_index_geo.sh # Índice do Atlas Search para o módulo Geo
│   └── generate-streaming-guide.py # Regenera o PDF do apresentador a partir do Markdown
├── docs/
│   ├── roteiro-apresentacao-streaming.md  # Roteiro editável do apresentador
│   └── roteiro-apresentacao-streaming.pdf # Guia de duas páginas gerado para o apresentador
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
