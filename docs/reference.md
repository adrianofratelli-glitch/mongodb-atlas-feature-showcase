# Referência

Dataset, organização do repositório, o monitor de latência avulso e notas diversas.

Voltar para o [README](../README.md).

## Dataset

As demos rodam contra duas coleções, ambas garantidas pelo `scripts/reset_demo.py`
(que chama o `backend/seed_data.py`):

| Coleção | Documentos (completo) | Descrição |
|---|---|---|
| `produtos` | ~5.000.000 | Produtos de e-commerce: preço, categoria, estoque, avaliações |
| `avaliacoes` | ~1.000.000 | Avaliações de produtos ligadas por `produto_id` |

O seed é idempotente: cada documento tem chave natural determinística (`produto_id`
via uuid5; `_id = "seed-av-<n>"` nas avaliações) e é gravado por upsert, então
reexecutar não duplica nada. Ele também garante os índices de que as demos dependem.
A execução padrão (100 mil/20 mil) é suficiente para exercitar todos os módulos.
Use `--full` para reproduzir o dataset em larga escala.

`produtos`/`avaliacoes` podem ser compartilhadas com outra PoV no mesmo banco (no
cluster de demo, o marketplace de busca usa `POC.produtos` com índice Atlas Search
próprio). Por isso nem o seed nem o reset dropam essas coleções, e o módulo 01 só
remove índices que ele mesmo criou (prefixo `demo01_`).

### Reset

```bash
# banco de teste (recomendado para ensaio e testes que escrevem)
MONGO_DB=POC_test STREAMING_DB=pix_test GEO_DB=geo_test backend/venv/bin/python scripts/reset_demo.py
# banco da demo: exige consentimento explícito e nunca durante uma apresentação
ALLOW_DEMO_DB_WRITE=1 backend/venv/bin/python scripts/reset_demo.py
# só verifica (usado pelo overview)
backend/venv/bin/python scripts/reset_demo.py --check
```

O reset dropa as coleções que só os módulos criam (`schema_demo`, `transacoes_cs_demo`,
`*_demo` das transações, `bench_*`, `tese_probe*`, `_monitor_heartbeat`), remove os
índices `demo01_*`, garante dados e índices B-tree e limpa a rodada de streaming
(`cleanup-streaming-data.py`). Não mexe em Online Archive, tier nem stream processors.

## Estrutura do projeto

O `ARCHITECTURE.md` tem o diagrama completo do caminho da requisição e uma tabela de
responsabilidades por arquivo. A versão curta:

```
.
├── bin/overview                 # Um comando: ambiente cloud + backend + frontend
├── scripts/
│   ├── ambiente.sh              # preflight + ASP/Kafka; cluster intocado
│   ├── prepare-demo.sh          # roda reset_demo.py + --check antes da demo
│   ├── reset_demo.py            # reset único e idempotente (guarda ALLOW_DEMO_DB_WRITE)
│   ├── cleanup-streaming-data.py # Remoção com escopo das coleções geradas pelo PIX
│   ├── kafka-local.sh           # Kafka nativo (KRaft) + Connect + plugin do Mongo
│   ├── setup-kafka-connector.sh # Registra o source connector
│   ├── setup-asp.js             # Cria o stream processor de janelas (mongosh)
│   ├── setup-asp-geo.js         # Cria o stream processor de risco geográfico (mongosh)
│   ├── capture_replay.py        # grava o fallback de replay do módulo 07
│   └── lib/expand_srv.py        # Reescreve a URI SRV para o connector pular o DNS
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
│   │   ├── replay.py            # fallback gravado do módulo 07
│   │   └── tese.py              # /tese/medir: uma operação medida por capacidade
│   ├── data/                    # municipios.json (canal cartão) e replay_streaming.json
│   └── tests/                   # pytest; não exige cluster ao vivo
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Casca, seletor compacto, roteamento por hash
│   │   ├── index.css            # Tokens de design e estilos base
│   │   ├── hooks/useApi.js      # Wrapper de fetch
│   │   ├── components/          # QueryBlock, Limites
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
- Testes: `pip install -r backend/requirements-dev.txt && pytest` (unitários e
  adversariais, com Mongo stubado — não é preciso cluster). Lint com `ruff check backend`.
- O GitHub Actions compila as duas aplicações, roda testes/lint e audita as dependências.


## Revisão de apresentação — setembro de 2026

A tela prioriza operação, consulta e resultado. Hot/Cold distingue estimativas por
amostragem, prévia por data no cluster e configuração real de Online Archive. A prévia
não acessa o endpoint federado. As abas de índices, agregação, schema, eventos,
transações e streaming mantêm suas operações, com afirmações limitadas à evidência:
leituras amostradas não comprovam ausência de bloqueios; digest XOR admite colisões;
o custo de uma transação inclui rede e servidor. A tese continua sendo convergência
funcional.

O módulo de consultas geoespaciais (antigo 08) saiu em 2026-09-11 para o repositório
[`mongodb-atlas-geo-showcase`](https://github.com/adrianofratelli-glitch/mongodb-atlas-geo-showcase),
com o mapa, os cinco operadores e o dataset.

Referências usadas para revisar os limites:
- [Aggregation pipeline limits](https://www.mongodb.com/docs/manual/core/aggregation-pipeline-limits/)
- [Validation level](https://www.mongodb.com/docs/manual/core/schema-validation/specify-validation-level/)
- [Change streams](https://www.mongodb.com/docs/manual/changeStreams/)
