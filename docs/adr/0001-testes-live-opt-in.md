# ADR 0001 — Testes live dos módulos, opt-in e só em bancos `*_test`

- Data: 2026-11-12
- Status: aceito

## Contexto

Os 242 testes do backend usavam Mongo stubado. Nenhum deles provava que os módulos funcionam contra o Atlas, que é a tese da PoV. O primeiro assessment ao vivo (2026-11-12, M20, bancos `POC_test`/`pix_test`/`geo_test`) achou um defeito que nenhum stub pegava. No módulo 07, o contador "Inseridos" ficava abaixo da coleção ao parar o gerador: 87 na tela e 92 no banco, com 5 workers em voo. O `stop` cancelava o `insert_one` assíncrono depois de o servidor já ter confirmado a escrita.

A CI do GitHub não tem credencial do Atlas. O smoke diário na nuvem só confere o health.

## Decisão

- `backend/tests/live/test_live_modulos.py` exercita os módulos 00 a 07 por HTTP, num backend próprio iniciado pelo teste numa porta livre. Os testes têm o marcador `live` e só rodam com `LIVE_TESTS=1`. Os três bancos (`MONGO_DB`, `STREAMING_DB`, `GEO_DB`) precisam ter o formato `showcase_*_test`; com qualquer outro nome, a suíte é pulada inteira, com o motivo. Bancos próprios, e não um `*_test` qualquer, porque os mesmos nomes são usados por outras PoVs no mesmo cluster. O `/streaming/reset` faz `delete_many({})` em `<GEO_DB>.sinais_ao_vivo`, e `geo_test` é o banco live do `mongodb-atlas-geo-showcase`. A revisão de 2026-11-12 pegou esse risco. Na execução daquele dia, `geo_test` não tinha `sinais_ao_vivo` (o `reset_demo.py` listou só `transacoes` e o reset manual removeu 0), então nada foi apagado.
- O backend do teste sobe com `ASP_ENABLED=false` e Kafka apontado para uma porta morta. O teste não liga stream processor (cobrado por segundo) nem disputa o consumer group de uma demo aberta. O 07 é verificado pelo caminho Change Streams: contagem, valor e digest de conteúdo reconciliam, e `inseridos` é igual à fonte.
- Atlas pausado vira skip com a mensagem do preflight, não falha. Sem `reset_demo.py` no `MONGO_DB`, também.
- A CI segue offline. Os testes live rodam localmente, antes de demo ou de release.

## Consequências

- Uma regressão de comportamento real (contagem, rollback, pre-image, `constraint`, reconciliação) aparece num comando. O custo é ≈ 2–4 min com RTT alto.
- Kafka e ASP ao vivo continuam sem teste automatizado: dependem de infraestrutura fora do repositório. O replay gravado segue como contingência declarada.
- O teste escreve só nos bancos `showcase_*_test`. No `MONGO_DB`, dropa e recria `schema_demo`, `transacoes_cs_demo` e as coleções do 06, e cria e remove um índice `demo01_`. No `STREAMING_DB`, limpa `transacoes`, `metricas_janela`, `dlq` e `dlq_audit` (o `/streaming/reset`). No `GEO_DB`, apaga todos os documentos de `sinais_ao_vivo`.
