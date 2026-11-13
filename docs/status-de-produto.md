# Status de produto dos recursos apresentados

Cada afirmação de versão mínima, GA ou Preview feita no README, nos briefings ou na tela, com a fonte
oficial consultada. **Consulta: 2026-11-13.** Cluster da demo nessa data: MongoDB **9.0.7** (Atlas M20).
O que a documentação atual não sustenta fica "a confirmar", com o motivo, e sai da tela.

| Recurso (módulo) | O que o repo afirma | Fonte oficial | Veredito |
|---|---|---|---|
| `validationLevel: "constraint"` (04) | Exige MongoDB 9.0+ | [Release notes 9.0](https://www.mongodb.com/docs/manual/release-notes/9.0/), seção *Document Validation*: "MongoDB 9.0 introduces a new schema validation level of `constraint`". 9.0.0 publicado em 2026-09-28; a série 9.0 é a *Stable Release* atual. Procedimento em [Specify Validation Level](https://www.mongodb.com/docs/manual/core/schema-validation/specify-validation-level/). | **Confirmado.** Introduzido na 9.0, sem rótulo de Preview nas release notes (o único rótulo de maturidade nelas é o GA de outro recurso, Queryable Encryption). Tratado como GA da 9.0. Validado ao vivo no 9.0.7. |
| Build de índice otimizado / *hybrid* (01) | "Desde o MongoDB 4.2" | [Index Builds on Populated Collections](https://www.mongodb.com/docs/manual/core/index-creation/) (manual 9.0 e 7.0) descreve o processo (lock exclusivo só no início e no fim) **sem** citar versão; as notas da 4.2 só existem na documentação legada. | **Comportamento confirmado; "4.2" a confirmar.** A versão saiu da tela e dos briefings; o comportamento vale em todas as versões do manual atual (7.0+). |
| Acumulador `$topN` (03) | "5.2+" | [Página do `$topN`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/) (manual 9.0, 7.0) sem anotação "New in version". | **"5.2" a confirmar** pelo mesmo motivo; versão removida do comentário exibido. Disponível em todas as versões do manual atual. |
| Pre/post images em Change Streams (05) | Usa `changeStreamPreAndPostImages` (sem versão) | [collMod](https://www.mongodb.com/docs/manual/reference/command/collMod/) e [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/): "Starting in MongoDB 6.0". | Confirmado: 6.0+. |
| Transações multi-documento (06) | 4.0+ em replica set | [Production Considerations](https://www.mongodb.com/docs/manual/core/transactions-production-consideration/): FCV mínimo 4.0 (replica set) e 4.2 (sharded). | Confirmado. |
| Online Archive (02) | Exige M10+; não existe em tier compartilhado | [Online Archive Overview and Limitations](https://www.mongodb.com/docs/atlas/online-archive/overview/): "available only on M10 and greater clusters"; Flex não suporta. | Confirmado. |
| Change Streams em Free/Flex (05, `docs/reference.md`) | Todo cluster Atlas atende, inclusive Free/Flex | [Atlas Triggers Limitations](https://www.mongodb.com/docs/atlas/atlas-ui/triggers/limitations/): limite de 5 change streams no Free e 10 no Flex. | Confirmado, com limite de quantidade. |
| MongoDB Search (08, novo) | `$search`/`$searchMeta`, índice criado por `createSearchIndexes` | [`$search`](https://www.mongodb.com/docs/search/query/aggregation-stages/search/): disponível no Atlas (e em Community/Enterprise 8.2+); [Query Reference › Empty Result Set](https://www.mongodb.com/docs/search/query/query-ref/): índice inexistente devolve vazio sem erro; [Index Performance](https://www.mongodb.com/docs/search/performance/index-performance/): consistência eventual. Free: até 3 índices; Flex: até 10 ([Compatibility and Limitations](https://www.mongodb.com/docs/search/deployment/feature-compatibility/)). | Recurso estável do Atlas (sem rótulo Preview nas páginas citadas). O nome atual na documentação é "MongoDB Search" (antes "Atlas Search"). |
| Endpoint REST de offsets do Kafka Connect (07, `docs/setup-streaming.md`) | "Kafka Connect 3.6+" | [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875%3A+First-class+offsets+support+in+Kafka+Connect) descreve `GET`/`PATCH`/`DELETE /connectors/{name}/offsets`, sem número de release. | **3.6 a confirmar** nas release notes do Apache Kafka (não é produto MongoDB). |

Atlas Stream Processing e o MongoDB Kafka Connector (07) não têm afirmação de status ou versão no repo.

Ao revisar: refaça a consulta, troque a data acima e mantenha a regra de `Limites.jsx` (item só se verificável
na doc do produto ou medido aqui).
