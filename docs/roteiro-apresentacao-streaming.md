# Roteiro - aba Streaming

**12-15 min.** A tese não é apenas volume. É quantos padrões de reação o mesmo dado confirmado alimenta sem dual-write nem código de CDC na aplicação.

## Antes da sala - obrigatório

- Rode `./scripts/prepare-demo.sh` e depois `./bin/overview`.
- Confirme no topo `Atlas M20/M30` e `Pronto`. Nunca entre na apresentação com `Pré-voo pendente`.
- Abra as três colunas antes do Play: Change Streams em espera, Kafka `RUNNING` e ASP `PROCESSOR ATIVO`.
- Desligue VPN/proxy. Aplicação e cluster devem sair pelo Brasil; RTT internacional destrói o argumento de latência.
- Não aperte Reset preventivamente: `overview` já inicia a rodada limpa.

## Talk track invisível ao cliente

Use esta explicação enquanto aponta as três colunas. Ela foi removida da interface para a tela mostrar evidência, não instruções ao apresentador.

| Caminho | Verbo | Quando usar | Trade-off que você deve verbalizar |
| --- | --- | --- | --- |
| Change Streams | Reagir | Notificação, workflow, cache, auditoria; poucos consumidores próximos da aplicação | Menor caminho. A aplicação mantém checkpoint e idempotência. |
| Kafka Connector | Distribuir | Muitas squads, legado, antifraude, data lake e analytics | Retenção e replay. Adiciona broker, connector, offsets e operação Kafka. |
| Atlas Stream Processing | Transformar | Janelas, agregação, validação, enriquecimento, sinais e DLQ | Estado e MQL gerenciados. Latência depende da janela escolhida. |

Frase: **"Não são três concorrentes. São três finalidades pós-commit que podem coexistir sobre o mesmo PIX."**

## Os cinco atos

| # | Ato | O que fazer | A frase |
| --- | --- | --- | --- |
| 1 | Enquadrar - 1 min | Três colunas paradas. Explique reagir, distribuir e transformar usando o talk track acima. | "Uma escrita confirmada; três destinos com responsabilidades diferentes." |
| 2 | Uma transação - 3 min | Preset Operação típica e Play. Aponte `1 insert = 1 PIX`, Escrita no Atlas e ACK do cliente. | "Servidor e rede estão separados. O número do banco não inclui o round-trip da aplicação." |
| 3 | Fan-out - 3 min | Mostre resume token, offset Kafka, janela, checkpoint e DLQ. | "O mesmo PIX alimenta os três caminhos sem a aplicação fazer dual-write." |
| 4 | A prova - 2 min | Espere a reconciliação fechar. Deixe a plateia ler os quatro totais. | "A integridade foi conferida por run_id e endToEndId, não por amostragem." |
| 5 | Falha - 3 min | Durante o fluxo, derrube o cursor e mostre a retomada pelo resume token. | "O consumidor retomou do checkpoint dentro da janela do oplog." |

Se sobrar tempo, use Headroom do tier para discutir folga. Não converta a execução em claim de sizing.

<!-- pagebreak -->

## O que a tela deve provar

| Evidência | Leitura correta |
| --- | --- |
| Escrita no Atlas | `opLatencies` server-side, cluster-wide durante uma rodada isolada; não contém rede. |
| ACK do cliente | Ponta a ponta da aplicação até a confirmação, incluindo RTT. |
| Change Streams e Kafka | Propagação pós-commit, não tempo de liquidação PIX. |
| ASP | Fecho da janela em event time; não é latência por evento. |
| Reconciliação | Fonte = Change Streams = Kafka = ASP + DLQ quando o estado final fica verde. |

## Perguntas difíceis

| Pergunta | Resposta |
| --- | --- |
| "Já fazemos esse volume." | "A diferença não é só volume: é quantos consumidores recebem uma escrita sem código de dual-write ou CDC na aplicação." |
| "Substitui Kafka?" | "Não. Uma coluna é o próprio Kafka. A escolha é por finalidade e os três podem coexistir." |
| "Como sei que não perdeu?" | "Reconciliação por identificador único nos três caminhos e contabilização explícita de DLQ." |
| "Qual o limite?" | "Nesta PoV satura primeiro o consumidor local. Produção exige workload, SLO e teste representativos." |
| "Change Streams elimina outbox?" | "Elimina dual-write em muitos fluxos derivados do dado confirmado. Efeitos externos ainda exigem idempotência e, conforme a semântica, outbox." |

## Plano B operacional

| Sintoma | Ação |
| --- | --- |
| `Pré-voo pendente` | Abra o diagnóstico. Não apresente ao vivo até todos os checks obrigatórios ficarem verdes. |
| Play em limpeza por mais de 10 s | Pare. Verifique DNS/Atlas; não clique repetidamente. Após uma rodada completa, o drop controlado pode levar cerca de 6 s. Use replay se a rede da sala estiver instável. |
| Kafka `FAILED` | Reinicie pela coluna. Persistiu: siga com Change Streams e ASP, declarando a limitação. |
| ASP não configurado | Mostre as duas primeiras colunas e trate ASP como arquitetura alvo. Não improvise números. |
| Sem rede | Use Replay de segurança e declare que é uma execução real previamente gravada. |

## Não diga

- Não chame de benchmark, sizing ou certificação de capacidade.
- Não apresente latência de rede como tempo de banco.
- Não prometa ordenação global: defina o escopo por cursor ou chave de partição.
- Não diga "zero integração". Diga **"sem dual-write e sem código de CDC na aplicação"**.
- Não diga que Change Streams elimina idempotência de efeitos externos.

## Encerramento

**"Uma escrita confirmada, três padrões de consumo e uma reconciliação que prova o caminho inteiro."**

Depois da sala, execute `./bin/overview down`: o processor cobra enquanto está ligado.
