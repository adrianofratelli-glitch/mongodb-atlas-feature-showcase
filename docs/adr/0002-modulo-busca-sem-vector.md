# ADR 0002 — Módulo 08 com MongoDB Search; Vector Search fica de fora

- Status: aceito
- Data: 2026-11-13

## Contexto

A avaliação do portfólio apontou a lacuna "sem Search/vetor" nesta vitrine de capacidades do Atlas. A pergunta era se
um módulo pequeno cabe sem mudar o objetivo da PoV (provar capacidades do Atlas contra um cluster real, sem LLM).

Fatos medidos e verificados:

- `produtos` (100 mil documentos no seed) já tem texto curto e campos categóricos (`categoria`, `marca`, `em_estoque`).
  Busca textual com erro de digitação e facetas é uma capacidade que um índice B-tree e `$regex` não entregam.
- No banco da demo, `POC.produtos` já tem o índice `produtos_search`, de outra PoV (marketplace). Reaproveitá-lo
  acoplaria as duas demos; removê-lo quebraria a outra.
- `$search` com índice inexistente devolve vazio sem erro (doc "Query Reference › Empty Result Set"), confirmado ao
  vivo em 2026-11-13 contra o cluster 9.0.7.
- Os nomes do seed são combinações de 18 bases × 7 marcas × 8 adjetivos. Similaridade semântica sobre esse texto não
  tem significado, e gerar embeddings exigiria um modelo (custo, credencial, gateway) numa PoV sem LLM.

## Decisão

1. **Módulo 08 · Busca** com MongoDB Search: índice próprio `showcase_busca` (mapeamento estático: `nome` com
   `lucene.portuguese`, `categoria`/`marca` como `token`, `em_estoque` booleano), `text` com `fuzzy` opcional,
   filtro `equals`, facetas por `$searchMeta`. O módulo cria só o seu índice e nunca remove nenhum.
2. A busca só executa depois que `$listSearchIndexes` confirma o índice `queryable`. Antes disso a API responde
   `executado: false` com o estado do índice (`ausente`, `construindo`, `falhou`, `indisponivel`) e devolve os
   pipelines em `pipeline_nao_executado`, nunca em `pipeline` (a tela só chama de "executado" o que rodou).
3. **Vector Search não entra.** Sem dado com semântica real, o módulo provaria só o encanamento, e a busca vetorial já
   é demonstrada nas PoVs de agentes do portfólio. Se um dia entrar, precisa de texto com conteúdo (descrições) e de
   embeddings pelo gateway do workspace; isso vira outra decisão.

## Consequências

- A tese ganha a linha "Busca textual (fuzzy, facetas)": no Atlas, o mesmo cluster; numa stack montada por peças,
  motor de busca + pipeline de sincronização.
- No banco da demo, o índice nasce pelo botão "Criar índice" da aba 08 (fora da janela da demo). Ele soma um índice
  de busca a `POC.produtos`, ao lado do da outra PoV. Leva cerca de 30–45 s para 100 mil documentos (medido no
  `showcase_poc_test`).
- O preflight mostra o estado do índice como check opcional: índice ausente não reprova o pré-voo.
- Testes offline cobrem a degradação; o teste live cria o índice em `showcase_poc_test` e não o remove (recriar a
  cada execução custaria um build).
