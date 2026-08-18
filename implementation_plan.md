# Atlas Feature Showcase — prompt de construção

> Esse é o briefing que eu entrego **antes de existir uma linha de código**. Não é documentação do que existe: é o que eu daria pra alguém (ou pro Claude) subir a PoV inteira do zero.

Um showcase interativo que exercita **oito capacidades centrais do Atlas contra um cluster de verdade** — reindexação online, Online Archive, agregação, `$jsonSchema`, change streams, transações ACID, streaming (três abordagens lado a lado) e risco geográfico. Backend FastAPI em `:8002`, frontend Vite/React em `:5174`, databases `POC`, `pix` e `geo`.

**Sem LLM nenhum aqui.** Essa é a PoV que responde "o Atlas aguenta?" com número, não com narrativa.

| Arquivo | O que responde |
|---|---|
| [`docs/prompts/01-arquitetura.md`](docs/prompts/01-arquitetura.md) | os oito módulos, arquitetura e portas, os dois middlewares, o módulo de Streaming e as injeções de falha, replay, ferramental de operação, armadilhas, ordem de trabalho |
| [`docs/prompts/02-mongodb.md`](docs/prompts/02-mongodb.md) | separação de databases, índices do Geo com o racional, os três pipelines (`explain-compare`, `$setWindowFields`, `$search`), reconciliação em três checagens, seed e limpeza |
| [`docs/prompts/03-interface-fluxos.md`](docs/prompts/03-interface-fluxos.md) | as duas regras de tela, `useApi`, disciplina de polling, layout do módulo 07, roteiro de demo |

Se for ler só um: o **01**, pela ordem de trabalho. O Streaming consome o projeto inteiro se vier primeiro.
