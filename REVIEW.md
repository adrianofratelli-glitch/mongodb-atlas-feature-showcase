> Estado vigente: melhoria `7739b64` aprovada pelo usuário e integrada em `main`. As menções abaixo a aprovação pendente são históricas. As propostas de core/schema/dataset continuam sem aplicação.

# Revisão de engenharia e design — atlas-showcase-capacidades

## Resultado

Nenhuma correção de código adicional justificada pela amostra revisada; relatório e sugestões.

Branch `review/codex-improvements`, criada de `main` em `c93df32bb8749edbe7ee26baa0c40ff0f01c1483`. Sem merge, push, troca de biblioteca core, alteração de schema ou dataset.

## Commits de correção

Nenhum commit de código; somente este relatório.

## Commits visible-change

Nenhum.

## Validação

- 165 testes unitários passaram.
- Build de produção passou; análise Ruff E9/F63/F7/F82 com target Python 3.12 passou.
- Browser com APIs bloqueadas: 1440×1000, 768×1024 e 360×800; sem pageerror e sem overflow horizontal no shell inicial; link de salto transfere foco ao conteúdo.
- As 14 cópias de pov-signature.css permanecem idênticas; lang pt-BR confirmado. Nenhuma alteração na camada compartilhada de CSS.
- Auditor de portas passou: registro e configurações alinhados.
- npm audit do lockfile após correções: 0 altos, 0 críticos, 0 moderados e 0 baixos.

## Sugestões não aplicadas e limites

- Ambiente Python instalado contém advisories listados abaixo; reconciliar com os requirements antes da próxima demo. Atualização de framework core não aplicada.
- Streaming/ASP/Kafka, failover e reconciliação real não foram reexecutados; exigem operações sobre o ambiente/dataset. Oito módulos são acessados por seletor compacto, preservando a exceção narrativa estabelecida.

A verificação visual cobre o shell offline e abas acessíveis sem backend, não todos os estados de dados. Não certifica contraste de cada componente, comportamento touch completo ou toda a navegação com Atlas. Fluxos reais de escrita/carga não foram executados para preservar datasets. Nenhuma comparação de performance foi inventada. Evidências locais: `/tmp/codex-portfolio-review/`.

## Dependências Python

Auditoria do ambiente instalado, não de uma resolução limpa do manifesto; ferramentas de desenvolvimento podem aparecer junto com runtime. Os IDs abaixo não equivalem a exploração confirmada na PoV. Reconciliar versões instaladas/manifests e testar compatibilidade; atualizações core/major ficaram fora desta rodada. Pacotes de ferramenta e componentes extras do venv também não foram alterados fora da branch.

| Pacote instalado | Versão | Advisory | Versões corrigidas informadas |
|---|---|---|---|
| pip | 26.1 | PYSEC-2026-196, PYSEC-2026-3721 | 26.1.2, 26.2 |
| pypdf | 6.15.0 | GHSA-jp53-mhqp-8xcg, GHSA-23w6-3w8w-8484, GHSA-763m-79hh-57f2 | 6.16.0, 6.16.1 |
| pytest | 8.4.2 | PYSEC-2026-1845 | 9.0.3 |
| starlette | 1.2.1 | PYSEC-2026-249, PYSEC-2026-248 | 1.3.0, 1.3.1 |

## Segredos e compartilhamento

Varredura por padrões de chaves privadas, chaves Anthropic/AWS e URI MongoDB autenticada no histórico Git local alcançável: nenhuma credencial real confirmada; matches encontrados eram placeholders conhecidos. Limite: não é scanner de entropia, não cobre objetos inacessíveis, texto em screenshots nem logs externos.

Nenhum import/referência estática a `_shared/grove_client.py` foi encontrado nesta PoV. Configuração própria de gateway/ambiente não constitui dependência de código desse módulo. `_shared` permaneceu intocado; consumidores externos/dinâmicos não são garantidos por busca estática. Relatório separado: `../REVIEW_SHARED.md`.


## Fechamento final — 2026-09-05

Esta seção atualiza o estado dos achados históricos acima.

- Aplicado/reavaliado: pip atualizado no ambiente; pypdf auxiliar atualizado para 6.16.1 sem adicionar dependência não utilizada ao manifesto.
- Validação: 165 testes; npm sem achados.
- Propostas e limites restantes: Starlette 1.2.1 → ≥1.3.1: corrige parsing/URL, mas requer compatibilidade com FastAPI; core não alterado. pytest 8.4.2 → 9.0.3: corrige advisory local, exige validação de plugins/CI. ASP/Kafka/failover/reconciliação precisam de janela e ambiente de integração; não disparados. Seletor de oito módulos mantém a exceção narrativa estabelecida.
- pip-audit atual: pytest 8.4.2: PYSEC-2026-1845; starlette 1.2.1: PYSEC-2026-249, PYSEC-2026-248
- Ambiente: pip 26.2.1 nos ambientes que possuem pip; FinScope mantém uv sem pip. Essa atualização local não altera arquivos de dependências das PoVs.
- `_shared`: nenhum importador estático comprovado nesta PoV; apenas smoke consome o helper no inventário.


## Homologação de resiliência e UI

- Melhoria: Impedir sobreposição de polling lento; ignorar chamadas com sinal já abortado.
- Isolamento: `review/codex-homologation`, baseada no HEAD `d576127`. Correção interna elegível para merge após testes.
- Validação: build passou; UI offline em 1440×1000, 768×1024 e 360×800 sem pageerror nem overflow horizontal; skip link transfere foco. 1 testes novos de transporte/polling neste repositório. As suítes locais anteriores foram reexecutadas; resultados consolidados no vault PoVs-Handoffs.
- Limite: teste offline/fixture não certifica cenário real completo nem ausência de bugs. Não houve alteração de schema, dataset ou dependência core.
- Propostas preservadas: Starlette 1.2.1 → ≥1.3.1: corrige parsing/URL, mas requer compatibilidade com FastAPI; core não alterado. pytest 8.4.2 → 9.0.3: corrige advisory local, exige validação de plugins/CI. ASP/Kafka/failover/reconciliação precisam de janela e ambiente de integração; não disparados. Seletor de oito módulos mantém a exceção narrativa estabelecida.
- `_shared` e daemon do portal não foram alterados nesta rodada.
