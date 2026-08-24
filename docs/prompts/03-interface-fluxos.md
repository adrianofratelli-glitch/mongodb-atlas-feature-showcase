# Atlas Feature Showcase — interface, fluxos e roteiro

> Terceiro dos três prompts. Oito capacidades, cada uma tendo que se provar sozinha na frente de alguém cético.

---
## Contrato visual do portfólio (v2)

Esta UI participa da assinatura MongoDB Dark das PoVs. O arquivo
`src/pov-signature.css` é uma cópia sincronizada entre os onze frontends e deve
ser importado **depois** do stylesheet local. O contêiner raiz carrega
`data-pov-shell`, existe um `.pov-skip-link` para `#conteudo-principal` e o
`index.html` declara pt-BR, dark color scheme, theme color e o favicon comum.

A camada compartilhada é dona da document rail, foco, touch targets e redução de
movimento. Este arquivo continua dono do fluxo e das exceções de domínio: não
achate uma tela operacional num template de landing page e não remova a tese
visual específica desta PoV. Qualquer mudança na assinatura precisa ser
replicada nas onze cópias e validada em 1440, 768 e 360 px, além do build de
produção e do estado offline.


## Duas regras que valem pra todos os módulos

1. **Nada de dado inventado.** Todo número na tela veio de uma chamada real ao cluster. Se o Atlas não responder, a tela diz isso — não preenche com placeholder.
2. **A query fica visível.** Todo módulo mostra o pipeline que rodou, num `QueryBlock`. Quem está assistindo tem que conseguir copiar e rodar no Compass.

## Stack

Mínima de propósito: React 18 + Vite, JSX puro, CSS escrito à mão. A única dependência de UI é `react-syntax-highlighter`, pra realçar os pipelines. **Sem router, sem biblioteca de estado, sem UI kit.**

Navegação por **hash** (`/#agg`, `/#streams`, `/#tx`), lida no boot e reagindo a `hashchange`. É o suficiente pra deep-link funcionar — eu preciso conseguir abrir um módulo específico direto na URL durante a apresentação.

`src/index.css` com os tokens dark do MongoDB: `--bg-primary #001E2B`, `--accent #00ED64`, Outfit + JetBrains Mono. Mesma paleta das outras PoVs do meu portfólio.

Um `DemoFlow` dando o passo a passo de cada módulo na própria tela — é o roteiro, pra eu não depender de decorar oito sequências diferentes.

O mapa do módulo Geo é **SVG inline com projeção escrita à mão**. Sem Leaflet, sem Mapbox, sem tiles, sem dependência nova. Quero o módulo renderizando e funcionando com a rede externa bloqueada — já apresentei em cliente com egress fechado. A única requisição externa do app é o link de fontes do Google no `index.html`; sem ele a tipografia cai pro sistema e mais nada muda.

## `useApi` — o hook que segura a demo

Todo fetch passa por `src/hooks/useApi.js`, e ele carrega mais decisão do que o tamanho sugere:

- **Timeout de 30s por requisição**, configurável até 300s com `AbortController` — criar índice ou Online Archive demora.
- **Erro traduzido pra linguagem de operador.** `Failed to fetch` vira "API indisponível — verifique se o backend está rodando na porta 8002". Numa demo, "Failed to fetch" não ajuda ninguém, inclusive a mim.
- **Abort de navegação não vira erro.** Trocar de módulo cancela as requisições da tela anterior; isso é esperado e não pode pintar um toast vermelho falso.
- **Contador de pendentes** em vez de booleano de loading, senão duas chamadas concorrentes fazem o spinner piscar cedo demais.
- **`X-Demo-Token`** injetado de `VITE_DEMO_API_TOKEN`, casando com o hardening do backend.
- **Erro global via `CustomEvent('api-error')`** — o shell mostra o toast sem eu precisar passar callback por toda a árvore.

E uma armadilha que custou tempo: o `loading` é **uma flag para todas as chamadas daquela instância**. Página que faz poll **e** tem botão de ação precisa de um `useApi()` separado pro poll, com o busy de cada ação controlado localmente. Senão os botões ficam desabilitados a cada tique do poll sem ninguém ter clicado — o módulo 08 perdia ~1 segundo a cada 4 desse jeito.

## Disciplina de polling

Oito módulos com relógio próprio viram uma rajada de requisições sem ninguém perceber. **Todo intervalo do app passa por `usePolling`:**

- `useVisivel()` — aba escondida, nada roda.
- `useIntervaloVisivel(fn, ms, ativo)` — o timer só existe enquanto a aba está visível **e** `ativo` é verdadeiro. Dispara uma vez ao reativar, pra tela não ficar com dado velho quando eu volto. **Guarda a função numa `ref`** — deixar a identidade dela nas dependências recria o timer a cada render, e é o erro clássico que transforma um poll de 5s em rajada.

Nada faz polling de dado que não pode mudar: o snapshot gravado só avança enquanto o relógio de reprodução está rodando.

O `useSse` fecha o `EventSource` quando a aba esconde, e **reconecta manualmente**. O `EventSource` só refaz a conexão sozinho em alguns casos, e cada conexão presa ocupa uma das ~6 por host do browser — as vazadas faziam fetch comum estourar em 30 segundos.

Quero isso medido e registrado no README. Minha baseline: **48 requisições/20s → 1 quando parado, 0 com a aba escondida.**

## Uma página por módulo

Cada uma tem que se sustentar sozinha: o que a capacidade é, o botão que dispara a operação real, o número medido, e o `QueryBlock` com o que rodou.

O módulo 07 é o único com layout próprio: **três colunas comparáveis**, com latência, throughput e custo lado a lado, os botões de injeção de falha, o painel de reconciliação (as três checagens) e o `/streaming/folga` mostrando o custo em CPU.

As colunas 2 e 3 aparecem como **"não configurado"** quando faltam as variáveis — nunca como coluna quebrada.

Em modo replay, a página carrega um **badge permanente de origem** e os botões que agem no ambiente ficam desabilitados. A página abre em modo ao vivo por padrão.

## O roteiro que eu preciso conseguir executar no fim

Abrir cada módulo direto pela URL, disparar a operação, e mostrar o número e a query lado a lado. Especificamente:

1. Subir um índice com o `live_monitor` rodando no terminal ao lado — leitura e escrita não param.
2. Mostrar o banco recusando um documento fora do `$jsonSchema`.
3. Escrever num terminal e ver o evento aparecer na tela pelo change stream.
4. Commit e rollback de transação, com o estado antes e depois.
5. Query atravessando dado quente e arquivado de forma transparente.
6. As três colunas de streaming rodando juntas, com latência, throughput e custo comparados.
7. **Quebrar de propósito** — parar o connector, mandar um evento inválido, e mostrar a reconciliação fechando depois, nas três checagens.
8. Disparar o failover do Atlas e mostrar as escritas rejeitadas ao lado das confirmadas.
9. `/streaming/folga` — o que a corrida custou de CPU no primário, na janela dela.
10. O sinal de viagem impossível aparecendo **em tempo de evento** no módulo 08, com plantado e emergente contados separados.
11. `explain` do mesmo `$geoWithin` com dois hints diferentes, e a viagem impossível calculada em MQL puro, com a seletividade e o custo nos dois escopos.
12. `overview down` no fim, na frente do cliente, e dizer por quê.

## Antes de apresentar

- `scripts/prepare-demo.sh` rodado: dataset Geo materializado e índice do Atlas Search em **READY**.
- `/preflight` limpo — inclusive a sonda real da Admin API (o IP de saída muda com VPN).
- `pgrep -f "uvicorn main:app"` devolvendo **um** PID.
- Notebook fora de VPN, senão a coluna de latência mede a rota e não o cluster.
