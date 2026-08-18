# Módulo de risco geográfico — setup

Como materializar o dataset `geo` e seu índice do Atlas Search, além do que o
módulo alega e do que ele não alega.

Voltar para o [README](../README.md).

O módulo Geo roda contra o próprio banco (`geo`, sobrescreva com `GEO_DB`), então
nunca toca nas coleções de `POC` ou `pix`. Materialize antes da demo;
o `overview` só roda a checagem rápida e somente leitura:

```bash
./scripts/prepare-demo.sh             # rode antes da apresentação
python scripts/seed_geo.py            # 2.000 clientes × 75 transações = 150 mil docs
python scripts/seed_geo.py --drop     # recria do zero
python scripts/seed_geo.py --ensure   # mantém se estiver atual; recria se estiver defasado/incompleto
./scripts/create_search_index_geo.sh  # cria/atualiza e espera até READY
```

⚠️ **O `--drop` apaga a coleção, e o índice do Atlas Search vai junto.**
Sempre rode `./scripts/create_search_index_geo.sh` depois, ou o painel 02 abre
como `nao_configurado` no palco.

O gerador usa uma semente fixa e carrega uma versão de dataset, então todo `endToEndId` é estável e
o índice único rejeita reinserções: rodar o seed duas vezes deixa 150 mil
documentos, não 300 mil. Os pontos são clusters gaussianos ao redor de 40 municípios
brasileiros reais, ponderados por população — coordenadas uniformemente aleatórias dentro do
bounding box do país parecem obviamente falsas num projetor.

**Dataset v5 — os casos plantados.** Quarenta clientes carregam um par plantado de
viagem impossível, listados em `backend/data/fraud_seeds.json` para que a tela
possa rotular cada resultado como `plantado` ou `emergente`. Cada par deriva seu intervalo
de uma **velocidade alvo** (uniforme entre 1.100 e 9.000 km/h) aplicada à distância real
entre as duas cidades — nunca o contrário. Sortear minutos diretamente
produzia 16.000–42.000 km/h, vinte vezes qualquer padrão real de cartão clonado, e um
intervalo fixo de 5 minutos deixava todas as quarenta linhas idênticas na tela. A posição na
sequência do cliente e o destino também são aleatorizados, dentro da mesma semente fixa de RNG,
de modo que o dataset segue reprodutível.

A localização nunca é apresentada como um campo do PIX. Todo ponto deste dataset é uma
compra de cartão presencial, e sua coordenada pertence ao terminal do adquirente —
dado cadastral, não o celular do cliente. Essa distinção é o argumento
inteiro: a posição de um terminal não é controlada por quem está pagando, ainda que
possa estar desatualizada ou errada no cadastro. A procedência viaja com o
ponto (id do terminal, canal, origem, qualidade), de modo que o sinal nunca pareça um
fato sem origem.

Quarenta clientes recebem um par deliberadamente impossível: duas transações a cerca de cinco
minutos e mais de 700 km de distância. Os IDs deles são gravados em
`backend/data/fraud_seeds.json` para que o painel de viagem impossível tenha um resultado
garantido no palco.

## O sinal em tempo de evento

O painel que abre o módulo não varre histórico algum. Ele lê
`geo.sinais_ao_vivo`, que o stream processor `geoSinais30s` preenche enquanto
o módulo 07 roda: ele agrupa o canal de cartão por portador em uma janela hopping de
30 segundos e roda haversine em MQL ali mesmo, dentro da janela.

![Viagem impossível detectada em tempo de evento](screenshots/08b-geo-aovivo.png)

Dois contadores, deliberadamente separados. O gerador injeta um par a cada seis
segundos para que o palco sempre tenha algo a mostrar — esses são os *plantados*.
Qualquer outro veio do tráfego comum e foi encontrado pelo pipeline,
não arranjado para ele. Apresentar um total único transformaria a garantia na
evidência, o que ela não é.

Um limiar de velocidade sozinho é uma máquina de falsos positivos: duas compras a 20 km
de distância capturadas com segundos de diferença dão 1.343 km/h. O sinal, portanto, exige
três condições — km/h acima do limite, ao menos 200 km de distância e ao
menos um minuto entre as duas capturas. Abaixo disso, "velocidade" é captura
simultânea, não deslocamento.

Repare nos dois timestamps do canal de cartão: `ts` é quando o evento entrou no
fluxo (e também o campo do TTL), enquanto `compradaEm` é quando a compra aconteceu no
terminal, o que pode ser minutos antes, porque a captura do adquirente atrasa. A
velocidade é calculada a partir de `compradaEm`. Colocar esse instante retrodatado no `ts`
fazia o TTL apagar a metade mais antiga de um par antes de a reconciliação rodar, e a
origem então contava menos que os consumidores — expiração com cara exata de
perda.
O resultado é explicitamente um sinal de risco retrospectivo, não uma decisão de fraude nem
um controle inline de bloqueio de pagamento. Produção ainda exige validação de procedência,
antispoofing, tratamento de múltiplos dispositivos, controles de LGPD e calibração de política.

Índices criados pelo seed:

| Índice | Usado por |
|---|---|
| `cliente_status_local_idx` — `{clienteId: 1, status: 1, local: "2dsphere"}` | Demo A, o plano composto |
| `local_2dsphere_idx` — `{local: "2dsphere"}` | Demo A, o plano só-geo contra o qual ele é comparado |
| `cliente_ts_idx` — `{clienteId: 1, ts: 1}` | Demo B, partição + ordenação do `$setWindowFields` |
| `categoria_local_idx` — `{"estabelecimento.categoria": 1, local: "2dsphere"}` | consultas geográficas por categoria |
| `uf_ts_idx` — `{uf: 1, ts: -1}` | recorte regional |
| `e2e_unq_idx` — único `{endToEndId: 1}` | idempotência do seed |

## O índice do Atlas Search

A demo C precisa de um índice do Atlas Search. Crie com:

```bash
./scripts/create_search_index_geo.sh
```

Até ele reportar `READY`, o painel de busca mostra um aviso de "não configurado"
em vez de inventar resultados. A definição que ele aplica:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "estabelecimento": {
        "type": "document",
        "fields": {
          "nome": { "type": "string", "analyzer": "lucene.portuguese" },
          "categoria": [{ "type": "token" }, { "type": "stringFacet" }]
        }
      },
      "uf": [{ "type": "token" }, { "type": "stringFacet" }],
      "local": { "type": "geo" }
    }
  }
}
```

`categoria` e `uf` são indexados duas vezes de propósito: `token` serve ao filtro
exato, `stringFacet` serve à faceta do `$searchMeta`.

## O que o módulo não alega

A página diz isto em voz alta, e este documento também: o MongoDB responde
*predicados* geoespaciais — está dentro, cruza, o que há por perto. Ele não tem
álgebra de geometria (sem buffer, união, interseção ou área), só WGS84 sem
reprojeção, e nada de raster, topologia ou roteamento. O `$geoNear` precisa ser o
primeiro estágio do pipeline, e o `filter` do `$vectorSearch` não aceita operadores
geoespaciais de forma alguma. Cargas que exigem construção de geometria, topologia,
roteamento ou análise GIS pesada precisam de um sistema geoespacial dedicado.
