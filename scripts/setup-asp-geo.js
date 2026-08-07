/*
 * Sinal de risco geográfico EM EVENT TIME — a ponte entre os módulos 07 e 08.
 *
 * Executar com mongosh CONTRA A STREAM PROCESSING INSTANCE:
 *
 *   mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp-geo.js
 *
 * Por que um segundo processor: um pipeline implantado tem um único sink
 * terminal. O `pixJanelas5s` já termina em $merge para pix.metricas_janela;
 * este termina em geo.sinais_ao_vivo. São dois consumidores independentes do
 * mesmo change stream, que é justamente o argumento da aba 07.
 *
 * O que ele faz:
 *   $source          — change stream de pix.transacoes
 *   $match           — só inserts do canal CARTAO_PRESENCIAL (PIX não tem ponto)
 *   $hoppingWindow   — 30 s de janela deslizando de 10 em 10 s, por tempo de
 *                      CHEGADA. A janela agrupa o que chegou junto; a
 *                      velocidade é calculada com `compradaEm`, o instante da
 *                      compra no terminal, que pode ser minutos antes de `ts`.
 *   $group           — por cliente, acumula os pontos da janela
 *   haversine em MQL — mesma fórmula do módulo 08, em operadores nativos
 *   $match           — só o que passa do limiar de km/h
 *   $merge           — geo.sinais_ao_vivo, _id determinístico por par
 *
 * A sobreposição das janelas faz o mesmo par aparecer mais de uma vez; o _id
 * determinístico (par de endToEndId ordenado) absorve isso com whenMatched:
 * keepExisting. Sem sobreposição, um par cujos dois pontos caíssem em lados
 * opostos da borda da janela simplesmente não seria visto.
 */

const CONNECTION = process.env.ASP_CONNECTION_NAME || 'atlasCluster';
const DB = process.env.STREAMING_DB || 'pix';
const GEO_DB = process.env.GEO_DB || 'geo';
const PROCESSOR = process.env.ASP_GEO_PROCESSOR_NAME || 'geoSinais30s';
const TIER = process.env.ASP_TIER || 'SP10';
const LIMITE_KMH = Number(process.env.STREAMING_SINAL_KMH || 900);
// Abaixo destes dois pisos, "velocidade" é ruído de captura, não deslocamento.
const MIN_KM = Number(process.env.STREAMING_SINAL_MIN_KM || 200);
const MIN_MINUTOS = Number(process.env.STREAMING_SINAL_MIN_MIN || 1);
const RECREATE = ['1', 'true', 'yes'].includes((process.env.ASP_RECREATE || '').toLowerCase());

const source = {
  $source: {
    connectionName: CONNECTION,
    db: DB,
    coll: 'transacoes',
    config: { fullDocument: 'updateLookup' },
  },
};

// Só compra presencial: é o único canal que carrega coordenada de terminal.
const somenteCartao = {
  $match: {
    operationType: 'insert',
    'fullDocument.canal': 'CARTAO_PRESENCIAL',
    'fullDocument.local.coordinates': { $exists: true },
    'fullDocument.compradaEm': { $exists: true },
  },
};

// Extremo da janela por cliente: a compra mais antiga e a mais recente
// por `compradaEm`.
// $reduce em vez de $sortArray — a fórmula fica válida em qualquer versão do
// motor de agregação, e a janela por cliente é pequena.
const extremos = (comparador) => ({
  $reduce: {
    input: '$pontos',
    initialValue: null,
    in: {
      $cond: [
        { $or: [{ $eq: ['$$value', null] }, { [comparador]: ['$$this.ts', '$$value.ts'] }] },
        '$$this',
        '$$value',
      ],
    },
  },
});

const janela = {
  $hoppingWindow: {
    boundary: 'processingTime',
    interval: { size: 30, unit: 'second' },
    hopSize: { size: 10, unit: 'second' },
    pipeline: [
      {
        $group: {
          _id: '$fullDocument.clienteId',
          run_id: { $last: '$fullDocument.run_id' },
          compras: { $count: {} },
          pontos: {
            $push: {
              endToEndId: '$fullDocument.endToEndId',
              // Instante da COMPRA no terminal, não o da chegada ao stream:
              // é a diferença entre os dois que dá sentido à velocidade.
              ts: '$fullDocument.compradaEm',
              coordinates: '$fullDocument.local.coordinates',
              municipio: '$fullDocument.municipio',
              uf: '$fullDocument.uf',
              terminal: '$fullDocument.dispositivo.id',
              origemSinal: '$fullDocument.origemSinal',
            },
          },
        },
      },
      { $match: { compras: { $gte: 2 } } },
      { $set: { de: extremos('$lt'), para: extremos('$gt') } },
      // Dois pontos distintos: com um só ponto repetido não há deslocamento.
      { $match: { $expr: { $ne: ['$de.endToEndId', '$para.endToEndId'] } } },
    ],
  },
};

// Haversine sobre [lon, lat], idêntica ao pipeline sob demanda do módulo 08.
const distancia = {
  $set: {
    km: {
      $let: {
        vars: {
          lat1: { $degreesToRadians: { $arrayElemAt: ['$de.coordinates', 1] } },
          lat2: { $degreesToRadians: { $arrayElemAt: ['$para.coordinates', 1] } },
          dLat: {
            $degreesToRadians: {
              $subtract: [
                { $arrayElemAt: ['$para.coordinates', 1] },
                { $arrayElemAt: ['$de.coordinates', 1] },
              ],
            },
          },
          dLon: {
            $degreesToRadians: {
              $subtract: [
                { $arrayElemAt: ['$para.coordinates', 0] },
                { $arrayElemAt: ['$de.coordinates', 0] },
              ],
            },
          },
        },
        in: {
          $multiply: [
            12742.0176, // 2 * raio médio da Terra em km
            {
              $asin: {
                $min: [
                  1,
                  {
                    $sqrt: {
                      $add: [
                        { $pow: [{ $sin: { $divide: ['$$dLat', 2] } }, 2] },
                        {
                          $multiply: [
                            { $cos: '$$lat1' },
                            { $cos: '$$lat2' },
                            { $pow: [{ $sin: { $divide: ['$$dLon', 2] } }, 2] },
                          ],
                        },
                      ],
                    },
                  },
                ],
              },
            },
          ],
        },
      },
    },
    minutos: {
      $divide: [{ $subtract: ['$para.ts', '$de.ts'] }, 60000],
    },
  },
};

const velocidade = {
  $set: {
    km: { $round: ['$km', 1] },
    minutos: { $round: [{ $abs: '$minutos' }, 1] },
    kmh: {
      $round: [{ $divide: ['$km', { $divide: [{ $abs: '$minutos' }, 60] }] }, 0],
    },
  },
};

// Três condições, não uma. Só o limiar de km/h transformava em "alerta" duas
// compras a 20 km de distância capturadas com segundos de diferença — um
// artefato de captura simultânea dentro da mesma cidade, não um cartão em dois
// lugares. Distância e intervalo mínimos são o que separa sinal de ruído.
const somenteSuspeitos = {
  $match: {
    $expr: {
      $and: [
        { $gte: ['$kmh', LIMITE_KMH] },
        { $gte: ['$km', MIN_KM] },
        { $gte: ['$minutos', MIN_MINUTOS] },
      ],
    },
  },
};

const forma = {
  $set: {
    clienteId: '$_id',
    detectadoEm: { $meta: 'stream.window.end' },
    // Plantado só se OS DOIS pontos vieram do injetor da demo. Um par formado
    // por um ponto plantado e um ponto de tráfego é emergente, e conta como tal.
    origem: {
      $cond: [
        { $and: [{ $eq: ['$de.origemSinal', 'plantado'] }, { $eq: ['$para.origemSinal', 'plantado'] }] },
        'plantado',
        'emergente',
      ],
    },
    _id: {
      $concat: [
        '$_id', '|',
        { $cond: [{ $lt: ['$de.endToEndId', '$para.endToEndId'] }, '$de.endToEndId', '$para.endToEndId'] }, '|',
        { $cond: [{ $lt: ['$de.endToEndId', '$para.endToEndId'] }, '$para.endToEndId', '$de.endToEndId'] },
      ],
    },
  },
};

const limpar = { $unset: ['pontos', 'compras'] };

const merge = {
  $merge: {
    into: { connectionName: CONNECTION, db: GEO_DB, coll: 'sinais_ao_vivo' },
    // Janelas sobrepostas reveem o mesmo par: a primeira detecção é a que vale.
    whenMatched: 'keepExisting',
    whenNotMatched: 'insert',
  },
};

const pipeline = [source, somenteCartao, janela, distancia, velocidade, somenteSuspeitos, forma, limpar, merge];

const options = {
  dlq: { connectionName: CONNECTION, db: DB, coll: 'dlq' },
  tier: TIER,
};

const existing = sp.listStreamProcessors({ name: PROCESSOR });
if (existing.length > 0) {
  if (!RECREATE) {
    print(`ℹ️ Processor '${PROCESSOR}' já existe; definição preservada e checkpoint mantido.`);
    print('   Para substituir: ASP_RECREATE=true mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp-geo.js');
    quit(0);
  }
  print(`▶ ASP_RECREATE=true — substituindo processor '${PROCESSOR}'.`);
  if (existing[0].state === 'STARTED') {
    sp[PROCESSOR].stop();
    for (let tentativa = 0; tentativa < 120; tentativa += 1) {
      const atual = sp.listStreamProcessors({ name: PROCESSOR });
      if (atual.length === 0 || atual[0].state === 'STOPPED') break;
      if (tentativa === 119) throw new Error(`Processor '${PROCESSOR}' não chegou a STOPPED.`);
      sleep(500);
    }
  }
  sp[PROCESSOR].drop();
}

sp.createStreamProcessor(PROCESSOR, pipeline, options);
sp[PROCESSOR].start({ tier: TIER });

print(`✅ Processor '${PROCESSOR}' criado e iniciado.`);
print(`   sinais  → ${GEO_DB}.sinais_ao_vivo (limiar ${LIMITE_KMH} km/h)`);
print(`   tier    → ${TIER}`);
