/*
 * COLUNA 3 do módulo Streaming — Atlas Stream Processing.
 *
 * Executar com mongosh CONTRA A STREAM PROCESSING INSTANCE (não contra o cluster):
 *
 *   mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js
 *
 * Pré-requisitos (feitos uma vez no Atlas UI, aba Stream Processing):
 *   1. Criar uma Stream Processing Instance (SPI) na mesma região do cluster.
 *   2. Criar nela uma conexão do tipo "Atlas Database" apontando para o cluster
 *      da PoV, com o nome usado abaixo em CONNECTION (default: atlasCluster).
 *   3. Copiar a connection string da SPI para ASP_CONNECTION_STRING em
 *      backend/.env e marcar ASP_ENABLED=true.
 *
 * O processor:
 *   $source     — change stream de pix.transacoes na conexão do cluster
 *   $match      — só inserts
 *   $validate   — documento malformado vai para a DLQ, sem derrubar o processor
 *   $tumblingWindow(5s) — agrega por uf + tipo: qtd, volume somado, ticket médio
 *   $merge      — grava cada janela fechada em pix.metricas_janela
 *
 * O backend NÃO consulta a SPI para exibir o resultado: ele assiste
 * pix.metricas_janela com um change stream. O resultado do ASP chega na tela
 * pela mecânica da coluna 1 — é o fecho didático das três colunas.
 */

const CONNECTION = process.env.ASP_CONNECTION_NAME || 'atlasCluster';
const DB = process.env.STREAMING_DB || 'pix';
const PROCESSOR = process.env.ASP_PROCESSOR_NAME || 'pixJanelas5s';
const RECREATE = ['1', 'true', 'yes'].includes((process.env.ASP_RECREATE || '').toLowerCase());

const source = {
  $source: {
    connectionName: CONNECTION,
    db: DB,
    coll: 'transacoes',
  },
};

const onlyInserts = { $match: { operationType: 'insert' } };

// Documento malformado (valor não numérico, tipo fora do enum, UF inválida) é
// desviado para a DLQ — o processor continua rodando.
const validate = {
  $validate: {
    validator: {
      $and: [
        { 'fullDocument.endToEndId': { $type: 'string' } },
        { 'fullDocument.run_id': { $type: 'string' } },
        { 'fullDocument.valor': { $type: ['decimal', 'double', 'int', 'long'] } },
        { 'fullDocument.tipo': { $eq: 'PIX' } },
        { 'fullDocument.uf': { $type: 'string' } },
      ],
    },
    validationAction: 'dlq',
  },
};

const window = {
  $tumblingWindow: {
    boundary: 'eventTime',
    // 5 s em vez de 10 s: a janela é o que distingue esta coluna das outras
    // duas, mas com 10 s o painel ficava mudo por 10 s a cada ciclo e quem
    // assiste 20 s da demo via no máximo duas rajadas. Com 5 s a semântica é a
    // mesma (tumbling, sem sobreposição) e a coluna se mexe o dobro.
    // allowedLateness cai junto: 3 s eram 30% da janela antiga e seriam 60% da
    // nova, atrasando a emissão mais do que a janela dura.
    interval: { size: 5, unit: 'second' },
    allowedLateness: { size: 2, unit: 'second' },
    pipeline: [
      {
        $group: {
          _id: {
            run_id: '$fullDocument.run_id',
            uf: '$fullDocument.uf',
            tipo: '$fullDocument.tipo',
          },
          qtd: { $count: {} },
          volume: { $sum: { $toDouble: '$fullDocument.valor' } },
          ticket: { $avg: { $toDouble: '$fullDocument.valor' } },
          alertas_valor_alto: {
            $sum: { $cond: [{ $gte: [{ $toDouble: '$fullDocument.valor' }, 5000] }, 1, 0] },
          },
          maior_valor: { $max: { $toDouble: '$fullDocument.valor' } },
        },
      },
      {
        $set: {
          run_id: '$_id.run_id',
          uf: '$_id.uf',
          tipo: '$_id.tipo',
          volume: { $round: ['$volume', 2] },
          ticket: { $round: ['$ticket', 2] },
          maior_valor: { $round: ['$maior_valor', 2] },
        },
      },
    ],
  },
};

// $meta expõe as bordas oficiais depois do estágio de janela. Diferente de
// $min/$max do timestamp do evento, elas não mudam quando chega dado atrasado.
const windowBounds = {
  $set: {
    window_start: { $meta: 'stream.window.start' },
    window_end: { $meta: 'stream.window.end' },
  },
};

// _id determinístico por (execução, janela, uf, tipo): o $merge grava uma linha por janela
// fechada, e o change stream do backend vê cada uma delas chegar.
const shape = {
  $set: {
    _id: {
      $concat: ['$run_id', '|', { $toString: '$window_start' }, '|', '$uf', '|', '$tipo'],
    },
  },
};

const merge = {
  $merge: {
    into: { connectionName: CONNECTION, db: DB, coll: 'metricas_janela' },
    whenMatched: 'replace',
    whenNotMatched: 'insert',
  },
};

const pipeline = [source, onlyInserts, validate, window, windowBounds, shape, merge];

const options = {
  dlq: { connectionName: CONNECTION, db: DB, coll: 'dlq' },
};

const existing = sp.listStreamProcessors({ name: PROCESSOR });
if (existing.length > 0) {
  if (!RECREATE) {
    print(`ℹ️ Processor '${PROCESSOR}' já existe; definição preservada e checkpoint mantido.`);
    print('   Para substituir deliberadamente: ASP_RECREATE=true mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js');
    quit(0);
  }
  print(`▶ ASP_RECREATE=true — substituindo processor '${PROCESSOR}' e descartando seu checkpoint.`);
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
sp[PROCESSOR].start();

print(`✅ Processor '${PROCESSOR}' criado e iniciado.`);
print(`   janelas  → ${DB}.metricas_janela`);
print(`   DLQ      → ${DB}.dlq`);
print(`   status:  sp.${PROCESSOR}.stats()`);
