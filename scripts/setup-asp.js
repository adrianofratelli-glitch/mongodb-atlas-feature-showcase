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
 *   $tumblingWindow(10s) — agrega por uf + tipo: qtd, volume somado, ticket médio
 *   $merge      — grava cada janela fechada em pix.metricas_janela
 *
 * O backend NÃO consulta a SPI para exibir o resultado: ele assiste
 * pix.metricas_janela com um change stream. O resultado do ASP chega na tela
 * pela mecânica da coluna 1 — é o fecho didático das três colunas.
 */

const CONNECTION = process.env.ASP_CONNECTION_NAME || 'atlasCluster';
const DB = process.env.STREAMING_DB || 'pix';
const PROCESSOR = process.env.ASP_PROCESSOR_NAME || 'pixJanelas10s';

const source = {
  $source: {
    connectionName: CONNECTION,
    db: DB,
    coll: 'transacoes',
    config: { fullDocument: 'required' },
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
        { 'fullDocument.valor': { $type: ['decimal', 'double', 'int', 'long'] } },
        { 'fullDocument.tipo': { $in: ['PIX', 'TED', 'BOLETO'] } },
        { 'fullDocument.uf': { $type: 'string' } },
      ],
    },
    validationAction: 'dlq',
  },
};

const window = {
  $tumblingWindow: {
    interval: { size: 10, unit: 'second' },
    pipeline: [
      {
        $group: {
          _id: { uf: '$fullDocument.uf', tipo: '$fullDocument.tipo' },
          qtd: { $count: {} },
          volume: { $sum: { $toDouble: '$fullDocument.valor' } },
          ticket: { $avg: { $toDouble: '$fullDocument.valor' } },
        },
      },
      {
        $set: {
          uf: '$_id.uf',
          tipo: '$_id.tipo',
          volume: { $round: ['$volume', 2] },
          ticket: { $round: ['$ticket', 2] },
        },
      },
    ],
  },
};

// _id determinístico por (janela, uf, tipo): o $merge grava uma linha por janela
// fechada, e o change stream do backend vê cada uma delas chegar.
const shape = {
  $set: {
    _id: {
      $concat: [
        { $toString: '$_stream_meta.window.start' }, '|', '$uf', '|', '$tipo',
      ],
    },
    window_start: '$_stream_meta.window.start',
    window_end: '$_stream_meta.window.end',
  },
};

const merge = {
  $merge: {
    into: { connectionName: CONNECTION, db: DB, coll: 'metricas_janela' },
    whenMatched: 'replace',
    whenNotMatched: 'insert',
  },
};

const pipeline = [source, onlyInserts, validate, window, shape, merge];

const options = {
  dlq: { connectionName: CONNECTION, db: DB, coll: 'dlq' },
};

const existing = sp.listStreamProcessors({ name: PROCESSOR });
if (existing.length > 0) {
  print(`▶ Processor '${PROCESSOR}' já existe — recriando.`);
  sp[PROCESSOR].stop();
  sp[PROCESSOR].drop();
}

sp.createStreamProcessor(PROCESSOR, pipeline, options);
sp[PROCESSOR].start();

print(`✅ Processor '${PROCESSOR}' criado e iniciado.`);
print(`   janelas  → ${DB}.metricas_janela`);
print(`   DLQ      → ${DB}.dlq`);
print(`   status:  sp.${PROCESSOR}.stats()`);
