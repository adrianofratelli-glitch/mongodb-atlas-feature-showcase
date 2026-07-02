import React from 'react'
import { SpeedControl, FlowStage, StatusRow, useFlowPlayer } from './DemoFlow'

const C = { cyan: '#06b6d4', orange: '#f97316', green: '#00ED64' }

// Diagrama: em cima o Redis (grava rápido, mas o dado durável precisa ir TAMBÉM
// para um banco — duas escritas, dois sistemas). Embaixo o Mongo (1 gravação; o
// aviso deriva dela). A ordem Redis→banco espelha a Etapa 1 e o uso Pró-Redis.
const FLOW = {
  lanes: 3, height: 300,
  nodes: [
    { id: 'rApp', col: 0, lane: 0.5, icon: '📱', label: 'Transação', sub: 'app' },
    { id: 'rRedis', col: 1.7, lane: 0, icon: '🟥', label: 'Redis', sub: 'o aviso (rápido)' },
    { id: 'rBanco', col: 1.7, lane: 1, icon: '🗄️', label: 'Banco durável', sub: 'o dado · 2º sistema' },
    { id: 'rCDC', col: 3, lane: 0.5, icon: '🔄', label: 'CDC / sync', sub: 'processo que VOCÊ opera' },
    { id: 'mApp', col: 0, lane: 2, icon: '📱', label: 'Transação', sub: 'app' },
    { id: 'mMongo', col: 1.7, lane: 2, icon: '🍃', label: 'MongoDB', sub: '1 gravação' },
    { id: 'mStream', col: 3, lane: 2, icon: '🔔', label: 'Aviso = CDC', sub: 'change stream nativo' },
  ],
  wires: [
    { from: 'rApp', to: 'rRedis' },
    { from: 'rApp', to: 'rBanco' },
    { from: 'rRedis', to: 'rCDC', dashed: true, color: 'rgba(249,115,22,.5)' },
    { from: 'rBanco', to: 'rCDC', dashed: true, color: 'rgba(249,115,22,.5)' },
    { from: 'mApp', to: 'mMongo' },
    { from: 'mMongo', to: 'mStream', color: 'rgba(0,237,100,.35)' },
  ],
  tokens: [
    { id: 'tRapido', color: C.orange },
    { id: 'tDuravel', color: C.cyan },
    { id: 'tMongo', color: C.green },
  ],
  statusItems: [
    { key: 'rSist', label: 'Redis · peças p/ operar' },
    { key: 'rEscr', label: 'Redis · escritas' },
    { key: 'mSist', label: 'Mongo · peças p/ operar' },
    { key: 'mEscr', label: 'Mongo · escritas' },
  ],
  steps: [
    { caption: 'Uma transação chega. Repare para onde vão o DADO e o AVISO — e o que é preciso para mantê-los juntos.',
      pos: { tRapido: 'rApp', tDuravel: 'rApp', tMongo: 'mApp' },
      set: { rSist: { text: '0', tone: 'idle' }, rEscr: { text: '0', tone: 'idle' }, mSist: { text: '0', tone: 'idle' }, mEscr: { text: '0', tone: 'idle' } } },
    { pos: { tRapido: 'rRedis' }, flash: 'rRedis',
      set: { rEscr: { text: '1 · aviso', tone: 'amber' }, rSist: { text: '1', tone: 'idle' } },
      caption: <>Redis leva o <b>aviso</b> — rápido. Mas o Redis é volátil: sozinho, o dado <b>não sobrevive a uma queda</b>.</> },
    { pos: { tDuravel: 'rBanco' }, flash: 'rBanco',
      set: { rEscr: { text: '2 · aviso + dado', tone: 'red' }, rSist: { text: '2 sistemas', tone: 'amber' } },
      caption: <>O <b>dado durável</b> vai para o banco — 2ª escrita, 2º sistema. Agora o evento vive em <b>dois lugares</b>.</> },
    { flash: 'rCDC',
      set: { rSist: { text: '3 (banco+Redis+CDC)', tone: 'red' } },
      caption: <>Para os dois <b>não divergirem</b>, você precisa de um <b>processo de CDC/sync</b> (dual-write atômico, outbox + relay). <b>Mais uma peça</b> para construir, operar e pagar — e com lag.</> },
    { pos: { tMongo: 'mMongo' }, flash: 'mMongo',
      set: { mEscr: { text: '1', tone: 'green' }, mSist: { text: '1 sistema', tone: 'green' } },
      caption: 'MongoDB: a transação é UMA gravação durável, num único sistema.' },
    { pos: { tMongo: 'mStream' }, flash: 'mStream',
      caption: <>O aviso <b>É o CDC</b>: o change stream deriva do mesmo commit, nativo. <b>Uma fonte de verdade</b> — sem 2º banco, sem processo de sync, nada para divergir.</> },
  ],
}

export default function SourceOfTruthDemo({ num, title, subtitle, seeing }) {
  const player = useFlowPlayer(FLOW.steps, { baseMs: 1150 })
  const ACCENT = '#e11d48'

  return (
    <div className="card" style={{ padding: '20px 24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fontWeight: 700, color: ACCENT, letterSpacing: '.1em' }}>{num}</span>
            <span style={{ fontWeight: 700, fontSize: 15.5 }}>{title}</span>
          </div>
          <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', marginTop: 3, lineHeight: 1.5 }}>{subtitle}</div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexShrink: 0 }}>
          {player.started && (
            <>
              {player.running && (
                <button className="btn btn-default btn-xs" onClick={player.togglePause}>
                  {player.paused ? '▶ Retomar' : '⏸ Pausar'}
                </button>
              )}
              <SpeedControl speed={player.speed} setSpeed={player.setSpeed} />
            </>
          )}
          <button className="btn btn-primary" style={{ fontSize: 13, padding: '9px 20px' }} onClick={player.play} disabled={player.running}>
            {player.running ? 'Rodando…' : (player.started ? '↺ Rodar de novo' : '▶ Ver o caminho do dado')}
          </button>
        </div>
      </div>

      <div className="flow-seeing" style={{ marginTop: 12 }}>👁️ {seeing}</div>

      <div className="flow-wrap">
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap', marginBottom: 4, fontSize: 11, color: 'var(--text-secondary)' }}>
          <span>Topo: Redis — <span style={{ color: C.orange }}>● aviso</span> + <span style={{ color: C.cyan }}>● dado (banco)</span> + 🔄 CDC/sync · Base: MongoDB — <span style={{ color: C.green }}>● 1 gravação (CDC nativo)</span></span>
        </div>
        <FlowStage nodes={FLOW.nodes} lanes={FLOW.lanes} wires={FLOW.wires} tokens={FLOW.tokens}
          view={player.view} speed={player.speed} height={FLOW.height} />
        <div className="flow-caption" style={{ marginTop: 30 }}>
          {player.view.caption || <span className="fc-dim">Clique “▶ Ver o caminho do dado” para ver as duas escritas do Redis — e a gravação única do MongoDB.</span>}
        </div>
        <StatusRow items={FLOW.statusItems} status={player.view.status} />
      </div>

      {/* Objeção honesta que um cliente Pró-Redis vai levantar */}
      <div style={{ marginTop: 14, padding: '13px 16px', borderRadius: 9, background: 'rgba(6,182,212,.05)', border: '1px solid rgba(6,182,212,.28)' }}>
        <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 6, color: '#06b6d4' }}>“E o Redis Streams? Não é durável?”</div>
        <div style={{ fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.6 }}>
          É durável, sim — diferente do Pub/Sub, o Stream persiste e é recuperável. Mas ele <strong>não resolve o ponto acima</strong>: o
          Stream é um <strong>2º sistema, separado da sua fonte de verdade</strong>. Você continua fazendo <strong>duas escritas</strong> (o
          dado + o evento) e precisa mantê-las atômicas (transactional outbox, reconciliação). No MongoDB o evento <strong>deriva do próprio
          commit do dado</strong> — não existe segunda escrita para divergir.
        </div>
        <div style={{ fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.6, marginTop: 8 }}>
          <strong>“E o Kafka?”</strong> — Complementar, não concorrente: o ponto aqui é <strong>onde nasce o evento</strong>.
          Com o change stream, o evento nasce do commit da fonte de verdade — e pode alimentar o Kafka via o{' '}
          <strong>MongoDB Kafka Connector</strong> oficial, sem operar um CDC de terceiros (Debezium) para extrair eventos do banco.
          O que a demo compara é a 2ª escrita que pode divergir vs o commit único.
        </div>
      </div>
    </div>
  )
}
