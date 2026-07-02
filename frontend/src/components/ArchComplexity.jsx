import React from 'react'
import { SpeedControl, useFlowPlayer } from './DemoFlow'

// Etapa 1 — "conte os sistemas". O ponto intuitivo e IRREFUTÁVEL: o Redis não
// SUBSTITUI seu banco, é SOMADO por cima. MongoDB = 1 sistema (aviso nativo);
// Redis = banco durável + Redis = 2 sistemas para operar e pagar.
const STEPS = [
  { caption: 'Para "reagir a uma mudança de dado com garantia", conte os SISTEMAS que cada arquitetura obriga você a operar.',
    set: { m1: { tone: 'idle' }, r1: { tone: 'idle' }, r2: { tone: 'idle' } } },
  { caption: <>MongoDB: <b>1 sistema</b>. O banco é a fonte de verdade <b>e</b> o change stream (o aviso) <b>já vem embutido</b>.</>,
    set: { m1: { tone: 'done' } } },
  { caption: <>Redis não é system-of-record — então você continua operando um <b>banco durável</b> (a sua fonte de verdade). <b>Sistema 1.</b></>,
    set: { r1: { tone: 'done' } } },
  { caption: <>Para o aviso rápido, você adiciona o <b>Redis</b> — <b>+1 sistema</b> para provisionar, monitorar, escalar e pagar. <b>Sistema 2.</b></>,
    set: { r2: { tone: 'done' } } },
  { caption: <><b>MongoDB: 1 sistema</b> (o aviso já vem no banco). <b>Redis: 2 sistemas</b> — porque o Redis é <b>somado</b> à sua stack, não subtraído dela.</>,
    set: {} },
]

function Layer({ st, icon, title, sub, badge }) {
  const tone = st?.tone || 'idle'
  return (
    <div className={`stack-layer ${tone}`}>
      <span className="stack-ic">{icon}</span>
      <div className="stack-l-main">
        <div className="stack-l-t">{title} {badge && <span style={{ fontSize: 10, color: '#ff6960', fontWeight: 800 }}>{badge}</span>}</div>
        <div className="stack-l-s">{sub}</div>
      </div>
      <span className="stack-check">✓</span>
    </div>
  )
}

const done = (st) => st && st.tone === 'done'

export default function ArchComplexity({ num, title, subtitle, seeing }) {
  const player = useFlowPlayer(STEPS, { baseMs: 1250 })
  const s = player.view.status
  const ACCENT = '#e11d48'
  const pecasRedis = [s.r1, s.r2].filter(done).length
  const pecasMongo = done(s.m1) ? 1 : 0

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
            {player.running ? 'Rodando…' : (player.started ? '↺ Rodar de novo' : '▶ Contar os sistemas')}
          </button>
        </div>
      </div>

      <div className="flow-seeing" style={{ marginTop: 12 }}>👁️ {seeing}</div>

      <div className="stack-wrap">
        {/* MongoDB: 1 sistema */}
        <div className="stack-col mongo">
          <div className="stack-title">🍃 MongoDB — o aviso já vem no banco</div>
          <Layer st={s.m1} icon="🍃" title="MongoDB" sub="fonte de verdade + change stream (o aviso) nativo, no mesmo sistema" />
          <div style={{ fontSize: 11.5, color: 'var(--text-secondary)', lineHeight: 1.55, padding: '8px 2px 2px', minHeight: 84 }}>
            Nada para adicionar: reagir à mudança de dado é uma capacidade do banco que você já tem. Um só sistema para operar.
          </div>
          <div className="stack-foot">
            <div className={`stack-badge ${pecasMongo ? 'green' : 'idle'}`}>
              <div className="sb-v">{pecasMongo}</div><div className="sb-l">sistema para operar</div>
            </div>
          </div>
        </div>

        {/* Redis: 2 sistemas */}
        <div className="stack-col redis">
          <div className="stack-title">🟥 Redis — somado à sua stack</div>
          <Layer st={s.r1} icon="🗄️" title="Banco durável" sub="a fonte de verdade — você já opera" />
          <Layer st={s.r2} icon="🟥" title="Redis" badge="+1 sistema" sub="cluster: provisionar · monitorar · escalar · pagar" />
          <div style={{ minHeight: 12 }} />
          <div className="stack-foot">
            <div className={`stack-badge ${pecasRedis >= 2 ? 'red' : (pecasRedis ? 'amber' : 'idle')}`}>
              <div className="sb-v">{pecasRedis}</div><div className="sb-l">sistemas para operar</div>
            </div>
          </div>
        </div>
      </div>

      <div className="flow-caption" style={{ marginTop: 12 }}>
        {player.view.caption || <span className="fc-dim">Clique “▶ Contar os sistemas” para ver a arquitetura de cada lado surgir passo a passo.</span>}
      </div>
    </div>
  )
}
