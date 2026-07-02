import React, { useState, useRef, useEffect, useCallback } from 'react'
import { useApi } from '../hooks/useApi'
import { SpeedControl } from './DemoFlow'

const brl = (v) => 'R$ ' + Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2 })

// Cor de cada linha/pill a partir da "tag" que o backend devolve.
const TONE = {
  'INCONSISTENTE': 'red', 'PERDIDO': 'red',
  'sinal': 'amber',
  'durável': 'green', 'consistente': 'green', 'avisado': 'green', 'recuperado': 'green',
}
const toneOf = (tag) => TONE[tag] || 'green'

// ─────────────────────────────────────────────────────────────────────────
// Uma coluna (Redis ou MongoDB): placar que acumula ao vivo + feed de linhas.
function Column({ side, title, icon, ops, score }) {
  const feedRef = useRef(null)
  useEffect(() => {
    if (feedRef.current) feedRef.current.scrollTop = feedRef.current.scrollHeight
  }, [ops.length])

  const chips = score(ops)   // [{ label, value, tone }]

  return (
    <div className={`battle-col ${side}`}>
      <div className="battle-head">{icon} {title}</div>
      <div className="score-row">
        {chips.map((c, i) => (
          <div key={i} className={`score-pill ${c.tone || 'idle'}`}>
            <div className="sp-val">{c.value}</div>
            <div className="sp-lbl">{c.label}</div>
          </div>
        ))}
      </div>
      <div className="battle-feed" ref={feedRef}>
        {ops.length === 0 && <div className="battle-empty">Aguardando operações…</div>}
        {ops.map((o, i) => {
          const s = o[side]
          const tone = toneOf(s.tag)
          return (
            <div key={i} className={`battle-row ${tone}`}>
              <span className="battle-idx">#{o.i}</span>
              <span className={`battle-pill ${tone}`}>{s.tag.toUpperCase()}</span>
              <div className="battle-main">
                <div className="bm-t">{o.tipo_label} · {brl(o.valor)}</div>
                <div className="bm-n">{s.note}</div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// LiveBattle — CTA → busca o lote → revela op a op nas 2 colunas ao vivo.
// Props: num, title, subtitle, seeing, cta, endpoint, scoreRedis, scoreMongo.
export default function LiveBattle({ num, title, subtitle, seeing, cta, endpoint, scoreRedis, scoreMongo }) {
  const { call } = useApi()
  const [phase, setPhase] = useState('idle')     // idle | running | done
  const [data, setData] = useState(null)
  const [shown, setShown] = useState(0)          // nº de ops reveladas
  const [speed, setSpeed] = useState(1)
  const timer = useRef(null)

  useEffect(() => () => clearTimeout(timer.current), [])

  // Revela uma op de cada vez enquanto rodando.
  useEffect(() => {
    if (phase !== 'running' || !data) return
    if (shown >= data.ops.length) { setPhase('done'); return }
    timer.current = setTimeout(() => setShown(s => s + 1), 430 * speed)
    return () => clearTimeout(timer.current)
  }, [phase, data, shown, speed])

  const run = useCallback(async () => {
    clearTimeout(timer.current)
    setPhase('running'); setData(null); setShown(0)
    const r = await call(endpoint, { method: 'POST' })
    if (!r) { setPhase('idle'); return }
    setData(r); setShown(0)
  }, [call, endpoint])

  const ACCENT = '#e11d48'
  const ops = data ? data.ops.slice(0, shown) : []
  const running = phase === 'running' && (!data || shown < data.ops.length)

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
          {(phase !== 'idle') && <SpeedControl speed={speed} setSpeed={setSpeed} />}
          <button className="btn btn-primary" style={{ fontSize: 13, padding: '9px 20px' }} onClick={run} disabled={running}>
            {running ? 'Rodando…' : (phase === 'done' ? '↺ Rodar de novo' : (cta || '▶ Rodar 20 operações'))}
          </button>
        </div>
      </div>

      <div className="flow-seeing" style={{ marginTop: 12 }}>👁️ {seeing}</div>

      {phase === 'idle' ? (
        <div style={{ textAlign: 'center', padding: '22px 0 6px', color: 'var(--text-secondary)', fontSize: 13 }}>
          Clique em <strong style={{ color: 'var(--text-primary)' }}>{cta || 'Rodar 20 operações'}</strong> — cada transação vai aparecer
          nas duas colunas ao vivo, e o placar embaixo vai contando a diferença.
        </div>
      ) : (
        <>
          <div className="battle-grid">
            <Column side="redis" title="Redis (Pub/Sub + dual-write)" icon="🟥" ops={ops} score={scoreRedis} />
            <Column side="mongo" title="MongoDB Change Streams" icon="🍃" ops={ops} score={scoreMongo} />
          </div>
          {phase === 'done' && data && (
            <div className="verdict">🏁 {data.placar.veredito}</div>
          )}
        </>
      )}
    </div>
  )
}
