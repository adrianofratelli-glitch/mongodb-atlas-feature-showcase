import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'

// ─────────────────────────────────────────────────────────────────────────
// Tooltip — mantém o termo técnico, explica em 1 linha ao passar o mouse.
export function Tooltip({ children, text }) {
  return (
    <span className="tt">
      {children}
      <span className="tt-body">{text}</span>
    </span>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// Controle de velocidade — Normal vs Câmera lenta (útil pra pausar na
// inconsistência durante uma apresentação ao vivo).
export function SpeedControl({ speed, setSpeed, disabled }) {
  return (
    <div className="speed-ctl" role="group" aria-label="Velocidade da animação">
      <button className={speed === 1 ? 'on' : ''} disabled={disabled} onClick={() => setSpeed(1)}>▶ Normal</button>
      <button className={speed === 3 ? 'on' : ''} disabled={disabled} onClick={() => setSpeed(3)}>🐢 Câmera lenta</button>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// useFlowPlayer — roda uma sequência de "steps" com pausa entre cada um.
// step = { delay?, caption?, pos?:{tokenId:nodeId|null}, dead?:[nodeId|tokenId],
//          set?:{statusKey:{text,tone}}, flash?:nodeId }
// Cada step é MESCLADO no estado acumulado (não é um snapshot completo), então
// só declaramos o que muda. Um re-render por step — o movimento é puro CSS.
export function useFlowPlayer(steps, { baseMs = 560 } = {}) {
  const [idx, setIdx] = useState(-1)
  const [running, setRunning] = useState(false)
  const [paused, setPaused] = useState(false)
  const [speed, setSpeed] = useState(1)
  const timer = useRef(null)

  useEffect(() => {
    if (!running || paused) return
    if (idx >= steps.length - 1) { setRunning(false); return }
    const next = steps[idx + 1]
    const d = (next.delay ?? baseMs) * speed
    timer.current = setTimeout(() => setIdx(i => i + 1), d)
    return () => clearTimeout(timer.current)
  }, [running, paused, idx, steps, speed, baseMs])

  const play = useCallback(() => { clearTimeout(timer.current); setPaused(false); setIdx(0); setRunning(true) }, [])
  const reset = useCallback(() => { clearTimeout(timer.current); setRunning(false); setPaused(false); setIdx(-1) }, [])
  const togglePause = useCallback(() => setPaused(p => !p), [])

  // Estado visual acumulado até o step atual.
  const view = useMemo(() => {
    const pos = {}, status = {}, dead = new Set()
    let caption = null, flash = null
    for (let i = 0; i <= idx; i++) {
      const s = steps[i]; if (!s) continue
      if (s.pos) Object.assign(pos, s.pos)
      if (s.set) Object.assign(status, s.set)
      if (s.dead) s.dead.forEach(d => dead.add(d))
      if (s.revive) s.revive.forEach(d => dead.delete(d))
      if (s.caption !== undefined) caption = s.caption
      flash = s.flash ?? null
    }
    return { pos, status, dead, caption, flash, done: idx >= steps.length - 1 && idx >= 0 }
  }, [idx, steps])

  return { play, reset, togglePause, running, paused, started: idx >= 0, speed, setSpeed, view }
}

// ─────────────────────────────────────────────────────────────────────────
// FlowStage — desenha o diagrama de sequência: nós posicionados por (col, lane),
// fios em SVG e tokens que animam de nó a nó via transição de left/top.
const PAD_X = 8, SPAN_X = 84   // percentuais: mantém margem nas laterais

export function FlowStage({ nodes, lanes, wires = [], tokens, view, speed, height = 160 }) {
  const maxCol = Math.max(1, ...nodes.map(n => n.col))
  const xPct = (col) => PAD_X + (col / maxCol) * SPAN_X
  const yPct = (lane) => ((lane + 0.5) / lanes) * 100
  const byId = useMemo(() => Object.fromEntries(nodes.map(n => [n.id, n])), [nodes])
  const coord = (id) => { const n = byId[id]; return n ? { x: xPct(n.col), y: yPct(n.lane) } : null }
  const tokDur = (0.5 * speed).toFixed(2) + 's'

  return (
    <div className="flow-track" style={{ height }}>
      <svg className="flow-svg" viewBox="0 0 100 100" preserveAspectRatio="none">
        {wires.map((w, i) => {
          const a = coord(w.from), b = coord(w.to)
          if (!a || !b) return null
          return (
            <line key={i} x1={a.x} y1={a.y} x2={b.x} y2={b.y}
              stroke={w.color || 'rgba(255,255,255,.13)'} strokeWidth={w.width || 1.5}
              strokeDasharray={w.dashed ? '4 3' : undefined} vectorEffect="non-scaling-stroke" />
          )
        })}
      </svg>

      {nodes.map(n => {
        const dead = view.dead.has(n.id)
        return (
          <div key={n.id}
            className={`flow-node${view.flash === n.id ? ' flash' : ''}${dead ? ' dead' : ''}`}
            style={{ left: `${xPct(n.col)}%`, top: `${yPct(n.lane)}%`, opacity: n.dim ? 0.4 : 1 }}>
            <div className="fn-box" style={n.boxStyle}>{n.icon}</div>
            <div className="fn-cap">
              <div className="fn-label">{n.label}</div>
              {n.sub && <div className="fn-sub">{n.sub}</div>}
            </div>
          </div>
        )
      })}

      {tokens.map(t => {
        const at = view.pos[t.id]
        const c = at ? coord(at) : null
        const dead = view.dead.has(t.id)
        return (
          <div key={t.id} className={`flow-token${dead ? ' dead' : ''}`}
            style={{
              left: c ? `${c.x}%` : '50%', top: c ? `${c.y}%` : '50%',
              color: t.color, background: t.color,
              opacity: c && !dead ? 1 : (dead ? undefined : 0),
              '--tok-dur': tokDur,
            }} />
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// StatusRow — chips que revelam o estado progressivamente, mudando de cor no
// momento exato em que o "evento" acontece.
export function StatusRow({ items, status }) {
  return (
    <div className="status-row">
      {items.map(({ key, label }) => {
        const s = status[key]
        const tone = s?.tone || 'idle'
        return (
          <span key={key} className={`status-chip ${tone}`}>
            <span className="sc-dot" />
            {label}: <b>{s?.text ?? '—'}</b>
          </span>
        )
      })}
    </div>
  )
}
