import React, { useState, useEffect, useRef } from 'react'
import { useApi } from '../hooks/useApi'

const OP_STYLE = {
  insert: { bg: 'rgba(0,237,100,.08)', border: 'rgba(0,237,100,.3)', text: '#00ED64', label: 'NOVA TRANSAÇÃO' },
  update: { bg: 'rgba(6,182,212,.08)', border: 'rgba(6,182,212,.3)', text: '#06b6d4', label: 'ATUALIZAÇÃO'    },
  delete: { bg: 'rgba(255,105,96,.08)', border: 'rgba(255,105,96,.35)', text: '#ff6960', label: 'REMOVIDA'       },
  ERROR:  { bg: 'rgba(255,105,96,.08)', border: 'rgba(255,105,96,.35)', text: '#ff6960', label: 'ERRO'           },
}

// 20 operações com ritmo variado para simular tráfego real
const DEMO_SEQUENCE = [
  { op: 'insert', delay: 800  },
  { op: 'insert', delay: 1600 },
  { op: 'insert', delay: 2200 },
  { op: 'update', delay: 2900 },
  { op: 'insert', delay: 3500 },
  { op: 'insert', delay: 4100 },
  { op: 'update', delay: 4600 },
  { op: 'insert', delay: 5200 },
  { op: 'update', delay: 5700 },
  { op: 'insert', delay: 6300 },
  { op: 'delete', delay: 6900 },
  { op: 'insert', delay: 7500 },
  { op: 'insert', delay: 8000 },
  { op: 'update', delay: 8600 },
  { op: 'insert', delay: 9200 },
  { op: 'update', delay: 9700 },
  { op: 'insert', delay: 10300 },
  { op: 'delete', delay: 10800 },
  { op: 'insert', delay: 11400 },
  { op: 'update', delay: 12000 },
]

export default function ChangeStreams() {
  const { call } = useApi()
  const [phase,    setPhase]    = useState('idle')
  const [events,   setEvents]   = useState([])
  const [collection, setCollection] = useState(null)
  const feedRef  = useRef(null)
  const timerRef = useRef([])
  const listRef  = useRef(null)

  useEffect(() => {
    if (listRef.current) listRef.current.scrollTop = listRef.current.scrollHeight
  }, [events])

  useEffect(() => () => {
    feedRef.current?.close()
    timerRef.current.forEach(clearTimeout)
  }, [])

  const startDemo = async () => {
    feedRef.current?.close()
    timerRef.current.forEach(clearTimeout)
    timerRef.current = []
    setPhase('running')
    setEvents([])
    const res = await call('/change-streams/start', { method: 'POST' })
    if (!res) { setPhase('idle'); return }

    const feed = new EventSource('/api/change-streams/feed')
    feed.onmessage = (e) => {
      try { setEvents(prev => [...prev, JSON.parse(e.data)]) } catch { /* keepalive */ }
    }
    feedRef.current = feed

    DEMO_SEQUENCE.forEach(({ op, delay }) => {
      const t = setTimeout(() => call(`/change-streams/trigger?operacao=${op}`, { method: 'POST' }), delay)
      timerRef.current.push(t)
    })

    const lastDelay = DEMO_SEQUENCE[DEMO_SEQUENCE.length - 1].delay + 1800
    const tStop = setTimeout(async () => {
      await call('/change-streams/stop', { method: 'POST' })
      feedRef.current?.close()
      const col = await call('/change-streams/collection')
      if (col) setCollection(col)
      setPhase('done')
    }, lastDelay)
    timerRef.current.push(tStop)
  }

  const reset = async () => {
    feedRef.current?.close()
    timerRef.current.forEach(clearTimeout)
    timerRef.current = []
    setPhase('stopping')
    setEvents([])
    setCollection(null)
    const stopped = await call('/change-streams/stop', { method: 'POST' })
    setPhase(stopped ? 'idle' : 'running')
  }

  const clearCollection = async () => {
    await call('/change-streams/clear', { method: 'DELETE' })
    setCollection(null)
  }

  const insertCount = events.filter(e => e.operation === 'insert').length
  const updateCount = events.filter(e => e.operation === 'update').length
  const alertCount  = events.filter(e => e.alerta).length

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Concept */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 10 }}>O que são Change Streams?</div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, fontSize: 13 }}>
          <div style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 6, border: '1px solid var(--border-color)' }}>
            <div style={{ fontWeight: 600, marginBottom: 6, color: 'var(--text-secondary)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '.05em' }}>Sem Change Streams</div>
            <div style={{ lineHeight: 1.65, color: 'var(--text-primary)' }}>
              O serviço de antifraude precisa ficar perguntando ao banco a cada N segundos:
              <em> "chegou alguma transação nova?"</em> — polling lento, caro e defasado.
            </div>
          </div>
          <div style={{ padding: '12px 14px', background: 'var(--bg-card)', borderRadius: 6, border: '2px solid #00ED64' }}>
            <div style={{ fontWeight: 600, marginBottom: 6, color: '#00ED64', fontSize: 11, textTransform: 'uppercase', letterSpacing: '.05em' }}>Com Change Streams</div>
            <div style={{ lineHeight: 1.65, color: 'var(--text-primary)' }}>
              O serviço se inscreve <strong>uma vez</strong> e recebe as mudanças em tempo quase real,
              sem consultar repetidamente a coleção.
            </div>
          </div>
        </div>
        <div style={{ marginTop: 12, fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.65, padding: '10px 12px', background: 'var(--bg-subtle)', borderRadius: 6 }}>
          💡 <strong>Exemplo:</strong> pagamento inserido → Change Stream publica a mudança →
          consumidores independentes podem acionar antifraude, notificação e auditoria sem polling no MongoDB.
          Nesta tela, o feed chega do backend por SSE.
        </div>
      </div>

      {/* Demo card */}
      <div className="card" style={{ padding: '20px 24px' }}>
        {phase === 'idle' && (
          <div style={{ textAlign: 'center', padding: '12px 0' }}>
            <div style={{ fontSize: 36, marginBottom: 10 }}>📡</div>
            <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 8 }}>Simulação: monitor de transações financeiras</div>
            <div style={{ fontSize: 13.5, color: 'var(--text-secondary)', maxWidth: 480, margin: '0 auto 20px', lineHeight: 1.7 }}>
              O MongoDB abre um change stream na coleção <code>transacoes_cs_demo</code>.
              Em seguida, <strong>20 operações</strong> são disparadas automaticamente simulando
              transações Pix, TED e cartão — e você vê cada evento chegar ao vivo.
            </div>
            <button className="btn btn-primary" style={{ fontSize: 14, padding: '10px 28px' }} onClick={startDemo}>
              ▶ Iniciar simulação
            </button>
          </div>
        )}

        {(phase === 'running' || phase === 'done') && (
          <div>
            {/* Header */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                {phase === 'running'
                  ? <><span className="spinner" style={{ borderTopColor: '#00ED64', width: 16, height: 16, borderWidth: 2.5 }} />
                      <span style={{ fontWeight: 600, fontSize: 14, color: '#00ED64' }}>Change stream ativo…</span></>
                  : <><span style={{ fontSize: 16 }}>✅</span>
                      <span style={{ fontWeight: 600, fontSize: 14 }}>Simulação concluída</span></>
                }
              </div>
              <button className="btn btn-default btn-sm" onClick={reset}>
                {phase === 'done' ? '↺ Rodar de novo' : 'Parar'}
              </button>
            </div>

            {/* Counters */}
            <div style={{ display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
              {[
                { label: 'Novas transações', value: insertCount, color: '#00ED64', bg: 'rgba(0,237,100,.08)', border: 'rgba(0,237,100,.3)' },
                { label: 'Atualizações',     value: updateCount, color: '#06b6d4', bg: 'rgba(6,182,212,.08)', border: 'rgba(6,182,212,.3)' },
                { label: '⚠️ Suspeitas',      value: alertCount,  color: '#f97316', bg: 'rgba(249,115,22,.08)', border: 'rgba(249,115,22,.4)' },
                { label: 'Total capturado',  value: events.length, color: 'var(--text-primary)', bg: 'var(--bg-subtle)', border: 'var(--border-color)' },
              ].map(c => (
                <div key={c.label} style={{ padding: '8px 14px', borderRadius: 6, background: c.bg, border: `1px solid ${c.border}`, textAlign: 'center' }}>
                  <div style={{ fontWeight: 700, fontSize: 20, color: c.color }}>{c.value}</div>
                  <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 1 }}>{c.label}</div>
                </div>
              ))}
              {phase === 'running' && (
                <div style={{ padding: '8px 14px', borderRadius: 6, background: 'var(--bg-subtle)', border: '1px solid var(--border-color)', fontSize: 11, color: 'var(--text-secondary)', alignSelf: 'center' }}>
                  {Math.max(0, DEMO_SEQUENCE.length - events.length)} operações restantes…
                </div>
              )}
            </div>

            {/* Event feed */}
            <div ref={listRef} style={{ display: 'flex', flexDirection: 'column', gap: 7, maxHeight: 380, overflowY: 'auto' }}>
              {events.length === 0 && (
                <div style={{ textAlign: 'center', padding: '28px 0', color: 'var(--text-secondary)', fontSize: 13 }}>
                  Aguardando primeiros eventos…
                </div>
              )}
              {events.map((ev, i) => {
                const s = OP_STYLE[ev.operation] || OP_STYLE.insert
                return (
                  <div key={i} style={{
                    display: 'flex', gap: 10, alignItems: 'center', padding: '9px 12px',
                    borderRadius: 6, background: ev.alerta ? 'rgba(249,115,22,.08)' : s.bg,
                    border: `1px solid ${ev.alerta ? 'rgba(249,115,22,.4)' : s.border}`,
                  }}>
                    <span style={{
                      padding: '2px 7px', borderRadius: 3, fontSize: 10, fontWeight: 800,
                      letterSpacing: '.05em', flexShrink: 0,
                      color: ev.alerta ? '#f97316' : s.text,
                      background: ev.alerta ? 'rgba(249,115,22,.4)' : s.border,
                    }}>
                      {ev.alerta ? '⚠️ SUSPEITA' : s.label}
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-primary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {ev.texto}
                      </div>
                      {ev.detalhe && (
                        <div style={{ fontSize: 11.5, color: 'var(--text-secondary)', marginTop: 1 }}>{ev.detalhe}</div>
                      )}
                    </div>
                    <span style={{ fontSize: 11, color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)', flexShrink: 0 }}>
                      {ev.ts}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>

      {/* Documentos reais persistidos na coleção */}
      {phase === 'done' && collection && (
        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6, flexWrap: 'wrap', gap: 8 }}>
            <div style={{ fontWeight: 700, fontSize: 15 }}>
              Documentos reais gravados na coleção
            </div>
            <button className="btn btn-default btn-sm" onClick={clearCollection}>🗑 Limpar coleção</button>
          </div>
          <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.6, marginBottom: 14 }}>
            Estes <strong>{collection.total}</strong> documentos estão persistidos em{' '}
            <code>{collection.db}.{collection.colecao}</code>. Não é algo só da tela — abra o{' '}
            <strong>Atlas Data Explorer</strong> nessa coleção e você verá exatamente os mesmos registros.
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table className="lg-table">
              <thead>
                <tr><th>transacao_id</th><th>pagador → recebedor</th><th>valor</th><th>tipo</th><th>status</th></tr>
              </thead>
              <tbody>
                {collection.documentos.slice(0, 12).map((d, i) => (
                  <tr key={i}>
                    <td><code style={{ fontSize: 11 }}>{(d.transacao_id || '').slice(0, 8)}…</code></td>
                    <td>{d.pagador} → {d.recebedor}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {Number(d.valor).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                    <td>{d.tipo}</td>
                    <td>
                      <span className={`badge ${d.status === 'recusada' ? 'badge-red' : d.status === 'pendente' ? 'badge-yellow' : 'badge-green'}`}>
                        {d.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {collection.total > 12 && (
            <div style={{ fontSize: 11.5, color: 'var(--text-secondary)', marginTop: 8 }}>
              Mostrando 12 de {collection.total} documentos.
            </div>
          )}
        </div>
      )}

      {/* How it works */}
      <div className="card" style={{ padding: '16px 18px' }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10 }}>Capacidades do Change Streams</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 10 }}>
          {[
            { icon: '📄', title: 'Pre/post-image',            desc: 'Mostra o estado anterior e posterior quando o recurso está habilitado e a imagem ainda está dentro de sua retenção' },
            { icon: '🔁', title: 'Resume token',             desc: 'Permite retomar dentro da janela do oplog. Persistência, reentrega e idempotência são demonstradas no módulo 07' },
            { icon: '🔍', title: 'Pipeline de filtro',       desc: 'Escuta apenas os eventos relevantes com $match e $project, reduzindo tráfego' },
            { icon: '🌐', title: 'Replica sets & sharded',   desc: 'Suportado em replica sets e sharded clusters; quantidade de cursores, pool e oplog entram no desenho de produção' },
          ].map(c => (
            <div key={c.title} style={{ padding: '12px 14px', background: 'var(--bg-subtle)', borderRadius: 6, border: '1px solid var(--border-color)' }}>
              <div style={{ fontSize: 18, marginBottom: 6 }}>{c.icon}</div>
              <div style={{ fontWeight: 600, fontSize: 12.5, marginBottom: 4 }}>{c.title}</div>
              <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.55 }}>{c.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
