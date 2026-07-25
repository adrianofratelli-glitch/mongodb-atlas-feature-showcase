import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useApi } from '../hooks/useApi'
import QueryBlock from '../components/QueryBlock'

const MAX_FEED = 60

// Hook de SSE: EventSource sobre /api/... (proxy do Vite → backend :8002).
// Só GET, então o mutation guard não exige o X-Demo-Token.
function useSse(path, onMessage, enabled = true) {
  const [connected, setConnected] = useState(false)
  const handler = useRef(onMessage)
  handler.current = onMessage

  useEffect(() => {
    if (!enabled) return undefined
    const es = new EventSource(`/api${path}`)
    es.onopen = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.onmessage = (e) => {
      try { handler.current(JSON.parse(e.data)) } catch { /* keepalive ou frame parcial */ }
    }
    return () => { es.close(); setConnected(false) }
  }, [path, enabled])

  return connected
}

const push = (list, item) => [item, ...list].slice(0, MAX_FEED)
const fmtMs = (v) => (v == null ? '—' : `${v} ms`)
const fmtBRL = (v) => (v == null ? '—' : Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }))

function Stat({ label, value, sub, color }) {
  return (
    <div className="str-stat">
      <div className="str-stat-l">{label}</div>
      <div className="str-stat-v" style={color ? { color } : undefined}>{value}</div>
      {sub && <div className="str-stat-s">{sub}</div>}
    </div>
  )
}

function NotConfigured({ title, detalhe, passos, children }) {
  return (
    <div className="str-offline">
      <div className="str-offline-t">⚠️ {title}</div>
      <div className="str-offline-d">{detalhe}</div>
      <ol className="str-offline-steps">
        {passos.map((p, i) => <li key={i}>{p}</li>)}
      </ol>
      {children}
    </div>
  )
}

// Sparkline de qtd por janela — SVG puro, sem dependência nova.
function Sparkline({ values }) {
  if (values.length < 2) return <div className="str-spark-empty">aguardando janelas…</div>
  const max = Math.max(...values, 1)
  const step = 100 / (values.length - 1)
  const points = values.map((v, i) => `${(i * step).toFixed(2)},${(28 - (v / max) * 26).toFixed(2)}`).join(' ')
  return (
    <svg className="str-spark" viewBox="0 0 100 28" preserveAspectRatio="none" aria-label="Quantidade por janela">
      <polyline points={points} fill="none" stroke="#a855f7" strokeWidth="1.4" vectorEffect="non-scaling-stroke" />
    </svg>
  )
}

export default function Streaming() {
  const { call } = useApi()

  // ── Gerador ──────────────────────────────────────────────────────────────
  const [tps, setTps] = useState(50)
  const [gen, setGen] = useState(null)

  const refreshGen = useCallback(async () => {
    const data = await call('/streaming/generator/status')
    if (data) setGen(data)
  }, [call])

  useEffect(() => {
    refreshGen()
    const t = setInterval(refreshGen, 1000)
    return () => clearInterval(t)
  }, [refreshGen])

  const startGen = async () => {
    await call('/streaming/generator/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tps }),
    })
    refreshGen()
  }

  const stopGen = async () => { await call('/streaming/generator/stop', { method: 'POST' }); refreshGen() }

  // ── Coluna 1 — Change Streams ────────────────────────────────────────────
  const [csEvents, setCsEvents] = useState([])
  const [csState, setCsState] = useState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })

  const csConnected = useSse('/streaming/changestream', (msg) => {
    if (msg.type === 'evento') {
      setCsEvents((prev) => push(prev, msg))
      setCsState((s) => ({ ...s, eventos: msg.eventos, recuperados: msg.recuperados, token: msg.token, fase: 'ativo' }))
    } else if (msg.type === 'derrubado') {
      setCsState((s) => ({ ...s, fase: 'derrubado', token: msg.token }))
    } else if (msg.type === 'aberto') {
      setCsState((s) => ({ ...s, fase: msg.retomado ? 'retomado' : 'ativo', token: msg.token }))
    } else if (msg.type === 'hello') {
      setCsState((s) => ({ ...s, eventos: msg.eventos ?? 0, recuperados: msg.recuperados ?? 0, token: msg.token }))
    } else if (msg.type === 'reset') {
      setCsEvents([]); setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    }
  })

  const dropResume = async () => { await call('/streaming/changestream/drop-resume', { method: 'POST' }) }

  // ── Coluna 2 — Kafka Connector ───────────────────────────────────────────
  const [kafkaMsgs, setKafkaMsgs] = useState([])
  const [kafkaStatus, setKafkaStatus] = useState(null)

  useSse('/streaming/kafka', (msg) => {
    if (msg.type === 'mensagem') setKafkaMsgs((prev) => push(prev, msg))
    else if (msg.type === 'reset') setKafkaMsgs([])
  })

  useEffect(() => {
    const tick = async () => { const d = await call('/streaming/kafka/status'); if (d) setKafkaStatus(d) }
    tick()
    const t = setInterval(tick, 4000)
    return () => clearInterval(t)
  }, [call])

  const connectorState = kafkaStatus?.connector?.estado
  const kafkaOk = connectorState === 'RUNNING'

  // ── Coluna 3 — Atlas Stream Processing ───────────────────────────────────
  const [janelas, setJanelas] = useState([])
  const [dlq, setDlq] = useState([])
  const [aspStatus, setAspStatus] = useState(null)

  useSse('/streaming/asp', (msg) => {
    if (msg.type === 'janela') setJanelas((prev) => push(prev, msg))
    else if (msg.type === 'dlq') setDlq((prev) => push(prev, msg))
    else if (msg.type === 'reset') { setJanelas([]); setDlq([]) }
  })

  useEffect(() => {
    const tick = async () => { const d = await call('/streaming/asp/status'); if (d) setAspStatus(d) }
    tick()
    const t = setInterval(tick, 5000)
    return () => clearInterval(t)
  }, [call])

  const aspOk = aspStatus?.estado === 'configurado'
  const injectInvalid = async () => { await call('/streaming/asp/inject-invalid', { method: 'POST' }) }

  // ── Reset global ─────────────────────────────────────────────────────────
  const resetAll = async () => {
    await call('/streaming/reset', { method: 'POST' })
    setCsEvents([]); setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    setKafkaMsgs([]); setJanelas([]); setDlq([])
    refreshGen()
  }

  const tpsDesvio = gen?.tps_alvo ? Math.round(Math.abs(gen.tps_medido - gen.tps_alvo) / gen.tps_alvo * 100) : null

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Enredo */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 10 }}>Três formas de reagir a uma mudança — o mesmo fluxo de escritas</div>
        <div style={{ fontSize: 13, color: 'var(--text-primary)', lineHeight: 1.65 }}>
          Um único gerador escreve em <code>pix.transacoes</code>. As três colunas abaixo consomem <strong>a mesma
          mudança</strong> por caminhos diferentes: dentro da aplicação (Change Streams), no barramento
          (MongoDB Kafka Connector) e num serviço gerenciado com janela e estado (Atlas Stream Processing).
          A pergunta não é qual é “melhor” — é <em>qual pergunta cada um responde</em>.
        </div>
        <div style={{ marginTop: 12, fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
          🔎 <strong>Nada aqui é sintético:</strong> todo número vem de uma operação real contra o Atlas ou o Kafka.
          Componente não configurado aparece como <em>“não configurado”</em>, com o passo a passo de setup — nunca com dado inventado.
        </div>
      </div>

      {/* Gerador */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 14 }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Gerador de transações</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 2 }}>
              <code>{gen?.colecao || 'pix.transacoes'}</code> · micro-batches a cada 100 ms · TTL de 2 h em <code>ts</code>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {gen?.running
              ? <button className="btn btn-danger btn-sm" onClick={stopGen}>■ Parar</button>
              : <button className="btn btn-primary btn-sm" onClick={startGen}>▶ Iniciar a {tps} TPS</button>}
            <button className="btn btn-default btn-sm" onClick={resetAll}>🗑 Reset</button>
          </div>
        </div>

        <div className="str-gen-row">
          <label htmlFor="tps" className="str-slider-label">TPS
            <input id="tps" type="range" min="1" max="200" value={tps} className="str-slider"
              onChange={(e) => {
                const v = Number(e.target.value)
                setTps(v)
                if (gen?.running) call('/streaming/generator/start', {
                  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tps: v }),
                })
              }} />
            <span className="str-slider-v">{tps}</span>
          </label>
          <div className="str-stats">
            <Stat label="TPS medido" value={gen?.tps_medido ?? 0}
              sub={tpsDesvio != null && gen?.running ? `${tpsDesvio}% do alvo (${gen.tps_alvo})` : 'gerador parado'}
              color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Inseridos" value={(gen?.inseridos ?? 0).toLocaleString('pt-BR')} sub="nesta sessão" />
            <Stat label="Na coleção" value={(gen?.docs_na_colecao ?? 0).toLocaleString('pt-BR')} sub="estimativa do Atlas" />
          </div>
        </div>
      </div>

      {/* As três colunas */}
      <div className="str-grid">

        {/* ── COLUNA 1 ── */}
        <div className="str-col str-col-cs">
          <div className="str-col-head">
            <span>🍃 Change Streams</span>
            <span className={`badge ${csConnected ? 'badge-green' : 'badge-yellow'}`}>
              {csConnected ? '● ao vivo' : '○ conectando'}
            </span>
          </div>
          <div className="str-col-body">
            <div className="str-stats">
              <Stat label="Eventos" value={csState.eventos} color="#00ED64" />
              <Stat label="Recuperados" value={csState.recuperados} color={csState.recuperados ? '#00ED64' : undefined} sub="via resume token" />
            </div>
            <div className="str-token">
              <span className="str-token-l">resume token</span>
              <code>{csState.token || '—'}</code>
            </div>
            {csState.fase === 'derrubado' && (
              <div className="str-alert">Cursor derrubado — o gerador continua escrevendo. Reabrindo em 3 s com <code>resume_after</code>…</div>
            )}
            <button className="btn btn-default btn-sm" style={{ width: '100%' }}
              onClick={dropResume} disabled={csState.fase === 'derrubado'}>
              ⚡ Derrubar e retomar
            </button>
            <div className="str-feed">
              {csEvents.length === 0 && <div className="str-empty">Inicie o gerador para ver eventos.</div>}
              {csEvents.map((e, i) => (
                <div key={`${e.endToEndId}-${i}`} className={`str-row${e.recuperado ? ' recovered' : ''}`}>
                  <span className="str-pill">{e.recuperado ? 'RECUPERADO' : e.tipo}</span>
                  <span className="str-row-main">{e.uf} · R$ {fmtBRL(e.valor)}</span>
                  <span className="str-row-ms">{fmtMs(e.latency_ms)}</span>
                </div>
              ))}
            </div>
            <div className="str-note">
              Roda <strong>dentro da aplicação</strong>, sem infraestrutura extra. Em compensação, o
              <strong> resume token é responsabilidade sua</strong>: persistir e retomar dele é o que garante que nada se perca.
            </div>
          </div>
        </div>

        {/* ── COLUNA 2 ── */}
        <div className="str-col str-col-kafka">
          <div className="str-col-head">
            <span>🔀 Kafka Connector</span>
            <span className={`badge ${kafkaOk ? 'badge-green' : 'badge-yellow'}`}>
              {connectorState ? `● ${connectorState}` : '○ verificando'}
            </span>
          </div>
          <div className="str-col-body">
            {kafkaStatus && !kafkaOk ? (
              <NotConfigured
                title="Kafka não configurado"
                detalhe={kafkaStatus.connector?.detalhe || kafkaStatus.consumidor?.detalhe}
                passos={[
                  <>Suba a infra local: <code>docker compose -f docker-compose.streaming.yml up -d</code></>,
                  <>Registre o connector: <code>./scripts/setup-kafka-connector.sh</code></>,
                  <>Confirme <code>KAFKA_BROKERS</code> e <code>CONNECT_URL</code> em <code>backend/.env</code></>,
                ]}
              >
                <div className="str-note" style={{ marginTop: 10 }}>
                  Tópico esperado: <code>{kafkaStatus.topico}</code> · brokers <code>{kafkaStatus.brokers}</code>
                </div>
              </NotConfigured>
            ) : (
              <>
                <div className="str-stats">
                  <Stat label="Mensagens" value={kafkaStatus?.consumidor?.mensagens ?? 0} color="#06b6d4" />
                  <Stat label="Offset atual" value={kafkaStatus?.consumidor?.offset_atual ?? '—'} sub="partição consumida" />
                </div>
                <div className="str-token">
                  <span className="str-token-l">tópico</span>
                  <code>{kafkaStatus?.topico}</code>
                </div>
                <div className="str-feed">
                  {kafkaMsgs.length === 0 && <div className="str-empty">Connector RUNNING. Aguardando mensagens no tópico…</div>}
                  {kafkaMsgs.map((m, i) => (
                    <div key={`${m.offset}-${i}`} className="str-row">
                      <span className="str-pill">{m.tipo}</span>
                      <span className="str-row-main">{m.uf} · offset {m.offset}</span>
                      <span className="str-row-ms">{fmtMs(m.latency_ms)}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
            <div className="str-note">
              O source connector usa <strong>change stream por baixo</strong> — o que ele acrescenta é
              <strong> gestão de offset</strong> e entrega no barramento. E <code>startup.mode=copy_existing</code>
              faz o <strong>backfill do histórico</strong> antes de emendar no stream.
            </div>
          </div>
        </div>

        {/* ── COLUNA 3 ── */}
        <div className="str-col str-col-asp">
          <div className="str-col-head">
            <span>🪟 Atlas Stream Processing</span>
            <span className={`badge ${aspOk ? 'badge-green' : 'badge-yellow'}`}>
              {aspOk ? '● processor ativo' : '○ não configurado'}
            </span>
          </div>
          <div className="str-col-body">
            {aspStatus && !aspOk ? (
              <NotConfigured
                title="Atlas Stream Processing não configurado"
                detalhe={aspStatus.detalhe}
                passos={[
                  <>Crie uma Stream Processing Instance no Atlas e uma conexão para o cluster desta PoV</>,
                  <>Preencha <code>ASP_CONNECTION_STRING</code> e <code>ASP_ENABLED=true</code> em <code>backend/.env</code></>,
                  <>Crie o processor: <code>mongosh "$ASP_CONNECTION_STRING" --file scripts/setup-asp.js</code></>,
                ]}
              >
                <QueryBlock label="Ver o pipeline do processor" query={aspStatus.pipeline || ''} />
              </NotConfigured>
            ) : (
              <>
                <div className="str-stats">
                  <Stat label="Janelas" value={aspStatus?.janelas ?? janelas.length} color="#a855f7" sub="tumbling de 10 s" />
                  <Stat label="DLQ" value={aspStatus?.dlq ?? dlq.length} color={(aspStatus?.dlq ?? 0) ? '#f97316' : undefined} sub="documentos rejeitados" />
                </div>
                <Sparkline values={janelas.slice(0, 24).map(j => j.qtd || 0).reverse()} />
                <button className="btn btn-default btn-sm" style={{ width: '100%' }} onClick={injectInvalid}>
                  🧪 Injetar documento inválido
                </button>
                <div style={{ overflowX: 'auto' }}>
                  <table className="lg-table">
                    <thead><tr><th>UF</th><th>Tipo</th><th>Qtd</th><th>Volume</th><th>Ticket</th></tr></thead>
                    <tbody>
                      {janelas.slice(0, 8).map((j, i) => (
                        <tr key={i}>
                          <td>{j.uf}</td><td>{j.tipo}</td><td>{j.qtd}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {fmtBRL(j.volume)}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {fmtBRL(j.ticket)}</td>
                        </tr>
                      ))}
                      {janelas.length === 0 && <tr><td colSpan={5} className="str-empty">Aguardando a primeira janela fechar (10 s)…</td></tr>}
                    </tbody>
                  </table>
                </div>
                {dlq.length > 0 && (
                  <div className="str-alert">
                    {dlq.length} documento(s) foram para a <strong>DLQ</strong> em <code>{aspStatus?.colecao_dlq}</code> —
                    o processor continuou rodando.
                  </div>
                )}
              </>
            )}
            <div className="str-note">
              O processor faz <code>$merge</code> em <code>pix.metricas_janela</code> e o backend
              <strong> assiste essa coleção com um change stream</strong>: o resultado do ASP chega nesta tela
              pela mecânica da coluna 1. É o fecho das três colunas.
            </div>
          </div>
        </div>
      </div>

      {/* Tabela comparativa — sempre visível */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 12 }}>Qual usar, e quando</div>
        <div style={{ overflowX: 'auto' }}>
          <table className="lg-table">
            <thead>
              <tr>
                <th></th><th>Pergunta que responde</th><th>Onde roda</th><th>Linguagem</th>
                <th>Estado e janela</th><th>Resiliência</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><strong style={{ color: '#00ED64' }}>Change Streams</strong></td>
                <td>Como reagir a isso na aplicação?</td>
                <td>Dentro do seu processo</td>
                <td>Pipeline de agregação no cursor</td>
                <td>Sem estado</td>
                <td>Resume token gerido por você</td>
              </tr>
              <tr>
                <td><strong style={{ color: '#06b6d4' }}>Kafka Connector</strong></td>
                <td>Como levar o evento ao barramento?</td>
                <td>Kafka Connect</td>
                <td>Config de connector</td>
                <td>Sem estado</td>
                <td>Offsets do Connect</td>
              </tr>
              <tr>
                <td><strong style={{ color: '#a855f7' }}>Atlas Stream Processing</strong></td>
                <td>Como processar o fluxo com janela e estado?</td>
                <td>Serviço gerenciado no Atlas</td>
                <td>Aggregation pipeline</td>
                <td>Janelas tumbling / hopping</td>
                <td>Checkpoint gerenciado + DLQ</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
