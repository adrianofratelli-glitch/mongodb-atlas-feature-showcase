import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useApi } from '../hooks/useApi'
import QueryBlock from '../components/QueryBlock'

const MAX_FEED = 40
const TPS_MAX = 12000
const TPS_STEP = 100

// Hook de SSE: EventSource sobre /api/... (proxy do Vite → backend :8002).
// Só GET, então o mutation guard não exige o X-Demo-Token.
//
// A reconexão é MANUAL de propósito: o EventSource só refaz a conexão sozinho
// quando o socket cai limpo. Se o backend responder qualquer coisa que não seja
// 200 text/event-stream — um 502 do proxy enquanto a API reinicia, por exemplo —
// ele desiste de vez, e as três colunas ficam mudas até alguém dar F5 no meio da
// apresentação. Aqui a gente fecha e reabre até conseguir.
const SSE_RETRY_MS = 2000

function useSse(path, onMessage, enabled = true) {
  const [connected, setConnected] = useState(false)
  const handler = useRef(onMessage)
  handler.current = onMessage

  useEffect(() => {
    if (!enabled) return undefined
    let es = null
    let timer = null
    let vivo = true

    const abrir = () => {
      if (!vivo) return
      es = new EventSource(`/api${path}`)
      es.onopen = () => setConnected(true)
      es.onmessage = (e) => {
        try { handler.current(JSON.parse(e.data)) } catch { /* keepalive ou frame parcial */ }
      }
      es.onerror = () => {
        setConnected(false)
        es.close()
        if (vivo) timer = setTimeout(abrir, SSE_RETRY_MS)
      }
    }
    abrir()

    return () => { vivo = false; clearTimeout(timer); if (es) es.close(); setConnected(false) }
  }, [path, enabled])

  return connected
}

const push = (list, item) => [item, ...list].slice(0, MAX_FEED)
const fmtMs = (v) => (v == null ? '—' : `${v} ms`)
const num = (v) => (v == null ? '—' : Number(v).toLocaleString('pt-BR'))
const fmtBRL = (v) => (v == null ? '—' : Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }))

// 1.234.567.890 → "1,2 bi" / "30 mi" — números de escala PIX não cabem por extenso.
function fmtEscala(v) {
  if (v == null) return '—'
  if (v >= 1e9) return `${(v / 1e9).toFixed(1).replace('.', ',')} bi`
  if (v >= 1e6) return `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1).replace('.', ',')} mi`
  if (v >= 1e3) return `${Math.round(v / 1e3)} mil`
  return String(v)
}

function Stat({ label, value, sub, color }) {
  return (
    <div className="str-stat">
      <div className="str-stat-l">{label}</div>
      <div className="str-stat-v" style={color ? { color } : undefined}>{value}</div>
      {sub && <div className="str-stat-s">{sub}</div>}
    </div>
  )
}

// p50/p95/p99 medidos sobre 100% dos eventos — o que uma squad de plataforma
// olha antes da média.
function Percentis({ m, color }) {
  return (
    <div className="str-pcts">
      {[['p50', m?.p50], ['p95', m?.p95], ['p99', m?.p99]].map(([k, v]) => (
        <div key={k} className="str-pct">
          <span className="str-pct-k">{k}</span>
          <span className="str-pct-v" style={v != null ? { color } : undefined}>{v == null ? '—' : `${v}ms`}</span>
        </div>
      ))}
      <div className="str-pct">
        <span className="str-pct-k">ev/s</span>
        <span className="str-pct-v" style={m?.eventos_s ? { color } : undefined}>{m?.eventos_s ?? '—'}</span>
      </div>
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

  // ── Cenário PIX (premissas declaradas pelo backend) ──────────────────────
  const [cenario, setCenario] = useState(null)
  const [rede, setRede] = useState(null)
  useEffect(() => {
    call('/streaming/cenario').then((d) => d && setCenario(d))
    call('/streaming/rede').then((d) => d && setRede(d))
  }, [call])

  // ── Gerador ──────────────────────────────────────────────────────────────
  const [tps, setTps] = useState(3472)
  const [gen, setGen] = useState(null)

  const tpsTocado = useRef(false)
  const refreshGen = useCallback(async () => {
    const data = await call('/streaming/generator/status')
    if (!data) return
    setGen(data)
    // Enquanto o operador não mexeu no slider, ele reflete o que o backend está
    // de fato rodando (inclusive se outra aba/curl mudou o TPS).
    if (!tpsTocado.current && data.running && data.tps_alvo) setTps(data.tps_alvo)
  }, [call])

  useEffect(() => {
    refreshGen()
    const t = setInterval(refreshGen, 1000)
    return () => clearInterval(t)
  }, [refreshGen])

  const aplicarTps = useCallback(async (valor) => {
    tpsTocado.current = true
    setTps(valor)
    await call('/streaming/generator/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tps: valor }),
    })
    refreshGen()
  }, [call, refreshGen])

  const stopGen = async () => { await call('/streaming/generator/stop', { method: 'POST' }); refreshGen() }

  // ── Coluna 1 — Change Streams ────────────────────────────────────────────
  const [csEvents, setCsEvents] = useState([])
  const [csState, setCsState] = useState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
  const [csMetrics, setCsMetrics] = useState(null)

  const csConnected = useSse('/streaming/changestream', (msg) => {
    if (msg.type === 'evento') {
      setCsEvents((prev) => push(prev, msg))
    } else if (msg.type === 'metricas') {
      setCsMetrics(msg)
      setCsState((s) => ({ ...s, eventos: msg.eventos, recuperados: msg.recuperados, token: msg.token }))
    } else if (msg.type === 'derrubado') {
      setCsState((s) => ({ ...s, fase: 'derrubado', token: msg.token }))
    } else if (msg.type === 'aberto') {
      setCsState((s) => ({ ...s, fase: msg.retomado ? 'retomado' : 'ativo', token: msg.token }))
    } else if (msg.type === 'hello') {
      setCsState((s) => ({ ...s, eventos: msg.eventos ?? 0, recuperados: msg.recuperados ?? 0, token: msg.token }))
    } else if (msg.type === 'reset') {
      setCsEvents([]); setCsMetrics(null)
      setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    }
  })

  const dropResume = async () => { await call('/streaming/changestream/drop-resume', { method: 'POST' }) }

  // ── Coluna 2 — Kafka Connector ───────────────────────────────────────────
  const [kafkaMsgs, setKafkaMsgs] = useState([])
  const [kafkaMetrics, setKafkaMetrics] = useState(null)
  const [kafkaStatus, setKafkaStatus] = useState(null)

  useSse('/streaming/kafka', (msg) => {
    if (msg.type === 'mensagem') setKafkaMsgs((prev) => push(prev, msg))
    else if (msg.type === 'metricas') setKafkaMetrics(msg)
    else if (msg.type === 'reset') { setKafkaMsgs([]); setKafkaMetrics(null) }
  })

  useEffect(() => {
    const tick = async () => { const d = await call('/streaming/kafka/status'); if (d) setKafkaStatus(d) }
    tick()
    const t = setInterval(tick, 4000)
    return () => clearInterval(t)
  }, [call])

  const connectorState = kafkaStatus?.connector?.estado
  const kafkaOk = connectorState === 'RUNNING'
  const kafkaQuebrado = ['FAILED', 'DEGRADADO', 'SEM_TASK'].includes(connectorState)
  const reiniciarKafka = async () => {
    await call('/streaming/kafka/restart', { method: 'POST' })
    const d = await call('/streaming/kafka/status')
    if (d) setKafkaStatus(d)
  }

  // ── Coluna 3 — Atlas Stream Processing ───────────────────────────────────
  const [janelas, setJanelas] = useState([])
  const [dlq, setDlq] = useState([])
  const [aspMetrics, setAspMetrics] = useState(null)
  const [aspStatus, setAspStatus] = useState(null)

  useSse('/streaming/asp', (msg) => {
    if (msg.type === 'janela') { setJanelas((prev) => push(prev, msg)); setAspMetrics(msg) }
    else if (msg.type === 'dlq') setDlq((prev) => push(prev, msg))
    else if (msg.type === 'reset') { setJanelas([]); setDlq([]); setAspMetrics(null) }
  })

  useEffect(() => {
    const tick = async () => { const d = await call('/streaming/asp/status'); if (d) setAspStatus(d) }
    tick()
    const t = setInterval(tick, 5000)
    return () => clearInterval(t)
  }, [call])

  // ── Tradução para números de negócio ────────────────────────────────────
  const [negocio, setNegocio] = useState(null)
  useEffect(() => {
    const tick = async () => { const d = await call('/streaming/negocio'); if (d) setNegocio(d) }
    tick()
    const t = setInterval(tick, 3000)
    return () => clearInterval(t)
  }, [call])

  const aspOk = aspStatus?.estado === 'configurado'
  const injectInvalid = async () => { await call('/streaming/asp/inject-invalid', { method: 'POST' }) }

  // ── Reset global ─────────────────────────────────────────────────────────
  const resetAll = async () => {
    await call('/streaming/reset', { method: 'POST' })
    setCsEvents([]); setCsMetrics(null)
    setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    setKafkaMsgs([]); setKafkaMetrics(null); setJanelas([]); setDlq([]); setAspMetrics(null)
    refreshGen()
  }

  const tpsDesvio = gen?.tps_alvo ? Math.round(Math.abs(gen.tps_medido - gen.tps_alvo) / gen.tps_alvo * 100) : null
  const presets = cenario?.presets || []
  const der = cenario?.derivados

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Enredo + escala do cenário */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 10 }}>Três formas de reagir a uma mudança — no volume do PIX</div>
        <div style={{ fontSize: 13, color: 'var(--text-primary)', lineHeight: 1.65 }}>
          Um único gerador escreve em <code>pix.transacoes</code> contra este cluster Atlas. As três colunas consomem
          <strong> a mesma mudança</strong> por caminhos diferentes: dentro da aplicação (Change Streams), no barramento
          (MongoDB Kafka Connector) e num serviço gerenciado com janela e estado (Atlas Stream Processing).
          A pergunta não é qual é “melhor” — é <em>qual pergunta cada um responde</em>, e se cada um aguenta o seu volume.
        </div>

        {der && (
          <div className="str-escala">
            {[
              { k: 'PIX Brasil', v: fmtEscala(cenario.premissas.pix_brasil_tx_dia), s: 'transações/dia' },
              { k: 'Inter (10%)', v: fmtEscala(der.inter_tx_dia), s: 'transações/dia' },
              { k: 'Média Inter', v: `${num(der.inter_tps_medio)}`, s: 'TPS sustentado' },
              { k: 'Pico Inter', v: `${num(der.inter_tps_pico)}`, s: 'TPS (3× a média)' },
            ].map((c) => (
              <div key={c.k} className="str-escala-c">
                <div className="str-escala-k">{c.k}</div>
                <div className="str-escala-v">{c.v}</div>
                <div className="str-escala-s">{c.s}</div>
              </div>
            ))}
          </div>
        )}

        <div style={{ marginTop: 12, fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
          📐 <strong>Premissas, não medições:</strong> o volume diário do PIX está na ordem de grandeza divulgada pelo BCB;
          a participação de 10% do Inter e o fator de pico de 3× são <em>premissas deste cenário</em> — servem de régua.
          O que a demo <strong>mede</strong> é o TPS que este cluster realmente sustenta, no painel abaixo.
        </div>
        <div style={{ marginTop: 6, fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
          🔎 <strong>Nada aqui é sintético:</strong> todo número vem de uma operação real contra o Atlas ou o Kafka.
          Componente não configurado aparece como <em>“não configurado”</em>, com o passo a passo — nunca com dado inventado.
        </div>
      </div>

      {/* Gerador */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 14 }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Gerador de transações</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 2 }}>
              <code>{gen?.colecao || 'pix.transacoes'}</code> · micro-batches a cada 100 ms ·
              TTL de {gen?.ttl_segundos ? (gen.ttl_segundos >= 60 ? `${Math.round(gen.ttl_segundos / 60)} min` : `${gen.ttl_segundos}s`) : '—'} em <code>ts</code> (rede de segurança; o Reset limpa na hora)
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {gen?.running
              ? <button className="btn btn-danger btn-sm" onClick={stopGen}>■ Parar</button>
              : <button className="btn btn-primary btn-sm" onClick={() => aplicarTps(tps)}>▶ Iniciar a {num(tps)} TPS</button>}
            <button className="btn btn-default btn-sm" onClick={resetAll}>🗑 Reset</button>
          </div>
        </div>

        {/* Presets do cenário PIX */}
        {presets.length > 0 && (
          <div className="str-presets">
            <span className="str-presets-l">Carga</span>
            {presets.map((p) => (
              <button key={p.label} title={p.detalhe}
                className={`tag${tps === p.tps ? ' active' : ''}`}
                onClick={() => aplicarTps(p.tps)}>
                {p.label} · <strong>{num(p.tps)} TPS</strong>
              </button>
            ))}
          </div>
        )}

        <div className="str-gen-row">
          <label htmlFor="tps" className="str-slider-label">TPS
            <input id="tps" type="range" min="500" max={TPS_MAX} step={TPS_STEP} value={tps} className="str-slider"
              onChange={(e) => {
                const v = Number(e.target.value)
                tpsTocado.current = true
                setTps(v)
                if (gen?.running) aplicarTps(v)
              }} />
            <span className="str-slider-v">{num(tps)}</span>
          </label>
          <div className="str-stats">
            <Stat label="TPS medido" value={num(gen?.tps_medido ?? 0)}
              sub={tpsDesvio != null && gen?.running ? `${tpsDesvio}% do alvo (${num(gen.tps_alvo)})` : 'gerador parado'}
              color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Inseridos" value={num(gen?.inseridos ?? 0)} sub="nesta sessão" />
            <Stat label="Projeção/dia" value={fmtEscala(gen?.projecao_dia)}
              sub="TPS medido × 86.400 s" color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Do dia do Inter" value={gen?.pct_dia_inter != null ? `${gen.pct_dia_inter}%` : '—'}
              sub="ritmo atual vs 30 mi/dia" />
          </div>
        </div>
      </div>

      {/* Linha de base de rede — sem isto, a latência das colunas é lida como
          custo do change stream quando é, em boa parte, distância. */}
      {rede?.rtt_ms != null && (
        <div className="str-rede">
          🌐 <strong>RTT desta máquina até o cluster: {rede.rtt_ms} ms</strong> (medido agora, com <code>ping</code> no admin).
          As latências das três colunas <strong>incluem esse ida-e-volta</strong> — a app roda aqui e o cluster está em
          outra região. Co-localizando app e cluster, o que sobra é o custo real da entrega, não a distância.
        </div>
      )}

      {/* Teto do ambiente — o número alto é do ambiente provisionado, não do produto */}
      {cenario?.ambiente && (
        <div className="str-teto">
          <div className="str-teto-t">⚙️ Ambiente desta PoV — e como ir além</div>
          <div className="str-teto-g">
            <div><span className="str-teto-k">Cluster</span><span className="str-teto-v">{cenario.ambiente.cluster}</span></div>
            <div><span className="str-teto-k">Stream Processing</span><span className="str-teto-v">{aspStatus?.tier || '—'}</span></div>
            <div><span className="str-teto-k">Partições de consumo</span><span className="str-teto-v">{cenario.ambiente.particoes_consumo}</span></div>
          </div>
          <div className="str-teto-n">{cenario.ambiente.nota}</div>
          {cenario.ambiente.asterisco && (
            <div className="str-teto-a">{cenario.ambiente.asterisco}</div>
          )}
        </div>
      )}

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
              <Stat label="Eventos" value={num(csState.eventos)} color="#00ED64" />
              <Stat label="Recuperados" value={num(csState.recuperados)} color={csState.recuperados ? '#00ED64' : undefined} sub="via resume token" />
            </div>
            <Percentis m={csMetrics} color="#00ED64" />
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
              O feed acima é uma <strong>amostra</strong> do fluxo (a aba não renderiza milhares de linhas por segundo) —
              contadores e percentis cobrem <strong>100% dos eventos</strong>, medidos no backend.
              Todo evento recuperado aparece, nenhum é suprimido.
            </div>
            <div className="str-note">
              Roda <strong>dentro da aplicação</strong>, sem infraestrutura extra. Em compensação, o
              <strong> resume token é responsabilidade sua</strong>: persistir e retomar dele é o que garante que nada se perca.
              Um cursor único satura por volta de <strong>5 mil eventos/s</strong>; acima disso o caminho é o mesmo de produção —
              <strong> particionar o consumo</strong> (aqui, {cenario?.ambiente?.particoes_consumo ?? '—'} cursores, um por partição de conta pagadora),
              e cada partição retoma pelo seu próprio token.
            </div>
          </div>
        </div>

        {/* ── COLUNA 2 ── */}
        <div className="str-col str-col-kafka">
          <div className="str-col-head">
            <span>🔀 Kafka Connector</span>
            <span className={`badge ${kafkaOk ? 'badge-green' : 'badge-yellow'}`}>
              {!connectorState ? '○ verificando'
                : connectorState === 'nao_configurado' ? '○ não configurado'
                : `● ${connectorState}`}
            </span>
          </div>
          <div className="str-col-body">
            {kafkaQuebrado ? (
              <div className="str-offline">
                <div className="str-offline-t">🔴 Connector com task parada</div>
                <div className="str-offline-d">{kafkaStatus.connector?.detalhe}</div>
                {kafkaStatus.connector?.tasks?.filter(t => t.trace).map(t => (
                  <div key={t.id} className="str-trace">task {t.id}: {t.trace}</div>
                ))}
                <button className="btn btn-default btn-sm" style={{ width: '100%', marginTop: 10 }} onClick={reiniciarKafka}>
                  ↻ Reiniciar connector
                </button>
              </div>
            ) : kafkaStatus && !kafkaOk ? (
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
                  <Stat label="Mensagens" value={num(kafkaMetrics?.mensagens ?? kafkaStatus?.consumidor?.mensagens ?? 0)} color="#06b6d4" />
                  <Stat label="Offset atual" value={num(kafkaMetrics?.offset_atual ?? kafkaStatus?.consumidor?.offset_atual)} sub="partição consumida" />
                </div>
                <Percentis m={kafkaMetrics} color="#06b6d4" />
                <div className="str-token">
                  <span className="str-token-l">tópico</span>
                  <code>{kafkaStatus?.topico}</code>
                </div>
                <div className="str-feed">
                  {kafkaMsgs.length === 0 && <div className="str-empty">Connector RUNNING. Aguardando mensagens no tópico…</div>}
                  {kafkaMsgs.map((m, i) => (
                    <div key={`${m.offset}-${i}`} className="str-row">
                      <span className="str-pill">{m.tipo}</span>
                      <span className="str-row-main">{m.uf} · offset {num(m.offset)}</span>
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
            <div className="str-note">
              O connector roda <strong>1 task por coleção</strong> (um cursor só), então acima de ~6 mil msg/s
              ele fica para trás e a latência do tópico cresce — dá para ver no p50 acima. A saída é a mesma
              da coluna 1: <strong>um connector por partição</strong>, cada um com seu filtro no pipeline.
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
                  <Stat label="Agregadas" value={fmtEscala(aspStatus?.transacoes_agregadas)} color="#a855f7"
                    sub={`${num(aspStatus?.janelas)} janelas de 10 s`} />
                  <Stat label="Volume" value={`R$ ${fmtEscala(aspStatus?.volume_agregado)}`} sub="somado pelo processor" />
                  <Stat label="DLQ" value={num(aspStatus?.dlq ?? 0)} color={(aspStatus?.dlq ?? 0) ? '#f97316' : undefined} sub="rejeitados" />
                </div>
                <Percentis m={aspMetrics} color="#a855f7" />
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
                          <td>{j.uf}</td><td>{j.tipo}</td><td>{num(j.qtd)}</td>
                          {/* forma compacta: a coluna é estreita e o valor cheio era cortado */}
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {fmtEscala(j.volume)}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {num(Math.round(j.ticket ?? 0))}</td>
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
              A latência aqui é do <strong>fecho da janela</strong> (última transação agregada → resultado na tela),
              não de um evento individual: são coisas diferentes de propósito.
            </div>
            <div className="str-note">
              O processor faz <code>$merge</code> em <code>pix.metricas_janela</code> e o backend
              <strong> assiste essa coleção com um change stream</strong>: o resultado do ASP chega nesta tela
              pela mecânica da coluna 1. É o fecho das três colunas.
            </div>
          </div>
        </div>
      </div>

      {/* O que isso significa em dinheiro — o painel para quem não é de plataforma */}
      {negocio && (
        <div className="card" style={{ padding: '18px 20px' }}>
          <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>O que isso significa para o negócio</div>
          <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 14 }}>
            Derivado dos números medidos acima, ao vivo — não são estimativas de catálogo.
          </div>
          <div className="str-neg">
            <div className="str-neg-c">
              <div className="str-neg-k">Custo por milhão de transações</div>
              <div className="str-neg-v">{negocio.custo_por_milhao_usd != null ? `US$ ${fmtBRL(negocio.custo_por_milhao_usd)}` : '—'}</div>
              <div className="str-neg-s">no ritmo atual, preço de lista do ambiente</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Janela de reação</div>
              <div className="str-neg-v">{negocio.latencia_reacao_ms != null ? `${negocio.latencia_reacao_ms} ms` : '—'}</div>
              <div className="str-neg-s">do PIX cair até o antifraude poder agir (p50)</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Fluxo financeiro</div>
              <div className="str-neg-v">R$ {fmtEscala(negocio.reais_por_segundo)}<span className="str-neg-u">/s</span></div>
              <div className="str-neg-s">ticket médio real × TPS medido</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Em trânsito na janela</div>
              <div className="str-neg-v">R$ {fmtEscala(negocio.valor_em_transito_brl)}</div>
              <div className="str-neg-s">valor que atravessa o pipeline a cada janela de reação</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Reconciliações evitadas</div>
              <div className="str-neg-v" style={{ color: negocio.reconciliacoes_evitadas ? '#00ED64' : undefined }}>
                {num(negocio.reconciliacoes_evitadas)}
              </div>
              <div className="str-neg-s">eventos recuperados na queda — sem resume token, viram trabalho manual</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Sistemas para operar</div>
              <div className="str-neg-v">{negocio.sistemas_com_mongo}<span className="str-neg-u"> vs {negocio.sistemas_sem_mongo}</span></div>
              <div className="str-neg-s">o aviso já vem do banco: sem broker e sem processo de sync</div>
            </div>
          </div>
          <div style={{ marginTop: 12, fontSize: 11.5, color: 'var(--text-secondary)', lineHeight: 1.55 }}>
            📐 <strong>O que é medido e o que é premissa:</strong> {negocio.premissas.nota} Custo considerado: US$ {negocio.premissas.custo_ambiente_usd_hora}/h.
          </div>
        </div>
      )}

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
