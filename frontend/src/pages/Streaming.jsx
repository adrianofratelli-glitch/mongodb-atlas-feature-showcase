import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useApi } from '../hooks/useApi'
import QueryBlock from '../components/QueryBlock'

const MAX_FEED = 40
// Teto do gerador. O backend é a autoridade (TPS_MAX em routers/streaming.py) e
// manda o valor em /streaming/cenario; este é só o fallback para uma API antiga.
// Existe para manter a demo reproduzível em M20 sem disparar o auto-scaling.
const TPS_MAX_PADRAO = 1000
const TPS_STEP = 10
const CONCEPT_PRESETS = [
  { label: 'Passo a passo', tps: 40, detalhe: 'fluxo leve para acompanhar cada mecanismo' },
  { label: 'Fluxo contínuo', tps: 200, detalhe: 'carga moderada para observar janelas e reconciliação' },
  { label: 'Rajada controlada', tps: 400, detalhe: 'backlog e recuperação sem pretensão de benchmark' },
]

// Hook de SSE: EventSource sobre /api/... (proxy do Vite → backend :8002).
// Só GET, então o mutation guard não exige o X-Demo-Token.
//
// A reconexão é MANUAL de propósito: o EventSource só refaz a conexão sozinho
// quando o socket cai limpo. Se o backend responder qualquer coisa que não seja
// 200 text/event-stream — um 502 do proxy enquanto a API reinicia, por exemplo —
// ele desiste de vez, e as três colunas ficam mudas até alguém dar F5 no meio da
// apresentação. Aqui a gente fecha e reabre até conseguir.
const SSE_RETRY_MS = 2000
// Ações que atuam no ambiente real (restart de connector, DLQ, checkpoint,
// reset). No replay não existe alvo: ficam desabilitadas em vez de mentir.
const SEM_ALVO_NO_REPLAY = 'Indisponível no replay: estas ações atuam sobre o ambiente real'

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
// Acima de 10 s, milissegundos com 5 dígitos obrigam a plateia a fazer conta.
const fmtMs = (v) => {
  if (v == null) return '—'
  return v >= 10000 ? `${(v / 1000).toFixed(1).replace('.', ',')} s` : `${Math.round(v)} ms`
}
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
          <span className="str-pct-v" style={v != null ? { color } : undefined}>{fmtMs(v)}</span>
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

  // ── Modo: ao vivo × replay ───────────────────────────────────────────────
  // O replay reproduz UMA execução real gravada (scripts/capture_replay.py) e
  // não escreve no banco. Existe porque M20/M30 são instâncias burstable e o
  // auto-scaling do Atlas dispara por CPU RELATIVA: 17,6% absolutos deram 88%
  // relativos e escalaram o cluster com o gerador já parado, só com esta tela
  // aberta. Com o replay a demo roda com o cluster pausado.
  //
  // Os números do replay são medições, não simulação — mas a tela precisa dizer
  // qual dos dois o operador está vendo, sempre. Ver o selo mais abaixo.
  const [modo, setModo] = useState('ao_vivo')
  const [manifest, setManifest] = useState(null)
  const replay = modo === 'replay'
  const base = replay ? '/replay' : ''

  useEffect(() => {
    // Sonda de capacidade: responde 200 com `disponivel: false` quando não há
    // gravação, então o seletor só aparece onde o modo realmente existe.
    call('/replay/manifest').then((d) => d && setManifest(d))
  }, [call])

  // ── Cenário PIX conceitual ────────────────────────────────────────────────
  const [cenario, setCenario] = useState(null)
  const [rede, setRede] = useState(null)
  const [tpsMax, setTpsMax] = useState(TPS_MAX_PADRAO)
  useEffect(() => {
    call(`${base}/streaming/cenario`).then((d) => {
      if (!d) return
      if (Number.isFinite(d.tps_max) && d.tps_max > 0) setTpsMax(d.tps_max)
      // Durante um rollout a UI nova pode conversar por alguns segundos com
      // uma API antiga. Claims legados de capacidade nunca voltam para a tela:
      // somente o novo contrato explícito de PoC é aceito.
      if (d.premissas?.sizing === false) {
        setCenario(d)
        return
      }
      setCenario({
        premissas: { workload: 'sintético', sizing: false },
        presets: CONCEPT_PRESETS,
        ambiente: {
          cluster: d.ambiente?.cluster || '—',
          particoes_consumo: d.ambiente?.particoes_consumo || 1,
          nota: 'Os números mostram apenas esta execução. Não são capacidade do produto nem sizing.',
        },
      })
    })
    call(`${base}/streaming/rede`).then((d) => d && setRede(d))
  }, [call, base])

  // ── Gerador ──────────────────────────────────────────────────────────────
  const [tps, setTps] = useState(200)
  const [gen, setGen] = useState(null)

  const tpsTocado = useRef(false)
  const refreshGen = useCallback(async () => {
    const data = await call(`${base}/streaming/generator/status`)
    if (!data) return
    setGen(data)
    // Enquanto o operador não mexeu no slider, ele reflete o que o backend está
    // de fato rodando (inclusive se outra aba/curl mudou o TPS).
    if (!tpsTocado.current && data.running && data.tps_alvo) {
      setTps(Math.min(tpsMax, Math.max(10, data.tps_alvo)))
    }
  }, [call, tpsMax, base])

  useEffect(() => {
    refreshGen()
    const t = setInterval(refreshGen, 1000)
    return () => clearInterval(t)
  }, [refreshGen])

  const aplicarTps = useCallback(async (valor) => {
    // No replay o "play" move o relógio da gravação; não há TPS a aplicar,
    // porque não há escrita. O gerador real fica intocado.
    if (replay) {
      await call('/replay/play', { method: 'POST' })
      refreshGen()
      return
    }
    const tpsSeguro = Math.min(tpsMax, Math.max(10, Number(valor) || 10))
    tpsTocado.current = true
    setTps(tpsSeguro)
    await call('/streaming/generator/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tps: tpsSeguro }),
    })
    refreshGen()
  }, [call, refreshGen, tpsMax, replay])

  const stopGen = async () => {
    await call(replay ? '/replay/stop' : '/streaming/generator/stop', { method: 'POST' })
    refreshGen()
  }

  // ── Coluna 1 — Change Streams ────────────────────────────────────────────
  const [csEvents, setCsEvents] = useState([])
  const [csState, setCsState] = useState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
  const [csMetrics, setCsMetrics] = useState(null)

  const csConnected = useSse(`${base}/streaming/changestream`, (msg) => {
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

  useSse(`${base}/streaming/kafka`, (msg) => {
    if (msg.type === 'mensagem') setKafkaMsgs((prev) => push(prev, msg))
    else if (msg.type === 'metricas') setKafkaMetrics(msg)
    else if (msg.type === 'reset') { setKafkaMsgs([]); setKafkaMetrics(null) }
  })

  useEffect(() => {
    const tick = async () => { const d = await call(`${base}/streaming/kafka/status`); if (d) setKafkaStatus(d) }
    tick()
    const t = setInterval(tick, 4000)
    return () => clearInterval(t)
  }, [call, base])

  const connectorState = kafkaStatus?.connector?.estado
  const kafkaOk = connectorState === 'RUNNING'
  const kafkaQuebrado = ['FAILED', 'DEGRADADO', 'SEM_TASK'].includes(connectorState)
  const reiniciarKafka = async () => {
    await call('/streaming/kafka/restart', { method: 'POST' })
    const d = await call(`${base}/streaming/kafka/status`)
    if (d) setKafkaStatus(d)
  }
  const reiniciarConsumidorKafka = async () => {
    await call('/streaming/kafka/consumer/restart', { method: 'POST' })
  }

  // ── Coluna 3 — Atlas Stream Processing ───────────────────────────────────
  const [janelas, setJanelas] = useState([])
  const [dlq, setDlq] = useState([])
  const [aspMetrics, setAspMetrics] = useState(null)
  const [aspStatus, setAspStatus] = useState(null)

  useSse(`${base}/streaming/asp`, (msg) => {
    if (msg.type === 'janela') { setJanelas((prev) => push(prev, msg)); setAspMetrics(msg) }
    else if (msg.type === 'dlq') setDlq((prev) => push(prev, msg))
    else if (msg.type === 'reset') { setJanelas([]); setDlq([]); setAspMetrics(null) }
  })

  useEffect(() => {
    const tick = async () => { const d = await call(`${base}/streaming/asp/status`); if (d) setAspStatus(d) }
    tick()
    const t = setInterval(tick, 5000)
    return () => clearInterval(t)
  }, [call, base])

  // ── Evidências de confiabilidade ─────────────────────────────────────────
  const [oplog, setOplog] = useState(null)
  const [leitura, setLeitura] = useState(null)
  const [dlqResumo, setDlqResumo] = useState(null)
  const [reconciliacao, setReconciliacao] = useState(null)
  useEffect(() => {
    const tick = async () => {
      const [o, l, d] = await Promise.all([
        call(`${base}/streaming/oplog`), call(`${base}/streaming/leitura`), call(`${base}/streaming/asp/dlq/resumo`),
      ])
      if (o) setOplog(o)
      if (l) setLeitura(l)
      if (d) setDlqResumo(d)
    }
    tick()
    const t = setInterval(tick, 4000)
    return () => clearInterval(t)
  }, [call, base])
  useEffect(() => {
    if (!gen?.run_id) {
      setReconciliacao(null)
      return undefined
    }
    // 5 s, e o laço PARA quando a execução fecha. A reconciliação é a chamada
    // mais cara da tela (conta a fonte no Atlas); mantê-la em laço depois que o
    // resultado já é final só gastava CPU do cluster com uma resposta que não
    // muda mais — e era o que sustentava a pressão que subia o tier.
    let t = null
    const parar = () => { if (t) { clearInterval(t); t = null } }
    const tick = async () => {
      const d = await call(`${base}/streaming/reconciliacao?run_id=${encodeURIComponent(gen.run_id)}`)
      if (!d) return
      setReconciliacao(d)
      if (d.final === 'reconciliado' && !d.gerador_ativo) parar()
    }
    tick()
    t = setInterval(tick, 5000)
    return parar
  }, [call, gen?.run_id, base])

  const aspOk = aspStatus?.estado === 'configurado'
  const injectInvalid = async (quantidade = 1) => {
    await call(`/streaming/asp/inject-invalid?quantidade=${quantidade}`, { method: 'POST' })
  }
  const reprocessarDlq = async () => {
    const r = await call('/streaming/asp/dlq/reprocessar?limite=1000', { method: 'POST', timeoutMs: 120_000 })
    if (r) { const d = await call(`${base}/streaming/asp/dlq/resumo`); if (d) setDlqResumo(d) }
  }
  const reiniciarAspCheckpoint = async () => {
    await call('/streaming/asp/restart-checkpoint', { method: 'POST', timeoutMs: 90_000 })
  }

  // ── Reset global ─────────────────────────────────────────────────────────
  const resetAll = async () => {
    const result = await call('/streaming/reset', { method: 'POST', timeoutMs: 210_000 })
    if (!result) return
    setCsEvents([]); setCsMetrics(null)
    setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    setKafkaMsgs([]); setKafkaMetrics(null); setJanelas([]); setDlq([]); setAspMetrics(null)
    refreshGen()
  }

  const tpsDesvio = gen?.tps_alvo ? Math.round(Math.abs(gen.tps_medido - gen.tps_alvo) / gen.tps_alvo * 100) : null
  const presets = (cenario?.presets || CONCEPT_PRESETS)
    .filter((p) => p.tps >= 10 && p.tps <= tpsMax)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Enredo + escala do cenário */}
      <div className="card str-hero">
        <div className="str-hero-copy">
          <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 6 }}>Uma execução PIX, três capacidades complementares</div>
          <div style={{ fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.55 }}>
          Um único gerador escreve em <code>pix.transacoes</code> contra este cluster Atlas. As três colunas consomem
          <strong> a mesma mudança</strong> por caminhos diferentes: dentro da aplicação (Change Streams), no barramento
          (MongoDB Kafka Connector) e num serviço gerenciado com janela e estado (Atlas Stream Processing).
          A prova é de <strong>integridade, retomada, fan-out, janela e tratamento de erro</strong> — não de sizing.
          </div>
          <div className="str-workload-note">
            Workload sintético, mecanismos reais · <code>run_id</code> reconcilia os três caminhos · TPS e latência valem apenas para esta execução.
          </div>
        </div>
        <div className="str-env-compact">
          <div><span>Cluster</span><strong>{cenario?.ambiente?.cluster || '—'}</strong></div>
          <div><span>ASP</span><strong>{aspStatus?.tier || '—'}</strong></div>
          <div><span>Cursores</span><strong>{cenario?.ambiente?.particoes_consumo || '—'}</strong></div>
          <div><span>RTT local</span><strong>{rede?.rtt_ms != null ? `${rede.rtt_ms} ms` : '—'}</strong></div>
        </div>
      </div>

      {/* Seletor de modo + selo de origem.
          O selo é deliberadamente grande e permanente: quem olha a tela precisa
          saber, sem perguntar, se está vendo escrita ao vivo ou a reprodução de
          uma execução gravada. Os números do replay são reais (medidos), mas
          apresentá-los como se estivessem acontecendo agora seria enganoso. */}
      {manifest?.disponivel && (
        <div className={`card str-modo${replay ? ' str-modo-replay' : ''}`}>
          <div className="str-modo-linha">
            <div className="str-modo-botoes">
              <button className={`tag${!replay ? ' active' : ''}`}
                onClick={() => setModo('ao_vivo')}>● Ao vivo</button>
              <button className={`tag${replay ? ' active' : ''}`}
                onClick={() => setModo('replay')}>▶ Replay</button>
            </div>
            {replay ? (
              <div className="str-modo-selo">
                <strong>▶ REPLAY — nenhuma escrita está sendo feita no Atlas.</strong>{' '}
                Reprodução da execução <code>{manifest.run_id}</code>, gravada em{' '}
                {manifest.gravado_em}. Os valores são medições reais dessa execução.
              </div>
            ) : (
              <div className="str-modo-selo">
                <strong>● AO VIVO</strong> — o gerador escreve de verdade em{' '}
                <code>pix.transacoes</code> e o cluster processa. Consome compute do Atlas.
              </div>
            )}
          </div>
        </div>
      )}

      {/* Gerador */}
      <div className="card str-generator">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 10 }}>
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
              : <button className="btn btn-primary btn-sm" onClick={() => aplicarTps(tps)}>
                  {replay ? '▶ Reproduzir execução gravada' : `▶ Iniciar a ${num(tps)} TPS`}
                </button>}
            {/* Reset apaga coleções reais — no replay não há o que apagar. */}
            <button className="btn btn-default btn-sm" onClick={resetAll} disabled={replay}
              title={replay ? 'Indisponível no replay: nada é escrito no banco' : undefined}>🗑 Reset</button>
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
            <input id="tps" type="range" min="10" max={tpsMax} step={TPS_STEP} value={tps} className="str-slider"
              onChange={(e) => {
                const v = Number(e.target.value)
                tpsTocado.current = true
                setTps(v)
              }}
              onPointerUp={(e) => { if (gen?.running) aplicarTps(Number(e.currentTarget.value)) }}
              onKeyUp={(e) => { if (gen?.running) aplicarTps(Number(e.currentTarget.value)) }}
              onBlur={(e) => { if (gen?.running) aplicarTps(Number(e.currentTarget.value)) }} />
            <span className="str-slider-v">{num(tps)}</span>
          </label>
          <div className="str-stats">
            <Stat label="TPS medido" value={num(gen?.tps_medido ?? 0)}
              sub={tpsDesvio != null && gen?.running ? `observado nesta execução · alvo ${num(gen.tps_alvo)}` : 'gerador parado'}
              color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Inseridos" value={num(gen?.inseridos ?? 0)} sub="confirmados pelo Atlas" />
            <Stat label="Execução" value={gen?.run_id ? gen.run_id.slice(-6).toUpperCase() : '—'}
              sub="run_id para reconciliação" color={gen?.run_id ? '#00ED64' : undefined} />
            <Stat label="Objetivo" value="Confiabilidade" sub="sem claim de sizing" />
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
              <Stat label="Eventos" value={num(csState.eventos)} color="#00ED64" />
              <Stat label="Recuperados" value={num(csState.recuperados)} color={csState.recuperados ? '#00ED64' : undefined} sub="via resume token" />
            </div>
            <Percentis m={csMetrics} color="#00ED64" />
            <div className="str-token">
              <span className="str-token-l">resume token</span>
              <code>{csState.token || '—'}</code>
            </div>
            <div className="str-token">
              <span className="str-token-l">janela do oplog</span>
              <code title={oplog?.detalhe}>
                {oplog?.janela_min != null
                  ? `${(oplog.janela_min / 60).toFixed(1).replace('.', ',')} h de queda recuperável`
                  : '—'}
              </code>
            </div>
            {csState.fase === 'derrubado' && (
              <div className="str-alert">Cursor derrubado — o gerador continua escrevendo. Reabrindo em 3 s com <code>resume_after</code>…</div>
            )}
            <button className="btn btn-default btn-sm" style={{ width: '100%' }}
              onClick={dropResume} disabled={replay || csState.fase === 'derrubado'}
              title={replay ? SEM_ALVO_NO_REPLAY : undefined}>
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
              <strong>Duplicados recebidos: {num(csMetrics?.duplicados ?? csState.duplicados ?? 0)}.</strong>{' '}
              A entrega é <strong>at-least-once</strong>: retomar por um token anterior pode reentregar evento.
              Por isso o consumidor precisa ser <strong>idempotente</strong> — aqui a chave é o
              <code> endToEndId</code>, e a agregação do ASP grava com <code>_id</code> determinístico
              (janela+uf+tipo), então reprocessar substitui em vez de somar duas vezes.
            </div>
            <div className="str-note">
              A recuperação vale <strong>enquanto o ponto de retomada estiver no oplog</strong> —
              hoje {oplog?.janela_min != null ? `${(oplog.janela_min / 60).toFixed(1).replace('.', ',')} h` : '—'}{' '}
              ({oplog?.detalhe}). É o prazo real da garantia, não uma promessa aberta.
            </div>
            <div className="str-note">
              O feed acima é uma <strong>amostra</strong> do fluxo (a aba não renderiza milhares de linhas por segundo) —
              contadores e percentis cobrem <strong>100% dos eventos</strong>, medidos no backend.
              Todo evento recuperado aparece, nenhum é suprimido.
            </div>
            <div className="str-note">
              Roda <strong>dentro da aplicação</strong>, sem infraestrutura extra. Em compensação, o
              <strong> resume token é responsabilidade sua</strong>. Aqui cada cursor persiste o checkpoint em
              <code> pix.consumer_checkpoints</code>; falhas transitórias retomam dele e podem reentregar, nunca avançar no escuro.
              Os {cenario?.ambiente?.particoes_consumo ?? '—'} cursores com filtros disjuntos são uma técnica demonstrativa,
              não equivalem a partições Kafka nem constituem recomendação de sizing.
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
                <button className="btn btn-default btn-sm" style={{ width: '100%', marginTop: 10 }} onClick={reiniciarKafka} disabled={replay}
                  title={replay ? SEM_ALVO_NO_REPLAY : undefined}>
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
                <div className="str-token">
                  <span className="str-token-l">connectors</span>
                  <code>{kafkaStatus?.connector?.connectors ?? '—'} × 1 task · {kafkaStatus?.particoes_consumo ?? '—'} partições</code>
                </div>
                <button className="btn btn-default btn-sm" style={{ width: '100%' }} onClick={reiniciarConsumidorKafka} disabled={replay}
                  title={replay ? SEM_ALVO_NO_REPLAY : undefined}>
                  ↻ Reiniciar consumidor pelo offset
                </button>
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
              <strong> gestão de offset</strong> e entrega no barramento. O connector inicia em
              <code> startup.mode=latest</code> e publica heartbeats; o observador usa um
              <code> group.id</code> estável e confirma offsets periodicamente.
            </div>
            <div className="str-note">
              Um connector é suficiente para provar o conceito. Connectors adicionais com filtros disjuntos são
              apenas um experimento; não são apresentados como partições nativas ou sizing de produção.
              O ambiente local usa broker único e sem TLS/SASL — HA, ACL e Schema Registry ficam explicitamente fora do escopo.
            </div>
          </div>
        </div>

        {/* ── COLUNA 3 ── */}
        <div className="str-col str-col-asp">
          <div className="str-col-head">
            <span>🪟 Atlas Stream Processing</span>
            <span className={`badge ${aspOk ? 'badge-green' : 'badge-yellow'}`}>
              {!aspStatus ? '○ verificando' : aspOk ? '● processor ativo' : '○ não configurado'}
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
                {aspStatus?.runtime?.disponivel && (
                  <div className="str-token">
                    <span className="str-token-l">checkpoint / watermark</span>
                    <code>
                      lag {num(aspStatus.runtime.lag_oplog_s ?? 0)}s · estado {fmtEscala(aspStatus.runtime.state_bytes ?? 0)}B
                      {aspStatus.runtime.watermark ? ` · ${String(aspStatus.runtime.watermark).slice(11, 19)} UTC` : ''}
                    </code>
                  </div>
                )}
                {aspStatus?.drenando_backlog ? (
                  <div className="str-alert">
                    ⏳ Processor <strong>drenando backlog</strong> — a última janela fechada tem
                    {' '}{Math.round(aspStatus.atraso_s)}s de atraso. Os percentis abaixo medem a fila, não o regime.
                    Aperte <strong>Reset</strong> para começar limpo.
                  </div>
                ) : (
                  <Percentis m={aspMetrics} color="#a855f7" />
                )}
                <Sparkline values={janelas.slice(0, 24).map(j => j.qtd || 0).reverse()} />
                <div className="str-dlq-acoes">
                  <button className="btn btn-default btn-xs" onClick={() => injectInvalid(1)} disabled={replay}
                    title={replay ? SEM_ALVO_NO_REPLAY : undefined}>🧪 1 inválido</button>
                  <button className="btn btn-default btn-xs" onClick={() => injectInvalid(1000)} disabled={replay}
                    title={replay ? SEM_ALVO_NO_REPLAY : undefined}>🧪 1.000</button>
                  <button className="btn btn-default btn-xs" onClick={reprocessarDlq} disabled={replay || !dlqResumo?.total}
                    title={replay ? SEM_ALVO_NO_REPLAY : undefined}>
                    ♻️ Reprocessar
                  </button>
                  <button className="btn btn-default btn-xs" onClick={reiniciarAspCheckpoint} disabled={replay}
                    title={replay ? SEM_ALVO_NO_REPLAY : undefined}>
                    ↻ Restart por checkpoint
                  </button>
                </div>
                {dlqResumo?.total > 0 && (
                  <div className="str-alert">
                    <strong>{num(dlqResumo.total)}</strong> na DLQ e o processor <strong>continuou rodando</strong>.
                    {dlqResumo.por_motivo?.[0] && (
                      <div style={{ marginTop: 4, fontSize: 11 }}>
                        motivo: <code>{dlqResumo.por_motivo[0].motivo}</code>
                      </div>
                    )}
                    <div style={{ marginTop: 4, fontSize: 11 }}>
                      Reprocessar corrige e reinsere com o mesmo <code>endToEndId</code> — rodar duas vezes não duplica.
                    </div>
                  </div>
                )}
                <div style={{ overflowX: 'auto' }}>
                  <table className="lg-table">
                    <thead><tr><th>UF</th><th>Qtd</th><th>Volume</th><th>Valores altos</th><th>Maior</th></tr></thead>
                    <tbody>
                      {janelas.slice(0, 8).map((j, i) => (
                        <tr key={i}>
                          <td>{j.uf}</td><td>{num(j.qtd)}</td>
                          {/* forma compacta: a coluna é estreita e o valor cheio era cortado */}
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {fmtEscala(j.volume)}</td>
                          <td style={{ color: j.alertas_valor_alto ? '#f97316' : undefined }}>{num(j.alertas_valor_alto ?? 0)}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>R$ {fmtEscala(j.maior_valor)}</td>
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
              <strong>Valores altos</strong> é um sinal operacional simples (PIX ≥ R$ 5 mil), não um motor antifraude.
              Ele prova que o mesmo pipeline pode manter estado de janela e produzir indicadores acionáveis sem mover o fluxo para batch.
            </div>
            <div className="str-note">
              O processor faz <code>$merge</code> em <code>pix.metricas_janela</code> e o backend
              <strong> assiste essa coleção com um change stream</strong>: o resultado do ASP chega nesta tela
              pela mecânica da coluna 1. É o fecho das três colunas.
            </div>
          </div>
        </div>
      </div>

      {/* Prova de integridade da execução, sem extrapolar capacidade */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Reconciliação ponta a ponta</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 3 }}>
              {reconciliacao?.run_id
                ? <>execução <code>{reconciliacao.run_id}</code></>
                : 'Inicie o gerador para criar uma execução identificável.'}
            </div>
          </div>
          {reconciliacao?.final && (
            <span className={`badge ${reconciliacao.final === 'reconciliado' ? 'badge-green' : 'badge-yellow'}`}>
              {reconciliacao.final === 'reconciliado' ? '✓ reconciliado' : '◌ drenando'}
            </span>
          )}
        </div>
        {reconciliacao?.fonte && (
          <div className="str-neg" style={{ marginTop: 14 }}>
            {[
              ['Fonte Atlas', reconciliacao.fonte.inseridas, 0, true],
              ['Change Streams', reconciliacao.change_streams.unicos, reconciliacao.change_streams.pendentes, reconciliacao.change_streams.reconciliado],
              ['Kafka', reconciliacao.kafka.unicos, reconciliacao.kafka.pendentes, reconciliacao.kafka.reconciliado],
              ['ASP + DLQ', reconciliacao.asp.contabilizadas, reconciliacao.asp.pendentes, reconciliacao.asp.reconciliado],
            ].map(([label, value, pending, ok]) => (
              <div className="str-neg-c" key={label}>
                <div className="str-neg-k">{label}</div>
                <div className="str-neg-v" style={ok ? { color: '#00ED64' } : undefined}>{num(value)}</div>
                <div className="str-neg-s">{ok ? 'contagem fechada' : `${num(pending)} ainda pendente(s)`}</div>
              </div>
            ))}
            <div className="str-neg-c">
              <div className="str-neg-k">Sinais de valor alto</div>
              <div className="str-neg-v" style={reconciliacao.asp.alertas_valor_alto ? { color: '#f97316' } : undefined}>
                {num(reconciliacao.asp.alertas_valor_alto)}
              </div>
              <div className="str-neg-s">produzidos pelo ASP nas janelas</div>
            </div>
            <div className="str-neg-c">
              <div className="str-neg-k">Consulta por endToEndId</div>
              <div className="str-neg-v" style={leitura?.p50 != null ? { color: '#00ED64' } : undefined}>
                {leitura?.p50 != null ? fmtMs(leitura.p50) : '—'}
              </div>
              <div className="str-neg-s">{leitura?.p95 != null ? `p95 ${fmtMs(leitura.p95)}` : 'aguardando execução'}</div>
            </div>
          </div>
        )}
        <div className="str-note" style={{ marginTop: 12 }}>
          Pare o gerador e aguarde a janela fechar: o estado final só fica verde quando a mesma execução está
          contabilizada nos três caminhos. Durante o fluxo, “pendente” significa backlog observável, não perda.
          Os contadores de CS/Kafka pertencem ao processo atual da API; Atlas, ASP e DLQ são consultados no banco.
        </div>
      </div>

      {/* Referência técnica sob demanda: mantém a narrativa principal compacta. */}
      <details className="card str-tech-details">
        <summary>Qual usar, e quando <span>comparar capacidades</span></summary>
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
                <td>Resume token persistido pela aplicação</td>
              </tr>
              <tr>
                <td><strong style={{ color: '#06b6d4' }}>Kafka Connector</strong></td>
                <td>Como levar o evento ao barramento?</td>
                <td>Kafka Connect</td>
                <td>Config de connector</td>
                <td>Sem estado</td>
                <td>Offsets do Connect + consumer group</td>
              </tr>
              <tr>
                <td><strong style={{ color: '#a855f7' }}>Atlas Stream Processing</strong></td>
                <td>Como processar o fluxo com janela e estado?</td>
                <td>Serviço gerenciado no Atlas</td>
                <td>Aggregation pipeline</td>
                <td>Janelas tumbling / hopping</td>
                <td>Checkpoint demonstrável + DLQ auditável</td>
              </tr>
            </tbody>
          </table>
        </div>
      </details>
    </div>
  )
}
