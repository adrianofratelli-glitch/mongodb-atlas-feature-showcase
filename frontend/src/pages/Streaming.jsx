import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useIntervaloVisivel, useVisivel } from '../hooks/usePolling'
import QueryBlock from '../components/QueryBlock'

const MAX_FEED = 40

// Hook de SSE: EventSource sobre /api/... (proxy do Vite → backend :8002).
// Só GET, então o mutation guard não exige o X-Demo-Token.
//
// A reconexão é MANUAL de propósito: o EventSource só refaz a conexão sozinho
// quando o socket cai limpo. Se o backend responder qualquer coisa que não seja
// 200 text/event-stream — um 502 do proxy enquanto a API reinicia, por exemplo —
// ele desiste de vez, e as três colunas ficam mudas até alguém dar F5 no meio da
// apresentação. Aqui a gente fecha e reabre até conseguir.
const SSE_RETRY_MS = 2000
// Ações que atuavam no ambiente real (restart de connector, DLQ, checkpoint).
// Numa execução gravada não existe alvo: ficam desabilitadas em vez de mentir.
const SEM_ALVO_NO_REPLAY = 'Indisponível: esta aba reproduz uma execução gravada, não há ambiente para agir'

function useSse(path, onMessage, enabled = true) {
  const [connected, setConnected] = useState(false)
  const visivel = useVisivel()
  const handler = useRef(onMessage)
  handler.current = onMessage

  useEffect(() => {
    // Aba escondida: fecha. Cada EventSource segura uma das ~6 conexões por
    // host do HTTP/1.1, e três colunas em abas esquecidas esgotam o pool —
    // foi assim que fetches comuns passaram a estourar 30 s de timeout.
    if (!enabled || !visivel) return undefined
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
  }, [path, enabled, visivel])

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

function fmtDataGravacao(v) {
  if (!v) return 'data não informada'
  const d = new Date(v)
  return Number.isNaN(d.getTime()) ? String(v) : d.toLocaleString('pt-BR', {
    dateStyle: 'short', timeStyle: 'short', timeZone: 'America/Sao_Paulo',
  })
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

function LatencyContext({ title, detail, tone }) {
  return (
    <div className={`str-latency-context str-latency-${tone}`}>
      <span>{title}</span>
      <small>{detail}</small>
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

  // Ao vivo e o modo principal para a conversa com o time PIX. O replay real
  // permanece como fallback operacional caso Kafka/ASP/rede nao estejam prontos.
  // Os cursores live so existem durante uma sessao iniciada nesta tela: isso
  // evita repetir a regressao em que tres cursores + polling ficaram pressionando
  // uma M20 com o gerador parado.
  const [modo, setModo] = useState('live')
  const replay = modo === 'replay'
  const base = replay ? '/replay' : ''
  const [sessaoLive, setSessaoLive] = useState(false)
  const [preparando, setPreparando] = useState(false)
  const [manifest, setManifest] = useState(null)

  useEffect(() => {
    // Responde 200 com `disponivel: false` quando não há gravação — nesse caso
    // a aba avisa em vez de mostrar painéis vazios sem explicação.
    call('/replay/manifest').then((d) => d && setManifest(d))
  }, [call])

  // ── Cenário PIX conceitual ────────────────────────────────────────────────
  const [cenario, setCenario] = useState(null)
  const [rede, setRede] = useState(null)
  const [tpsSelecionado, setTpsSelecionado] = useState(8_000)
  // O preset carrega o modo junto: o patamar de headroom só é alcançável em
  // lote, e escolhê-lo sem trocar o modo mostraria "alvo 12.000, medido 2.000".
  const [modoSelecionado, setModoSelecionado] = useState(null)
  useEffect(() => {
    call(`${base}/streaming/cenario`).then((d) => {
      if (!d) return
      setCenario(d)
      if (!replay && d.default_tps) setTpsSelecionado(d.default_tps)
      if (!replay && d.modo_escrita) setModoSelecionado(d.modo_escrita)
    })
    call(`${base}/streaming/rede`).then((d) => d && setRede(d))
  }, [call, base, replay])

  // ── Relógio da reprodução ────────────────────────────────────────────────
  const [gen, setGen] = useState(null)
  // Injeção de falha: qual está em curso e o que dizer sobre a última.
  const [falha, setFalha] = useState(null)
  const [falhaMsg, setFalhaMsg] = useState('')
  const genRequestSeq = useRef(0)
  const activeRunRef = useRef(null)

  const refreshGen = useCallback(async () => {
    const seq = ++genRequestSeq.current
    const data = await call(`${base}/streaming/generator/status`)
    if (data && seq === genRequestSeq.current) {
      activeRunRef.current = data.run_id || null
      setGen(data)
    }
  }, [call, base])

  useEffect(() => { refreshGen() }, [refreshGen])
  // Sem sessao live, nada consulta a collection remotamente em loop.
  useIntervaloVisivel(refreshGen, 1000, Boolean(gen?.running) || Boolean(gen?.stopping) || sessaoLive || replay)

  const iniciar = useCallback(async () => {
    if (replay) {
      await call('/replay/play', { method: 'POST' })
      refreshGen()
      return
    }
    setPreparando(true)
    try {
      // Toda rodada começa isolada. Sem isto, documentos/janelas e o run_id da
      // rodada anterior podiam aparecer durante o novo Play e a reconciliação
      // comparava snapshots de duas execuções diferentes.
      const reset = await call('/streaming/reset', { method: 'POST', timeoutMs: 220_000 })
      if (!reset?.reset) return
      window.dispatchEvent(new Event('preflight-refresh'))
      genRequestSeq.current += 1 // invalida um /status antigo ainda em voo
      activeRunRef.current = null
      setGen(null)
      setCsEvents([]); setCsMetrics(null)
      setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
      setKafkaMsgs([]); setKafkaMetrics(null)
      setJanelas([]); setDlq([]); setAspMetrics(null)
      setReconciliacao(null)

      // Abre CS/Kafka/ASP antes da primeira escrita para nenhum evento nascer
      // fora da observação. `preparando` impede o efeito de fechamento de usar
      // a reconciliação antiga durante esta espera.
      setSessaoLive(true)
      await new Promise((resolve) => setTimeout(resolve, 800))
      const started = await call('/streaming/generator/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tps: tpsSelecionado,
          duration_s: cenario?.default_duration_s ?? 30,
          ...(modoSelecionado ? { modo: modoSelecionado } : {}),
        }),
      })
      if (!started?.run_id) {
        setSessaoLive(false)
        return
      }
      genRequestSeq.current += 1
      activeRunRef.current = started.run_id
      setGen(started)
      refreshGen()
    } finally {
      setPreparando(false)
    }
  }, [call, cenario?.default_duration_s, refreshGen, replay, tpsSelecionado, modoSelecionado])

  const stopGen = async () => {
    await call(replay ? '/replay/stop' : '/streaming/generator/stop', { method: 'POST' })
    refreshGen()
  }

  const derrubarConnector = async () => {
    setFalha('connector'); setFalhaMsg('')
    const d = await call('/streaming/falha/connector', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ segundos: 8 }), timeoutMs: 60_000,
    })
    setFalha(null)
    setFalhaMsg(d?.derrubado
      ? `connector fora por ${d.segundos}s e retomado pelo offset — confira a reconciliação fechar`
      : (d?.detalhe || 'não foi possível derrubar o connector'))
  }

  const injetarInvalido = async () => {
    setFalha('evento'); setFalhaMsg('')
    const d = await call('/streaming/falha/evento-invalido', { method: 'POST' })
    setFalha(null)
    setFalhaMsg(d?.injetado
      ? `evento inválido gravado (${d.endToEndId.slice(0, 10)}…) — deve aparecer na DLQ da coluna 3`
      : 'não foi possível injetar o evento')
  }

  // Reset: volta o relógio ao início E limpa os painéis. O /replay/stop já zera
  // a posição no backend, mas os feeds/métricas vivem no estado desta tela e só
  // seriam limpos pelo evento `reset` do SSE, que depende de o laço da gravação
  // dar a volta. Sem isto, parar deixa a tela congelada no meio da execução.
  const resetExecucao = async () => {
    await call(replay ? '/replay/stop' : '/streaming/reset', { method: 'POST', timeoutMs: 220_000 })
    setCsEvents([]); setCsMetrics(null)
    setCsState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
    setKafkaMsgs([]); setKafkaMetrics(null)
    setJanelas([]); setDlq([]); setAspMetrics(null)
    setReconciliacao(null)
    activeRunRef.current = null
    if (!replay) setSessaoLive(false)
    refreshGen()
  }

  // ── Coluna 1 — Change Streams ────────────────────────────────────────────
  const [csEvents, setCsEvents] = useState([])
  const [csState, setCsState] = useState({ eventos: 0, recuperados: 0, token: null, fase: 'ativo' })
  const [csMetrics, setCsMetrics] = useState(null)

  const observar = replay ? Boolean(manifest?.disponivel) : sessaoLive
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
  }, observar)

  const dropResume = async () => { await call('/streaming/changestream/drop-resume', { method: 'POST' }) }

  // ── Coluna 2 — Kafka Connector ───────────────────────────────────────────
  const [kafkaMsgs, setKafkaMsgs] = useState([])
  const [kafkaMetrics, setKafkaMetrics] = useState(null)
  const [kafkaStatus, setKafkaStatus] = useState(null)

  useSse(`${base}/streaming/kafka`, (msg) => {
    if (msg.type === 'mensagem') setKafkaMsgs((prev) => push(prev, msg))
    else if (msg.type === 'metricas') setKafkaMetrics(msg)
    else if (msg.type === 'reset') { setKafkaMsgs([]); setKafkaMetrics(null) }
  }, observar)

  // Snapshot gravado: só muda com o relógio andando. Uma carga inicial para o
  // painel não nascer vazio, e poll apenas durante a reprodução.
  const lerKafkaStatus = useCallback(async () => {
    const d = await call(`${base}/streaming/kafka/status`)
    if (d) setKafkaStatus(d)
  }, [call, base])
  useEffect(() => { lerKafkaStatus() }, [lerKafkaStatus])
  useIntervaloVisivel(lerKafkaStatus, 4000, observar)

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
  }, observar)

  const lerAspStatus = useCallback(async () => {
    const d = await call(`${base}/streaming/asp/status`)
    if (d) setAspStatus(d)
  }, [call, base])
  useEffect(() => { lerAspStatus() }, [lerAspStatus])
  useIntervaloVisivel(lerAspStatus, 5000, observar)

  // ── Evidências de confiabilidade ─────────────────────────────────────────
  const [oplog, setOplog] = useState(null)
  const [leitura, setLeitura] = useState(null)
  const [dlqResumo, setDlqResumo] = useState(null)
  const [reconciliacao, setReconciliacao] = useState(null)
  useIntervaloVisivel(useCallback(async () => {
    const [o, l, d] = await Promise.all([
      call(`${base}/streaming/oplog`), call(`${base}/streaming/leitura`), call(`${base}/streaming/asp/dlq/resumo`),
    ])
    if (o) setOplog(o)
    if (l) setLeitura(l)
    if (d) setDlqResumo(d)
  }, [call, base]), 4000, observar)

  // Reconciliação só enquanto a reprodução anda. O relógio repete a gravação
  // com o mesmo run_id; portanto, mesmo depois de chegar a "reconciliado", o
  // poll precisa continuar enquanto estiver rodando para voltar ao snapshot
  // inicial no próximo ciclo. Parado, o resultado não muda e não é consultado.
  useEffect(() => {
    if (!gen?.run_id) { setReconciliacao(null); return }
    const runId = gen.run_id
    let cancelado = false
    call(`${base}/streaming/reconciliacao?run_id=${encodeURIComponent(runId)}`)
      .then((d) => {
        if (!cancelado && d?.run_id === runId && activeRunRef.current === runId) setReconciliacao(d)
      })
    return () => { cancelado = true }
  }, [gen?.run_id, call, base])
  useIntervaloVisivel(useCallback(async () => {
    const runId = gen?.run_id
    if (!runId) return
    const d = await call(`${base}/streaming/reconciliacao?run_id=${encodeURIComponent(runId)}`)
    if (d?.run_id === runId && activeRunRef.current === runId) setReconciliacao(d)
  }, [call, base, gen?.run_id]), 5000, Boolean(gen?.run_id) && observar)

  // Assim que uma execucao finita estiver reconciliada, fecha os tres cursores
  // e todos os polls Atlas-facing. Os numeros permanecem congelados na tela.
  useEffect(() => {
    if (!replay && !preparando && !gen?.running && !gen?.stopping && reconciliacao?.final === 'reconciliado') setSessaoLive(false)
  }, [gen?.running, gen?.stopping, preparando, reconciliacao?.final, replay])

  const aspOk = aspStatus?.estado === 'configurado'
  // "individual" = 1 insert por PIX. Muda o significado do número de latência
  // na tela, então precisa ser visível, não implícito.
  const modoIndividual = !replay && (gen?.modo ?? cenario?.modo_escrita) === 'individual'
  const csPendentes = reconciliacao?.change_streams?.pendentes
  const kafkaPendentes = reconciliacao?.kafka?.pendentes
  const aspPendentes = reconciliacao?.asp?.pendentes

  // Tamanho da janela lido das bordas da própria janela ($meta stream.window),
  // não fixado no código: gravações antigas podem ter outra configuração.
  // Deixar o número escrito na tela faria a legenda mentir. Sem janela ainda,
  // cai no valor configurado hoje.
  const janelaSegundos = (() => {
    const j = janelas[0]
    if (j?.window_start && j?.window_end) {
      const dt = (new Date(`${String(j.window_end).replace(' ', 'T')}Z`)
        - new Date(`${String(j.window_start).replace(' ', 'T')}Z`)) / 1000
      if (Number.isFinite(dt) && dt > 0) return Math.round(dt)
    }
    return 5
  })()

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


  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Enredo + escala do cenário */}
      <div className="card str-hero">
        <div className="str-hero-copy">
          <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 6 }}>Um PIX gravado, três consumos, zero ETL</div>
          <div style={{ fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.55 }}>
          Uma escrita em <code>pix.transacoes</code>, três consumos independentes — sem ETL, sem dual-write.
          A pergunta não é "aguenta o volume", e sim <strong>quantos sistemas o mesmo dado alimenta sem você
          construir integração</strong>, com perda zero comprovada por reconciliação.
          </div>
          {/* A prosa longa cabe na fala do apresentador; na tela ela competia
              com as três colunas, que são a evidência. Fica um clique atrás. */}
          <details className="str-hero-mais">
            <summary>Contexto e premissas</summary>
            <p>
              Sem pipeline de cópia, sem job noturno, sem uma segunda base para análise: as três colunas consomem
              <strong> a mesma mudança</strong> — dentro da aplicação (Change Streams), no barramento para o
              ecossistema (Kafka Connector) e num processador gerenciado com janela e estado (ASP).
            </p>
            <p>
              No Data Explorer, acompanhe <code>pix</code> → <code>transacoes</code>;
              <code>pix_poc.pix_transactions</code> é outra collection e não recebe esta carga.
            </p>
            <p>
              Workload sintético, mecanismos reais · <code>run_id</code> reconcilia os três caminhos ·
              TPS e latência valem apenas para esta execução.
            </p>
          </details>
        </div>
        <div className="str-env-compact">
          <div><span>Cluster</span><strong>{cenario?.ambiente?.cluster || '—'}</strong></div>
          <div><span>ASP</span><strong>{aspStatus?.tier || '—'}</strong></div>
          <div><span>Cursores</span><strong>{cenario?.ambiente?.particoes_consumo || '—'}</strong></div>
          <div><span>RTT local</span><strong>{rede?.rtt_ms != null ? `${rede.rtt_ms} ms` : '—'}</strong></div>
        </div>
      </div>

      <div className="str-selo-linha" style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: 10 }}>
        <span className="str-selo-txt">
          <strong>Modo da demonstração:</strong> ao vivo é o caminho principal; replay é contingência.
        </span>
        <span style={{ display: 'flex', gap: 6 }}>
          <button className={`btn btn-sm ${!replay ? 'btn-primary' : ''}`} onClick={() => setModo('live')} disabled={preparando || gen?.running || gen?.stopping}>
            ● Ao vivo
          </button>
          <button className={`btn btn-sm ${replay ? 'btn-primary' : ''}`} onClick={() => { setSessaoLive(false); setModo('replay') }} disabled={preparando || gen?.running || gen?.stopping}>
            ▶ Replay de segurança
          </button>
        </span>
      </div>

      {/* Transparencia e parte da evidencia: replay nunca parece live. */}
      {replay && manifest?.disponivel && (
        <div className="str-selo-linha">
          <span className="str-selo-tag">execução real gravada</span>
          <span className="str-selo-txt">
            {fmtDataGravacao(manifest.gravado_em)} · <code>{manifest.run_id}</code> · reprodução sem escrita no Atlas
          </span>
        </div>
      )}
      {replay && manifest && !manifest.disponivel && (
        <div className="str-selo-linha">
          <span className="str-selo-tag str-selo-tag-alerta">sem gravação</span>
          <span className="str-selo-txt">
            rode <code>python scripts/capture_replay.py</code> com o ambiente ligado
          </span>
        </div>
      )}
      {!replay && gen && !Object.prototype.hasOwnProperty.call(gen, 'write_ack') && (
        <div className="str-selo-linha">
          <span className="str-selo-tag str-selo-tag-alerta">backend desatualizado</span>
          <span className="str-selo-txt">reinicie o <code>overview</code> para habilitar ACK Atlas e o cenário padrão de 8.000 TPS</span>
        </div>
      )}

      {/* Gerador */}
      <div className="card str-generator">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 10 }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Fluxo PIX da execução</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 2 }}>
              {replay ? 'Escrita original' : 'Escrita ao vivo'} em <code>{gen?.colecao || 'pix.transacoes'}</code>
              {modoIndividual ? <> · <strong>1 insert = 1 PIX</strong>{gen?.workers ? `, ${gen.workers} em voo` : ''}</> : null}
              {' '}· TTL de {gen?.ttl_segundos ? (gen.ttl_segundos >= 60 ? `${Math.round(gen.ttl_segundos / 60)} min` : `${gen.ttl_segundos}s`) : '—'} em <code>ts</code>.
              {replay ? ' O Play reproduz a medição; o banco não é tocado.' : ' Contadores e latências abaixo são medidos nesta execução.'}
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {gen?.running || gen?.stopping
              ? <button className="btn btn-danger btn-sm" onClick={stopGen} disabled={gen?.stopping}
                  title="Fecha a carga e avança o watermark para reconciliar a última janela">
                  {gen?.stopping ? '◌ Fechando janelas…' : '■ Parar e reconciliar'}
                </button>
              : <button className="btn btn-primary btn-sm" onClick={iniciar} disabled={preparando || (replay && !manifest?.disponivel)}>
                {preparando ? '◌ Limpando rodada anterior…' : (replay ? '▶ Play' : `▶ Play · ${num(tpsSelecionado)} TPS por ${cenario?.default_duration_s ?? 30}s`)}
              </button>}
            <button className="btn btn-sm" onClick={resetExecucao} disabled={preparando}
              title={replay ? 'Volta ao início e limpa os painéis' : 'Para a sessão e limpa somente os dados PIX da PoV'}>↺ Reset</button>
          </div>
        </div>

        {/* Injeção de falha: a evidência que o caminho feliz não dá. Só ao
            vivo — em replay não há o que derrubar, a execução já terminou. */}
        {!replay && (
          <div className="str-falhas">
            <span className="str-falhas-rotulo">Quebrar de propósito</span>
            <button className="btn btn-xs" onClick={derrubarConnector}
              disabled={falha === 'connector' || !gen?.running}
              title="Para o connector por 8 s no meio do fluxo. Ele volta pelo offset guardado e a reconciliação tem de fechar mesmo assim.">
              {falha === 'connector' ? '◌ connector fora…' : '⚡ Derrubar o Kafka Connector (8s)'}
            </button>
            <button className="btn btn-xs" onClick={injetarInvalido} disabled={falha === 'evento'}
              title="Grava uma transação com `valor` em texto. O ASP desvia para a DLQ e segue rodando.">
              {falha === 'evento' ? '◌ enviando…' : '☠ Injetar evento inválido → DLQ'}
            </button>
            {falhaMsg && <span className="str-falhas-msg">{falhaMsg}</span>}
            {!gen?.running && (
              <span className="str-falhas-msg">derrubar o connector só faz sentido com o fluxo rodando</span>
            )}
          </div>
        )}

        {!replay && (
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
            {cenario?.presets?.map((preset) => (
              <button key={preset.tps} className={`btn btn-sm ${tpsSelecionado === preset.tps ? 'btn-primary' : ''}`}
                onClick={() => { setTpsSelecionado(preset.tps); setModoSelecionado(preset.modo || cenario?.modo_escrita) }}
                disabled={preparando || gen?.running || gen?.stopping} title={preset.detalhe}>
                {preset.label} · {num(preset.tps)} TPS
              </button>
            ))}
          </div>
        )}
        <div className="str-gen-row">
          <div className="str-stats">
            <Stat label={replay ? 'TPS da execução' : 'TPS medido'} value={num(replay ? (gen?.tps_alvo ?? manifest?.tps_alvo ?? 0) : (gen?.tps_medido ?? 0))}
              sub={replay ? (gen?.running ? 'reproduzindo' : 'medido na gravação') : `alvo ${num(gen?.tps_alvo || tpsSelecionado)}`}
              color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Inseridos" value={num(gen?.inseridos ?? 0)} sub={replay ? 'confirmados pelo Atlas na gravação' : 'confirmados pelo Atlas agora'} />
            <Stat label="Escrita no Atlas" value={fmtMs(gen?.ingestao_servidor?.p50)}
              color={gen?.ingestao_servidor?.p50 != null ? '#00ED64' : undefined}
              sub={modoIndividual ? 'p50 server-side · janela isolada' : 'p50 server-side (lote)'} />
            <Stat label="ACK do cliente" value={fmtMs(gen?.write_ack?.p50)}
              sub="p50 ponta a ponta, inclui rede" />
            <Stat label="Execução" value={gen?.run_id ? gen.run_id.slice(-6).toUpperCase() : '—'}
              sub="run_id para reconciliação" color={gen?.run_id ? '#00ED64' : undefined} />
            <Stat label="Escala de referência" value={`${num(cenario?.referencia_pix?.pico_sustentado_fatia_tps ?? 1000)} TPS`}
              sub={`${cenario?.referencia_pix?.premissa_participacao_pct ?? 10}% do pico sustentado BCB`} />
            <Stat label="Duração" value={`${gen?.duration_s ?? cenario?.default_duration_s ?? 30} s`}
              sub="stop automático + reconciliação" />
          </div>
        </div>
        {/* O número de destaque é o do servidor; a rede fica ao lado como
            contexto. Sem essa separação a plateia lê latência de rede como se
            fosse tempo de banco. */}
        {!replay && gen?.ingestao_servidor?.disponivel && (
          <div className="str-workload-note" style={{ marginTop: 12 }}>
            <strong>Onde estão os milissegundos:</strong> as escritas observadas pelo mongod levaram{' '}
            <strong>{fmtMs(gen.ingestao_servidor.p50)} (p50)</strong> e{' '}
            {fmtMs(gen.ingestao_servidor.p99)} (p99), sobre {num(gen.ingestao_servidor.comandos)}{' '}
            comandos no intervalo. O ACK de {fmtMs(gen?.write_ack?.p50)} inclui
            o round-trip da aplicação até o cluster
            {rede?.rtt_ms ? <> (RTT medido {fmtMs(rede.rtt_ms)})</> : null}.
            {modoIndividual
              ? <> Como a carga usa <strong>um insert por PIX</strong>, a amostra representa um PIX quando
                  esta PoV está isolada. <code>opLatencies</code> é cluster-wide e não filtra por <code>run_id</code>.</>
              : null}
            {' '}Os percentis vêm dos buckets do histograma do servidor e são aproximados por faixa.
          </div>
        )}
        {!replay && cenario?.referencia_pix && (
          <div className="str-workload-note" style={{ marginTop: 12 }}>
            <strong>Escala do PIX, por números públicos do BCB:</strong> o recorde de {num(cenario.referencia_pix.recorde_brasil_transacoes_dia)} PIX
            em {cenario.referencia_pix.recorde_brasil_data} equivale a {num(cenario.referencia_pix.media_brasil_tps)} TPS médios no Brasil,
            e o planejamento do BCB fala em {num(cenario.referencia_pix.pico_sustentado_brasil_tps)} TPS de pico sustentado.
            Os presets partem de <strong>{cenario.referencia_pix.premissa_participacao_pct}%</strong> desse pico
            ({num(cenario.referencia_pix.pico_sustentado_fatia_tps)} TPS) — premissa de apresentação, ajustável.
            É equivalência de carga, não certificação de capacidade ou sizing de produção.
          </div>
        )}
        {modoIndividual && (
          <div className="str-workload-note" style={{ marginTop: 8 }}>
            <strong>Onde está o limite:</strong> quem satura primeiro é o <strong>consumidor local</strong>,
            não o Atlas. O servidor segue gravando cada PIX em poucos milissegundos, com as conexões em
            torno de 5% do limite do cluster — a folga que aparece nos contadores é do banco, não da máquina
            que apresenta.
          </div>
        )}
      </div>

      {/* Argumento de negócio: entra quando o cliente pergunta "e daí?".
          Aberto por padrão ele empurrava as três colunas para fora da dobra. */}
      <details className="card str-tech-details" aria-labelledby="str-bank-impact-title">
        <summary id="str-bank-impact-title">
          Um commit, três padrões — sem dual-write <span>impacto para uma plataforma bancária</span>
        </summary>
        <div className="str-capability-grid">
          <article className="str-capability str-capability-cs">
            <div className="str-capability-title"><span style={{ color: '#00ED64' }}>Consistência</span><strong>Uma fonte operacional</strong></div>
            <p>O evento nasce do dado já confirmado. A aplicação não precisa gravar MongoDB e publicar outro sistema na mesma requisição.</p>
          </article>
          <article className="str-capability str-capability-kafka">
            <div className="str-capability-title"><span style={{ color: '#06b6d4' }}>Integração</span><strong>Fan-out sem acoplamento</strong></div>
            <p>Kafka continua disponível para o ecossistema; o connector remove código de CDC da aplicação e preserva offsets e replay.</p>
          </article>
          <article className="str-capability str-capability-asp">
            <div className="str-capability-title"><span style={{ color: '#a855f7' }}>Operação</span><strong>Streaming em MQL gerenciado</strong></div>
            <p>Janela, estado, checkpoint, materialização e DLQ ficam no Atlas, reduzindo um plano de processamento separado para estes casos.</p>
          </article>
        </div>
        <div className="str-note" style={{ marginTop: 12 }}>
          A PoV prova integridade, retomada e latência deste ambiente. Ela não elimina idempotência em efeitos externos nem substitui
          sizing, HA, segurança e SLOs de produção.
        </div>
      </details>

      {/* As três colunas */}
      <div className="str-grid">

        {/* ── COLUNA 1 ── */}
        <div className="str-col str-col-cs">
          <div className="str-col-head">
            <span>🍃 Change Streams</span>
            <span className={`badge ${csConnected ? 'badge-green' : 'badge-yellow'}`}>
              {csConnected ? (replay ? '● reproduzindo' : '● ao vivo') : (observar ? '○ conectando' : '○ em espera')}
            </span>
          </div>
          <div className="str-col-body">
            <div className="str-stats">
              <Stat label="Eventos" value={num(csState.eventos)} color="#00ED64" />
              <Stat label="Recuperados" value={num(csState.recuperados)} color={csState.recuperados ? '#00ED64' : undefined} sub="via resume token" />
              <Stat label="Pendentes" value={num(csPendentes ?? 0)} color={csPendentes ? '#f97316' : undefined} sub="backlog pós-commit" />
            </div>
            <LatencyContext tone="cs" title="Propagação pós-commit"
              detail="timestamp persistido → worker da aplicação; não é tempo de liquidação" />
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
                : `● ${connectorState}${replay ? ' na gravação' : ''}`}
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
                  <Stat label="Pendentes" value={num(kafkaPendentes ?? 0)} color={kafkaPendentes ? '#f97316' : undefined} sub="backlog pós-commit" />
                </div>
                <LatencyContext tone="kafka" title="Propagação pelo barramento"
                  detail="timestamp persistido → connector → Kafka → observador local" />
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
              Nesta PoV a key publicada é a <strong>document key do MongoDB</strong> e o valor é JSON sem contrato
              de Schema Registry. Em produção PIX, <strong>chave, particionamento e schema versionado</strong> são
              decisões explícitas: a key define o escopo de ordenação, e troca de schema precisa ser compatível
              com todos os consumidores. Offset não elimina duplicidade de negócio.
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
              {!aspStatus ? '○ verificando' : aspOk ? (replay ? '● ativo na gravação' : '● processor ativo') : '○ não configurado'}
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
                    sub={`${num(aspStatus?.janelas)} janelas de ${janelaSegundos} s`} />
                  <Stat label="Volume" value={`R$ ${fmtEscala(aspStatus?.volume_agregado)}`} sub="somado pelo processor" />
                  <Stat label="DLQ" value={num(aspStatus?.dlq ?? 0)} color={(aspStatus?.dlq ?? 0) ? '#f97316' : undefined} sub="rejeitados" />
                  <Stat label="Pendentes" value={num(aspPendentes ?? 0)} color={aspPendentes ? '#f97316' : undefined} sub="janela ou backlog" />
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
                  <>
                    <LatencyContext tone="asp" title="Latência da materialização"
                      detail={`fim da janela → $merge → tela; a janela de ${janelaSegundos}s é intencional`} />
                    <Percentis m={aspMetrics} color="#a855f7" />
                  </>
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
                      {janelas.length === 0 && <tr><td colSpan={5} className="str-empty">Aguardando a primeira janela fechar ({janelaSegundos} s)…</td></tr>}
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
              A janela usa <strong>event time</strong> e aceita 2 s de atraso. Ela fecha quando o
              <strong> watermark avança</strong>; se a fonte ficar ociosa, a última janela pode permanecer aberta.
              Documento que chega tarde demais é contabilizado na DLQ, não silenciosamente descartado.
            </div>
            <div className="str-note">
              <strong>Valores altos</strong> é um sinal operacional simples (PIX ≥ R$ 5 mil), não um motor antifraude.
              Ele prova que o mesmo pipeline pode manter estado de janela e produzir indicadores acionáveis sem mover o fluxo para batch.
            </div>
            <div className="str-note">
              O processor faz <code>$merge</code> em <code>pix.metricas_janela</code> e o backend
              <strong> assiste essa coleção com um change stream</strong>: o resultado do ASP chega nesta tela
              pela mecânica da coluna 1. Um processor tem um único sink terminal; fan-out adicional usa
              processadores encadeados ou consumidores da coleção materializada.
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
        {/* O veredito em números absolutos: volume, perdas e o tier em que isso
            rodou. Sem comparação com nenhuma instituição — cada plateia faz a
            própria conta a partir da escala de referência do BCB. */}
        {!replay && reconciliacao?.final === 'reconciliado' && reconciliacao?.fonte?.inseridas > 0 && (
          <div className="str-veredito">
            <div>
              <span>{num(reconciliacao.fonte.inseridas)}</span>
              <small>transações nesta execução</small>
            </div>
            <div>
              <span style={{ color: '#00ED64' }}>0</span>
              <small>perdidas · conferido nos três caminhos</small>
            </div>
            <div>
              <span>{cenario?.ambiente?.cluster || '—'}{aspStatus?.tier ? ` + ${aspStatus.tier}` : ''}</span>
              <small>tier em que esta execução rodou</small>
            </div>
          </div>
        )}
        {reconciliacao?.fonte && (
          <div className="str-neg" style={{ marginTop: 14 }}>
            {[
              ['Fonte Atlas', reconciliacao.fonte.inseridas, 0,
                reconciliacao.final === 'reconciliado',
                reconciliacao.gerador_ativo ? 'escritas confirmadas até agora' : 'total persistido da execução'],
              ['Change Streams', reconciliacao.change_streams.unicos, reconciliacao.change_streams.pendentes, reconciliacao.change_streams.reconciliado],
              ['Kafka', reconciliacao.kafka.unicos, reconciliacao.kafka.pendentes, reconciliacao.kafka.reconciliado],
              ['ASP + DLQ', reconciliacao.asp.contabilizadas, reconciliacao.asp.pendentes, reconciliacao.asp.reconciliado],
            ].map(([label, value, pending, ok, status]) => (
              <div className="str-neg-c" key={label}>
                <div className="str-neg-k">{label}</div>
                <div className="str-neg-v" style={ok ? { color: '#00ED64' } : undefined}>{num(value)}</div>
                <div className="str-neg-s">{status || (ok ? 'contagem fechada' : `${num(pending)} ainda pendente(s)`)}</div>
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
        {/* Um arquiteto de pagamentos precisa desta linha ANTES de qualquer
            número de throughput: entrega ao menos uma vez é a semântica real, e
            a chave única é o que a torna segura. Estava só num rodapé. */}
        <div className="str-garantia">
          <span className="str-garantia-tag">semântica de entrega</span>
          <span>
            Change Streams e Kafka entregam <strong>ao menos uma vez</strong>: depois de uma retomada, o mesmo
            evento pode chegar de novo. O índice único em <code>endToEndId</code> é o que torna o reprocessamento
            seguro — o consumidor precisa ser idempotente, e é assim que esta PoV conta 0 duplicados.
            A ordem é garantida <strong>dentro da partição</strong> (<code>particao</code>, derivada do pagador),
            não entre partições.
          </span>
        </div>

        {/* As ressalvas são necessárias, mas quem pergunta por elas pergunta
            depois de ver os números — não por cima deles. */}
        <details className="str-note-details" style={{ marginTop: 12 }}>
          <summary>Como ler estes números</summary>
          <div className="str-note" style={{ marginTop: 8 }}>
            O estado final só fica verde quando a mesma execução está contabilizada nos três caminhos. Em event time,
            parar a fonte não força a última janela a fechar: ela depende do avanço do watermark. Durante o fluxo,
            “pendente” significa backlog ou janela ainda aberta, não evidência de perda.
            Durante a execução, snapshots coletados em instantes diferentes podem divergir momentaneamente. Os contadores
            de CS/Kafka pertencem ao processo {replay ? 'da gravação; Atlas, ASP e DLQ foram consultados no banco durante a captura.' :
              'atual da API; Atlas, ASP e DLQ são consultados no banco ao vivo.'}
          </div>
        </details>
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

      <details className="card str-tech-details">
        <summary>Checklist de produção PIX <span>o que a PoV prova e o que ainda é decisão</span></summary>
        <div style={{ overflowX: 'auto' }}>
          <table className="lg-table">
            <thead><tr><th>Preocupação</th><th>Evidência nesta PoV</th><th>Decisão de produção</th></tr></thead>
            <tbody>
              <tr><td>Perda e duplicidade</td><td>Reconciliação, resume/offset/checkpoint e chave idempotente</td><td>SLO, retenção do oplog, política de replay e deduplicação durável</td></tr>
              <tr><td>Ordenação</td><td>Ordem observada por cursor/partição</td><td>Chave Kafka e escopo exigido: conta, cliente, endToEndId ou agregado</td></tr>
              <tr><td>Backpressure</td><td>Lag, percentis, backlog e estado da task</td><td>Limites, autoscaling, alertas e teste de carga representativo</td></tr>
              <tr><td>Contrato</td><td>Validação ASP e DLQ por motivo</td><td>Schema versionado, compatibilidade e ownership entre squads</td></tr>
              <tr><td>Segurança</td><td>Credenciais fora do frontend e mutações protegidas</td><td>TLS/SASL, ACL/RBAC, PrivateLink, rotação e segregação de ambientes</td></tr>
              <tr><td>Continuidade</td><td>Restart controlado e retomada demonstrável</td><td>RTO/RPO, HA do Kafka/Connect, DR regional e runbooks testados</td></tr>
            </tbody>
          </table>
        </div>
      </details>
    </div>
  )
}
