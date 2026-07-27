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

  // ── Esta aba roda SEMPRE sobre uma execução gravada ──────────────────────
  // Não existe modo ao vivo aqui. A escrita ao vivo estressava o cluster sem
  // necessidade: M20/M30 são instâncias burstable e o auto-scaling do Atlas
  // dispara por CPU RELATIVA — 17,6% absolutos deram 88% relativos e escalaram
  // o cluster com o gerador JÁ PARADO, só com esta tela aberta e consumindo.
  //
  // O que se reproduz é medição real (scripts/capture_replay.py grava os mesmos
  // eventos SSE e snapshots que a tela consumia ao vivo); nada é sintetizado.
  // Mas número medido ontem apresentado sem contexto vira número de hoje na
  // cabeça de quem assiste — por isso o selo abaixo é permanente e não
  // condicional. Ele é a única coisa que separa "gravado" de "ao vivo" para a
  // plateia. Não remover.
  const replay = true
  const base = '/replay'
  const [manifest, setManifest] = useState(null)

  useEffect(() => {
    // Responde 200 com `disponivel: false` quando não há gravação — nesse caso
    // a aba avisa em vez de mostrar painéis vazios sem explicação.
    call('/replay/manifest').then((d) => d && setManifest(d))
  }, [call])

  // ── Cenário PIX conceitual ────────────────────────────────────────────────
  const [cenario, setCenario] = useState(null)
  const [rede, setRede] = useState(null)
  useEffect(() => {
    // O cenário vem da gravação: descreve o ambiente em que a execução foi
    // medida, não o ambiente de agora.
    call(`${base}/streaming/cenario`).then((d) => d && setCenario(d))
    call(`${base}/streaming/rede`).then((d) => d && setRede(d))
  }, [call, base])

  // ── Relógio da reprodução ────────────────────────────────────────────────
  const [gen, setGen] = useState(null)

  const refreshGen = useCallback(async () => {
    const data = await call(`${base}/streaming/generator/status`)
    if (data) setGen(data)
  }, [call, base])

  // Enquanto o relógio anda, 1 s. Parado, um heartbeat lento só para notar um
  // play disparado de outra aba — a posição não muda sozinha.
  useIntervaloVisivel(refreshGen, gen?.running ? 1000 : 15000)

  // Play move o relógio da gravação. Nenhuma escrita sai daqui.
  const play = useCallback(async () => {
    await call('/replay/play', { method: 'POST' })
    refreshGen()
  }, [call, refreshGen])

  const stopGen = async () => {
    await call('/replay/stop', { method: 'POST' })
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

  // Snapshot gravado: só muda com o relógio andando. Uma carga inicial para o
  // painel não nascer vazio, e poll apenas durante a reprodução.
  const lerKafkaStatus = useCallback(async () => {
    const d = await call(`${base}/streaming/kafka/status`)
    if (d) setKafkaStatus(d)
  }, [call, base])
  useEffect(() => { lerKafkaStatus() }, [lerKafkaStatus])
  useIntervaloVisivel(lerKafkaStatus, 4000, Boolean(gen?.running))

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

  const lerAspStatus = useCallback(async () => {
    const d = await call(`${base}/streaming/asp/status`)
    if (d) setAspStatus(d)
  }, [call, base])
  useEffect(() => { lerAspStatus() }, [lerAspStatus])
  useIntervaloVisivel(lerAspStatus, 5000, Boolean(gen?.running))

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
  }, [call, base]), 4000, Boolean(gen?.running))

  // Reconciliação só enquanto a reprodução anda. Parada, o resultado é o mesmo
  // a cada consulta — repetir não acrescenta nada.
  const reconciliadoRef = useRef(false)
  useEffect(() => {
    reconciliadoRef.current = false
    if (!gen?.run_id) { setReconciliacao(null); return }
    call(`${base}/streaming/reconciliacao?run_id=${encodeURIComponent(gen.run_id)}`)
      .then((d) => d && setReconciliacao(d))
  }, [gen?.run_id, call, base])
  useIntervaloVisivel(useCallback(async () => {
    if (!gen?.run_id || reconciliadoRef.current) return
    const d = await call(`${base}/streaming/reconciliacao?run_id=${encodeURIComponent(gen.run_id)}`)
    if (!d) return
    setReconciliacao(d)
    // Fechou: para de consultar até a próxima execução mexer no run_id.
    if (d.final === 'reconciliado' && !d.gerador_ativo) reconciliadoRef.current = true
  }, [call, base, gen?.run_id]), 5000, Boolean(gen?.run_id) && Boolean(gen?.running))

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

      {/* Selo de origem — permanente, não condicional a modo.
          Os números abaixo foram medidos de verdade contra o Atlas, mas estão
          sendo reproduzidos. Quem assiste precisa saber disso sem perguntar. */}
      {manifest?.disponivel ? (
        // Uma linha só, discreta, mas sempre presente. O `title` guarda o
        // detalhe para quem passar o mouse ou for perguntado na hora.
        <div className="str-selo-linha"
          title={`Reprodução de ${manifest.run_id}, medida em ${manifest.gravado_em} contra o cluster real. `
            + 'Os valores são medições daquela execução, não simulação. Nada é escrito no banco durante a reprodução.'}>
          <span className="str-selo-tag">▶ execução gravada</span>
          <span className="str-selo-txt">
            {manifest.run_id} · {String(manifest.gravado_em).slice(0, 10)} · nada é escrito no Atlas
          </span>
        </div>
      ) : (
        <div className="str-selo-linha">
          <span className="str-selo-tag str-selo-tag-alerta">sem gravação</span>
          <span className="str-selo-txt">
            rode <code>python scripts/capture_replay.py</code> com o ambiente ligado
          </span>
        </div>
      )}

      {/* Gerador */}
      <div className="card str-generator">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', marginBottom: 10 }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Fluxo PIX da execução</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 2 }}>
              Escrita original em <code>{gen?.colecao || 'pix.transacoes'}</code> · micro-batches a
              cada 100 ms · TTL de {gen?.ttl_segundos ? (gen.ttl_segundos >= 60 ? `${Math.round(gen.ttl_segundos / 60)} min` : `${gen.ttl_segundos}s`) : '—'} em <code>ts</code>.
              O Play reproduz essa execução; o banco não é tocado.
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {gen?.running
              ? <button className="btn btn-danger btn-sm" onClick={stopGen}>■ Parar</button>
              : <button className="btn btn-primary btn-sm" onClick={play}>▶ Play</button>}
          </div>
        </div>

        {/* Sem presets nem slider de TPS: o Play reproduz a execução como ela
            foi medida. Um controle de carga aqui prometeria o que a reprodução
            não faz — a carga foi decidida na gravação. */}
        <div className="str-gen-row">
          <div className="str-stats">
            <Stat label="TPS da execução" value={num(gen?.tps_alvo ?? manifest?.tps_alvo ?? 0)}
              sub={gen?.running ? 'reproduzindo' : 'medido na gravação'}
              color={gen?.running ? '#00ED64' : undefined} />
            <Stat label="Inseridos" value={num(gen?.inseridos ?? 0)} sub="confirmados pelo Atlas na gravação" />
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
            {/* "ao vivo" aqui contradiria o selo: o stream está conectado, mas
                o que trafega nele é a gravação sendo reproduzida. */}
            <span className={`badge ${csConnected ? 'badge-green' : 'badge-yellow'}`}>
              {csConnected ? '● reproduzindo' : '○ conectando'}
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
