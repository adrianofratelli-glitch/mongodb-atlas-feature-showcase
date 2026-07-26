import React, { useState, useEffect, useRef } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useApi } from '../hooks/useApi'

SyntaxHighlighter.registerLanguage('javascript', js)

const SCENARIOS = [
  {
    key: 'simples', title: 'Índice Simples', fields: ['categoria'],
    description: 'Índice em campo único para filtros de categoria — base para a maioria das queries',
    command: 'db.produtos.createIndex({ categoria: 1 })',
    query: `// Desde o 4.2, todo build é "hybrid":\n// leituras e escritas seguem durante a maior parte do build\ndb.produtos.createIndex(\n  { categoria: 1 }\n)`,
  },
  {
    key: 'composto', title: 'Índice Composto', fields: ['categoria', '-preco'],
    description: 'Cobre queries combinadas (categoria + preço desc) — elimina COLLSCAN',
    command: 'db.produtos.createIndex({ categoria: 1, preco: -1 })',
    query: `db.produtos.createIndex(\n  { categoria: 1, preco: -1 }\n)`,
  },
  {
    key: 'parcial', title: 'Índice Parcial', fields: ['preco'],
    description: 'Apenas sobre produtos em estoque — menor footprint na RAM e disco',
    command: 'db.produtos.createIndex({ preco: 1 }, { partialFilterExpression: { em_estoque: true } })',
    query: `db.produtos.createIndex(\n  { preco: 1 },\n  { partialFilterExpression: { em_estoque: true } }\n)`,
    partial_filter: { em_estoque: true },
  },
]

const EXPLAIN_SCENARIOS = [
  { key: 'simples',  label: 'Filtro por categoria',        hint: 'find({ categoria: "Eletrônicos" })' },
  { key: 'composto', label: 'Categoria + preço desc',      hint: 'find({ categoria }).sort({ preco: -1 })' },
  { key: 'parcial',  label: 'Em estoque + preço < 100',    hint: 'find({ em_estoque: true, preco: { $lt: 100 } })' },
]

export default function Reindexacao() {
  const { call, loading } = useApi()
  const [indexes, setIndexes] = useState([])
  const [result, setResult] = useState(null)
  const [selected, setSelected] = useState(null)
  const [probe, setProbe] = useState(null)          // leituras concorrentes durante o build
  const [explainSel, setExplainSel] = useState('composto')
  const [explainRes, setExplainRes] = useState(null)
  const [explainLoading, setExplainLoading] = useState(false)
  const [showAllIndexes, setShowAllIndexes] = useState(false)
  const cancelled = useRef(false)

  const fetchIndexes = async () => {
    const data = await call('/reindexacao/indexes')
    if (data) setIndexes(data.indexes)
  }

  useEffect(() => {
    cancelled.current = false
    fetchIndexes()
    return () => { cancelled.current = true }
  }, [])

  const sleep = (ms) => new Promise(r => setTimeout(r, ms))

  const handleCreate = async (scenario) => {
    cancelled.current = false
    setSelected(scenario.key)
    setResult({ phase: 'starting', name: null })
    setProbe(null)

    const params = new URLSearchParams()
    scenario.fields.forEach(f => params.append('fields', f))
    const opts = { method: 'POST' }
    if (scenario.partial_filter) {
      opts.headers = { 'Content-Type': 'application/json' }
      opts.body = JSON.stringify(scenario.partial_filter)
    }

    const data = await call(`/reindexacao/create?${params.toString()}`, opts)
    if (!data) { setResult(null); return }

    if (data.status === 'exists') {
      setResult({ phase: 'exists', name: data.index_name })
      await fetchIndexes()
      return
    }

    // Build em andamento: acompanha o progresso e prova com leituras
    // concorrentes que a coleção continua respondendo durante o build.
    setResult({ phase: 'building', name: data.index_name, note: data.note })
    await fetchIndexes()
    let probeCount = 0
    let probeStop = false
    const probeLoop = (async () => {
      while (!probeStop && !cancelled.current) {
        const p = await call('/reindexacao/read-probe')
        if (p && p.ok) {
          probeCount += 1
          setProbe({ count: probeCount, lastMs: p.latency_ms })
        }
        await sleep(700)
      }
    })()

    try {
      for (let i = 0; i < 60; i++) {
        await sleep(2000)
        if (cancelled.current) return
        const st = await call(`/reindexacao/build-status?name=${data.index_name}`)
        if (!st) continue
        if (st.status === 'done') {
          setResult({ phase: 'done', name: data.index_name, elapsed: st.elapsed_seconds, note: data.note, probes: probeCount })
          await fetchIndexes()
          return
        }
        if (st.status === 'error') {
          setResult({ phase: 'error', name: data.index_name, error: st.error })
          return
        }
      }
      setResult({
        phase: 'timeout',
        name: data.index_name,
        error: 'O acompanhamento expirou após 120 s. O build pode continuar no cluster; consulte o status novamente.',
      })
    } finally {
      probeStop = true
      await probeLoop
    }
  }

  const runExplain = async () => {
    setExplainLoading(true)
    const d = await call(`/reindexacao/explain?scenario=${explainSel}`)
    if (d) setExplainRes(d)
    setExplainLoading(false)
  }

  const handleDrop = async (name) => {
    await call(`/reindexacao/drop/${name}`, { method: 'DELETE' })
    await fetchIndexes()
    setResult(null)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          <strong>Index build sem downtime</strong> — desde o MongoDB 4.2, todo build de índice usa o processo
          otimizado (<em>hybrid build</em>): a coleção continua atendendo <strong>leituras e escritas durante a maior parte
          da construção</strong>. Locks exclusivos curtos ainda ocorrem no início e no fim; não há bloqueio prolongado.
        </div>
      </div>

      <div className="grid-auto">
        {SCENARIOS.map(s => (
          <div key={s.key} className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <strong style={{ fontSize: 15 }}>{s.title}</strong>
              <span className="badge badge-green">Online Build</span>
            </div>
            <p style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{s.description}</p>
            <code className="idx-command" title={s.command}>{s.command}</code>
            <details className="code-details">
              <summary>Ver comando comentado</summary>
              <SyntaxHighlighter language="javascript" style={atomOneDark} customStyle={{ borderRadius: 6, fontSize: 11.5, margin: '8px 0 0' }}>
                {s.query}
              </SyntaxHighlighter>
            </details>
            <button className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }}
              onClick={() => handleCreate(s)}
              disabled={selected === s.key && result && (result.phase === 'starting' || result.phase === 'building')}>
              {selected === s.key && result && (result.phase === 'starting' || result.phase === 'building')
                ? <><span className="spinner" /> Construindo...</>
                : 'Criar Índice'}
            </button>
          </div>
        ))}
      </div>

      {result && result.phase && (
        <div className="card" style={{
          background: ['error', 'timeout'].includes(result.phase) ? 'rgba(255,105,96,.08)' : 'rgba(0,237,100,.08)',
          borderColor: ['error', 'timeout'].includes(result.phase) ? 'rgba(255,105,96,.35)' : 'rgba(0,237,100,.3)',
        }}>
          {result.phase === 'starting' && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}><span className="spinner" /> Iniciando build…</div>
          )}
          {result.phase === 'building' && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span className="spinner" /> Construindo <strong>{result.name}</strong> (hybrid build — coleção liberada)…
              </div>
              {probe && (
                <div style={{ marginTop: 8, padding: '8px 12px', background: 'rgba(0,237,100,.08)', border: '1px solid rgba(0,237,100,.3)', borderRadius: 6, fontSize: 12.5 }}>
                  📖 <strong>{probe.count}</strong> leituras concorrentes atendidas durante o build · última em <strong>{probe.lastMs}ms</strong> — a coleção não bloqueou.
                </div>
              )}
              <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginTop: 6 }}>{result.note}</div>
            </>
          )}
          {result.phase === 'done' && (
            <>
              <div>✅ Índice criado: <strong>{result.name}</strong></div>
              <div style={{ marginTop: 4 }}>⏱ Build concluído em <strong>{result.elapsed}s</strong></div>
              {result.probes > 0 && (
                <div style={{ marginTop: 4 }}>📖 <strong>{result.probes}</strong> leituras concorrentes atendidas durante o build — zero downtime, comprovado.</div>
              )}
              <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginTop: 6 }}>{result.note}</div>
            </>
          )}
          {result.phase === 'exists' && (
            <div>ℹ️ O índice <strong>{result.name}</strong> já existe na coleção.</div>
          )}
          {result.phase === 'error' && (
            <div>❌ {result.error}</div>
          )}
          {result.phase === 'timeout' && (
            <div>⚠️ {result.error}</div>
          )}
        </div>
      )}

      {/* Prova objetiva: explain() antes/depois do índice */}
      <div className="card" style={{ borderColor: 'rgba(0,237,100,.3)' }}>
        <strong style={{ fontSize: 15, display: 'block', marginBottom: 4 }}>🔬 Prova com <code>explain("executionStats")</code></strong>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 12 }}>
          Rode a query <strong>antes</strong> e <strong>depois</strong> de criar o índice do cenário — o ganho medido
          pelo próprio banco. O número que conta a história é <strong>docs examinados vs retornados</strong>.
          Dica: se já aparecer IXSCAN, remova o índice na lista abaixo e rode de novo para ver o "antes".
        </p>
        <div className="idx-plan-flow" aria-label="Evolução esperada do plano de execução">
          <span className="badge badge-red">COLLSCAN</span>
          <span>criar índice adequado</span>
          <span aria-hidden="true">→</span>
          <span className="badge badge-green">IXSCAN</span>
          <span>menos documentos examinados</span>
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 }}>
          {EXPLAIN_SCENARIOS.map(sc => (
            <button key={sc.key} className={`tag ${explainSel === sc.key ? 'active' : ''}`}
              style={explainSel === sc.key ? { borderColor: '#00ED64', color: '#00ED64', background: 'rgba(0,237,100,.08)' } : {}}
              onClick={() => setExplainSel(sc.key)} title={sc.hint}>
              {sc.label}
            </button>
          ))}
          <button className="btn btn-primary btn-sm" onClick={runExplain} disabled={explainLoading}>
            {explainLoading ? <><span className="spinner" /> Medindo…</> : '▶ Rodar explain()'}
          </button>
        </div>
        {explainRes && (
          <div style={{ padding: '12px 14px', borderRadius: 8, fontSize: 13,
            background: explainRes.scan === 'IXSCAN' ? 'rgba(0,237,100,.08)' : 'rgba(255,105,96,.08)',
            border: `1px solid ${explainRes.scan === 'IXSCAN' ? 'rgba(0,237,100,.3)' : 'rgba(255,105,96,.35)'}` }}>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 8, flexWrap: 'wrap' }}>
              <span className={`badge ${explainRes.scan === 'IXSCAN' ? 'badge-green' : 'badge-red'}`}>{explainRes.scan}</span>
              {explainRes.index_name && <code style={{ fontSize: 12 }}>{explainRes.index_name}</code>}
              <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{explainRes.stages.join(' → ')}</span>
            </div>
            <div style={{ display: 'flex', gap: 18, flexWrap: 'wrap', fontFamily: 'var(--font-mono)', fontSize: 12.5 }}>
              <span>⏱ <strong>{explainRes.execution_ms}ms</strong></span>
              <span>docs examinados: <strong>{explainRes.docs_examined?.toLocaleString()}</strong></span>
              <span>chaves examinadas: <strong>{explainRes.keys_examined?.toLocaleString()}</strong></span>
              <span>retornados: <strong>{explainRes.n_returned?.toLocaleString()}</strong></span>
            </div>
            {explainRes.scan === 'COLLSCAN' && (
              <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}>
                💡 Sem índice, o banco examinou <strong>{explainRes.docs_examined?.toLocaleString()}</strong> documentos para
                devolver {explainRes.n_returned}. Crie o índice acima e rode de novo.
              </div>
            )}
            {explainRes.scan === 'IXSCAN' && explainRes.docs_examined > 2 * (explainRes.n_returned || 1) && (
              <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}>
                💡 Este plano usa um índice, mas ainda examina <strong>{explainRes.docs_examined?.toLocaleString()}</strong> docs
                para devolver {explainRes.n_returned}. Crie o índice do cenário (que cobre filtro <em>e</em> ordenação) e
                rode de novo — deve cair para ~{explainRes.n_returned}.
              </div>
            )}
          </div>
        )}
      </div>

      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <strong style={{ fontSize: 15 }}>Índices Ativos — <code style={{ fontSize: 13 }}>produtos</code></strong>
          <button className="btn btn-sm btn-default" onClick={fetchIndexes}>↻ Atualizar</button>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {(showAllIndexes ? indexes : indexes.filter(idx =>
            idx.name === '_id_' || ['categoria_1', 'categoria_1_preco_-1', 'preco_1_partial'].includes(idx.name)
          )).map(idx => (
            <div key={idx.name} className="result-row" style={{ justifyContent: 'space-between' }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <span className={`badge ${idx.name === '_id_' ? 'badge-blue' : 'badge-green'}`}>
                  {idx.name === '_id_' ? 'Sistema' : 'Custom'}
                </span>
                <code style={{ fontSize: 12 }}>{idx.name}</code>
                <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>→ {JSON.stringify(idx.key)}</span>
                {idx.sparse && <span className="badge badge-blue">sparse</span>}
                {idx.partial_filter && <span className="badge badge-blue">partial</span>}
              </div>
              {idx.name !== '_id_' && (
                <button className="btn btn-xs btn-danger" onClick={() => handleDrop(idx.name)}>Remover</button>
              )}
            </div>
          ))}
        </div>
        {indexes.length > 4 && (
          <button className="btn btn-sm btn-default" style={{ marginTop: 12 }}
            onClick={() => setShowAllIndexes(v => !v)}>
            {showAllIndexes ? 'Mostrar apenas índices da demo' : `Ver todos os ${indexes.length} índices`}
          </button>
        )}
      </div>
    </div>
  )
}
